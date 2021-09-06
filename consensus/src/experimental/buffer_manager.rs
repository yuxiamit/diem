// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use consensus_types::executed_block::ExecutedBlock;
use diem_types::ledger_info::{LedgerInfoWithSignatures, LedgerInfo};
use futures::channel::{mpsc::UnboundedReceiver, oneshot};
use std::{collections::LinkedList, sync::Arc};
use diem_crypto::HashValue;
use crate::experimental::execution_phase::{ExecutionRequest, ExecutionResponse};
use futures::channel::mpsc::UnboundedSender;
use crate::experimental::signing_phase::{SigningRequest, SigningResponse};
use diem_types::account_address::AccountAddress;
use crate::round_manager::VerifiedEvent;
use crate::network::NetworkSender;
use crate::experimental::persisting_phase::{PersistingRequest, PersistingResponse};
use crate::counters;
use diem_logger::prelude::*;
use std::collections::{VecDeque, HashMap, BTreeMap};
use std::collections::vec_deque::Iter;
use futures::{StreamExt, SinkExt};
use crate::experimental::linkedlist::{List, Link, Node, get_next, get_elem, set_elem, get_elem_mut};
use consensus_types::block::Block;
use std::borrow::{Borrow, BorrowMut};
use std::ops::Deref;
use anyhow::anyhow;
use consensus_types::common::Author;
use crate::network_interface::ConsensusMsg;
use consensus_types::experimental::commit_vote::CommitVote;
use diem_crypto::ed25519::Ed25519Signature;
use diem_types::validator_verifier::ValidatorVerifier;
use crate::state_replication::StateComputerCommitCallBackType;
use std::rc::Rc;

pub type ResetAck = ();
pub fn reset_ack_new() -> ResetAck {}

pub struct ResetRequest {
    tx: oneshot::Sender<ResetAck>,
    reconfig: bool,
}

pub struct OrderedBlocks {
    pub blocks: Vec<ExecutedBlock>,
    pub finality_proof: LedgerInfoWithSignatures,
    pub callback: StateComputerCommitCallBackType,
}

pub enum BufferItem {
    Block(Arc<ExecutedBlock>), // TODO: remove Arc
    // the second item is to store a signature received from a commit vote
    // before the prefix of blocks have been executed
    FinalityProof(LedgerInfoWithSignatures, BTreeMap<AccountAddress, Ed25519Signature>, StateComputerCommitCallBackType),
}

pub struct LedgerInfoBufferItem {
    // signatures are collected in ledger_info
    pub commit_ledger_info: LedgerInfoWithSignatures, // duplicating for efficiency
    // jump back to BufferItem cursor
    pub link: Link<BufferItem>,
}

pub type BufferItemRootType = Link<BufferItem>;
pub type LedgerInfoRootType = Link<LedgerInfoBufferItem>;
pub type Sender<T> = UnboundedSender<T>;
pub type Receiver<T> = UnboundedReceiver<T>;

/// StateManager handles the states of ordered blocks and
/// interacts with the execution phase, the signing phase, and
/// the persisting phase.
pub struct StateManager {
    author: Author,

    buffer: List<BufferItem>,
    li_buffer: List<LedgerInfoBufferItem>,
    // the second item is for updating aggregation_root and jumping between LI's easily

    execution_root: BufferItemRootType,
    execution_phase_tx: Sender<ExecutionRequest>,
    execution_phase_rx: Receiver<ExecutionResponse>,

    signing_root: LedgerInfoRootType,
    signing_phase_tx: Sender<SigningRequest>,
    signing_phase_rx: Receiver<SigningResponse>,

    aggregation_root: RootType,
    commit_msg_tx: NetworkSender,
    commit_msg_rx: channel::diem_channel::Receiver<AccountAddress, VerifiedEvent>,

    persisting_phase_tx: Sender<PersistingRequest>,
    persisting_phase_rx: Receiver<PersistingResponse>,

    block_rx: UnboundedReceiver<OrderedBlocks>,
    reset_rx: UnboundedReceiver<ResetRequest>,
    end_epoch: bool,

    verifier: ValidatorVerifier,
}

impl StateManager {
    pub fn new(
        author: Author,
        execution_phase_tx: Sender<ExecutionRequest>,
        execution_phase_rx: Receiver<ExecutionResponse>,
        signing_phase_tx: Sender<SigningRequest>,
        signing_phase_rx: Receiver<SigningResponse>,
        commit_msg_tx: NetworkSender,
        commit_msg_rx: channel::diem_channel::Receiver<AccountAddress, VerifiedEvent>,
        persisting_phase_tx: Sender<PersistingRequest>,
        persisting_phase_rx: Receiver<PersistingResponse>,
        block_rx: UnboundedReceiver<OrderedBlocks>,
        reset_rx: UnboundedReceiver<ResetRequest>,
        verifier: ValidatorVerifier,
    ) -> Self {
        let buffer = List::<BufferItem>::new();
        let li_buffer =  List::<LedgerInfoBufferItem>::new();

        // point the roots to the head
        let execution_root = buffer.head.as_ref().cloned();
        let signing_root = li_buffer.head.as_ref().cloned();
        let aggregation_root = buffer.head.as_ref().cloned();

        Self {
            author,

            buffer,
            li_buffer,

            execution_root,
            execution_phase_tx,
            execution_phase_rx,

            signing_root,
            signing_phase_tx,
            signing_phase_rx,

            aggregation_root,
            commit_msg_tx,
            commit_msg_rx,

            persisting_phase_tx,
            persisting_phase_rx,

            block_rx,
            reset_rx,
            end_epoch: false,

            verifier,
        }
    }

    async fn process_ordered_blocks(&mut self, ordered_blocks: OrderedBlocks) -> anyhow::Result<()> {
        let OrderedBlocks {
            blocks, finality_proof, callback
        } = ordered_blocks;

        // push blocks to buffer
        blocks.iter().map(|eb| {
            self.buffer.push_back(BufferItem::Block(Arc::new(eb.clone())));
        });
        self.buffer.push_back(
            BufferItem::FinalityProof(
                finality_proof.clone(),
                BTreeMap::new(),
                callback,
            ));

        // send blocks to execution phase
        self.execution_phase_tx.send(ExecutionRequest {
            blocks: blocks.iter().map(|eb| { eb.block().clone() }).collect::<Vec<Block>>()
        }).await?;
        Ok(())
    }

    async fn process_reset_req(&mut self, reset_event: ResetRequest) -> anyhow::Result<()> {
        let ResetRequest {
            tx,
            reconfig
        } = reset_event;

        // clear the buffer
        while self.buffer.pop_back().is_some() { }
        while self.li_buffer.pop_back().is_some() { }

        self.end_epoch = self.end_epoch || reconfig;

        tx.send(reset_ack_new()).unwrap();
        Ok(())
    }

    async fn update_buffer_execution_resp(&mut self, execution_resp: ExecutionResponse) -> anyhow::Result<()> {
        let ExecutionResponse {
            inner
        } = execution_resp;

        let executed_blocks = inner?;

        let mut executed_blocks_iter = executed_blocks.iter();
        let mut current_cursor = get_next(&self.execution_root); // must be Some

        if current_cursor.is_some() {
            // update buffer
            for executed_block in executed_blocks_iter
            {
                let buffer_item = get_elem(&current_cursor);
                if let BufferItem::Block(block) = buffer_item {
                    assert_eq!(
                        executed_block.block().id(),
                        block.id(),
                    );
                    // update the element
                    set_elem(
                        &current_cursor,
                        BufferItem::Block(Arc::new(executed_block.clone()))
                    );
                    current_cursor = get_next(&current_cursor);
                }
                else {
                    return Err(anyhow!("Invalid response from execution phase: mismatching with buffer."));
                }
            }
            let buffer_item = get_elem(&current_cursor);
            if let BufferItem::FinalityProof(
                finality_proof,
                sig_cache,
                _,
            ) = buffer_item {
                let commit_ledger_info = LedgerInfo::new(
                    executed_blocks.last().unwrap().block_info(),
                    finality_proof.ledger_info().consensus_data_hash(),
                );
                self.li_buffer.push_back(LedgerInfoBufferItem {
                    commit_ledger_info: LedgerInfoWithSignatures::new(
                        commit_ledger_info.clone(),
                        sig_cache.clone(),
                    ),
                    link: current_cursor.clone(),
                });
                self.signing_phase_tx.send(SigningRequest {
                    ordered_ledger_info: finality_proof.clone(),
                    commit_ledger_info,
                }).await?;
            } else {
                return Err(anyhow!("Invalid response from execution phase: missing finality proof."));
            }
            self.execution_root = current_cursor;
        } else {
            // in case of a reset, we restore the execution_root
            self.execution_root = self.buffer.head.as_ref().cloned();
        }
        Ok(())
    }

    async fn process_execution_resp(&mut self, execution_resp: ExecutionResponse) -> anyhow::Result<()> {
        let res = self.update_buffer_execution_resp(execution_resp).await;
        // TODO: if parent not found, we need to revert execution root back.
        // try next batch
        let mut current_cursor = get_next(&self.execution_root); // must be Some
        let mut batch: Vec<Block> = vec![];
        loop {
            if current_cursor.is_some() {
                let buffer_item = get_elem(&current_cursor);
                if let BufferItem::Block(block) = buffer_item {
                    batch.push(block.block().clone());
                }
                else {
                    break; // reach the next batch
                }
                current_cursor = get_next(&current_cursor);
            }
            else {
                break; // reach the end
            }
        }
        if !batch.is_empty() {
            // send blocks to execution phase
            self.execution_phase_tx.send(ExecutionRequest {
                blocks: batch
            }).await?;
        }
        res
    }

    async fn update_signing_root_for_nonempty_buffer(&mut self) {
        // update signing_root
        let mut current_cursor = get_next(&self.signing_root);
        while current_cursor.is_some() {
            let LedgerInfoBufferItem {
                commit_ledger_info: ledger_info_with_sig, link: _,
            } = get_elem(&current_cursor);
            if ledger_info_with_sig.signatures().contains_key(&self.author) {
                self.signing_root = current_cursor.clone();
            } else {
                break;
            }
            current_cursor = get_next(&current_cursor);
        }
    }

    async fn update_signing_root(&mut self) {
        // update signing_root
        let mut current_cursor = get_next(&self.signing_root);
        if current_cursor.is_some() {
            self.update_signing_root_for_nonempty_buffer().await;
        } else {
            self.signing_root = self.li_buffer.head.as_ref().cloned();
        }
    }

    async fn update_li_buffer_signing_resp(&mut self, sig: Ed25519Signature, commit_ledger_info: LedgerInfo) -> anyhow::Result<()> {
        let mut current_cursor = get_next(&self.signing_root);
        if current_cursor.is_some() {
            // this is important because the response might not in the order because
            // retrying a failed signature and finishing execution will incur
            // requests to signing phase
            while current_cursor.is_some() {
                // update signature
                let LedgerInfoBufferItem{
                    commit_ledger_info: ledger_info_with_sig, link: _,
                } = get_elem_mut(&current_cursor);
                if ledger_info_with_sig.ledger_info() == &commit_ledger_info {
                    // found the place
                    ledger_info.add_signature(self.author, sig.clone());

                    // send out commit vote
                    let commit_vote = CommitVote::new_with_signature(
                        self.author,
                        ledger_info.ledger_info().clone(),
                        sig
                    );
                    self.commit_msg_tx
                        .broadcast(ConsensusMsg::CommitVoteMsg(Box::new(commit_vote)))
                        .await?;

                    break;
                }
                current_cursor = get_next(&current_cursor);
            }
            // update signing root
            self.update_signing_root_for_nonempty_buffer().await;
        }
        else {
            // reset happened
            self.signing_root = self.li_buffer.head.as_ref().cloned();
        }
        Ok(())
    }

    async fn process_signing_resp(&mut self, signing_resp: SigningResponse) -> anyhow::Result<()> {
        let SigningResponse {
            some_signature,
            commit_ledger_info,
        } = signing_resp;
        if let Some(sig) = some_signature {
            self.update_li_buffer_signing_resp(sig, commit_ledger_info).await
        } else {
            // update the signing root and ...
            self.update_signing_root().await;

            // try next signature if signing failure
            // note that we are not retrying exactly the failed sig
            // the failed sig will be re-tried in the future, unless a reset happens
            let mut current_cursor = get_next(&self.signing_root); // must be Some
            if current_cursor.is_some() {
                let LedgerInfoBufferItem{
                    commit_ledger_info, link,
                } = get_elem(&current_cursor);
                let buffer_item = get_elem(link);
                if let BufferItem::FinalityProof(finality_proof, _, _) = buffer_item {
                    self.signing_phase_tx.send(SigningRequest {
                        ordered_ledger_info: finality_proof.clone(),
                        commit_ledger_info: commit_ledger_info.ledger_info().clone(),
                    }).await?;
                }
                else {
                    return Err(anyhow!("Corrupted Buffer: this should be a finality proof"));
                }
            }
        }
        res
    }

    async fn update_aggregation_root_for_nonempty_buffer(&mut self) {
        // update signing_root
        let mut current_cursor = get_next(&self.aggregation_root);
        while current_cursor.is_some() {
            let LedgerInfoBufferItem {
                commit_ledger_info: ledger_info_with_sig, link: _,
            } = get_elem(&current_cursor);
            if ledger_info_with_sig.signatures().contains_key(&self.author) {
                self.signing_root = current_cursor.clone();
            } else {
                break;
            }
            current_cursor = get_next(&current_cursor);
        }
    }

    async fn update_aggregation_root(&mut self) -> anyhow::Result<()> {
        // update aggregation root
        // scan signed ledger info buffer (from head to signing root) for qualified signatures
        let mut current_cursor = self.li_buffer.head.clone();
        if current_cursor.is_some() {
            // signing root must be some
            while current_cursor.unwrap() != self.signing_root.unwrap() {
                let LedgerInfoBufferItem {
                    commit_ledger_info,
                    link,
                } = get_elem(&current_cursor);
                if commit_ledger_info.check_voting_power(&self.verifier) {
                    // TODO: ideally we should send a single batch to persisting phase every time
                    let mut batch: Vec<Arc<ExecutedBlock>> = vec![];
                    let mut current_buffer_cursor = get_next(self.aggregation_root);
                    while current_buffer_cursor.unwrap() != link.unwrap() {
                        let buffer_item = get_elem(&current_buffer_cursor);
                        if let BufferItem::Block(block) = buffer_item {
                            batch.push(block.clone());
                        } else {
                            return Err(anyhow!("Bad buffer item: should be block")); // TODO: make errors structured
                        }
                        current_buffer_cursor = get_next(&current_buffer_cursor);
                    }
                    let buffer_item = get_elem(link);
                    if let BufferItem::FinalityProof(_, _, callback) = buffer_item {
                        self.persisting_phase_tx.send(PersistingRequest {
                            blocks: batch,
                            commit_ledger_info: commit_ledger_info.clone(),
                            callback: Rc::clone(callback),
                        }).await?;
                    }
                    else {
                        return Err(anyhow!("Bad buffer item: should be finality proof"));
                    }
                    self.aggregation_root = current_cursor.clone();
                }
            }
        } else {
            self.signing_root = self.li_buffer.head.as_ref().cloned();
        }

        Ok(())
    }

    async fn process_commit_msg(&mut self, commit_msg: VerifiedEvent) -> anyhow::Result<()> {
        match commit_msg {
            VerifiedEvent::CommitVote(cv) => {
                {
                    // travel the li_buffer to find li
                    let mut current_cursor = self.li_buffer.head.clone();
                    while current_cursor.is_some() {
                        let LedgerInfoBufferItem {
                            commit_ledger_info, link: _,
                        } = get_elem_mut(&current_cursor);
                        if commit_ledger_info.ledger_info() == cv.ledger_info() {
                            commit_ledger_info.add_signature(
                                cv.author(),
                                cv.signature().clone()
                            );
                            return Ok(())
                        }
                        current_cursor = get_next(&current_cursor);
                    }
                }
                {
                    // travel the un-executed part of buffer (execution root to the end)
                    let mut current_cursor = self.execution_root.clone();
                    while current_cursor.is_some() {
                        let buffer_item = get_elem_mut(&current_cursor);
                        if let BufferItem::FinalityProof(
                            finality_proof,
                            sig_cache,
                            _,
                        ) = buffer_item {
                            if finality_proof.ledger_info()
                                .commit_info()
                                .match_ordered_only(cv.ledger_info().commit_info()) {
                                sig_cache.insert(cv.author(), cv.signature().clone())
                            }
                            return Ok(())
                        }
                        current_cursor = get_next(&current_cursor);
                    }
                }
            }
            _ => {
                unimplemented!();
            }
        }
        Ok(())
    }

    async fn process_persisting_resp(&mut self, _persisting_resp: PersistingResponse) -> anyhow::Result<()> {
        Ok(())
    }

    async fn start(mut self) {

        // loop receving new blocks or reset
        // while !self.end_epoch {

        // select from all rx channels,
        // if new from block_rx, push to buffer
        // if new from reset_rx, make a mark that stops all the following ops
        // if new from execution_phase_rx,
        //   if execution failure, send all the blocks to execution_phase.
        //   Otherwise,
        //     update execution_root and send the blocks from execution_root to end to execution_phase
        // if new from signing_phase_rx,
        //   update sigining_root and send the blocks from signing_root to execution_root to signing_phase
        // if new from commit_msg_rx,
        //   collect sig and update the sigs
        //   if aggregated,
        //     update aggregation_root
        // if new from persisting_phase_rx,
        //   pop blocks from buffer, and continue to post-committing ops
        //   send the blocks from aggregation_root to the end to persisting_phase

        // if not reset, retry sending the commit_vote msg via commit_msg_tx
        // }

        while !self.end_epoch {
            // check li_buffer and update aggregation_root
            self.update_aggregation_root().await;
            // check new messages
            if let Err(e) = tokio::select! {
                Some(blocks) = self.block_rx.next() => {
                    self.process_ordered_blocks(blocks).await
                }
                Some(reset_event) = self.reset_rx.next() => {
                    self.process_reset_req(reset_event).await
                }
                Some(execution_resp) = self.execution_phase_rx.next() => {
                    self.process_execution_resp(execution_resp).await
                }
                Some(signing_resp) = self.signing_phase_rx.next() => {
                    self.process_signing_resp(signing_resp).await
                }
                Some(commit_msg) = self.commit_msg_rx.next() => {
                    self.process_commit_msg(commit_msg).await
                }
                Some(persisting_resp) = self.persisting_phase_rx.next() => {
                    self.process_persisting_resp(persisting_resp).await
                }
            } {
                counters::ERROR_COUNT.inc();
                error!("BufferManager error: {}", e.to_string());
            }
        }


    }
}
