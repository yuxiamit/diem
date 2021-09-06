// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use consensus_types::executed_block::ExecutedBlock;
use diem_types::ledger_info::LedgerInfoWithSignatures;
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
use proc_macro::error;
use diem_logger::prelude::*;

pub type ResetAck = ();
pub fn reset_ack_new() -> ResetAck {}

pub struct ResetRequest {
    tx: oneshot::Sender<ResetAck>,
    reconfig: bool,
}

pub struct OrderedBlocks {
    pub blocks: Vec<ExecutedBlock>,
    pub finality_proof: LedgerInfoWithSignatures,
}

pub type RootType = HashValue;
pub type Sender<T> = UnboundedSender<T>;
pub type Receiver<T> = UnboundedReceiver<T>;

/// StateManager handles the states of ordered blocks and
/// interacts with the execution phase, the signing phase, and
/// the persisting phase.
pub struct StateManager {
    buffer: LinkedList<Arc<ExecutedBlock>>,
    li_buffer: LinkedList<(LedgerInfoWithSignatures, Arc<ExecutedBlock>)>,
    // the second item is for updating aggregation_root easily

    execution_root: RootType,
    execution_phase_tx: Sender<ExecutionRequest>,
    execution_phase_rx: Receiver<ExecutionResponse>,

    signing_root: RootType,
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
}

impl StateManager {
    pub fn new(
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
    ) -> Self {
        // TODO: add a sentinel
        Self {
            buffer: LinkedList::<Arc<ExecutedBlock>>::new(),
            li_buffer: LinkedList::<(LedgerInfoWithSignatures, Arc<ExecutedBlock>)>::new(),

            execution_root: RootType::zero(),
            execution_phase_tx,
            execution_phase_rx,

            signing_root: RootType::zero(),
            signing_phase_tx,
            signing_phase_rx,

            aggregation_root: RootType::zero(),
            commit_msg_tx,
            commit_msg_rx,

            persisting_phase_tx,
            persisting_phase_rx,

            block_rx,
            reset_rx,
            end_epoch: False,
        }
    }

    async fn process_ordered_blocks(self, _blocks: OrderedBlocks) -> anyhow::Result<()> {
        Ok(())
    }

    async fn process_reset_req(self, _reset_event: ResetRequest) -> anyhow::Result<()> {
        Ok(())
    }

    async fn process_execution_resp(self, _execution_resp: ExecutionResponse) -> anyhow::Result<()> {
        Ok(())
    }

    async fn process_signing_resp(self, _signing_resp: SigningResponse) -> anyhow::Result<()> {
        Ok(())
    }

    async fn process_commit_msg(self, _commit_msg: VerifiedEvent) -> anyhow::Result<()> {
        Ok(())
    }

    async fn process_persisting_resp(self, _persisting_resp: PersistingResponse) -> anyhow::Result<()> {
        Ok(())
    }

    async fn start(self) {

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
