// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    counters, experimental::errors::Error, metrics_safety_rules::MetricsSafetyRules,
    network::NetworkSender, network_interface::ConsensusMsg, round_manager::VerifiedEvent,
    state_replication::StateComputer,
};
use channel::{diem_channel, Receiver, Sender};
use consensus_types::{
    common::Author,
    executed_block::ExecutedBlock,
    experimental::{commit_decision::CommitDecision, commit_vote::CommitVote},
};
use core::sync::atomic::Ordering;
use diem_crypto::ed25519::Ed25519Signature;
use diem_infallible::Mutex;
use diem_logger::prelude::*;
use diem_metrics::monitor;
use diem_types::{
    account_address::AccountAddress,
    block_info::BlockInfo,
    ledger_info::{LedgerInfo, LedgerInfoWithSignatures},
    validator_verifier::{ValidatorVerifier, VerifyError},
};
use executor_types::Error as ExecutionError;
use futures::{FutureExt, SinkExt, StreamExt};
use safety_rules::TSafetyRules;
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicU64, Arc},
};
use tokio::time;

use crate::{
    experimental::execution_phase::reset_ack_new,
    state_replication::StateComputerCommitCallBackType,
};
use anyhow::anyhow;

use crate::experimental::{
    execution_phase::{notify_downstream_reset, ResetEventType},
    persisting_phase::PersistingChannelType,
};
use core::hint;
use diem_config::config::DEFAULT_COMMIT_DECISION_GRACE_PERIOD;
use diem_types::epoch_change::EpochChangeProof;
use futures::{
    channel::mpsc::{UnboundedReceiver, UnboundedSender},
    prelude::stream::FusedStream,
};
use std::collections::VecDeque;

/*
Commit phase takes in the executed blocks from the execution
phase and commit them. Specifically, commit phase signs a commit
vote message containing the execution result and broadcast it.
Upon collecting a quorum of agreeing votes to a execution result,
the commit phase commits the blocks as well as broadcasts a commit
decision message together with the quorum of signatures. The commit
decision message helps the slower nodes to quickly catch up without
having to collect the signatures.
*/

macro_rules! report_err {
    ($result:expr, $error_string:literal) => {
        if let Err(err) = $result {
            counters::ERROR_COUNT.inc();
            error!(error = err.to_string(), $error_string,)
        }
    };
}

const COMMIT_PHASE_TIMEOUT_MILLISEC: u64 = 200; // retry timeout in milli seconds

pub struct CommitChannelType(
    pub Vec<ExecutedBlock>,
    pub LedgerInfoWithSignatures,
    pub StateComputerCommitCallBackType,
);

#[derive(Eq, PartialEq, Hash, Clone)]
pub enum CommitPhaseMessageType {
    CommitVoteType,
    CommitDecisionType,
}

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct CommitPhaseMessageKey {
    pub author: AccountAddress,
    pub round: u64,
    pub message_type: CommitPhaseMessageType,
}

impl CommitPhaseMessageKey {
    pub fn new(author: AccountAddress, round: u64, message_type: CommitPhaseMessageType) -> Self {
        Self {
            author,
            round,
            message_type,
        }
    }
}

impl std::fmt::Debug for CommitChannelType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::fmt::Display for CommitChannelType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CommitChannelType({:?}, {})", self.0, self.1)
    }
}

//#[derive(Clone)]
pub struct PendingBlocks {
    blocks: Vec<ExecutedBlock>,
    ledger_info_sig: LedgerInfoWithSignatures,
    block_info: BlockInfo,
    callback: StateComputerCommitCallBackType,
}

impl PendingBlocks {
    pub fn new(
        blocks: Vec<ExecutedBlock>,
        ledger_info_sig: LedgerInfoWithSignatures,
        callback: StateComputerCommitCallBackType,
    ) -> Self {
        assert!(!blocks.is_empty()); // the commit phase should not accept empty blocks.
        let block_info = blocks.last().unwrap().block_info();
        Self {
            blocks,
            ledger_info_sig,
            block_info,
            callback,
        }
    }

    pub fn block_info(&self) -> &BlockInfo {
        &self.block_info
    }

    pub fn round(&self) -> u64 {
        self.block_info().round()
    }

    pub fn take_callback(self) -> StateComputerCommitCallBackType {
        self.callback
    }

    pub fn blocks(&self) -> &Vec<ExecutedBlock> {
        &self.blocks
    }

    pub fn ledger_info_sig(&self) -> &LedgerInfoWithSignatures {
        &self.ledger_info_sig
    }

    pub fn ledger_info_sig_mut(&mut self) -> &mut LedgerInfoWithSignatures {
        &mut self.ledger_info_sig
    }

    pub fn replace_ledger_info_sig(&mut self, new_ledger_info_sig: LedgerInfoWithSignatures) {
        self.ledger_info_sig = new_ledger_info_sig
    }

    pub fn verify(&self, verifier: &ValidatorVerifier) -> ::std::result::Result<(), VerifyError> {
        if &self.block_info == self.ledger_info_sig.ledger_info().commit_info() {
            self.ledger_info_sig.verify_signatures(verifier)
        } else {
            Err(VerifyError::InconsistentBlockInfo)
        }
    }
}

#[derive(Debug)]
pub enum CommitPhaseTimeoutEvent {
    BroadcastCommitVote(CommitVote),
    SignCommitVote(LedgerInfoWithSignatures),
}

pub struct CommitPhase {
    commit_channel_recv: UnboundedReceiver<CommitChannelType>,
    execution_proxy: Arc<dyn StateComputer>,
    blocks: Option<PendingBlocks>,
    commit_msg_rx: diem_channel::Receiver<CommitPhaseMessageKey, VerifiedEvent>,
    verifier: ValidatorVerifier,
    safety_rules: Arc<Mutex<MetricsSafetyRules>>,
    author: Author,
    back_pressure: Arc<AtomicU64>,
    network_sender: NetworkSender,
    timeout_event_tx: Sender<CommitPhaseTimeoutEvent>,
    timeout_event_rx: Receiver<CommitPhaseTimeoutEvent>,
    reset_event_rx: Receiver<ResetEventType>,
    in_epoch: bool,
    local_commit_decision_history: VecDeque<LedgerInfoWithSignatures>,
    persist_phase_channel: Option<UnboundedSender<PersistingChannelType>>,
    persist_phase_reset_tx: Option<Sender<ResetEventType>>,
}

/// Wrapper for ExecutionProxy.commit
pub async fn commit(
    execution_proxy: &Arc<dyn StateComputer>,
    pending_blocks: PendingBlocks,
) -> Result<(), ExecutionError> {
    let blocks_to_commit = pending_blocks
        .blocks()
        .iter()
        .map(|eb| Arc::new(eb.clone()))
        .collect::<Vec<Arc<ExecutedBlock>>>();
    execution_proxy
        .commit(
            &blocks_to_commit,
            pending_blocks.ledger_info_sig().clone(),
            pending_blocks.take_callback(),
            None,
        )
        .await
        .expect("Failed to persist commit");

    Ok(())
}

/// shortcut for sending a message with a timeout retry event
async fn sleep_and_retry(
    commit_phase_timeout_event: CommitPhaseTimeoutEvent,
    mut notification: Sender<CommitPhaseTimeoutEvent>,
) {
    time::sleep(time::Duration::from_millis(COMMIT_PHASE_TIMEOUT_MILLISEC)).await;
    report_err!(
        notification.send(commit_phase_timeout_event).await,
        "Error in sending timeout events"
    )
}

// This trivial implementation of `drop` adds a print to console.
impl Drop for CommitPhase {
    fn drop(&mut self) {
        info!(
            "Dropping commit phase, execution proxy counter {}, safety_rules {}, back pressure {}",
            Arc::strong_count(&self.execution_proxy),
            Arc::strong_count(&self.safety_rules),
            Arc::strong_count(&self.back_pressure),
        );
    }
}

impl CommitPhase {
    pub fn new_with_persisting_phase(
        commit_channel_recv: UnboundedReceiver<CommitChannelType>,
        execution_proxy: Arc<dyn StateComputer>,
        commit_msg_rx: diem_channel::Receiver<CommitPhaseMessageKey, VerifiedEvent>,
        verifier: ValidatorVerifier,
        safety_rules: Arc<Mutex<MetricsSafetyRules>>,
        author: Author,
        back_pressure: Arc<AtomicU64>,
        network_sender: NetworkSender,
        reset_event_rx: Receiver<ResetEventType>,
        persist_phase_channel: Option<UnboundedSender<PersistingChannelType>>,
        persist_phase_reset_tx: Option<Sender<ResetEventType>>,
    ) -> Self {
        info!("commit phase new");
        let (timeout_event_tx, timeout_event_rx) = channel::new::<CommitPhaseTimeoutEvent>(
            30,
            &counters::DECOUPLED_EXECUTION__COMMIT_MESSAGE_TIMEOUT_CHANNEL,
        );
        Self {
            commit_channel_recv,
            execution_proxy,
            blocks: None,
            commit_msg_rx,
            verifier,
            safety_rules,
            author,
            back_pressure,
            network_sender,
            timeout_event_tx,
            timeout_event_rx,
            reset_event_rx,
            in_epoch: true,
            local_commit_decision_history: VecDeque::new(),
            persist_phase_channel,
            persist_phase_reset_tx,
        }
    }

    pub fn new(
        commit_channel_recv: UnboundedReceiver<CommitChannelType>,
        execution_proxy: Arc<dyn StateComputer>,
        commit_msg_rx: diem_channel::Receiver<CommitPhaseMessageKey, VerifiedEvent>,
        verifier: ValidatorVerifier,
        safety_rules: Arc<Mutex<MetricsSafetyRules>>,
        author: Author,
        back_pressure: Arc<AtomicU64>,
        network_sender: NetworkSender,
        reset_event_rx: Receiver<ResetEventType>,
    ) -> Self {
        Self::new_with_persisting_phase(
            commit_channel_recv,
            execution_proxy,
            commit_msg_rx,
            verifier,
            safety_rules,
            author,
            back_pressure,
            network_sender,
            reset_event_rx,
            None,
            None,
        )
    }

    /// Notified when receiving a commit vote message (assuming verified)
    pub async fn process_commit_vote(
        &mut self,
        commit_vote: &CommitVote,
    ) -> anyhow::Result<(), Error> {
        if let Some(pending_blocks) = self.blocks.as_mut() {
            let commit_ledger_info = commit_vote.ledger_info();

            let history = commit_vote.history();
            if let Some(li) = history
                .iter()
                .find(|li| li.commit_info() == pending_blocks.block_info())
            {
                li.verify_signatures(&self.verifier)
                    .map_err(|_| Error::VerificationError)?;
                pending_blocks.replace_ledger_info_sig(li.clone());
                self.commit_verified().await?; // skip one round of verification
            } else {
                // if the block infos do not match
                if commit_ledger_info.commit_info() != pending_blocks.block_info() {
                    return Err(Error::InconsistentBlockInfo(
                        commit_ledger_info.commit_info().clone(),
                        pending_blocks.block_info().clone(),
                    )); // ignore the message
                }

                // add the signature into the signature tree
                pending_blocks
                    .ledger_info_sig_mut()
                    .add_signature(commit_vote.author(), commit_vote.signature().clone());
            }
        } else {
            info!("Ignore the commit vote message because the commit phase does not have a pending block.")
        }

        Ok(())
    }

    /// Notified when receiving a commit decision message (assuming verified)
    pub async fn process_commit_decision(
        &mut self,
        commit_decision: &CommitDecision,
    ) -> anyhow::Result<(), Error> {
        if let Some(pending_blocks) = self.blocks.as_mut() {
            let commit_ledger_info = commit_decision.ledger_info();

            // if the block infos do not match
            if commit_ledger_info.commit_info() != pending_blocks.block_info() {
                return Err(Error::InconsistentBlockInfo(
                    commit_ledger_info.ledger_info().commit_info().clone(),
                    pending_blocks.block_info().clone(),
                )); // ignore the message
            }

            // replace the signature tree
            pending_blocks.replace_ledger_info_sig(commit_ledger_info.clone());
            self.commit_verified().await?;
        } else {
            info!("Ignore the commit decision message because the commit phase does not have a pending block.")
        }

        Ok(())
    }

    /// try committing when the pending block is definitely ready
    pub async fn commit_verified(&mut self) -> anyhow::Result<(), Error> {
        // asynchronously broadcast the commit decision first to
        // save the time of other nodes.

        let commit_ledger_info = self.blocks.as_ref().unwrap().ledger_info_sig().clone();

        /*
        self.network_sender
            .broadcast(ConsensusMsg::CommitDecisionMsg(Box::new(
                CommitDecision::new(self.author, pending_blocks.ledger_info_sig().clone()),
            )))
            .await;
         */

        let pending_blocks = self.blocks.take().unwrap();
        let round = pending_blocks.round();

        if let Some(persist_channel) = self.persist_phase_channel.as_mut() {
            info!(
                "Send to the persist phase {}",
                pending_blocks.ledger_info_sig()
            );
            persist_channel
                .send(PersistingChannelType(
                    pending_blocks.blocks.clone(),
                    pending_blocks.ledger_info_sig().clone(),
                    pending_blocks.take_callback(),
                ))
                .await
                .expect("Failed to send the executed blocks to the persisting phase");
        } else {
            commit(&self.execution_proxy, pending_blocks)
                .await
                .expect("Failed to commit the executed blocks.");

            // update the back pressure
            self.back_pressure.store(round, Ordering::SeqCst);
        }

        if commit_ledger_info.ledger_info().ends_epoch() {
            debug!(
                "notify epoch change: {:?}",
                commit_ledger_info.ledger_info().next_epoch_state()
            );
            self.network_sender
                .notify_epoch_change(EpochChangeProof::new(
                    vec![commit_ledger_info.clone()],
                    /* more = */ false,
                ))
                .await;
            self.in_epoch = false;
        }

        self.local_commit_decision_history
            .push_back(commit_ledger_info);
        if self.local_commit_decision_history.len() > DEFAULT_COMMIT_DECISION_GRACE_PERIOD {
            self.local_commit_decision_history.pop_front();
        }

        // now self.blocks is none, ready for the next batch of blocks
        Ok(())
    }

    pub async fn check_commit(&mut self) -> anyhow::Result<()> {
        // we cannot assume self.blocks is some as the previous messages are invalid
        if let Some(pending_blocks) = self.blocks.as_ref() {
            if pending_blocks.verify(&self.verifier).is_ok() {
                self.commit_verified().await?;
            }
        }

        Ok(())
    }

    pub async fn broadcast_with_retry(&mut self, commit_vote: CommitVote) {
        let retry_event = CommitPhaseTimeoutEvent::BroadcastCommitVote(commit_vote.clone());

        self.network_sender
            .broadcast(ConsensusMsg::CommitVoteMsg(Box::new(commit_vote)))
            .await;

        tokio::spawn(sleep_and_retry(retry_event, self.timeout_event_tx.clone()));
    }

    pub async fn retry_sign_pending_blocks(
        &mut self,
        ordered_ledger_info: LedgerInfoWithSignatures,
    ) -> anyhow::Result<()> {
        if let Some(pending_block) = self.blocks.as_ref() {
            if pending_block
                .block_info()
                .match_ordered_only(ordered_ledger_info.commit_info())
            {
                let commit_ledger_info = pending_block.ledger_info_sig().ledger_info();
                let sign_result = self.safety_rules.lock().sign_commit_vote(
                    ordered_ledger_info.clone(),
                    pending_block.blocks.last().unwrap().clone(),
                );

                if let Ok(signature) = sign_result {
                    let commit_vote = CommitVote::new_with_signature_history(
                        self.author,
                        commit_ledger_info.clone(),
                        signature,
                        self.local_commit_decision_history
                            .iter()
                            .cloned()
                            .collect::<Vec<LedgerInfoWithSignatures>>(),
                    );

                    // asynchronously broadcast the message.
                    // note that this message will also reach the node itself
                    // if the message delivery fails, it needs to resend the message, or otherwise the liveness might compromise.
                    self.broadcast_with_retry(commit_vote).await;

                    Ok(())
                } else {
                    tokio::spawn(sleep_and_retry(
                        CommitPhaseTimeoutEvent::SignCommitVote(ordered_ledger_info),
                        self.timeout_event_tx.clone(),
                    ));

                    Err(anyhow!("Safety-rule fails"))
                }
            } else {
                info!("received a stale attempt (before the reset), ignoring");
                Ok(())
            }
        } else {
            info!("reset event stops a retrying signing attempt");
            Ok(())
        }
    }

    pub async fn process_executed_blocks(
        &mut self,
        blocks: Vec<ExecutedBlock>,
        ordered_ledger_info: LedgerInfoWithSignatures,
        callback: StateComputerCommitCallBackType,
    ) -> anyhow::Result<()> {
        let commit_ledger_info = LedgerInfo::new(
            blocks.last().unwrap().block_info(),
            ordered_ledger_info.ledger_info().consensus_data_hash(),
        );

        let commit_ledger_info_with_sig = LedgerInfoWithSignatures::new(
            commit_ledger_info.clone(),
            BTreeMap::<AccountAddress, Ed25519Signature>::new(),
        );

        // add the block to the pending block, if not already
        let pending_block = PendingBlocks::new(blocks, commit_ledger_info_with_sig, callback);

        self.set_blocks(Some(pending_block));

        self.retry_sign_pending_blocks(ordered_ledger_info).await
    }

    pub fn set_blocks(&mut self, blocks_or_none: Option<PendingBlocks>) {
        self.blocks = blocks_or_none;
    }

    pub fn blocks(&self) -> &Option<PendingBlocks> {
        &self.blocks
    }

    pub fn load_back_pressure(&self) -> u64 {
        self.back_pressure.load(Ordering::SeqCst)
    }

    pub async fn process_reset_event(&mut self, reset_event: ResetEventType) -> anyhow::Result<()> {
        // reset the commit phase
        info!("resetting..");

        let ResetEventType {
            reset_callback,
            reconfig,
        } = reset_event;

        if let Some(reset_tx) = self.persist_phase_reset_tx.as_mut() {
            if let Err(e) = notify_downstream_reset(reset_tx, reconfig).await {
                error!(
                    "Error in requesting persisting phase to reset: {}",
                    e.to_string()
                );
            }
        }

        // exhaust the commit channel
        while !self.commit_msg_rx.is_terminated()
            && !self.commit_channel_recv.is_terminated()
            && self.commit_channel_recv.next().now_or_never().is_some()
        {
            hint::spin_loop();
        }

        /*
        // we should also exhaust the message channel
        // as the we are going to set self.blocks to be None,
        // otherwise it might block the epoch manager
        while !self.commit_msg_rx.is_terminated() && self.commit_msg_rx.next().now_or_never().is_some() {}
        */

        // reset local block
        self.blocks = None;

        if reconfig {
            self.in_epoch = false;
        }

        // activate the callback
        reset_callback
            .send(reset_ack_new())
            .map_err(|_| Error::ResetDropped)?;

        info!("reset finished");

        Ok(())
    }

    pub async fn process_retry_event(&mut self, retry_event: CommitPhaseTimeoutEvent) {
        match retry_event {
            CommitPhaseTimeoutEvent::BroadcastCommitVote(retry_cv) => {
                if let Some(pending_blocks) = self.blocks.as_ref() {
                    if pending_blocks.block_info() == retry_cv.commit_info() {
                        // retry broadcasting the message if the blocks are still pending
                        self.broadcast_with_retry(retry_cv).await;
                    }
                }
            }
            CommitPhaseTimeoutEvent::SignCommitVote(ordered_ledger_info) => {
                report_err!(
                        // receive new blocks from execution phase
                        self.retry_sign_pending_blocks(ordered_ledger_info).await,
                        "Error in retrying processing received blocks due to a previous safety-rule failure"
                    );
            }
        }
    }

    pub async fn start(mut self) {
        info!("commit phase starts");
        while self.in_epoch {
            debug!("Channel status self.block.is_some: {}, commit_msg_rx alive:{}, reset_event_rx alive:{}, commit_channel_recv alive:{}",
                self.blocks.is_some(),
                !self.commit_msg_rx.is_terminated(),
                !self.reset_event_rx.is_terminated(),
                !self.commit_channel_recv.is_terminated(),
            );

            // if we are still collecting the signatures
            if self.commit_channel_recv.is_terminated() || self.commit_msg_rx.is_terminated() {
                break;
            }
            tokio::select! {
                biased;
                // process messages dispatched from epoch_manager
                Some(msg) = self.commit_msg_rx.next(), if !self.commit_channel_recv.is_terminated() && !self.commit_msg_rx.is_terminated() && self.blocks.is_some() => {
                    info!("commit_msg_rx");
                    match msg {
                            VerifiedEvent::CommitVote(cv) => {
                                monitor!(
                                    "process_commit_vote",
                                    report_err!(self.process_commit_vote(&*cv).await, "Error in processing commit vote.")
                                );
                            }
                            VerifiedEvent::CommitDecision(cd) => {
                                monitor!(
                                    "process_commit_decision",
                                    report_err!(self.process_commit_decision(&*cd).await, "Error in processing commit decision.")
                                );
                            }
                            _ => {
                                unreachable!("Unexpected messages: something wrong with message dispatching.")
                            }
                    };
                    report_err!(
                            // check if the blocks are ready to commit
                            self.check_commit().await,
                            "Error in checking whether self.block is ready to commit."
                    );
                }
                Some(CommitChannelType(blocks, ordered_ledger_info, callback)) = self.commit_channel_recv.next(),
                    if !self.commit_channel_recv.is_terminated() && !self.commit_msg_rx.is_terminated() && self.blocks.is_none() => {
                        info!("commit_channel_recv");
                        monitor!("process_executed_blocks",
                            report_err!(
                                // receive new blocks from execution phase
                                self.process_executed_blocks(blocks, ordered_ledger_info, callback)
                                    .await,
                                "Error in processing received blocks"
                            )
                        )
                }
                Some(retry_event) = self.timeout_event_rx.next(), if !self.timeout_event_rx.is_terminated() && !self.commit_channel_recv.is_terminated() && !self.commit_msg_rx.is_terminated()  => {
                    info!("timeout_event_rx");
                    monitor!("process_retry_event", self.process_retry_event(retry_event).await);
                }
                // callback event might come when self.blocks is not empty
                Some(reset_event_callback) = self.reset_event_rx.next(), if !self.reset_event_rx.is_terminated() && !self.commit_channel_recv.is_terminated() && !self.commit_msg_rx.is_terminated() => {
                    info!("reset_event_rx");
                    monitor!("process_reset_event",
                        self.process_reset_event(reset_event_callback).await.map_err(|e| ExecutionError::InternalError {
                            error: e.to_string(),
                        })
                        .unwrap()
                    );
                }
                else => break,
            }
        }
        info!("commit phase stops");
        debug!("Channel status self.block.is_some: {}, commit_msg_rx alive:{}, reset_event_rx alive:{}, commit_channel_recv alive:{}",
                self.blocks.is_some(),
                !self.commit_msg_rx.is_terminated(),
                !self.reset_event_rx.is_terminated(),
                !self.commit_channel_recv.is_terminated(),
        );
    }
}
