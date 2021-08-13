// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    experimental::{commit_phase::CommitChannelType, errors::Error},
    state_replication::{StateComputer, StateComputerCommitCallBackType},
};
use channel::{Receiver, Sender};
use consensus_types::{block::Block, executed_block::ExecutedBlock};
use core::hint;
use diem_logger::prelude::*;
use diem_types::ledger_info::LedgerInfoWithSignatures;
use executor_types::Error as ExecutionError;
use futures::{
    channel::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    prelude::stream::FusedStream,
    select, FutureExt, SinkExt, StreamExt,
};
use std::sync::Arc;

/// [ This class is used when consensus.decoupled = true ]
/// ExecutionPhase is a singleton that receives ordered blocks from
/// the ordering state computer and execute them. After the execution is done,
/// ExecutionPhase sends the ordered blocks to the commit phase.
///

#[derive(Debug)]
pub struct ResetEventType {
    pub reset_callback: oneshot::Sender<ResetAck>,
    pub reconfig: bool,
}

pub async fn notify_downstream_reset(
    reset_tx: &Sender<ResetEventType>,
    reconfig: bool,
) -> anyhow::Result<()> {
    // notify the commit phase
    let (tx, rx) = oneshot::channel::<ResetAck>();
    reset_tx
        .clone()
        .send(ResetEventType {
            reset_callback: tx,
            reconfig,
        })
        .await?;
    rx.await?;
    Ok(())
}

pub type ExecutionPhaseCallBackType =
    Option<Box<dyn Fn(LedgerInfoWithSignatures) -> Vec<Block> + Send + Sync>>;

#[cfg(test)]
pub fn empty_execute_phase_callback() -> ExecutionPhaseCallBackType {
    None
}

pub type ResetAck = ();
pub fn reset_ack_new() -> ResetAck {}

pub struct ExecutionChannelType(
    pub Vec<Block>,
    pub LedgerInfoWithSignatures,
    pub ExecutionPhaseCallBackType,
    pub StateComputerCommitCallBackType,
);

impl std::fmt::Debug for ExecutionChannelType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::fmt::Display for ExecutionChannelType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ExecutionChannelType({:?}, {})", self.0, self.1)
    }
}

pub struct ExecutionPendingBlocks {
    pub pending_channel_type: Option<ExecutionChannelType>,
    pub original_blocks: Vec<Block>,
}

impl ExecutionPendingBlocks {
    pub fn update_blocks(&mut self, blocks: Vec<Block>) {
        let pending_channel_type = self.pending_channel_type.take().unwrap();
        self.pending_channel_type = Some(ExecutionChannelType(
            blocks,
            pending_channel_type.1,
            pending_channel_type.2,
            pending_channel_type.3,
        ))
    }

    pub fn vecblocks(&self) -> Vec<Block> {
        self.pending_channel_type.as_ref().unwrap().0.clone()
    }
    pub fn ledger_info(&self) -> LedgerInfoWithSignatures {
        self.pending_channel_type.as_ref().unwrap().1.clone()
    }
    pub fn execution_failure_callback(&self) -> &ExecutionPhaseCallBackType {
        &self.pending_channel_type.as_ref().unwrap().2
    }
}

pub struct ExecutionPhase {
    executor_channel_rx: UnboundedReceiver<ExecutionChannelType>,
    execution_proxy: Arc<dyn StateComputer>,
    commit_channel_tx: UnboundedSender<CommitChannelType>,
    reset_event_channel_rx: Receiver<ResetEventType>,
    commit_phase_reset_event_tx: Sender<ResetEventType>,
    in_epoch: bool,
    pending_blocks: Option<ExecutionPendingBlocks>,
}

impl ExecutionPhase {
    pub fn new(
        executor_channel_rx: UnboundedReceiver<ExecutionChannelType>,
        execution_proxy: Arc<dyn StateComputer>,
        commit_channel_tx: UnboundedSender<CommitChannelType>,
        reset_event_channel_rx: Receiver<ResetEventType>,
        commit_phase_reset_event_tx: Sender<ResetEventType>,
    ) -> Self {
        Self {
            executor_channel_rx,
            execution_proxy,
            commit_channel_tx,
            reset_event_channel_rx,
            commit_phase_reset_event_tx,
            in_epoch: true,
            pending_blocks: None,
        }
    }

    pub async fn process_reset_event(&mut self, reset_event: ResetEventType) -> anyhow::Result<()> {
        let ResetEventType {
            reset_callback,
            reconfig,
        } = reset_event;
        // reset the execution phase

        if let Err(e) = notify_downstream_reset(&self.commit_phase_reset_event_tx, reconfig).await {
            error!(
                "Error in requesting commit phase to reset: {}",
                e.to_string()
            );
        }

        // exhaust the executor channel
        while !self.executor_channel_rx.is_terminated()
            && !self.reset_event_channel_rx.is_terminated()
            && self.executor_channel_rx.next().now_or_never().is_some()
        {
            hint::spin_loop();
        }

        self.pending_blocks = None;

        if reconfig {
            self.in_epoch = false;
        }
        // activate the callback
        reset_callback
            .send(reset_ack_new())
            .map_err(|_| Error::ResetDropped)?;

        Ok(())
    }

    pub fn execute_blocks(&self, blocks: Vec<Block>) -> Result<Vec<ExecutedBlock>, ExecutionError> {
        let executed_blocks: Result<Vec<_>, _> = blocks
            .into_iter()
            .map(|b| -> Result<_, _> {
                let state_compute_result = self.execution_proxy.compute(&b, b.parent_id())?;
                Ok(ExecutedBlock::new(b, state_compute_result))
            })
            .collect();
        executed_blocks
    }

    pub async fn try_execute_blocks(&mut self) {
        info!("try_execute_blocks");
        if let Some(pd) = self.pending_blocks.as_ref() {
            let vecblock = pd.vecblocks();
            let pending_ledger_info = pd.ledger_info();
            info!("trying to execute blocks {:?}", vecblock);
            let execution_result = self.execute_blocks(vecblock.clone());
            match execution_result {
                Ok(executed_blocks) => {
                    // assert this is consistent with our batch
                    assert_eq!(
                        executed_blocks.last().unwrap().id(),
                        pd.original_blocks.last().unwrap().id()
                    );
                    assert!(executed_blocks.len() >= vecblock.len());
                    let starting_idx = executed_blocks.len() - pd.original_blocks.len();
                    assert_eq!(
                        executed_blocks[starting_idx].id(),
                        pd.original_blocks.first().unwrap().id()
                    );
                    // pass the executed blocks into the commit phase

                    if self
                        .commit_channel_tx
                        .send(CommitChannelType(
                            executed_blocks[starting_idx..].to_vec(),
                            pending_ledger_info.clone(),
                            self.pending_blocks
                                .take()
                                .unwrap()
                                .pending_channel_type
                                .take()
                                .unwrap()
                                .3,
                        ))
                        .await
                        .map_err(|e| ExecutionError::InternalError {
                            error: e.to_string(),
                        })
                        .is_err()
                    {
                        // if the commit phase stops (due to epoch change),
                        // execution phase also needs to stop
                        self.in_epoch = false;
                    }
                }
                Err(ExecutionError::BlockNotFound(_)) => {
                    // there must be a callback
                    let refetched_block = pd.execution_failure_callback().as_ref().unwrap()(
                        pending_ledger_info.clone(),
                    );

                    self.pending_blocks
                        .as_mut()
                        .unwrap()
                        .update_blocks(refetched_block);
                }
                Err(e) => {
                    // retry locally
                    error!("Retry execution caused by : Error in executor: {:?}", e);
                }
            }
        }
    }

    pub async fn process_ordered_blocks(&mut self, execution_channel_type: ExecutionChannelType) {
        info!("process_ordered_blocks");
        let blocks_to_push = execution_channel_type.0.clone();
        // execute the blocks with execution_correctness_client

        self.pending_blocks = Some(ExecutionPendingBlocks {
            pending_channel_type: Some(execution_channel_type),
            original_blocks: blocks_to_push,
        });

        self.try_execute_blocks().await;
    }

    pub async fn start(mut self) {
        // main loop
        while self.in_epoch {
            if self.pending_blocks.is_none() {
                select! {
                    executor_channel_msg = self.executor_channel_rx.select_next_some() => {
                        self.process_ordered_blocks(executor_channel_msg).await;
                    }
                    reset_event_callback = self.reset_event_channel_rx.select_next_some() => {
                        self.process_reset_event(reset_event_callback).await.map_err(|e| ExecutionError::InternalError {
                            error: e.to_string(),
                        })
                        .unwrap();
                    }
                    complete => break,
                };
            } else {
                self.try_execute_blocks().await;
                if self.pending_blocks.is_none() {
                    continue;
                }
                select! {
                    reset_event_callback = self.reset_event_channel_rx.select_next_some() => {
                        self.process_reset_event(reset_event_callback).await.map_err(|e| ExecutionError::InternalError {
                            error: e.to_string(),
                        }).unwrap();
                    }
                    default => { continue }
                }
            }
        }
    }
}
