// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    experimental::{commit_phase::CommitChannelType, errors::Error},
    state_replication::{StateComputer, StateComputerCommitCallBackType},
};
use channel::{Receiver, Sender};
use consensus_types::{block::Block, executed_block::ExecutedBlock};
use diem_crypto::HashValue;
use diem_logger::prelude::*;
use diem_types::ledger_info::LedgerInfoWithSignatures;
use executor_types::Error as ExecutionError;
use futures::{channel::oneshot, select, FutureExt, SinkExt, StreamExt};
use std::sync::Arc;
use futures::channel::mpsc::UnboundedReceiver;

/// [ This class is used when consensus.decoupled = true ]
/// ExecutionPhase is a singleton that receives ordered blocks from
/// the ordering state computer and execute them. After the execution is done,
/// ExecutionPhase sends the ordered blocks to the commit phase.
///

pub type ResetAck = ();
pub fn reset_ack_new() -> ResetAck {}

pub type ExecutionPhaseCallBackType = Option<Box<dyn FnOnce(HashValue, Block) + Send + Sync>>;

#[cfg(test)]
pub fn empty_execute_phase_callback() -> ExecutionPhaseCallBackType {
    None
}

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

pub struct ExecutionPhase {
    executor_channel_rx: UnboundedReceiver<ExecutionChannelType>,
    execution_proxy: Arc<dyn StateComputer>,
    commit_channel_tx: Sender<CommitChannelType>,
    reset_event_channel_rx: Receiver<oneshot::Sender<ResetAck>>,
    commit_phase_reset_event_tx: Sender<oneshot::Sender<ResetAck>>,
    in_epoch: bool,
}

impl ExecutionPhase {
    pub fn new(
        executor_channel_rx: UnboundedReceiver<ExecutionChannelType>,
        execution_proxy: Arc<dyn StateComputer>,
        commit_channel_tx: Sender<CommitChannelType>,
        reset_event_channel_rx: Receiver<oneshot::Sender<ResetAck>>,
        commit_phase_reset_event_tx: Sender<oneshot::Sender<ResetAck>>,
    ) -> Self {
        Self {
            executor_channel_rx,
            execution_proxy,
            commit_channel_tx,
            reset_event_channel_rx,
            commit_phase_reset_event_tx,
            in_epoch: true,
        }
    }

    pub async fn process_reset_event(
        &mut self,
        reset_event_callback: oneshot::Sender<ResetAck>,
    ) -> anyhow::Result<()> {
        // reset the execution phase

        // notify the commit phase
        let (tx, rx) = oneshot::channel::<ResetAck>();
        self.commit_phase_reset_event_tx.send(tx).await?;
        rx.await?;

        // exhaust the executor channel
        while self.executor_channel_rx.next().now_or_never().is_some() {}

        // activate the callback
        reset_event_callback
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

    pub async fn process_ordered_blocks(&mut self, execution_channel_type: ExecutionChannelType) {
        let ExecutionChannelType(vecblock, ledger_info, execution_failure_callback, callback) =
            execution_channel_type;
        // execute the blocks with execution_correctness_client
        let execution_result = self.execute_blocks(vecblock.clone());
        match execution_result {
            Ok(executed_blocks) => {
                // pass the executed blocks into the commit phase
                if self
                    .commit_channel_tx
                    .send(CommitChannelType(executed_blocks, ledger_info, callback))
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
            Err(ExecutionError::BlockNotFound(parent_block_id)) => {
                // execution failure callback
                // this vector does not goto the commit phase

                // find the block that triggers the error, which must exist
                let target_block = vecblock
                    .iter()
                    .find(|b| b.parent_id() == parent_block_id)
                    .unwrap();

                // there must be a callback
                execution_failure_callback.unwrap()(parent_block_id, target_block.clone());
            }
            _ => {
                unimplemented!()
            }
        }
    }

    pub async fn start(mut self) {
        // main loop
        info!("execution phase started");
        while self.in_epoch {
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
        }
        info!("execution phase stops");
    }
}
