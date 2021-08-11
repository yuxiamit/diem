// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    experimental::{commit_phase::CommitChannelType, errors::Error},
    state_replication::{StateComputer, StateComputerCommitCallBackType},
};
use channel::{Receiver, Sender};
use consensus_types::{block::Block, executed_block::ExecutedBlock};

use crate::experimental::execution_phase::{
    reset_ack_new, ExecutionChannelType, ExecutionPendingBlocks, ResetEventType,
};
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
    select,
    task::Poll,
    FutureExt, SinkExt, Stream, StreamExt,
};
use std::{pin::Pin, sync::Arc, thread::sleep, time::Duration};

/// [ This class is used when consensus.decoupled = true ]
/// ExecutionPhase is a singleton that receives ordered blocks from
/// the ordering state computer and execute them. After the execution is done,
/// ExecutionPhase sends the ordered blocks to the commit phase.
///

pub struct MockExecutionPhase {
    executor_channel_rx: UnboundedReceiver<ExecutionChannelType>,
    reset_event_channel_rx: Receiver<ResetEventType>,
    in_epoch: bool,
    pending_blocks: Option<ExecutionPendingBlocks>,
}

impl Drop for MockExecutionPhase {
    fn drop(&mut self) {
        info!("execution phase dropped");
        debug!("execution phase status: pending {}, executor_channel_rx alive {}, reset_event_channel_rx {}",
                self.pending_blocks.is_some(),
                !self.executor_channel_rx.is_terminated(),
                !self.reset_event_channel_rx.is_terminated());
    }
}

impl MockExecutionPhase {
    pub fn new(
        executor_channel_rx: UnboundedReceiver<ExecutionChannelType>,
        reset_event_channel_rx: Receiver<ResetEventType>,
    ) -> Self {
        info!("execution phase new");
        Self {
            executor_channel_rx,
            reset_event_channel_rx,
            in_epoch: true,
            pending_blocks: None,
        }
    }

    pub async fn process_reset_event(&mut self, reset_event: ResetEventType) -> anyhow::Result<()> {
        info!("process_reset_event");

        let ResetEventType {
            reset_callback: tx,
            reconfig: _,
        } = reset_event;

        tx.send(reset_ack_new()).ok();

        Ok(())
    }

    pub async fn process_ordered_blocks(&mut self, _execution_channel_type: ExecutionChannelType) {
        info!("process_ordered_blocks");
    }

    pub async fn start(mut self) {
        // main loop
        info!("execution phase started");
        while self.in_epoch {
            if self.pending_blocks.is_none() {
                debug!("pending blocks is none");

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
                debug!("got message");
            } else {
                debug!("pending blocks is some");
                if let Some(Some(reset_event_callback)) =
                    self.reset_event_channel_rx.next().now_or_never()
                {
                    self.process_reset_event(reset_event_callback)
                        .await
                        .map_err(|e| ExecutionError::InternalError {
                            error: e.to_string(),
                        })
                        .unwrap();
                }
            }
        }
        info!("execution phase stops");
    }
}
