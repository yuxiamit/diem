// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use diem_types::ledger_info::LedgerInfoWithSignatures;
use crate::state_replication::{StateComputerCommitCallBackType, StateComputer};
use futures::channel::mpsc::UnboundedReceiver;
use consensus_types::executed_block::ExecutedBlock;
use std::sync::Arc;
use futures::{StreamExt, FutureExt};
use std::sync::atomic::{AtomicU64, Ordering};
use diem_logger::prelude::*;
use channel::Receiver;
use crate::experimental::execution_phase::{ResetEventType, reset_ack_new};
use futures::prelude::stream::FusedStream;
use core::hint;
use crate::experimental::errors::Error;

pub struct PersistingChannelType(
    pub Vec<ExecutedBlock>,
    pub LedgerInfoWithSignatures,
    pub StateComputerCommitCallBackType,
);

pub struct PersistingPhase {
    persist_channel_rx: UnboundedReceiver<PersistingChannelType>,
    reset_channel_rx: Receiver<ResetEventType>,
    execution_proxy: Arc<dyn StateComputer>,
    back_pressure: Arc<AtomicU64>,
    in_epoch: bool,
}

impl PersistingPhase {
    pub fn new(
        persist_channel_rx: UnboundedReceiver<PersistingChannelType>,
        reset_channel_rx: Receiver<ResetEventType>,
        execution_proxy: Arc<dyn StateComputer>,
        back_pressure: Arc<AtomicU64>,
    ) -> Self {
        Self {
            persist_channel_rx,
            reset_channel_rx,
            execution_proxy,
            back_pressure,
            in_epoch: true,
        }
    }

    pub async fn process_channel_item(&mut self, persist_channel_item: PersistingChannelType) {
        info!("process_channel_item");
        let PersistingChannelType(blocks, ledger_info, callback) = persist_channel_item;
        let blocks_to_commit = blocks
            .iter()
            .map(|eb| Arc::new(eb.clone()))
            .collect::<Vec<Arc<ExecutedBlock>>>();

        if ledger_info.ledger_info().ends_epoch() {
            self.in_epoch = false;
        }

        self.execution_proxy.commit(
            &blocks_to_commit,
            ledger_info,
            callback,
            None,
        ).await.expect("Failed to persist commit");

        self.back_pressure.store(blocks.last().unwrap().round(), Ordering::SeqCst);
    }

    pub async fn process_reset_item(&mut self, reset_item: ResetEventType) -> anyhow::Result<()> {
        info!("resetting..");

        let ResetEventType { reset_callback, reconfig } = reset_item;

        while !self.persist_channel_rx.is_terminated() && self.persist_channel_rx.next().now_or_never().is_some() {
            hint::spin_loop();
        }

        if reconfig {
            self.in_epoch = false;
        }

        reset_callback
            .send(reset_ack_new())
            .map_err(|_| Error::ResetDropped)?;

        info!("reset finished.");

        Ok(())
    }

    pub async fn start(mut self) {
        info!("persisting phase starts");
        while self.in_epoch {
            futures::select! {
                persist_channel_item = self.persist_channel_rx.select_next_some() => {
                    self.process_channel_item(persist_channel_item).await;
                }
                reset_item = self.reset_channel_rx.select_next_some() => {
                    if let Err(e) = self.process_reset_item(reset_item).await {
                        error!("error in processing reset event {}", e.to_string());
                    }
                }
                complete => break,
            }
        }
    }
}
