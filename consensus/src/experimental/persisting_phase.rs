// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use diem_types::ledger_info::LedgerInfoWithSignatures;
use crate::state_replication::{StateComputerCommitCallBackType, StateComputer};
use futures::channel::mpsc::UnboundedReceiver;
use consensus_types::executed_block::ExecutedBlock;
use std::sync::Arc;
use futures::StreamExt;
use std::sync::atomic::{AtomicU64, Ordering};
use diem_logger::prelude::*;

pub struct PersistingChannelType(
    pub Vec<ExecutedBlock>,
    pub LedgerInfoWithSignatures,
    pub StateComputerCommitCallBackType,
);

pub struct PersistingPhase {
    persist_channel_rx: UnboundedReceiver<PersistingChannelType>,
    execution_proxy: Arc<dyn StateComputer>,
    back_pressure: Arc<AtomicU64>,
    in_epoch: bool,
}

impl PersistingPhase {
    pub fn new(
        persist_channel_rx: UnboundedReceiver<PersistingChannelType>,
        execution_proxy: Arc<dyn StateComputer>,
        back_pressure: Arc<AtomicU64>,
    ) -> Self {
        Self {
            persist_channel_rx,
            execution_proxy,
            back_pressure,
            in_epoch: true,
        }
    }

    pub async fn process_channel_item(&mut self, persist_channel_item: PersistingChannelType) {
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

    pub async fn start(mut self) {
        info!("persisting phase starts");
        while self.in_epoch {
            if let Some(persist_channel_item) = self.persist_channel_rx.next().await {
                self.process_channel_item(persist_channel_item).await;
            }
            else {
                break
            }
        }
    }
}
