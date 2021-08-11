// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    error::StateSyncError, experimental::execution_phase::ExecutionPhaseCallBackType,
    state_replication::StateComputer,
};
use anyhow::Result;
use channel::Sender;
use consensus_types::{block::Block, executed_block::ExecutedBlock};
use diem_crypto::HashValue;
use diem_logger::prelude::*;
use diem_types::ledger_info::LedgerInfoWithSignatures;
use executor_types::{Error as ExecutionError, StateComputeResult};
use fail::fail_point;
use futures::{channel::mpsc::UnboundedSender, SinkExt};
use std::{boxed::Box, sync::Arc};

use crate::{
    experimental::execution_phase::{
        notify_downstream_reset, ExecutionChannelType, ResetEventType,
    },
    state_replication::StateComputerCommitCallBackType,
};

/// Ordering-only execution proxy
/// implements StateComputer traits.
/// Used only when node_config.validator.consensus.decoupled = true.
pub struct OrderingStateComputer {
    // the channel to pour vectors of blocks into
    // the real execution phase (will be handled in ExecutionPhase).
    executor_channel: UnboundedSender<ExecutionChannelType>,
    state_computer_for_sync: Arc<dyn StateComputer>,
    reset_event_channel_tx: Sender<ResetEventType>,
    name: String,
}

impl OrderingStateComputer {
    pub fn new(
        executor_channel: UnboundedSender<ExecutionChannelType>,
        state_computer_for_sync: Arc<dyn StateComputer>,
        reset_event_channel_tx: Sender<ResetEventType>,
    ) -> Self {
        info!("ordering state computer new");
        Self {
            executor_channel,
            state_computer_for_sync,
            reset_event_channel_tx,
            name: String::from(""),
        }
    }

    pub fn new_with_name(
        executor_channel: UnboundedSender<ExecutionChannelType>,
        state_computer_for_sync: Arc<dyn StateComputer>,
        reset_event_channel_tx: Sender<ResetEventType>,
        name: String,
    ) -> Self {
        info!("ordering state computer new");
        Self {
            executor_channel,
            state_computer_for_sync,
            reset_event_channel_tx,
            name,
        }
    }
}

impl Drop for OrderingStateComputer {
    fn drop(&mut self) {
        info!("Start dropping");
        /*
        if let Err(e) = block_on(notify_downstream_reset(&self.reset_event_channel_tx, true)) {
            error!("Error in reseting before get dropped {}", e.to_string());
        }
        */
        info!(
            "ordering state computer [{}] dropped, inner state computer ref count {}",
            self.name,
            Arc::strong_count(&self.state_computer_for_sync),
        );
    }
}

#[async_trait::async_trait]
impl StateComputer for OrderingStateComputer {
    fn compute(
        &self,
        // The block to be executed.
        _block: &Block,
        // The parent block id.
        _parent_block_id: HashValue,
    ) -> Result<StateComputeResult, ExecutionError> {
        // Return dummy block and bypass the execution phase.
        // This will break the e2e smoke test (for now because
        // no one is actually handling the next phase) if the
        // decoupled execution feature is turned on.
        Ok(StateComputeResult::new_dummy())
    }

    /// Send ordered blocks to the real execution phase through the channel.
    /// A future is fulfilled right away when the blocks are sent into the channel.
    async fn commit(
        &self,
        blocks: &[Arc<ExecutedBlock>],
        finality_proof: LedgerInfoWithSignatures,
        callback: StateComputerCommitCallBackType,
        executor_failure_callback: ExecutionPhaseCallBackType,
    ) -> Result<(), ExecutionError> {
        assert!(!blocks.is_empty());

        let ordered_block = blocks.iter().map(|b| b.block().clone()).collect();

        if let Err(e) = self
            .executor_channel
            .clone()
            .send(ExecutionChannelType(
                ordered_block,
                finality_proof,
                executor_failure_callback,
                callback,
            ))
            .await
        {
            // probably the execution phase is gone
            error!("Send failure {}", e.to_string());
        }
        Ok(())
    }

    /// Synchronize to a commit that not present locally.
    async fn sync_to(&self, target: LedgerInfoWithSignatures) -> Result<(), StateSyncError> {
        fail_point!("consensus::sync_to", |_| {
            Err(anyhow::anyhow!("Injected error in sync_to").into())
        });

        debug!(
            "ordering state computer sync to with target {}",
            target.clone()
        );

        // reset execution phase and commit phase
        if let Err(e) = notify_downstream_reset(
            &self.reset_event_channel_tx,
            target.ledger_info().ends_epoch(),
        )
        .await
        {
            error!(
                "Error in requesting execution phase to reset: {}",
                e.to_string()
            );
        }

        debug!("resetting executor");

        // has to after reset to avoid racing (committing the blocks after the sync)
        self.state_computer_for_sync.sync_to(target).await?;

        debug!("sync finished");

        Ok(())
    }
}
