// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    block_storage::BlockStore,
    counters,
    error::{error_kind, DbError},
    experimental::{
        commit_phase::{
            CommitChannelType, CommitPhase, CommitPhaseMessageKey, CommitPhaseMessageType,
        },
        execution_phase::{
            notify_downstream_reset, ExecutionChannelType, ExecutionPhase, ResetEventType,
        },
        ordering_state_computer::OrderingStateComputer,
        persisting_phase::{PersistingChannelType, PersistingPhase},
    },
    liveness::{
        leader_reputation::{ActiveInactiveHeuristic, DiemDBBackend, LeaderReputation},
        proposal_generator::ProposalGenerator,
        proposer_election::ProposerElection,
        rotating_proposer_election::{choose_leader, RotatingProposer},
        round_proposer_election::RoundProposer,
        round_state::{ExponentialTimeInterval, RoundState, RoundStateLogSchema},
    },
    logging::{LogEvent, LogSchema},
    metrics_safety_rules::MetricsSafetyRules,
    network::{IncomingBlockRetrievalRequest, NetworkReceivers, NetworkSender},
    network_interface::{ConsensusMsg, ConsensusNetworkSender},
    persistent_liveness_storage::{LedgerRecoveryData, PersistentLivenessStorage, RecoveryData},
    round_manager::{RecoveryManager, RoundManager, UnverifiedEvent, VerifiedEvent},
    state_replication::{StateComputer, TxnManager},
    util::time_service::TimeService,
};
use anyhow::{anyhow, bail, ensure, Context};
use channel::{diem_channel, message_queues::QueueStyle, Sender};
use consensus_types::{
    common::{Author, Round},
    epoch_retrieval::EpochRetrievalRequest,
    experimental::{commit_decision::CommitDecision, commit_vote::CommitVote},
};
use diem_config::config::{ConsensusConfig, ConsensusProposerType, NodeConfig};
use diem_infallible::{duration_since_epoch, Mutex};
use diem_logger::prelude::*;
use diem_metrics::monitor;
use diem_types::{
    account_address::AccountAddress,
    epoch_change::EpochChangeProof,
    epoch_state::EpochState,
    on_chain_config::{OnChainConfigPayload, ValidatorSet},
};
use futures::{channel::mpsc::unbounded, select, StreamExt};
use network::protocols::network::Event;
use safety_rules::SafetyRulesManager;
use std::{
    cmp::Ordering,
    sync::{atomic::AtomicU64, Arc},
    thread::sleep,
    time::Duration,
};
//use diem_types::on_chain_config::OnChainConsensusConfig;

/// RecoveryManager is used to process events in order to sync up with peer if we can't recover from local consensusdb
/// RoundManager is used for normal event handling.
/// We suppress clippy warning here because we expect most of the time we will have RoundManager
#[allow(clippy::large_enum_variant)]
pub enum RoundProcessor {
    Recovery(RecoveryManager),
    Normal(RoundManager),
}

#[allow(clippy::large_enum_variant)]
pub enum LivenessStorageData {
    RecoveryData(RecoveryData),
    LedgerRecoveryData(LedgerRecoveryData),
}

impl LivenessStorageData {
    pub fn expect_recovery_data(self, msg: &str) -> RecoveryData {
        match self {
            LivenessStorageData::RecoveryData(data) => data,
            LivenessStorageData::LedgerRecoveryData(_) => panic!("{}", msg),
        }
    }
}

// Manager the components that shared across epoch and spawn per-epoch RoundManager with
// epoch-specific input.
pub struct EpochManager {
    author: Author,
    config: ConsensusConfig,
    time_service: Arc<dyn TimeService>,
    self_sender: channel::Sender<Event<ConsensusMsg>>,
    network_sender: ConsensusNetworkSender,
    timeout_sender: channel::Sender<Round>,
    txn_manager: Arc<dyn TxnManager>,
    commit_state_computer: Arc<dyn StateComputer>,
    storage: Arc<dyn PersistentLivenessStorage>,
    safety_rules_manager: SafetyRulesManager,
    processor: Option<RoundProcessor>,
    reconfig_events: diem_channel::Receiver<(), OnChainConfigPayload>,
    commit_msg_tx: Option<diem_channel::Sender<CommitPhaseMessageKey, VerifiedEvent>>,
    commit_phase_reset_tx: Option<Sender<ResetEventType>>,
    back_pressure: Arc<AtomicU64>,
}

impl EpochManager {
    pub fn new(
        node_config: &NodeConfig,
        time_service: Arc<dyn TimeService>,
        self_sender: channel::Sender<Event<ConsensusMsg>>,
        network_sender: ConsensusNetworkSender,
        timeout_sender: channel::Sender<Round>,
        txn_manager: Arc<dyn TxnManager>,
        commit_state_computer: Arc<dyn StateComputer>,
        storage: Arc<dyn PersistentLivenessStorage>,
        reconfig_events: diem_channel::Receiver<(), OnChainConfigPayload>,
    ) -> Self {
        let author = node_config.validator_network.as_ref().unwrap().peer_id();
        let config = node_config.consensus.clone();
        let sr_config = &node_config.consensus.safety_rules;
        if sr_config.decoupled_execution != config.decoupled_execution {
            panic!("Inconsistent decoupled-execution configuration of consensus and safety-rules\nMake sure consensus.decoupled = safety_rules.decoupled_execution.")
        }
        let safety_rules_manager = SafetyRulesManager::new(sr_config);
        let back_pressure = Arc::new(AtomicU64::new(0));
        Self {
            author,
            config,
            time_service,
            self_sender,
            network_sender,
            timeout_sender,
            txn_manager,
            commit_state_computer,
            storage,
            safety_rules_manager,
            processor: None,
            reconfig_events,
            commit_msg_tx: None,
            commit_phase_reset_tx: None,
            back_pressure,
        }
    }

    fn epoch_state(&self) -> &EpochState {
        match self
            .processor
            .as_ref()
            .expect("EpochManager not started yet")
        {
            RoundProcessor::Normal(p) => p.epoch_state(),
            RoundProcessor::Recovery(p) => p.epoch_state(),
        }
    }

    fn epoch(&self) -> u64 {
        self.epoch_state().epoch
    }

    fn create_round_state(
        &self,
        time_service: Arc<dyn TimeService>,
        timeout_sender: channel::Sender<Round>,
    ) -> RoundState {
        // 1.5^6 ~= 11
        // Timeout goes from initial_timeout to initial_timeout*11 in 6 steps
        let time_interval = Box::new(ExponentialTimeInterval::new(
            Duration::from_millis(self.config.round_initial_timeout_ms),
            1.2,
            6,
        ));
        RoundState::new(time_interval, time_service, timeout_sender)
    }

    /// Create a proposer election handler based on proposers
    fn create_proposer_election(
        &self,
        epoch_state: &EpochState,
    ) -> Box<dyn ProposerElection + Send + Sync> {
        let proposers = epoch_state
            .verifier
            .get_ordered_account_addresses_iter()
            .collect::<Vec<_>>();
        match &self.config.proposer_type {
            ConsensusProposerType::RotatingProposer => Box::new(RotatingProposer::new(
                proposers,
                self.config.contiguous_rounds,
            )),
            // We don't really have a fixed proposer!
            ConsensusProposerType::FixedProposer => {
                let proposer = choose_leader(proposers);
                Box::new(RotatingProposer::new(
                    vec![proposer],
                    self.config.contiguous_rounds,
                ))
            }
            ConsensusProposerType::LeaderReputation(heuristic_config) => {
                let backend = Box::new(DiemDBBackend::new(proposers.len(), self.storage.diem_db()));
                let heuristic = Box::new(ActiveInactiveHeuristic::new(
                    self.author,
                    heuristic_config.active_weights,
                    heuristic_config.inactive_weights,
                ));
                Box::new(LeaderReputation::new(proposers, backend, heuristic))
            }
            ConsensusProposerType::RoundProposer(round_proposers) => {
                // Hardcoded to the first proposer
                let default_proposer = proposers.get(0).unwrap();
                Box::new(RoundProposer::new(
                    round_proposers.clone(),
                    *default_proposer,
                ))
            }
        }
    }

    async fn process_epoch_retrieval(
        &mut self,
        request: EpochRetrievalRequest,
        peer_id: AccountAddress,
    ) -> anyhow::Result<()> {
        debug!(
            LogSchema::new(LogEvent::ReceiveEpochRetrieval)
                .remote_peer(peer_id)
                .epoch(self.epoch()),
            "[EpochManager] receive {}", request,
        );
        let proof = self
            .storage
            .diem_db()
            .get_epoch_ending_ledger_infos(request.start_epoch, request.end_epoch)
            .map_err(DbError::from)
            .context("[EpochManager] Failed to get epoch proof")?;
        let msg = ConsensusMsg::EpochChangeProof(Box::new(proof));
        self.network_sender.send_to(peer_id, msg).context(format!(
            "[EpochManager] Failed to send epoch proof to {}",
            peer_id
        ))
    }

    async fn process_different_epoch(
        &mut self,
        different_epoch: u64,
        peer_id: AccountAddress,
    ) -> anyhow::Result<()> {
        debug!(
            LogSchema::new(LogEvent::ReceiveMessageFromDifferentEpoch)
                .remote_peer(peer_id)
                .epoch(self.epoch()),
            remote_epoch = different_epoch,
        );
        match different_epoch.cmp(&self.epoch()) {
            // We try to help nodes that have lower epoch than us
            Ordering::Less => {
                self.process_epoch_retrieval(
                    EpochRetrievalRequest {
                        start_epoch: different_epoch,
                        end_epoch: self.epoch(),
                    },
                    peer_id,
                )
                .await
            }
            // We request proof to join higher epoch
            Ordering::Greater => {
                let request = EpochRetrievalRequest {
                    start_epoch: self.epoch(),
                    end_epoch: different_epoch,
                };
                let msg = ConsensusMsg::EpochRetrievalRequest(Box::new(request));
                self.network_sender.send_to(peer_id, msg).context(format!(
                    "[EpochManager] Failed to send epoch retrieval to {}",
                    peer_id
                ))
            }
            Ordering::Equal => {
                bail!("[EpochManager] Same epoch should not come to process_different_epoch");
            }
        }
    }

    async fn start_new_epoch(&mut self, proof: EpochChangeProof) -> anyhow::Result<()> {
        let ledger_info = proof
            .verify(self.epoch_state())
            .context("[EpochManager] Invalid EpochChangeProof")?;
        debug!(
            LogSchema::new(LogEvent::NewEpoch).epoch(ledger_info.ledger_info().next_block_epoch()),
            "Received verified epoch change",
        );

        // make sure storage is on this ledger_info too, it should be no-op if it's already committed
        self.commit_state_computer
            .sync_to(ledger_info.clone())
            .await
            .context(format!(
                "[EpochManager] State sync to new epoch {}",
                ledger_info
            ))?;

        monitor!("reconfig", self.expect_new_epoch().await);
        Ok(())
    }

    fn prepare_decoupled_execution(
        &mut self,
        epoch: u64,
        recovery_data: RecoveryData,
        epoch_state: EpochState,
        round_state: RoundState,
        proposer_election: Box<dyn ProposerElection + Send + Sync>,
        safety_rules_container: Arc<Mutex<MetricsSafetyRules>>,
        network_sender: NetworkSender,
    ) -> anyhow::Result<(RoundManager, ExecutionPhase, CommitPhase, PersistingPhase)> {
        let (execution_phase_tx, execution_phase_rx) = unbounded::<ExecutionChannelType>();

        let (execution_phase_reset_tx, execution_phase_reset_rx) = channel::new::<ResetEventType>(
            1,
            &counters::DECOUPLED_EXECUTION__EXECUTION_PHASE_RESET_CHANNEL,
        );

        let (commit_phase_reset_tx, commit_phase_reset_rx) = channel::new::<ResetEventType>(
            1,
            &counters::DECOUPLED_EXECUTION__COMMIT_PHASE_RESET_CHANNEL,
        );

        let state_computer: Arc<dyn StateComputer> =
            Arc::new(OrderingStateComputer::new_with_name(
                execution_phase_tx,
                self.commit_state_computer.clone(),
                execution_phase_reset_tx,
                format!("Ordering State Computer Epoch {}", epoch),
            ));

        info!(epoch = epoch, "Create BlockStore");
        let block_store = Arc::new(BlockStore::new(
            Arc::clone(&self.storage),
            recovery_data,
            state_computer,
            self.config.max_pruned_blocks_in_mem,
            Arc::clone(&self.time_service),
        ));

        info!(epoch = epoch, "Create ProposalGenerator");
        // txn manager is required both by proposal generator (to pull the proposers)
        // and by event processor (to update their status).
        let proposal_generator = ProposalGenerator::new(
            self.author,
            block_store.clone(),
            self.txn_manager.clone(),
            self.time_service.clone(),
            self.config.max_block_size,
        );

        let (commit_phase_tx, commit_phase_rx) = unbounded::<CommitChannelType>();

        self.commit_phase_reset_tx = Some(commit_phase_reset_tx.clone());

        let execution_phase = ExecutionPhase::new(
            execution_phase_rx,
            self.commit_state_computer.clone(),
            commit_phase_tx,
            execution_phase_reset_rx,
            commit_phase_reset_tx,
        );

        let (commit_msg_tx, commit_msg_rx) =
            diem_channel::new::<CommitPhaseMessageKey, VerifiedEvent>(
                QueueStyle::FIFO,
                self.config.channel_size,
                Some(&counters::DECOUPLED_EXECUTION__COMMIT_MESSAGE_CHANNEL),
            );

        self.commit_msg_tx = Some(commit_msg_tx);

        self.back_pressure
            .store(0, std::sync::atomic::Ordering::SeqCst);

        let (persist_phase_tx, persist_phase_rx) = unbounded::<PersistingChannelType>();

        let (persist_phase_reset_tx, persist_phase_reset_rx) = channel::new::<ResetEventType>(
            1,
            &counters::DECOUPLED_EXECUTION__PERSISTING_PHASE_RESET_CHANNEL,
        );

        let commit_phase = CommitPhase::new_with_persisting_phase(
            commit_phase_rx,
            self.commit_state_computer.clone(),
            commit_msg_rx,
            epoch_state.verifier.clone(),
            Arc::clone(&safety_rules_container),
            self.author,
            self.back_pressure.clone(),
            network_sender.clone(),
            commit_phase_reset_rx,
            Some(persist_phase_tx),
            Some(persist_phase_reset_tx),
        );

        let persisting_phase = PersistingPhase::new(
            persist_phase_rx,
            persist_phase_reset_rx,
            self.commit_state_computer.clone(),
            self.back_pressure.clone(),
        );

        let round_manager = RoundManager::new_with_decoupled_execution(
            epoch_state,
            block_store,
            round_state,
            proposer_election,
            proposal_generator,
            safety_rules_container,
            network_sender,
            self.txn_manager.clone(),
            self.storage.clone(),
            self.config.sync_only,
            self.back_pressure.clone(),
            self.config.back_pressure_limit,
        );

        Ok((
            round_manager,
            execution_phase,
            commit_phase,
            persisting_phase,
        ))
    }

    async fn start_round_manager(&mut self, recovery_data: RecoveryData, epoch_state: EpochState) {
        // Release the previous RoundManager, especially the SafetyRule client
        self.processor = None;
        // Release the previous Commit Phase
        self.commit_msg_tx = None;
        if let Some(reset_tx) = self.commit_phase_reset_tx.as_ref() {
            if let Err(e) = notify_downstream_reset(reset_tx, true).await {
                error!("Error in resetting commit phase {}", e.to_string());
            }
        }

        // let the execution phase drop
        sleep(Duration::from_millis(200));
        let epoch = epoch_state.epoch;
        counters::EPOCH.set(epoch_state.epoch as i64);
        counters::CURRENT_EPOCH_VALIDATORS.set(epoch_state.verifier.len() as i64);
        info!(
            epoch = epoch_state.epoch,
            validators = epoch_state.verifier.to_string(),
            root_block = recovery_data.root_block(),
            "Starting new epoch",
        );
        let last_vote = recovery_data.last_vote();

        info!(epoch = epoch, "Update SafetyRules");

        let mut safety_rules =
            MetricsSafetyRules::new(self.safety_rules_manager.client(), self.storage.clone());
        if let Err(error) = safety_rules.perform_initialize() {
            error!(
                epoch = epoch,
                error = error,
                "Unable to initialize safety rules.",
            );
        }

        info!(epoch = epoch, "Create RoundState");
        let round_state =
            self.create_round_state(self.time_service.clone(), self.timeout_sender.clone());

        info!(epoch = epoch, "Create ProposerElection");
        let proposer_election = self.create_proposer_election(&epoch_state);
        let network_sender = NetworkSender::new(
            self.author,
            self.network_sender.clone(),
            self.self_sender.clone(),
            epoch_state.verifier.clone(),
        );

        let safety_rules_container = Arc::new(Mutex::new(safety_rules));

        let mut processor = if self.config.decoupled_execution {
            info!(epoch = epoch, "starting decoupled execution");
            let (round_manager, execution_phase, commit_phase, persisting_phase) = self
                .prepare_decoupled_execution(
                    epoch,
                    recovery_data,
                    epoch_state,
                    round_state,
                    proposer_election,
                    safety_rules_container,
                    network_sender,
                )
                .unwrap();

            info!(epoch = epoch, "prepared decoupled execution");

            tokio::spawn(execution_phase.start());
            tokio::spawn(commit_phase.start());
            tokio::spawn(persisting_phase.start());

            round_manager
        } else {
            info!(epoch = epoch, "Create BlockStore");
            let block_store = Arc::new(BlockStore::new(
                Arc::clone(&self.storage),
                recovery_data,
                self.commit_state_computer.clone(),
                self.config.max_pruned_blocks_in_mem,
                Arc::clone(&self.time_service),
            ));

            info!(epoch = epoch, "Create ProposalGenerator");
            // txn manager is required both by proposal generator (to pull the proposers)
            // and by event processor (to update their status).
            let proposal_generator = ProposalGenerator::new(
                self.author,
                block_store.clone(),
                self.txn_manager.clone(),
                self.time_service.clone(),
                self.config.max_block_size,
            );

            RoundManager::new(
                epoch_state,
                block_store,
                round_state,
                proposer_election,
                proposal_generator,
                safety_rules_container,
                network_sender,
                self.txn_manager.clone(),
                self.storage.clone(),
                self.config.sync_only,
            )
        };

        processor.start(last_vote).await;
        self.processor = Some(RoundProcessor::Normal(processor));
        info!(epoch = epoch, "RoundManager started");
    }

    // Depending on what data we can extract from consensusdb, we may or may not have an
    // event processor at startup. If we need to sync up with peers for blocks to construct
    // a valid block store, which is required to construct an event processor, we will take
    // care of the sync up here.
    async fn start_recovery_manager(
        &mut self,
        ledger_recovery_data: LedgerRecoveryData,
        epoch_state: EpochState,
    ) {
        let epoch = epoch_state.epoch;
        let network_sender = NetworkSender::new(
            self.author,
            self.network_sender.clone(),
            self.self_sender.clone(),
            epoch_state.verifier.clone(),
        );

        // TODO: create a ordering only state computer

        self.processor = Some(RoundProcessor::Recovery(RecoveryManager::new(
            epoch_state,
            network_sender,
            self.storage.clone(),
            self.commit_state_computer.clone(),
            ledger_recovery_data.commit_round(),
        )));
        info!(epoch = epoch, "SyncProcessor started");
    }

    async fn start_processor(&mut self, payload: OnChainConfigPayload) {
        let validator_set: ValidatorSet = payload
            .get()
            .expect("failed to get ValidatorSet from payload");
        let epoch_state = EpochState {
            epoch: payload.epoch(),
            verifier: (&validator_set).into(),
        };

        /*
        // update decoupled-execution configs, if there is any
        if let Ok(decoupled_execution_config) = payload.get::<OnChainConsensusConfig>() {
            self.config.back_pressure_limit = decoupled_execution_config.inner.back_pressure_limit;
            self.config.decoupled_execution = decoupled_execution_config.inner.decoupled_execution;
            self.config.channel_size = decoupled_execution_config.inner.channel_size;
        }
        */

        match self.storage.start() {
            LivenessStorageData::RecoveryData(initial_data) => {
                self.start_round_manager(initial_data, epoch_state).await
            }
            LivenessStorageData::LedgerRecoveryData(ledger_recovery_data) => {
                self.start_recovery_manager(ledger_recovery_data, epoch_state)
                    .await
            }
        }
    }

    async fn process_message(
        &mut self,
        peer_id: AccountAddress,
        consensus_msg: ConsensusMsg,
    ) -> anyhow::Result<()> {
        // we can't verify signatures from a different epoch
        let maybe_unverified_event = self.process_epoch(peer_id, consensus_msg).await?;

        if let Some(unverified_event) = maybe_unverified_event {
            // same epoch -> run well-formedness + signature check
            let verified_event = unverified_event
                .clone()
                .verify(&self.epoch_state().verifier)
                .context("[EpochManager] Verify event")
                .map_err(|err| {
                    error!(
                        SecurityEvent::ConsensusInvalidMessage,
                        remote_peer = peer_id,
                        error = ?err,
                        unverified_event = unverified_event
                    );
                    err
                })?;

            // process the verified event
            self.process_event(peer_id, verified_event).await?;
        }
        Ok(())
    }

    async fn process_epoch(
        &mut self,
        peer_id: AccountAddress,
        msg: ConsensusMsg,
    ) -> anyhow::Result<Option<UnverifiedEvent>> {
        match msg {
            ConsensusMsg::ProposalMsg(_)
            | ConsensusMsg::SyncInfo(_)
            | ConsensusMsg::VoteMsg(_)
            | ConsensusMsg::CommitVoteMsg(_)
            | ConsensusMsg::CommitDecisionMsg(_) => {
                let event: UnverifiedEvent = msg.clone().into();
                if event.epoch() == self.epoch() {
                    return Ok(Some(event));
                } else if !matches!(
                    msg,
                    ConsensusMsg::CommitVoteMsg(_) | ConsensusMsg::CommitDecisionMsg(_)
                ) {
                    monitor!(
                        "process_different_epoch_consensus_msg",
                        self.process_different_epoch(event.epoch(), peer_id).await?
                    );
                }
            }
            ConsensusMsg::EpochChangeProof(proof) => {
                let msg_epoch = proof.epoch()?;
                debug!(
                    LogSchema::new(LogEvent::ReceiveEpochChangeProof)
                        .remote_peer(peer_id)
                        .epoch(self.epoch()),
                    "Proof from epoch {}", msg_epoch,
                );
                if msg_epoch == self.epoch() {
                    monitor!("process_epoch_proof", self.start_new_epoch(*proof).await?);
                } else {
                    bail!(
                        "[EpochManager] Unexpected epoch proof from epoch {}, local epoch {}",
                        msg_epoch,
                        self.epoch()
                    );
                }
            }
            ConsensusMsg::EpochRetrievalRequest(request) => {
                ensure!(
                    request.end_epoch <= self.epoch(),
                    "[EpochManager] Received EpochRetrievalRequest beyond what we have locally"
                );
                monitor!(
                    "process_epoch_retrieval",
                    self.process_epoch_retrieval(*request, peer_id).await?
                );
            }
            _ => {
                bail!("[EpochManager] Unexpected messages: {:?}", msg);
            }
        }
        Ok(None)
    }

    async fn process_commit_vote(&mut self, commit_vote: Box<CommitVote>) -> anyhow::Result<()> {
        if let Some(sender) = &self.commit_msg_tx {
            sender.clone().push(
                CommitPhaseMessageKey::new(
                    commit_vote.author(),
                    commit_vote.ledger_info().commit_info().round(),
                    CommitPhaseMessageType::CommitVoteType,
                ),
                VerifiedEvent::CommitVote(commit_vote),
            )
        } else {
            bail!(
                "Commit Phase not started but received Commit Message (CommitVote/CommitDecision)"
            );
        }
    }

    async fn process_commit_decision(
        &mut self,
        commit_decision: Box<CommitDecision>,
    ) -> anyhow::Result<()> {
        if let Some(sender) = &self.commit_msg_tx {
            sender.clone().push(
                CommitPhaseMessageKey::new(
                    commit_decision.author(),
                    commit_decision.ledger_info().commit_info().round(),
                    CommitPhaseMessageType::CommitDecisionType,
                ),
                VerifiedEvent::CommitDecision(commit_decision),
            )
        } else {
            bail!(
                "Commit Phase not started but received Commit Message (CommitVote/CommitDecision)"
            );
        }
    }

    async fn process_event(
        &mut self,
        peer_id: AccountAddress,
        event: VerifiedEvent,
    ) -> anyhow::Result<()> {
        match self.processor_mut() {
            RoundProcessor::Recovery(p) => {
                let recovery_data = match event {
                    VerifiedEvent::ProposalMsg(proposal) => p.process_proposal_msg(*proposal).await,
                    VerifiedEvent::VoteMsg(vote) => p.process_vote_msg(*vote).await,
                    VerifiedEvent::SyncInfo(sync_info) => p.sync_up(&sync_info, peer_id).await,
                    VerifiedEvent::CommitVote(_) | VerifiedEvent::CommitDecision(_) => {
                        return Err(anyhow!(
                            "Ignoring commit vote/decision message during recovery"
                        )); //ignore
                    }
                }?;
                let epoch_state = p.epoch_state().clone();
                info!("Recovered from SyncProcessor");
                self.start_round_manager(recovery_data, epoch_state).await;
                Ok(())
            }
            RoundProcessor::Normal(p) => match event {
                VerifiedEvent::ProposalMsg(proposal) => {
                    monitor!("process_proposal", p.process_proposal_msg(*proposal).await)
                }
                VerifiedEvent::VoteMsg(vote) => {
                    monitor!("process_vote", p.process_vote_msg(*vote).await)
                }
                VerifiedEvent::SyncInfo(sync_info) => monitor!(
                    "process_sync_info",
                    p.process_sync_info_msg(*sync_info, peer_id).await
                ),
                VerifiedEvent::CommitVote(cv) => self.process_commit_vote(cv).await,
                VerifiedEvent::CommitDecision(cd) => self.process_commit_decision(cd).await,
            },
        }
    }

    fn processor_mut(&mut self) -> &mut RoundProcessor {
        self.processor
            .as_mut()
            .expect("[EpochManager] not started yet")
    }

    async fn process_block_retrieval(
        &mut self,
        request: IncomingBlockRetrievalRequest,
    ) -> anyhow::Result<()> {
        match self.processor_mut() {
            RoundProcessor::Normal(p) => p.process_block_retrieval(request).await,
            _ => bail!("[EpochManager] RoundManager not started yet"),
        }
    }

    async fn process_local_timeout(&mut self, round: u64) -> anyhow::Result<()> {
        match self.processor_mut() {
            RoundProcessor::Normal(p) => p.process_local_timeout(round).await,
            _ => unreachable!("RoundManager not started yet"),
        }
    }

    async fn expect_new_epoch(&mut self) {
        if let Some(payload) = self.reconfig_events.next().await {
            self.start_processor(payload).await;
        } else {
            panic!("Reconfig sender dropped, unable to start new epoch.");
        }
    }

    pub async fn start(
        mut self,
        mut round_timeout_sender_rx: channel::Receiver<Round>,
        mut network_receivers: NetworkReceivers,
    ) {
        // initial start of the processor
        self.expect_new_epoch().await;
        loop {
            let result = monitor!(
                "main_loop",
                select! {
                    msg = network_receivers.consensus_messages.select_next_some() => {
                        let (peer, msg) = (msg.0, msg.1);
                        monitor!("process_message", self.process_message(peer, msg).await.with_context(|| format!("from peer: {}", peer)))
                    }
                    block_retrieval = network_receivers.block_retrieval.select_next_some() => {
                        monitor!("process_block_retrieval", self.process_block_retrieval(block_retrieval).await)
                    }
                    round = round_timeout_sender_rx.select_next_some() => {
                        monitor!("process_local_timeout", self.process_local_timeout(round).await)
                    }
                }
            );
            let round_state = if let RoundProcessor::Normal(p) = self.processor_mut() {
                Some(p.round_state())
            } else {
                None
            };
            match result {
                Ok(_) => trace!(RoundStateLogSchema::new(round_state)),
                Err(e) => {
                    counters::ERROR_COUNT.inc();
                    error!(error = ?e, kind = error_kind(&e), RoundStateLogSchema::new(round_state));
                }
            }

            // Continually capture the time of consensus process to ensure that clock skew between
            // validators is reasonable and to find any unusual (possibly byzantine) clock behavior.
            counters::OP_COUNTERS
                .gauge("time_since_epoch_ms")
                .set(duration_since_epoch().as_millis() as i64);
        }
    }
}
