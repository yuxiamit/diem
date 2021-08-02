// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{common::Round, quorum_cert::QuorumCert, timeout_certificate::TimeoutCertificate};
use anyhow::{ensure, Context};
use diem_types::{
    block_info::BlockInfo, ledger_info::LedgerInfoWithSignatures,
    validator_verifier::ValidatorVerifier,
};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter};

#[derive(Deserialize, Serialize, Clone, Eq, PartialEq)]
/// This struct describes basic synchronization metadata.
pub struct SyncInfo {
    /// Highest quorum certificate known to the peer.
    highest_quorum_cert: QuorumCert,
    /// Highest ledger info known to the peer.
    #[serde(alias = "highest_commit_cert")]
    highest_ordered_cert: Option<QuorumCert>,
    /// Highest commit decision ledger info
    #[serde(default, skip_serializing_if = "Option::is_none")]
    highest_ledger_info: Option<LedgerInfoWithSignatures>,
    /// Optional highest timeout certificate if available.
    highest_timeout_cert: Option<TimeoutCertificate>,
}

// this is required by structured log
impl Debug for SyncInfo {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl Display for SyncInfo {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let htc_repr = match self.highest_timeout_certificate() {
            Some(tc) => format!("{}", tc.round()),
            None => "None".to_string(),
        };
        write!(
            f,
            "SyncInfo[HQC: {}, HOC: {}, HTC: {}, HLI: {}]",
            self.highest_certified_round(),
            self.highest_ordered_round(),
            htc_repr,
            self.highest_ledger_info_round(),
        )
    }
}

impl SyncInfo {
    pub fn new_decoupled(
        highest_quorum_cert: QuorumCert,
        highest_ordered_cert: QuorumCert,
        highest_ledger_info: Option<LedgerInfoWithSignatures>,
        highest_timeout_cert: Option<TimeoutCertificate>,
    ) -> Self {
        // No need to include HTC if it's lower than HQC
        let highest_timeout_cert = highest_timeout_cert
            .filter(|tc| tc.round() > highest_quorum_cert.certified_block().round());

        let highest_ordered_cert =
            Some(highest_ordered_cert).filter(|hoc| hoc != &highest_quorum_cert);
        let highest_ledger_info = highest_ledger_info.filter(|hli| hli.commit_info().round() > 0);

        Self {
            highest_quorum_cert,
            highest_ordered_cert,
            highest_ledger_info,
            highest_timeout_cert,
        }
    }

    pub fn new(
        highest_quorum_cert: QuorumCert,
        highest_ordered_cert: QuorumCert,
        highest_timeout_cert: Option<TimeoutCertificate>,
    ) -> Self {
        Self::new_decoupled(
            highest_quorum_cert,
            highest_ordered_cert,
            None,
            highest_timeout_cert,
        )
    }

    /// Highest quorum certificate
    pub fn highest_quorum_cert(&self) -> &QuorumCert {
        &self.highest_quorum_cert
    }

    /// Highest ordered certificate
    pub fn highest_ordered_cert(&self) -> &QuorumCert {
        self.highest_ordered_cert
            .as_ref()
            .unwrap_or(&self.highest_quorum_cert)
    }

    /// Highest ledger info
    pub fn highest_ledger_info(&self) -> &LedgerInfoWithSignatures {
        self.highest_ledger_info
            .as_ref()
            .unwrap_or_else(|| self.highest_ordered_cert().ledger_info())
    }

    /// Highest timeout certificate if available
    pub fn highest_timeout_certificate(&self) -> Option<&TimeoutCertificate> {
        self.highest_timeout_cert.as_ref()
    }

    pub fn highest_certified_round(&self) -> Round {
        self.highest_quorum_cert.certified_block().round()
    }

    pub fn highest_timeout_round(&self) -> Round {
        self.highest_timeout_certificate()
            .map_or(0, |tc| tc.round())
    }

    pub fn highest_ordered_round(&self) -> Round {
        self.highest_ordered_cert().commit_info().round()
    }

    pub fn highest_ledger_info_round(&self) -> Round {
        self.highest_ledger_info().commit_info().round()
    }

    /// The highest round the SyncInfo carries.
    pub fn highest_round(&self) -> Round {
        std::cmp::max(self.highest_certified_round(), self.highest_timeout_round())
    }

    pub fn verify(&self, validator: &ValidatorVerifier) -> anyhow::Result<()> {
        let epoch = self.highest_quorum_cert.certified_block().epoch();
        ensure!(
            epoch == self.highest_ordered_cert().certified_block().epoch(),
            "Multi epoch in SyncInfo - HOC and HQC"
        );
        if let Some(tc) = &self.highest_timeout_cert {
            ensure!(epoch == tc.epoch(), "Multi epoch in SyncInfo - TC and HQC");
        }

        ensure!(
            self.highest_quorum_cert.certified_block().round()
                >= self.highest_ordered_cert().certified_block().round(),
            "HQC has lower round than HOC"
        );

        ensure!(
            self.highest_ordered_cert().certified_block().round()
                >= self.highest_ledger_info_round(),
            "HOC has lower round than HLI"
        );

        ensure!(
            *self.highest_ordered_cert().commit_info() != BlockInfo::empty(),
            "HOC has no committed block"
        );

        ensure!(
            *self.highest_ledger_info().commit_info() != BlockInfo::empty(),
            "HLI has empty commit info"
        );

        self.highest_quorum_cert
            .verify(validator)
            .and_then(|_| {
                self.highest_ordered_cert
                    .as_ref()
                    .map_or(Ok(()), |cert| cert.verify(validator))
            })
            .and_then(|_| {
                if let Some(tc) = &self.highest_timeout_cert {
                    tc.verify(validator)?;
                }
                Ok(())
            })
            .and_then(|_| {
                if let Some(hli) = self.highest_ledger_info.as_ref() {
                    hli.verify_signatures(validator)?;
                }
                Ok(())
            })
            .context("Fail to verify SyncInfo")?;
        Ok(())
    }

    pub fn epoch(&self) -> u64 {
        self.highest_quorum_cert.certified_block().epoch()
    }

    pub fn has_newer_certificates(&self, other: &SyncInfo) -> bool {
        // it is important that HLI has the first priority:
        // otherwise, consider a node with a qc of round 200, but it has slow execution at HLI 10,
        // and it was set to be sync_only. So this blocks the sync_up path until the local
        // qc is smaller than remote qc. However, during sync_only, the node can still
        // update the local qc through the process_vote path.
        self.highest_ledger_info_round() > other.highest_ledger_info_round()
            || self.highest_certified_round() > other.highest_certified_round()
            || self.highest_timeout_round() > other.highest_timeout_round()
            || self.highest_ordered_round() > other.highest_ordered_round()
    }
}
