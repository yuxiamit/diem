// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::on_chain_config::OnChainConfig;
use anyhow::{format_err, Result};
use move_core_types::gas_schedule::{CostTable, GasConstants};
use serde::{Deserialize, Serialize};
use std::str;

/// Defines all the on chain configuration data needed by VM.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OnChainConsensusConfig {
    pub decoupled_execution: bool,
    pub back_pressure_limit: u64,
    pub channel_size: u32, // compatible with 32bit machines
}

impl OnChainConsensusConfig {
    pub fn produce_move_argument(&self) -> Vec<u8> {
        let raw_config = OnChainConsensusConfigRaw::new(&self);
        bcs::to_bytes(&raw_config).map_err(|e| {
            format_err!("Failed second round of serialization for OnChainConsensusConfigRaw: {}", e)
        }).unwrap()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OnChainConsensusConfigRaw {
    #[serde(with = "serde_bytes")] // better handling of bytes
    config: Vec<u8>,
}

impl OnChainConsensusConfigRaw {
    fn new(consensus_config: &OnChainConsensusConfig) -> Self {
        // I assume serde_json uses utf-8 encoding by default
        let config_raw =  serde_json::to_vec(consensus_config).map_err(|e|{
            format_err!("Failed converting first round of serialization for OnChainConsensusConfig")
        }).unwrap();
        Self {
            config: config_raw,
        }
    }
    fn deserialize_raw(bytes: &[u8]) -> Result<OnChainConsensusConfig>{
        if bytes.is_empty() {
            // current on-chain consensus configuration should be interpreted as the old one
            Ok(OnChainConsensusConfig {
                decoupled_execution: false,
                back_pressure_limit: 0,
                channel_size: 0,
            })
        }
        // use json for flexibility
        let consensus_config = serde_json::from_str::<OnChainConsensusConfig>(
            str::from_utf8(&bytes).unwrap()
        ).map_err(
            |e| {
                format_err!(
                "Failed second round of deserialization for DiemConsensusConfig: {}",
                e
            )}
        )?;
        Ok(consensus_config)
    }
}

impl OnChainConfig for OnChainConsensusConfig {
    const IDENTIFIER: &'static str = "DiemConsensusConfig";

    fn deserialize_into_config(bytes: &[u8]) -> Result<Self> {
        let config_raw = bcs::from_bytes::<OnChainConsensusConfigRaw>(&bytes).map_err(|e| {
            format_err!(
                "Failed first round of deserialization for DiemConsensusConfig: {}",
                e
            )
        })?;

        OnChainConsensusConfigRaw::deserialize_raw(&config_raw.config)
    }
}
