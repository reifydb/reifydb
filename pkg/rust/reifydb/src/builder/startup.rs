// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Resolution of the restart-only config keys that must be fixed before the stores exist. Both the
//! commit buffer and the persistent tier are consulted: a value written by a previous run lives only
//! in persistent, and reading the buffer alone would silently fall back to the default.

use reifydb_catalog::bootstrap::read_configs;
use reifydb_core::interface::catalog::config::ConfigKey;
use reifydb_runtime::pool::PoolConfig;
use reifydb_store_cdc::{
	config::CdcCommitConfig,
	tier::{commit::CdcCommitBufferTier, read::CdcReadConfig},
};
use reifydb_store_multi::tier::{
	commit::buffer::MultiCommitBufferTier, persistent::MultiPersistentTier, point::MultiPointConfig,
	range::MultiRangeConfig,
};
use reifydb_store_operator::tier::{point::OperatorPointConfig, range::OperatorRangeConfig};
use reifydb_value::{byte_size::ByteSize, value::Value};

use crate::Result;

pub(crate) struct StartupConfig {
	pub pools: PoolConfig,
	pub multi_point: Option<MultiPointConfig>,
	pub multi_range: Option<MultiRangeConfig>,
	pub operator_point: Option<OperatorPointConfig>,
	pub operator_range: Option<OperatorRangeConfig>,
	pub multi_wal_autocheckpoint: u32,
	pub cdc_wal_autocheckpoint: u32,
	pub cdc_commit: CdcCommitConfig,
	pub cdc_read: Option<CdcReadConfig>,
}

const STARTUP_KEYS: &[ConfigKey] = &[
	ConfigKey::ThreadsAsync,
	ConfigKey::ThreadsCoordination,
	ConfigKey::ThreadsFlow,
	ConfigKey::ThreadsTask,
	ConfigKey::ThreadsCompute,
	ConfigKey::MultiPointBufferBytes,
	ConfigKey::MultiRangeBufferBytes,
	ConfigKey::OperatorPointBufferBytes,
	ConfigKey::OperatorRangeBufferBytes,
	ConfigKey::MultiWalAutocheckpoint,
	ConfigKey::CdcWalAutocheckpoint,
	ConfigKey::CdcCommitBufferBytes,
	ConfigKey::CdcBlockCutBytes,
	ConfigKey::CdcReadBufferBytes,
];

pub(crate) fn resolve_startup_configs(
	buffer: &MultiCommitBufferTier,
	persistent: Option<&MultiPersistentTier>,
	overrides: &[(ConfigKey, Value)],
) -> Result<StartupConfig> {
	let persisted = read_configs(Some(buffer), persistent, STARTUP_KEYS)?;

	let resolve = |key: ConfigKey| -> Value {
		overrides
			.iter()
			.rev()
			.find(|(k, _)| *k == key)
			.and_then(|(_, v)| key.accept(v.clone()).ok())
			.unwrap_or_else(|| persisted[&key].clone())
	};

	let threads = |key: ConfigKey| -> usize {
		match resolve(key) {
			Value::Uint2(v) => v as usize,
			other => panic!("config key {key} expected Uint2, got {other:?}"),
		}
	};

	let uint8 = |key: ConfigKey| -> u64 {
		match resolve(key) {
			Value::Uint8(v) => v,
			other => panic!("config key {key} expected Uint8, got {other:?}"),
		}
	};

	let uint8_opt = |key: ConfigKey| -> Option<u64> {
		match resolve(key) {
			Value::Uint8(v) => Some(v),
			Value::None {
				..
			} => None,
			other => panic!("config key {key} expected Uint8 or none, got {other:?}"),
		}
	};

	let pools = PoolConfig {
		coordination_threads: threads(ConfigKey::ThreadsCoordination),
		flow_threads: threads(ConfigKey::ThreadsFlow),
		maintenance_threads: 1,
		task_threads: threads(ConfigKey::ThreadsTask),
		compute_threads: threads(ConfigKey::ThreadsCompute),
		async_threads: threads(ConfigKey::ThreadsAsync),
	};

	let multi_point = uint8_opt(ConfigKey::MultiPointBufferBytes).map(|resident_bytes| MultiPointConfig {
		resident_bytes: Some(ByteSize::from_bytes(resident_bytes)),
		shards: MultiPointConfig::default().shards,
	});

	let multi_range = uint8_opt(ConfigKey::MultiRangeBufferBytes).map(|resident_bytes| MultiRangeConfig {
		resident_bytes: Some(ByteSize::from_bytes(resident_bytes)),
		shards: MultiRangeConfig::default().shards,
		gap_guard: MultiRangeConfig::default().gap_guard,
	});

	let operator_point = uint8_opt(ConfigKey::OperatorPointBufferBytes).map(|resident_bytes| OperatorPointConfig {
		resident_bytes: Some(ByteSize::from_bytes(resident_bytes)),
		shards: OperatorPointConfig::default().shards,
	});

	let operator_range = uint8_opt(ConfigKey::OperatorRangeBufferBytes).map(|resident_bytes| OperatorRangeConfig {
		resident_bytes: Some(ByteSize::from_bytes(resident_bytes)),
		shards: OperatorRangeConfig::default().shards,
		gap_guard: OperatorRangeConfig::default().gap_guard,
	});

	let cut_bytes = ByteSize::from_bytes(uint8(ConfigKey::CdcBlockCutBytes));
	let ceiling = ByteSize::from_bytes(uint8(ConfigKey::CdcCommitBufferBytes));
	let cdc_commit = CdcCommitConfig {
		storage: CdcCommitBufferTier::new(cut_bytes, ceiling),
		cut_bytes,
		ceiling,
	};

	let cdc_read = uint8_opt(ConfigKey::CdcReadBufferBytes).map(|resident_bytes| CdcReadConfig {
		resident_bytes: Some(ByteSize::from_bytes(resident_bytes)),
		shards: CdcReadConfig::default().shards,
	});

	Ok(StartupConfig {
		pools,
		multi_point,
		multi_range,
		operator_point,
		operator_range,
		multi_wal_autocheckpoint: uint8(ConfigKey::MultiWalAutocheckpoint) as u32,
		cdc_wal_autocheckpoint: uint8(ConfigKey::CdcWalAutocheckpoint) as u32,
		cdc_commit,
		cdc_read,
	})
}
