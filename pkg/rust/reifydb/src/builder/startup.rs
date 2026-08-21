// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Resolution of the restart-only config keys that must be fixed before the stores exist. Both the
//! commit buffer and the persistent tier are consulted: a value written by a previous run lives only
//! in persistent, and reading the buffer alone would silently fall back to the default.

use reifydb_catalog::bootstrap::read_configs;
use reifydb_core::interface::catalog::config::ConfigKey;
use reifydb_runtime::pool::PoolConfig;
use reifydb_store_multi::tier::{
	commit::buffer::MultiCommitBufferTier, persistent::MultiPersistentTier, read::ReadBufferConfig,
};
use reifydb_store_operator::tier::read::OperatorReadBufferConfig;
use reifydb_value::{byte_size::ByteSize, value::Value};

use crate::Result;

pub(crate) struct StartupConfig {
	pub pools: PoolConfig,
	pub read: Option<ReadBufferConfig>,
	pub operator_read: Option<OperatorReadBufferConfig>,
	pub multi_wal_autocheckpoint: u32,
}

const STARTUP_KEYS: &[ConfigKey] = &[
	ConfigKey::ThreadsAsync,
	ConfigKey::ThreadsCoordination,
	ConfigKey::ThreadsFlow,
	ConfigKey::ThreadsTask,
	ConfigKey::ThreadsCompute,
	ConfigKey::MultiReadBufferPages,
	ConfigKey::MultiReadBufferPageSize,
	ConfigKey::MultiReadBufferBytes,
	ConfigKey::OperatorReadBufferBytes,
	ConfigKey::MultiWalAutocheckpoint,
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

	let read = uint8_opt(ConfigKey::MultiReadBufferBytes).map(|resident_bytes| ReadBufferConfig {
		resident_pages: uint8(ConfigKey::MultiReadBufferPages) as usize,
		resident_bytes: Some(ByteSize::from_bytes(resident_bytes)),
		shards: ReadBufferConfig::default().shards,
		bucket_shift: uint8(ConfigKey::MultiReadBufferPageSize).max(1).trailing_zeros() as u8,
	});

	let operator_read =
		uint8_opt(ConfigKey::OperatorReadBufferBytes).map(|resident_bytes| OperatorReadBufferConfig {
			resident_bytes: Some(ByteSize::from_bytes(resident_bytes)),
			range_resident_bytes: OperatorReadBufferConfig::default().range_resident_bytes,
			shards: OperatorReadBufferConfig::default().shards,
		});

	Ok(StartupConfig {
		pools,
		read,
		operator_read,
		multi_wal_autocheckpoint: uint8(ConfigKey::MultiWalAutocheckpoint) as u32,
	})
}
