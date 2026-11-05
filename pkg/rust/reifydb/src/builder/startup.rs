// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Resolution of the restart-only config keys that must be fixed before the stores exist. Both the
//! commit buffer and the persistent tier are consulted: a value written by a previous run lives only
//! in persistent, and reading the buffer alone would silently fall back to the default.

use reifydb_catalog::{bootstrap::read_configs, error::CatalogError};
use reifydb_core::interface::catalog::config::ConfigKey;
use reifydb_runtime::pool::PoolConfig;
use reifydb_store::coverage::plan::DEFAULT_GAP_GUARD;
use reifydb_store_commit::store::CommitStore;
use reifydb_store_multi::tier::{persistent::MultiPersistentTier, point::MultiPointConfig, range::MultiRangeConfig};
use reifydb_store_operator::{range::OperatorRangeConfig, resident::ResidentLimits};
use reifydb_value::{
	byte_size::ByteSize,
	value::{Value, duration::Duration},
};

use crate::Result;

pub(crate) struct StartupConfig {
	pub pools: PoolConfig,
	pub multi_point: Option<MultiPointConfig>,
	pub multi_range: Option<MultiRangeConfig>,
	pub operator_range: Option<OperatorRangeConfig>,
	pub multi_wal_autocheckpoint: u32,
	pub operator_wal_autocheckpoint: u32,
	pub operator_resident: ResidentLimits,
	pub operator_flush_interval: Duration,
}

const STARTUP_KEYS: &[ConfigKey] = &[
	ConfigKey::ThreadsAsync,
	ConfigKey::ThreadsCoordination,
	ConfigKey::ThreadsFlow,
	ConfigKey::ThreadsTask,
	ConfigKey::ThreadsCompute,
	ConfigKey::ThreadsMaintenance,
	ConfigKey::MultiPointBufferTotalBytes,
	ConfigKey::MultiRangeBufferTotalBytes,
	ConfigKey::OperatorRangeTierBytes,
	ConfigKey::MultiPointBufferShards,
	ConfigKey::MultiRangeBufferShards,
	ConfigKey::MultiWalAutocheckpoint,
	ConfigKey::OperatorWalAutocheckpoint,
	ConfigKey::OperatorResidentBytes,
	ConfigKey::OperatorDirtyBytes,
	ConfigKey::OperatorResidentFlushEvery,
	ConfigKey::MultiFlushSliceBytes,
];

pub(crate) fn resolve_startup_configs(
	buffer: &CommitStore,
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

	let shard_count = |key: ConfigKey| -> usize {
		match resolve(key) {
			Value::Uint2(v) => v as usize,
			other => panic!("config key {key} expected Uint2, got {other:?}"),
		}
	};

	let duration = |key: ConfigKey| -> Duration {
		match resolve(key) {
			Value::Duration(v) => v,
			other => panic!("config key {key} expected Duration, got {other:?}"),
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
		maintenance_threads: threads(ConfigKey::ThreadsMaintenance),
		task_threads: threads(ConfigKey::ThreadsTask),
		compute_threads: threads(ConfigKey::ThreadsCompute),
		async_threads: threads(ConfigKey::ThreadsAsync),
	};

	let split = |total_key: ConfigKey, shards_key: ConfigKey| -> Result<Option<(ByteSize, usize)>> {
		let shards = shard_count(shards_key);
		let Some(total) = uint8_opt(total_key) else {
			return Ok(None);
		};
		if total < shards as u64 {
			return Err(CatalogError::ConfigInvalidValue {
				key: total_key.to_string(),
				reason: format!("total bytes {total} must be at least {shards_key} ({shards})"),
			}
			.into());
		}
		Ok(Some((ByteSize::from_bytes(total), shards)))
	};

	let multi_point = split(ConfigKey::MultiPointBufferTotalBytes, ConfigKey::MultiPointBufferShards)?.map(
		|(total_bytes, shards)| MultiPointConfig {
			total_bytes: Some(total_bytes),
			shards,
		},
	);

	let multi_range = split(ConfigKey::MultiRangeBufferTotalBytes, ConfigKey::MultiRangeBufferShards)?.map(
		|(total_bytes, shards)| MultiRangeConfig {
			total_bytes: Some(total_bytes),
			shards,
			gap_guard: DEFAULT_GAP_GUARD,
		},
	);

	let operator_range = uint8_opt(ConfigKey::OperatorRangeTierBytes).map(|tier_bytes| OperatorRangeConfig {
		tier_bytes: Some(ByteSize::from_bytes(tier_bytes)),
		gap_guard: DEFAULT_GAP_GUARD,
	});

	Ok(StartupConfig {
		pools,
		multi_point,
		multi_range,
		operator_range,
		multi_wal_autocheckpoint: uint8(ConfigKey::MultiWalAutocheckpoint) as u32,
		operator_wal_autocheckpoint: uint8(ConfigKey::OperatorWalAutocheckpoint) as u32,
		operator_resident: ResidentLimits {
			budget: ByteSize::from_bytes(uint8(ConfigKey::OperatorResidentBytes)),
			dirty_budget: ByteSize::from_bytes(uint8(ConfigKey::OperatorDirtyBytes)),
			..ResidentLimits::default()
		},
		operator_flush_interval: duration(ConfigKey::OperatorResidentFlushEvery),
	})
}

#[cfg(test)]
mod tests {
	use reifydb_core::default;
	use reifydb_store_commit::store::CommitStore;

	use super::*;

	#[test]
	fn the_flush_interval_resolves_from_the_catalog() {
		// The flush actor schedules this interval once at spawn, so a value that never leaves the
		// catalog would leave every deployment on the compile-time constant and no override would have
		// any effect. With memory pressure the only other trigger, that also decides how long the
		// durable checkpoint may sit still while historical versions wait on it.
		let buffer = CommitStore::new();

		let resolved = resolve_startup_configs(&buffer, None, &[]).unwrap();
		assert_eq!(resolved.operator_flush_interval, default::store::OPERATOR_RESIDENT_FLUSH_EVERY_TESTING);

		let shortened = [(ConfigKey::OperatorResidentFlushEvery, Value::duration_seconds(5))];
		let resolved = resolve_startup_configs(&buffer, None, &shortened).unwrap();
		assert_eq!(resolved.operator_flush_interval, Duration::from_seconds_const(5));
	}

	#[test]
	fn a_total_below_the_shard_count_is_rejected() {
		// A total smaller than the shard count leaves every shard a zero-byte cache that silently caches
		// nothing.
		let buffer = CommitStore::new();

		for (total_key, shards_key) in [
			(ConfigKey::MultiPointBufferTotalBytes, ConfigKey::MultiPointBufferShards),
			(ConfigKey::MultiRangeBufferTotalBytes, ConfigKey::MultiRangeBufferShards),
		] {
			let overrides = [(total_key, Value::Uint8(1)), (shards_key, Value::Uint2(2))];
			let Err(err) = resolve_startup_configs(&buffer, None, &overrides) else {
				panic!("{total_key} below {shards_key} must be rejected");
			};
			let rendered = err.to_string();
			assert_eq!(err.diagnostic().code, "CA_053", "{total_key}");
			assert!(
				rendered.contains(&total_key.to_string()),
				"the error must name {total_key}: {rendered}"
			);
		}
	}

	#[test]
	fn a_total_equal_to_the_shard_count_is_accepted() {
		// The bound is inclusive; rejecting one byte per shard would refuse the smallest valid cache.
		let buffer = CommitStore::new();

		let overrides = [
			(ConfigKey::MultiPointBufferTotalBytes, Value::Uint8(2)),
			(ConfigKey::MultiPointBufferShards, Value::Uint2(2)),
		];
		let resolved = resolve_startup_configs(&buffer, None, &overrides).unwrap();
		assert_eq!(resolved.multi_point.unwrap().total_bytes, Some(ByteSize::from_bytes(2)));
	}

	#[test]
	fn the_totals_and_shard_counts_resolve_from_the_catalog() {
		// A hard-coded shard count would ignore the override.
		let buffer = CommitStore::new();

		let overrides = [
			(ConfigKey::MultiPointBufferTotalBytes, Value::Uint8(ByteSize::from_mib(64).as_bytes())),
			(ConfigKey::MultiPointBufferShards, Value::Uint2(16)),
			(ConfigKey::MultiRangeBufferTotalBytes, Value::Uint8(ByteSize::from_mib(32).as_bytes())),
			(ConfigKey::MultiRangeBufferShards, Value::Uint2(8)),
		];
		let resolved = resolve_startup_configs(&buffer, None, &overrides).unwrap();

		let point = resolved.multi_point.unwrap();
		assert_eq!((point.total_bytes, point.shards), (Some(ByteSize::from_mib(64)), 16));
		let range = resolved.multi_range.unwrap();
		assert_eq!((range.total_bytes, range.shards), (Some(ByteSize::from_mib(32)), 8));
	}
}
