// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Resolution of the restart-only config keys that must be fixed before the stores exist. Both the
//! commit buffer and the persistent tier are consulted: a value written by a previous run lives only
//! in persistent, and reading the buffer alone would silently fall back to the default.

use reifydb_catalog::{bootstrap::read_configs, error::CatalogError};
use reifydb_core::interface::catalog::config::ConfigKey;
use reifydb_runtime::pool::PoolConfig;
use reifydb_store::coverage::plan::DEFAULT_GAP_GUARD;
use reifydb_store_cdc::{
	config::CdcCommitConfig,
	tier::{commit::CdcCommitBufferTier, read::CdcReadConfig},
};
use reifydb_store_commit::store::CommitStore;
use reifydb_store_multi::tier::{persistent::MultiPersistentTier, point::MultiPointConfig, range::MultiRangeConfig};
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
	pub operator_wal_autocheckpoint: u32,
	pub operator_flush_budget: ByteSize,
	pub cdc_commit: CdcCommitConfig,
	pub cdc_read: Option<CdcReadConfig>,
}

const STARTUP_KEYS: &[ConfigKey] = &[
	ConfigKey::ThreadsAsync,
	ConfigKey::ThreadsCoordination,
	ConfigKey::ThreadsFlow,
	ConfigKey::ThreadsTask,
	ConfigKey::ThreadsCompute,
	ConfigKey::MultiPointBufferShardBytes,
	ConfigKey::MultiRangeBufferShardBytes,
	ConfigKey::OperatorPointTierBytes,
	ConfigKey::OperatorRangeTierBytes,
	ConfigKey::MultiPointBufferShards,
	ConfigKey::MultiRangeBufferShards,
	ConfigKey::MultiWalAutocheckpoint,
	ConfigKey::CdcWalAutocheckpoint,
	ConfigKey::OperatorWalAutocheckpoint,
	ConfigKey::OperatorResidentBudget,
	ConfigKey::MultiFlushBudgetBytes,
	ConfigKey::CdcCommitBufferBytes,
	ConfigKey::CdcBlockCutBytes,
	ConfigKey::CdcReadBufferBytes,
];

pub(crate) fn resolve_startup_configs(
	buffer: &CommitStore,
	persistent: Option<&MultiPersistentTier>,
	overrides: &[(ConfigKey, Value)],
	cdc_memory: bool,
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

	let multi_point = uint8_opt(ConfigKey::MultiPointBufferShardBytes).map(|shard_bytes| MultiPointConfig {
		shard_bytes: Some(ByteSize::from_bytes(shard_bytes)),
		shards: shard_count(ConfigKey::MultiPointBufferShards),
	});

	let multi_range = uint8_opt(ConfigKey::MultiRangeBufferShardBytes).map(|shard_bytes| MultiRangeConfig {
		shard_bytes: Some(ByteSize::from_bytes(shard_bytes)),
		shards: shard_count(ConfigKey::MultiRangeBufferShards),
		gap_guard: DEFAULT_GAP_GUARD,
	});

	let operator_point = uint8_opt(ConfigKey::OperatorPointTierBytes).map(|tier_bytes| OperatorPointConfig {
		tier_bytes: Some(ByteSize::from_bytes(tier_bytes)),
	});

	let operator_range = uint8_opt(ConfigKey::OperatorRangeTierBytes).map(|tier_bytes| OperatorRangeConfig {
		tier_bytes: Some(ByteSize::from_bytes(tier_bytes)),
		gap_guard: DEFAULT_GAP_GUARD,
	});

	let cut_bytes = ByteSize::from_bytes(uint8(ConfigKey::CdcBlockCutBytes));
	let ceiling = ByteSize::from_bytes(uint8(ConfigKey::CdcCommitBufferBytes));
	let cdc_commit = CdcCommitConfig {
		storage: CdcCommitBufferTier::new(cut_bytes, ceiling),
		cut_bytes,
		ceiling,
	};

	let cdc_read = if cdc_memory {
		if overrides.iter().any(|(key, value)| {
			*key == ConfigKey::CdcReadBufferBytes && !matches!(value, Value::None { .. })
		}) {
			return Err(CatalogError::ConfigInvalidValue {
				key: ConfigKey::CdcReadBufferBytes.to_string(),
				reason: "an in-memory CDC persistent tier already holds every block resident, \
					 so a block cache in front of it would duplicate each block in the heap; \
					 set it to none or use a persistent CDC tier"
					.to_string(),
			}
			.into());
		}
		None
	} else {
		uint8_opt(ConfigKey::CdcReadBufferBytes).map(|resident_bytes| CdcReadConfig {
			resident_bytes: Some(ByteSize::from_bytes(resident_bytes)),
			shards: CdcReadConfig::default().shards,
		})
	};

	Ok(StartupConfig {
		pools,
		multi_point,
		multi_range,
		operator_point,
		operator_range,
		multi_wal_autocheckpoint: uint8(ConfigKey::MultiWalAutocheckpoint) as u32,
		cdc_wal_autocheckpoint: uint8(ConfigKey::CdcWalAutocheckpoint) as u32,
		operator_wal_autocheckpoint: uint8(ConfigKey::OperatorWalAutocheckpoint) as u32,
		operator_flush_budget: ByteSize::from_bytes(uint8(ConfigKey::OperatorResidentBudget)),
		cdc_commit,
		cdc_read,
	})
}

#[cfg(test)]
mod tests {
	use reifydb_store_commit::store::CommitStore;
	use reifydb_value::value::value_type::ValueType;

	use super::*;

	#[test]
	fn an_in_memory_cdc_tier_builds_no_read_buffer() {
		// The memory persistent tier keeps every sealed block resident, so a read tier in front of it
		// holds a second Arc<Block> for the same data. cdc_read must be None whatever the catalog
		// resolves, otherwise every flushed block is charged and retained twice in the heap.
		let buffer = CommitStore::new();

		let memory = resolve_startup_configs(&buffer, None, &[], true).unwrap();
		assert!(memory.cdc_read.is_none(), "an in-memory cdc tier must not build a block cache");

		let persistent = resolve_startup_configs(&buffer, None, &[], false).unwrap();
		assert!(
			persistent.cdc_read.is_some(),
			"a persistent cdc tier still needs its block cache to amortise deserialization"
		);
	}

	#[test]
	fn an_explicit_read_buffer_with_an_in_memory_cdc_tier_is_rejected() {
		// Silently dropping the setting would let an operator believe a block cache is configured
		// while none exists, so the contradiction must fail the build rather than be ignored.
		let buffer = CommitStore::new();

		let sized = [(ConfigKey::CdcReadBufferBytes, Value::Uint8(ByteSize::from_mib(64).as_bytes()))];
		let Err(err) = resolve_startup_configs(&buffer, None, &sized, true) else {
			panic!("a sized read buffer must be rejected against an in-memory cdc tier");
		};
		assert_eq!(err.diagnostic().code, "CA_053");

		let disabled = [(
			ConfigKey::CdcReadBufferBytes,
			Value::None {
				inner: ValueType::Uint8,
			},
		)];
		let resolved = resolve_startup_configs(&buffer, None, &disabled, true).unwrap();
		assert!(resolved.cdc_read.is_none(), "an explicit none agrees with the memory tier");
	}

	#[test]
	fn a_read_buffer_override_still_applies_to_a_persistent_cdc_tier() {
		// The reject must be scoped to the memory tier; narrowing it wrongly would break the only
		// supported way to size the block cache.
		let buffer = CommitStore::new();

		let sized = [(ConfigKey::CdcReadBufferBytes, Value::Uint8(ByteSize::from_mib(64).as_bytes()))];
		let resolved = resolve_startup_configs(&buffer, None, &sized, false).unwrap();
		assert_eq!(resolved.cdc_read.unwrap().resident_bytes, Some(ByteSize::from_mib(64)));
	}
}
