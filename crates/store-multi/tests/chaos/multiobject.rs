// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Multi-object isolation chaos: commit / flush / row-TTL / physical-delete across several tables at
//! once. An operation scoped to one object must leave the others byte-for-byte intact, which is what
//! guards the object scoping of delete, buffer drops and range bounds. Reads are at the current version.

use std::collections::{BTreeMap, HashMap};

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{
		catalog::{id::TableId, storage::StorageId},
		store::{EntryKind, MultiVersionCommit, MultiVersionGet},
	},
	key::row::RowKey,
};
use reifydb_store_commit::MultiVersionScope;
use reifydb_store_multi::store::StandardMultiStore;
use reifydb_testing_chaos::fuzz::pick;
use reifydb_value::util::cowvec::CowVec;

use crate::{
	fixtures::{build_bytes, flush, sync_persistent_store, sync_persistent_store_with_tiers, tiny_tiers},
	workload::distinct_rows,
};

const STORAGES: [StorageId; 3] =
	[StorageId::Table(TableId(1)), StorageId::Table(TableId(2)), StorageId::Table(TableId(3))];

fn storage(idx: usize) -> StorageId {
	STORAGES[idx]
}

/// Per-(object, row) current value + commit version. The version decides TTL eligibility (a row is
/// expired once its current version is at or below the sweep's cutoff version).
#[derive(Default)]
struct MsOracle {
	current: BTreeMap<(usize, u64), (Vec<u8>, u64)>,
}

impl MsOracle {
	fn set(&mut self, s: usize, row: u64, value: Vec<u8>, version: u64) {
		self.current.insert((s, row), (value, version));
	}

	fn remove(&mut self, s: usize, row: u64) {
		self.current.remove(&(s, row));
	}

	fn get(&self, s: usize, row: u64) -> Option<(Vec<u8>, u64)> {
		self.current.get(&(s, row)).cloned()
	}

	fn scan(&self, s: usize, reverse: bool) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
		let mut rows: Vec<(Vec<u8>, Vec<u8>, u64)> = self
			.current
			.iter()
			.filter(|((storage_idx, _), _)| *storage_idx == s)
			.map(|((_, row), (value, version))| {
				(RowKey::encoded(storage(s), *row).to_vec(), value.clone(), *version)
			})
			.collect();
		rows.sort_by(|a, b| a.0.cmp(&b.0));
		if reverse {
			rows.reverse();
		}
		rows
	}
}

/// Object-scoped version-anchored TTL sweep: buffer drop first, then persistent delete and a read-cache
/// clear on a hit. Scoping it to a single object is what lets the test assert isolation.
fn ttl_sweep_storage(store: &StandardMultiStore, storage_id: StorageId, rows: &[u64], cutoff_version: CommitVersion) {
	let kind = EntryKind::Source(storage_id);
	let keys: Vec<EncodedKey> = rows.iter().map(|&r| RowKey::encoded(storage_id, r)).collect();
	{
		let buffer = store.commit();
		let mut batch: Vec<(EncodedKey, CommitVersion)> = Vec::new();
		for key in &keys {
			for (v, _) in buffer.get_all_versions(kind, key.as_ref()).unwrap() {
				if v <= cutoff_version {
					batch.push((key.clone(), v));
				}
			}
		}
		if !batch.is_empty() {
			buffer.compact(HashMap::from([(kind, batch)])).unwrap();
		}
	}
	for key in &keys {
		store.invalidate_read_key(key);
	}
	if let Some(persistent) = store.persistent() {
		let deleted = persistent.delete_below_version(kind, cutoff_version, None, None, usize::MAX).unwrap().0;
		if !deleted.is_empty() {
			store.clear_read();
		}
	}
}

fn physical_delete_storage(store: &StandardMultiStore, storage_id: StorageId, rows: &[u64]) {
	let kind = EntryKind::Source(storage_id);
	let keys: Vec<EncodedKey> = rows.iter().map(|&r| RowKey::encoded(storage_id, r)).collect();
	if let Some(persistent) = store.persistent() {
		persistent.delete_keys(kind, &keys).unwrap();
	}
	{
		let buffer = store.commit();
		let mut batch: Vec<(EncodedKey, CommitVersion)> = Vec::new();
		for key in &keys {
			for (v, _) in buffer.get_all_versions(kind, key.as_ref()).unwrap() {
				batch.push((key.clone(), v));
			}
		}
		if !batch.is_empty() {
			buffer.compact(HashMap::from([(kind, batch)])).unwrap();
		}
	}
	for key in &keys {
		store.invalidate_read_key(key);
	}
}

fn check_get_ms(configs: &[(&str, StandardMultiStore)], oracle: &MsOracle, s: usize, row: u64, read: u64, step: u32) {
	let key = RowKey::encoded(storage(s), row);
	let expected = oracle.get(s, row);
	for (name, store) in configs {
		let got = store.get(&key, CommitVersion(read)).unwrap().map(|r| (r.bytes.to_vec(), r.version.0));
		assert_eq!(
			got, expected,
			"MS GET mismatch: config={name} step={step} object={s} row={row} read={read} store={got:?} oracle={expected:?}"
		);
	}
}

fn collect_range_ms(
	store: &StandardMultiStore,
	s: usize,
	read: u64,
	batch: usize,
	reverse: bool,
) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
	let scope = MultiVersionScope::AsOf {
		read: CommitVersion(read),
	};
	let rows = if reverse {
		store.range_rev(RowKey::full_scan(storage(s)), scope, batch).collect::<Result<Vec<_>, _>>().unwrap()
	} else {
		store.range(RowKey::full_scan(storage(s)), scope, batch).collect::<Result<Vec<_>, _>>().unwrap()
	};
	rows.into_iter().map(|r| (r.key.to_vec(), r.bytes.to_vec(), r.version.0)).collect()
}

fn check_range_ms(
	configs: &[(&str, StandardMultiStore)],
	oracle: &MsOracle,
	s: usize,
	read: u64,
	batch: usize,
	step: u32,
) {
	let expected_fwd = oracle.scan(s, false);
	let expected_rev = oracle.scan(s, true);
	for (name, store) in configs {
		let fwd = collect_range_ms(store, s, read, batch, false);
		let rev = collect_range_ms(store, s, read, batch, true);
		assert_eq!(
			fwd,
			expected_fwd,
			"MS RANGE fwd mismatch: config={name} step={step} object={s} batch={batch} (store {} vs oracle {} rows)",
			fwd.len(),
			expected_fwd.len()
		);
		assert_eq!(
			rev,
			expected_rev,
			"MS RANGE rev mismatch: config={name} step={step} object={s} batch={batch} (store {} vs oracle {} rows)",
			rev.len(),
			expected_rev.len()
		);
		let mut rev_reversed = rev.clone();
		rev_reversed.reverse();
		assert_eq!(
			fwd, rev_reversed,
			"MS RANGE fwd != rev-reversed: config={name} step={step} object={s} batch={batch}"
		);
	}
}

pub struct Params {
	pub keyspace: u64,
	pub min_steps: u32,
	pub max_steps: u32,
	pub commit_pct: u32,
	pub flush_pct: u32,
	pub ttl_pct: u32,
	pub delete_pct: u32,
	pub remove_pct: u32,
	pub max_deltas: u64,
	pub max_batch: u64,
}

pub fn drive(seed: u64, p: Params) {
	let mut rng = StdRng::seed_from_u64(seed);
	let mut oracle = MsOracle::default();

	let memory = StandardMultiStore::testing_memory();
	let (persistent, _g1) = sync_persistent_store();
	let (point, range) = tiny_tiers(pick(&mut rng, &[2u64, 4]));
	let (tiny, _g2) = sync_persistent_store_with_tiers(point, range);
	let configs: Vec<(&str, StandardMultiStore)> =
		vec![("memory", memory), ("persistent", persistent), ("tiny_cache", tiny)];

	let mut version: u64 = 0;

	let steps = rng.random_range(p.min_steps..=p.max_steps);
	for step in 0..steps {
		let roll = rng.random_range(0u32..100);
		let flush_hi = p.commit_pct + p.flush_pct;
		let ttl_hi = flush_hi + p.ttl_pct;
		let delete_hi = ttl_hi + p.delete_pct;
		let s = rng.random_range(0u32..STORAGES.len() as u32) as usize;

		if version == 0 || roll < p.commit_pct {
			version += 1;
			let count = rng.random_range(1..=p.max_deltas);
			let rows = distinct_rows(&mut rng, count, p.keyspace);
			let mut values: Vec<(u64, Option<Vec<u8>>)> = Vec::new();
			for row in rows {
				if rng.random_range(0u32..100) < p.remove_pct {
					oracle.remove(s, row);
					values.push((row, None));
				} else {
					let payload = format!("s{s}r{row}@v{version}").into_bytes();
					let bytes = build_bytes(&payload).0.to_vec();
					oracle.set(s, row, bytes.clone(), version);
					values.push((row, Some(bytes)));
				}
			}
			for (_, store) in &configs {
				let deltas: Vec<Delta> = values
					.iter()
					.map(|(row, value)| match value {
						Some(bytes) => Delta::Set {
							key: RowKey::encoded(storage(s), *row),
							bytes: EncodedBytes(CowVec::new(bytes.clone())),
						},
						None => Delta::remove_silent(RowKey::encoded(storage(s), *row)),
					})
					.collect();
				MultiVersionCommit::commit(store, CowVec::new(deltas), CommitVersion(version)).unwrap();
			}
		} else if roll < flush_hi {
			let cutoff = rng.random_range(1..=version);
			for (_, store) in &configs {
				if store.persistent().is_some() {
					flush(store, CommitVersion(cutoff));
				}
			}
		} else if roll < ttl_hi {
			// The sweep is scoped to object `s`, so rows of every other object must come through
			// untouched.
			let cutoff_version = rng.random_range(1..=version);
			let expired: Vec<u64> = oracle
				.current
				.iter()
				.filter(|((storage_idx, _), (_, v))| *storage_idx == s && *v <= cutoff_version)
				.map(|((_, row), _)| *row)
				.collect();
			for (_, store) in &configs {
				ttl_sweep_storage(store, storage(s), &expired, CommitVersion(cutoff_version));
			}
			for row in expired {
				oracle.remove(s, row);
			}
		} else if roll < delete_hi {
			let count = rng.random_range(1u64..=4);
			let rows = distinct_rows(&mut rng, count, p.keyspace);
			for (_, store) in &configs {
				physical_delete_storage(store, storage(s), &rows);
			}
			for row in rows {
				oracle.remove(s, row);
			}
		} else if rng.random_range(0u32..2) == 0 {
			let row = rng.random_range(1..=p.keyspace);
			check_get_ms(&configs, &oracle, s, row, version, step);
		} else {
			let batch = rng.random_range(1..=p.max_batch) as usize;
			check_range_ms(&configs, &oracle, s, version, batch, step);
		}
	}

	// Any cross-object bleed that accumulated over the run surfaces in this final per-object comparison.
	for (s, _) in STORAGES.iter().enumerate() {
		check_range_ms(&configs, &oracle, s, version, 16, steps);
	}
}
