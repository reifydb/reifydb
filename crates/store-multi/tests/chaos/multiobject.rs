// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Multi-object isolation chaos: commit / flush across several tables at once. An operation on one object
//! must leave the others byte-for-byte intact, which is what guards the object scoping of range bounds.
//! Reads are at the current version.

use std::{collections::BTreeMap, slice::from_ref};

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	common::CommitVersion,
	delta::{Delta, SetPre},
	interface::{
		catalog::{id::TableId, storage::StorageId},
		store::{MultiVersionCommit, MultiVersionGet},
	},
	key::{any::TaggedKey, row::RowKey},
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

/// Per-(object, row) current value + commit version.
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

fn check_get_ms(configs: &[(&str, StandardMultiStore)], oracle: &MsOracle, s: usize, row: u64, read: u64, step: u32) {
	let key = TaggedKey::from(RowKey::new(storage(s), row));
	let expected = oracle.get(s, row);
	for (name, store) in configs {
		let got = store
			.get(from_ref(&key), CommitVersion(read))
			.unwrap()
			.remove(0)
			.map(|r| (r.bytes.to_vec(), r.version.0));
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
		store.range_rev(RowKey::full_scan(storage(s)).encode(), scope, batch)
			.collect::<Result<Vec<_>, _>>()
			.unwrap()
	} else {
		store.range(RowKey::full_scan(storage(s)).encode(), scope, batch)
			.collect::<Result<Vec<_>, _>>()
			.unwrap()
	};
	rows.into_iter().map(|r| (r.key.encode().to_vec(), r.bytes.to_vec(), r.version.0)).collect()
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
							key: RowKey::new(storage(s), *row).into(),
							bytes: EncodedBytes(CowVec::new(bytes.clone())),
							pre: SetPre::Unknown,
						},
						None => Delta::remove_silent(RowKey::new(storage(s), *row).into()),
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
