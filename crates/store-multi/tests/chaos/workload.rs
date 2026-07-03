// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Seeded operation generator + the per-read differential checks against the oracle.

use std::ops::Bound;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_codec::{
	encoded::row::EncodedRow,
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::store::{MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionGetPrevious},
	key::row::RowKey,
};
use reifydb_store_multi::store::StandardMultiStore;
use reifydb_value::util::cowvec::CowVec;

use crate::{
	SHAPE,
	fixtures::{flush, sync_persistent_store},
	oracle::{Oracle, RangeFilter, Scope},
};

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

pub fn distinct_rows(rng: &mut StdRng, count: u64, keyspace: u64) -> Vec<u64> {
	let mut rows: Vec<u64> = Vec::new();
	let mut attempts = 0u64;
	while (rows.len() as u64) < count && attempts < count * 4 + 8 {
		let row = rng.random_range(1..=keyspace);
		if !rows.contains(&row) {
			rows.push(row);
		}
		attempts += 1;
	}
	rows
}

pub fn check_get(configs: &[(&str, StandardMultiStore)], oracle: &Oracle, row: u64, read: u64, step: u32) {
	let key = RowKey::encoded(SHAPE, row);
	let expected = oracle.resolve(
		row,
		Scope::AsOf {
			read,
		},
	);
	for (name, store) in configs {
		let got = store.get(&key, CommitVersion(read)).unwrap().map(|r| (r.row.to_vec(), r.version.0));
		assert_eq!(
			got, expected,
			"GET mismatch: config={name} step={step} row={row} read={read} store={got:?} oracle={expected:?}"
		);
	}
}

pub fn check_get_many(configs: &[(&str, StandardMultiStore)], oracle: &Oracle, rows: &[u64], read: u64, step: u32) {
	let keys: Vec<EncodedKey> = rows.iter().map(|&row| RowKey::encoded(SHAPE, row)).collect();
	// Distinct rows that resolve to a present value - the exact set get_many must return, regardless of how
	// many duplicates the input `rows` contained.
	let mut distinct_present: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
	for &row in rows {
		if oracle
			.resolve(
				row,
				Scope::AsOf {
					read,
				},
			)
			.is_some()
		{
			distinct_present.insert(row);
		}
	}
	for (name, store) in configs {
		let found = store.get_many(&keys, CommitVersion(read)).unwrap();
		assert_eq!(
			found.len(),
			distinct_present.len(),
			"GET_MANY count mismatch: config={name} step={step} read={read} store returned {} distinct, oracle {} (dups in input must collapse, absent keys must not appear)",
			found.len(),
			distinct_present.len()
		);
		for &row in rows {
			let key = RowKey::encoded(SHAPE, row);
			let expected = oracle.resolve(
				row,
				Scope::AsOf {
					read,
				},
			);
			let got = found.get(&key).map(|r| (r.row.to_vec(), r.version.0));
			assert_eq!(
				got, expected,
				"GET_MANY mismatch: config={name} step={step} row={row} read={read} store={got:?} oracle={expected:?}"
			);
		}
	}
}

fn collect_range(
	store: &StandardMultiStore,
	range: EncodedKeyRange,
	scope: Scope,
	batch: usize,
	reverse: bool,
) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
	let rows = if reverse {
		store.range_rev(range, scope.store(), batch).collect::<Result<Vec<_>, _>>().unwrap()
	} else {
		store.range(range, scope.store(), batch).collect::<Result<Vec<_>, _>>().unwrap()
	};
	rows.into_iter().map(|r| (r.key.to_vec(), r.row.to_vec(), r.version.0)).collect()
}

pub fn check_range(configs: &[(&str, StandardMultiStore)], oracle: &Oracle, scope: Scope, batch: usize, step: u32) {
	check_range_inner(configs, oracle, scope, batch, step, RowKey::full_scan(SHAPE), None);
}

/// A random sub-range over the keyspace: returns the store's `EncodedKeyRange` and the oracle's matching
/// `RangeFilter` (both in encoded-key space, so descending row encoding is handled identically). Endpoints
/// are built from `RowKey::encoded` of two random rows, each side independently Included/Excluded; the
/// "open" choice uses the shape's own start/end key (what `RowKey::full_scan` uses) rather than a raw
/// `Bound::Unbounded`. A shapeless `Unbounded` endpoint classifies to the catch-all Multi table by design
/// (see range_cache.rs `non_source_range_reads_through_with_warm_cache`), so to scan a shape's rows the
/// endpoints must carry the shape - which `shape_start`/`shape_end` do.
pub fn random_sub_range(rng: &mut StdRng, keyspace: u64) -> (EncodedKeyRange, RangeFilter) {
	let a = RowKey::encoded(SHAPE, rng.random_range(1..=keyspace)).to_vec();
	let b = RowKey::encoded(SHAPE, rng.random_range(1..=keyspace)).to_vec();
	let (lo, hi) = if a <= b {
		(a, b)
	} else {
		(b, a)
	};
	let shape_lo = RowKey::shape_start(SHAPE).to_vec();
	let shape_hi = RowKey::shape_end(SHAPE).to_vec();
	let start = match rng.random_range(0u32..3) {
		0 => Bound::Included(lo),
		1 => Bound::Excluded(lo),
		_ => Bound::Included(shape_lo),
	};
	let end = match rng.random_range(0u32..3) {
		0 => Bound::Included(hi),
		1 => Bound::Excluded(hi),
		_ => Bound::Included(shape_hi),
	};
	let store_range = EncodedKeyRange::new(
		match &start {
			Bound::Included(k) => Bound::Included(EncodedKey::new(k.clone())),
			Bound::Excluded(k) => Bound::Excluded(EncodedKey::new(k.clone())),
			Bound::Unbounded => Bound::Unbounded,
		},
		match &end {
			Bound::Included(k) => Bound::Included(EncodedKey::new(k.clone())),
			Bound::Excluded(k) => Bound::Excluded(EncodedKey::new(k.clone())),
			Bound::Unbounded => Bound::Unbounded,
		},
	);
	(
		store_range,
		RangeFilter {
			start,
			end,
		},
	)
}

pub fn check_range_filtered(
	configs: &[(&str, StandardMultiStore)],
	oracle: &Oracle,
	scope: Scope,
	batch: usize,
	step: u32,
	store_range: EncodedKeyRange,
	filter: RangeFilter,
) {
	check_range_inner(configs, oracle, scope, batch, step, store_range, Some(filter));
}

fn check_range_inner(
	configs: &[(&str, StandardMultiStore)],
	oracle: &Oracle,
	scope: Scope,
	batch: usize,
	step: u32,
	store_range: EncodedKeyRange,
	filter: Option<RangeFilter>,
) {
	let expected_fwd = oracle.scan_range(scope, false, filter.as_ref());
	let expected_rev = oracle.scan_range(scope, true, filter.as_ref());
	for (name, store) in configs {
		let fwd = collect_range(store, store_range.clone(), scope, batch, false);
		let rev = collect_range(store, store_range.clone(), scope, batch, true);
		assert_eq!(
			fwd,
			expected_fwd,
			"RANGE fwd mismatch: config={name} step={step} scope={scope:?} batch={batch} filter={filter:?} (store {} vs oracle {} rows)",
			fwd.len(),
			expected_fwd.len()
		);
		assert_eq!(
			rev,
			expected_rev,
			"RANGE rev mismatch: config={name} step={step} scope={scope:?} batch={batch} filter={filter:?} (store {} vs oracle {} rows)",
			rev.len(),
			expected_rev.len()
		);
		let mut rev_reversed = rev.clone();
		rev_reversed.reverse();
		assert_eq!(
			fwd, rev_reversed,
			"RANGE fwd != rev-reversed: config={name} step={step} scope={scope:?} batch={batch} filter={filter:?}"
		);
	}
}

pub fn check_contains(configs: &[(&str, StandardMultiStore)], oracle: &Oracle, row: u64, read: u64, step: u32) {
	let key = RowKey::encoded(SHAPE, row);
	let expected = oracle
		.resolve(
			row,
			Scope::AsOf {
				read,
			},
		)
		.is_some();
	for (name, store) in configs {
		let got = store.contains(&key, CommitVersion(read)).unwrap();
		assert_eq!(
			got, expected,
			"CONTAINS mismatch: config={name} step={step} row={row} read={read} store={got} oracle={expected}"
		);
	}
}

pub fn check_prev(configs: &[(&str, StandardMultiStore)], oracle: &Oracle, row: u64, before: u64, step: u32) {
	let key = RowKey::encoded(SHAPE, row);
	let expected = oracle.prev(row, before);
	for (name, store) in configs {
		let got = store
			.get_previous_version(&key, CommitVersion(before))
			.unwrap()
			.map(|r| (r.row.to_vec(), r.version.0));
		assert_eq!(
			got, expected,
			"PREV mismatch: config={name} step={step} row={row} before={before} store={got:?} oracle={expected:?}"
		);
	}
}

pub fn drive(seed: u64, p: Params) {
	let mut rng = StdRng::seed_from_u64(seed);
	let mut oracle = Oracle::default();

	let memory = StandardMultiStore::testing_memory();
	let (persistent, _g1) = sync_persistent_store();
	let (tiny, _g2) = sync_persistent_store();
	let pages = [2usize, 3, 4, 6][rng.random_range(0u32..4) as usize];
	let page_rows = [4u64, 8, 16, 32][rng.random_range(0u32..4) as usize];
	tiny.configure_read_buffer(pages, page_rows);

	let configs: Vec<(&str, StandardMultiStore)> =
		vec![("memory", memory), ("persistent", persistent), ("tiny_cache", tiny)];

	let mut version: u64 = 0;
	let mut watermark: u64 = 0;

	let steps = rng.random_range(p.min_steps..=p.max_steps);
	for step in 0..steps {
		let roll = rng.random_range(0u32..100);
		if version == 0 || roll < p.commit_pct {
			let count = rng.random_range(1..=p.max_deltas);
			let rows = distinct_rows(&mut rng, count, p.keyspace);
			version += 1;
			let deltas: Vec<(u64, Option<Vec<u8>>)> = rows
				.into_iter()
				.map(|row| {
					if rng.random_range(0u32..100) < p.remove_pct {
						(row, None)
					} else {
						(row, Some(format!("r{row}@v{version}").into_bytes()))
					}
				})
				.collect();
			oracle.apply(version, &deltas);
			for (_, store) in &configs {
				let store_deltas: Vec<Delta> = deltas
					.iter()
					.map(|(row, value)| match value {
						Some(bytes) => Delta::Set {
							key: RowKey::encoded(SHAPE, *row),
							row: EncodedRow(CowVec::new(bytes.clone())),
						},
						None => Delta::Remove {
							key: RowKey::encoded(SHAPE, *row),
						},
					})
					.collect();
				MultiVersionCommit::commit(store, CowVec::new(store_deltas), CommitVersion(version))
					.unwrap();
			}
		} else if roll < p.commit_pct + p.flush_pct {
			let cutoff = rng.random_range(1..=version);
			watermark = watermark.max(cutoff);
			for (_, store) in &configs {
				if store.persistent().is_some() {
					flush(store, CommitVersion(cutoff));
				}
			}
		} else {
			let read = rng.random_range(watermark.max(1)..=version);
			match rng.random_range(0u32..6) {
				0 => {
					let row = rng.random_range(1..=p.keyspace);
					check_get(&configs, &oracle, row, read, step);
				}
				1 => {
					let count = rng.random_range(1u64..=8);
					let mut rows = distinct_rows(&mut rng, count, p.keyspace);
					// Sometimes inject duplicate keys to exercise get_many's dedup.
					if !rows.is_empty() && rng.random_range(0u32..2) == 0 {
						let dup = rows[rng.random_range(0..rows.len() as u32) as usize];
						rows.push(dup);
					}
					check_get_many(&configs, &oracle, &rows, read, step);
				}
				2 => {
					let row = rng.random_range(1..=p.keyspace);
					check_contains(&configs, &oracle, row, read, step);
				}
				3 => {
					// before-1 must stay >= watermark for the oracle's prev() to be sound; allow
					// before = version+1 so "previous of the current version" is covered.
					let before = rng.random_range((watermark + 1)..=(version + 1));
					let row = rng.random_range(1..=p.keyspace);
					check_prev(&configs, &oracle, row, before, step);
				}
				_ => {
					let scope = if rng.random_range(0u32..2) == 0 {
						Scope::AsOf {
							read,
						}
					} else {
						let after = rng.random_range(watermark..=read);
						Scope::Between {
							after,
							read,
						}
					};
					let batch = rng.random_range(1..=p.max_batch) as usize;
					// Half full-scan, half random sub-range (bounded scan path).
					if rng.random_range(0u32..2) == 0 {
						check_range(&configs, &oracle, scope, batch, step);
					} else {
						let (store_range, filter) = random_sub_range(&mut rng, p.keyspace);
						check_range_filtered(
							&configs,
							&oracle,
							scope,
							batch,
							step,
							store_range,
							filter,
						);
					}
				}
			}
		}
	}
}
