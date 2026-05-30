 // SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Seeded operation generator + the per-read differential checks against the oracle.

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::store::{MultiVersionCommit, MultiVersionGet},
	key::row::RowKey,
};
use reifydb_store_multi::store::StandardMultiStore;
use reifydb_value::util::cowvec::CowVec;

use crate::{
	SHAPE,
	fixtures::{flush, sync_persistent_store},
	oracle::{Oracle, Scope},
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
	for (name, store) in configs {
		let found = store.get_many(&keys, CommitVersion(read)).unwrap();
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
	scope: Scope,
	batch: usize,
	reverse: bool,
) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
	let rows = if reverse {
		store.range_rev(RowKey::full_scan(SHAPE), scope.store(), batch).collect::<Result<Vec<_>, _>>().unwrap()
	} else {
		store.range(RowKey::full_scan(SHAPE), scope.store(), batch).collect::<Result<Vec<_>, _>>().unwrap()
	};
	rows.into_iter().map(|r| (r.key.to_vec(), r.row.to_vec(), r.version.0)).collect()
}

pub fn check_range(configs: &[(&str, StandardMultiStore)], oracle: &Oracle, scope: Scope, batch: usize, step: u32) {
	let expected_fwd = oracle.scan(scope, false);
	let expected_rev = oracle.scan(scope, true);
	for (name, store) in configs {
		let fwd = collect_range(store, scope, batch, false);
		let rev = collect_range(store, scope, batch, true);
		assert_eq!(
			fwd,
			expected_fwd,
			"RANGE fwd mismatch: config={name} step={step} scope={scope:?} batch={batch} (store {} vs oracle {} rows)",
			fwd.len(),
			expected_fwd.len()
		);
		assert_eq!(
			rev,
			expected_rev,
			"RANGE rev mismatch: config={name} step={step} scope={scope:?} batch={batch} (store {} vs oracle {} rows)",
			rev.len(),
			expected_rev.len()
		);
		let mut rev_reversed = rev.clone();
		rev_reversed.reverse();
		assert_eq!(
			fwd, rev_reversed,
			"RANGE fwd != rev-reversed: config={name} step={step} scope={scope:?} batch={batch}"
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
			match rng.random_range(0u32..4) {
				0 => {
					let row = rng.random_range(1..=p.keyspace);
					check_get(&configs, &oracle, row, read, step);
				}
				1 => {
					let count = rng.random_range(1u64..=8);
					let rows = distinct_rows(&mut rng, count, p.keyspace);
					check_get_many(&configs, &oracle, &rows, read, step);
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
					check_range(&configs, &oracle, scope, batch, step);
				}
			}
		}
	}
}
