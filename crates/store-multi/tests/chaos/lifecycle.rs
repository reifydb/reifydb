// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Tombstone, flush and historical-GC lifecycle chaos across the three configs, checked against an exact
//! oracle. Reads are taken at the CURRENT version only: historical GC drops superseded versions a lower
//! read would need, so only the current version is exactly modelled.

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	common::CommitVersion,
	delta::{Delta, SetPre},
	interface::store::{EntryKind, EntryLayout, MultiVersionCommit},
	key::row::RowKey,
};
use reifydb_store_multi::store::StandardMultiStore;
use reifydb_testing_chaos::fuzz::pick;
use reifydb_value::util::cowvec::CowVec;

use crate::{
	STORAGE,
	fixtures::{build_bytes, flush, sync_persistent_store, sync_persistent_store_with_tiers, tiny_tiers},
	oracle::{Oracle, Scope},
	workload::{check_get, check_get_many, check_range, distinct_rows},
};

pub struct Params {
	pub keyspace: u64,
	pub min_steps: u32,
	pub max_steps: u32,
	pub commit_pct: u32,
	pub flush_pct: u32,
	pub histgc_pct: u32,
	pub remove_pct: u32,
	pub max_deltas: u64,
	pub max_batch: u64,
}

fn historical_gc(store: &StandardMultiStore, cutoff: CommitVersion) {
	// Buffer-only: superseded versions below the cutoff go, the current version must survive untouched.
	let buffer = store.commit();
	let kind = EntryKind::Source(STORAGE, EntryLayout::Row);
	loop {
		let (_, remaining) = buffer.gc(kind, cutoff, 64);
		if remaining == 0 {
			break;
		}
	}
}

pub fn drive(seed: u64, p: Params) {
	let mut rng = StdRng::seed_from_u64(seed);
	let mut oracle = Oracle::default();

	let memory = StandardMultiStore::testing_memory();
	let (persistent, _g1) = sync_persistent_store();
	// A span is covered only once fully scanned, so keep the budget small enough that eviction churns.
	let (point, range) = tiny_tiers(pick(&mut rng, &[1u64, 2, 4]));
	let (tiny, _g2) = sync_persistent_store_with_tiers(point, range);
	let configs: Vec<(&str, StandardMultiStore)> =
		vec![("memory", memory), ("persistent", persistent), ("tiny_cache", tiny)];

	let mut version: u64 = 0;

	let steps = rng.random_range(p.min_steps..=p.max_steps);
	for step in 0..steps {
		let roll = rng.random_range(0u32..100);
		let flush_hi = p.commit_pct + p.flush_pct;
		let histgc_hi = flush_hi + p.histgc_pct;

		if version == 0 || roll < p.commit_pct {
			version += 1;
			let count = rng.random_range(1..=p.max_deltas);
			let rows = distinct_rows(&mut rng, count, p.keyspace);
			let mut deltas: Vec<(u64, Option<Vec<u8>>)> = Vec::new();
			for row in rows {
				if rng.random_range(0u32..100) < p.remove_pct {
					deltas.push((row, None));
				} else {
					let payload = format!("r{row}@v{version}").into_bytes();
					deltas.push((row, Some(build_bytes(&payload).0.to_vec())));
				}
			}
			oracle.apply(version, &deltas);
			for (_, store) in &configs {
				let store_deltas: Vec<Delta> = deltas
					.iter()
					.map(|(row, value)| match value {
						Some(bytes) => Delta::Set {
							key: RowKey::new(STORAGE, *row).into(),
							bytes: EncodedBytes(CowVec::new(bytes.clone())),
							pre: SetPre::Unknown,
						},
						None => Delta::remove_silent(RowKey::new(STORAGE, *row).into()),
					})
					.collect();
				MultiVersionCommit::commit(store, CowVec::new(store_deltas), CommitVersion(version))
					.unwrap();
			}
		} else if roll < flush_hi {
			let cutoff = rng.random_range(1..=version);
			for (_, store) in &configs {
				if store.persistent().is_some() {
					flush(store, CommitVersion(cutoff));
				}
			}
		} else if roll < histgc_hi {
			let cutoff = rng.random_range(1..=version);
			for (_, store) in &configs {
				historical_gc(store, CommitVersion(cutoff));
			}
		} else {
			// Reads are at the current version (see module docs).
			match rng.random_range(0u32..4) {
				0 => {
					let row = rng.random_range(1..=p.keyspace);
					check_get(&configs, &oracle, row, version, step);
				}
				1 => {
					let count = rng.random_range(1u64..=8);
					let rows = distinct_rows(&mut rng, count, p.keyspace);
					check_get_many(&configs, &oracle, &rows, version, step);
				}
				_ => {
					let batch = rng.random_range(1..=p.max_batch) as usize;
					check_range(
						&configs,
						&oracle,
						Scope::AsOf {
							read: version,
						},
						batch,
						step,
					);
				}
			}
		}
	}
}
