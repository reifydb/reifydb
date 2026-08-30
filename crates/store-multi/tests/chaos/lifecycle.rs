// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Delete / physical-removal / row-TTL lifecycle chaos across the three configs, checked against an exact
//! oracle. Reads are taken at the CURRENT version only: row TTL removes by commit version and can drop a
//! historical version a lower read would need, so only the current version is exactly modelled.

use std::collections::HashMap;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::store::{EntryKind, MultiVersionCommit},
	key::row::RowKey,
};
use reifydb_store_commit::HistoricalCursor;
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
	pub ttl_pct: u32,
	pub delete_pct: u32,
	pub histgc_pct: u32,
	pub remove_pct: u32,
	pub max_deltas: u64,
	pub max_batch: u64,
}

fn ttl_sweep(store: &StandardMultiStore, rows: &[u64], cutoff_version: CommitVersion) {
	// Deterministic stand-in for version-anchored TTL eviction, in the same buffer-then-persistent,
	// mutate-then-invalidate order the actor uses.
	let kind = EntryKind::Source(STORAGE);
	let keys: Vec<EncodedKey> = rows.iter().map(|&r| RowKey::encoded(STORAGE, r)).collect();
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

fn physical_delete(store: &StandardMultiStore, rows: &[u64]) {
	// Delete-then-invalidate is the order that stops a stale complete page from resurrecting the row.
	let kind = EntryKind::Source(STORAGE);
	let keys: Vec<EncodedKey> = rows.iter().map(|&r| RowKey::encoded(STORAGE, r)).collect();
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

fn historical_gc(store: &StandardMultiStore, cutoff: CommitVersion) {
	// Buffer-only: superseded versions below the cutoff go, the current version must survive untouched.
	let buffer = store.commit();
	let kind = EntryKind::Source(STORAGE);
	let mut cursor = HistoricalCursor::new();
	loop {
		let entries = buffer.scan_historical_below(kind, cutoff, &mut cursor, 64).unwrap();
		if entries.is_empty() {
			break;
		}
		buffer.compact(HashMap::from([(kind, entries)])).unwrap();
		if cursor.is_exhausted() {
			break;
		}
	}
}

pub fn drive(seed: u64, p: Params) {
	let mut rng = StdRng::seed_from_u64(seed);
	let mut oracle = Oracle::default();
	// last-write commit version of each present (live-current) key - mirrors what the version-anchored TTL
	// scanner reads from the store, so we can predict eviction exactly.
	let mut row_version: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();

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
		let ttl_hi = flush_hi + p.ttl_pct;
		let delete_hi = ttl_hi + p.delete_pct;
		let histgc_hi = delete_hi + p.histgc_pct;

		if version == 0 || roll < p.commit_pct {
			version += 1;
			let count = rng.random_range(1..=p.max_deltas);
			let rows = distinct_rows(&mut rng, count, p.keyspace);
			let mut deltas: Vec<(u64, Option<Vec<u8>>)> = Vec::new();
			for row in rows {
				if rng.random_range(0u32..100) < p.remove_pct {
					row_version.remove(&row);
					deltas.push((row, None));
				} else {
					row_version.insert(row, version);
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
							key: RowKey::encoded(STORAGE, *row),
							bytes: EncodedBytes(CowVec::new(bytes.clone())),
						},
						None => Delta::remove_silent(RowKey::encoded(STORAGE, *row)),
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
		} else if roll < ttl_hi {
			// Evict every key whose last-write version is at or below the cutoff; drawing the cutoff
			// at random fuzzes the full range of eviction depths.
			let cutoff_version = rng.random_range(1..=version);
			let expired: Vec<u64> = row_version
				.iter()
				.filter(|&(_, &v)| v <= cutoff_version)
				.map(|(&row, _)| row)
				.collect();
			for (_, store) in &configs {
				ttl_sweep(store, &expired, CommitVersion(cutoff_version));
			}
			for row in expired {
				oracle.remove_key(row);
				row_version.remove(&row);
			}
		} else if roll < delete_hi {
			let count = rng.random_range(1u64..=4);
			let rows = distinct_rows(&mut rng, count, p.keyspace);
			for (_, store) in &configs {
				physical_delete(store, &rows);
			}
			for row in rows {
				oracle.remove_key(row);
				row_version.remove(&row);
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
