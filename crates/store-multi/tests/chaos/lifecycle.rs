// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Delete / physical-removal / row-TTL lifecycle chaos.
//!
//! Interleaves commits (rows carry test-controlled header timestamps), tombstones, flushes, row-TTL
//! sweeps, and direct physical deletes across the three configs, asserting no ghost / no premature loss /
//! cross-config agreement against an exact oracle.
//!
//! Reads are taken at the CURRENT version only. Row TTL and `delete_expired` remove rows by wall-clock
//! age, which can drop a historical version a `read < current` would need; at the current version a key
//! is present iff its current version survives (no tombstone, not TTL'd, not physically deleted), which
//! the oracle models exactly. (Version-gated historical reads live in the base `workload` harness, where
//! reclamation is version-based.)

use std::collections::HashMap;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::store::{EntryKind, MultiVersionCommit},
	key::row::RowKey,
	row::{Ttl, TtlAnchor, TtlCleanupMode},
};
use reifydb_store_multi::{
	gc::row::{
		ScanStats,
		scanner::{ScanResult, drop_expired_keys, scan_shape_by_created_at, scan_shape_by_updated_at},
	},
	store::StandardMultiStore,
	tier::{HistoricalCursor, RangeCursor, TierStorage},
};
use reifydb_value::util::cowvec::CowVec;

use crate::{
	SHAPE,
	fixtures::{build_row, flush, sync_persistent_store},
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
	pub max_time_step: u64,
	pub max_ttl: u64,
}

/// Deterministic stand-in for the row-TTL actor (`gc/row/actor.rs` ordering). Drains the commit buffer to
/// a fixpoint (each pass drops the expired current version, promoting older ones for the next pass), then
/// removes expired rows from the persistent tier and clears the read cache.
fn ttl_sweep(store: &StandardMultiStore, ttl: &Ttl, now_nanos: u64) {
	if let Some(buffer) = store.commit() {
		loop {
			let mut cursor = RangeCursor::new();
			let mut stats = ScanStats::default();
			let mut removed_any = false;
			loop {
				let (expired, result) = match ttl.anchor {
					TtlAnchor::Created => {
						scan_shape_by_created_at(buffer, SHAPE, ttl, now_nanos, 64, &mut cursor)
							.unwrap()
					}
					TtlAnchor::Updated => {
						scan_shape_by_updated_at(buffer, SHAPE, ttl, now_nanos, 64, &mut cursor)
							.unwrap()
					}
				};
				if !expired.is_empty() {
					removed_any = true;
					for row in &expired {
						store.invalidate_read_key(&row.key);
					}
					drop_expired_keys(buffer, &expired, &mut stats).unwrap();
				}
				if matches!(result, ScanResult::Exhausted) {
					break;
				}
			}
			if !removed_any {
				break;
			}
		}
	}
	if let Some(persistent) = store.persistent() {
		let cutoff = now_nanos.saturating_sub(ttl.duration_nanos);
		let deleted = persistent.delete_expired(EntryKind::Source(SHAPE), ttl.anchor, cutoff, None).unwrap();
		if deleted > 0 {
			store.clear_read();
		}
	}
}

/// Physically remove keys from every tier (the drop path): delete from persistent, drop all versions
/// from the commit buffer, and invalidate the read cache - the delete-then-invalidate order that stops a
/// stale complete page from resurrecting the row.
fn physical_delete(store: &StandardMultiStore, rows: &[u64]) {
	let kind = EntryKind::Source(SHAPE);
	let keys: Vec<EncodedKey> = rows.iter().map(|&r| RowKey::encoded(SHAPE, r)).collect();
	if let Some(persistent) = store.persistent() {
		persistent.delete_keys(kind, &keys).unwrap();
	}
	if let Some(buffer) = store.commit() {
		let mut batch: Vec<(EncodedKey, CommitVersion)> = Vec::new();
		for key in &keys {
			for (v, _) in buffer.get_all_versions(kind, key.as_ref()).unwrap() {
				batch.push((key.clone(), v));
			}
		}
		if !batch.is_empty() {
			buffer.drop(HashMap::from([(kind, batch)])).unwrap();
		}
	}
	for key in &keys {
		store.invalidate_read_key(key);
	}
}

/// Deterministic historical-version GC (the `tests/gc_historical.rs` loop): drop every superseded version
/// below `cutoff` from the commit buffer, keeping the current version. Buffer-only; current-version reads
/// are unaffected, which is exactly what this asserts (GC must not touch the current version).
fn historical_gc(store: &StandardMultiStore, cutoff: CommitVersion) {
	let Some(buffer) = store.commit() else {
		return;
	};
	let kind = EntryKind::Source(SHAPE);
	let mut cursor = HistoricalCursor::new();
	loop {
		let entries = buffer.scan_historical_below(kind, cutoff, &mut cursor, 64).unwrap();
		if entries.is_empty() {
			break;
		}
		buffer.drop(HashMap::from([(kind, entries)])).unwrap();
		if cursor.is_exhausted() {
			break;
		}
	}
}

pub fn drive(seed: u64, p: Params) {
	let mut rng = StdRng::seed_from_u64(seed);
	let mut oracle = Oracle::default();
	// current (created_nanos, updated_nanos) of each present (live-current) key - mirrors what the TTL
	// scanner reads from the row header, so we can predict eviction exactly.
	let mut ts: std::collections::BTreeMap<u64, (u64, u64)> = std::collections::BTreeMap::new();

	let memory = StandardMultiStore::testing_memory();
	let (persistent, _g1) = sync_persistent_store();
	let (tiny, _g2) = sync_persistent_store();
	// Page sizes large enough that a flushed page exceeds WARM_THRESHOLD (128) and becomes range_complete,
	// with few resident pages so a multi-page keyspace also churns eviction - this is what exercises the
	// complete-page serve path that delete/TTL must not let resurrect a row.
	let pages = [1usize, 2, 3][rng.random_range(0u32..3) as usize];
	let page_rows = [256u64, 512][rng.random_range(0u32..2) as usize];
	tiny.configure_read_buffer(pages, page_rows);
	let configs: Vec<(&str, StandardMultiStore)> =
		vec![("memory", memory), ("persistent", persistent), ("tiny_cache", tiny)];

	let mut version: u64 = 0;
	let mut now: u64 = 1000;

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
					ts.remove(&row);
					deltas.push((row, None));
				} else {
					let created = ts.get(&row).map(|(c, _)| *c).unwrap_or(now);
					ts.insert(row, (created, now));
					let payload = format!("r{row}@v{version}").into_bytes();
					deltas.push((row, Some(build_row(&payload, created, now).0.to_vec())));
				}
			}
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
		} else if roll < flush_hi {
			let cutoff = rng.random_range(1..=version);
			for (_, store) in &configs {
				if store.persistent().is_some() {
					flush(store, CommitVersion(cutoff));
				}
			}
		} else if roll < ttl_hi {
			now += rng.random_range(1..=p.max_time_step);
			let dur = rng.random_range(1..=p.max_ttl);
			let anchor = if rng.random_range(0u32..2) == 0 {
				TtlAnchor::Created
			} else {
				TtlAnchor::Updated
			};
			let ttl = Ttl {
				duration_nanos: dur,
				anchor,
				cleanup_mode: TtlCleanupMode::Drop,
			};
			let cutoff = now.saturating_sub(dur);
			let mut expired: Vec<u64> = Vec::new();
			for (row, (created, updated)) in &ts {
				let anchor_ts = match anchor {
					TtlAnchor::Created => *created,
					TtlAnchor::Updated => *updated,
				};
				if anchor_ts <= cutoff {
					expired.push(*row);
				}
			}
			for (_, store) in &configs {
				ttl_sweep(store, &ttl, now);
			}
			for row in expired {
				oracle.remove_key(row);
				ts.remove(&row);
			}
		} else if roll < delete_hi {
			let count = rng.random_range(1u64..=4);
			let rows = distinct_rows(&mut rng, count, p.keyspace);
			for (_, store) in &configs {
				physical_delete(store, &rows);
			}
			for row in rows {
				oracle.remove_key(row);
				ts.remove(&row);
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
