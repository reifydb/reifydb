// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Multi-shape isolation chaos.
//!
//! Drives commit / flush / row-TTL / physical-delete across SEVERAL tables (ShapeIds) at once and asserts
//! that an operation scoped to one shape never touches another: a TTL sweep or delete on shape A must
//! leave shape B byte-for-byte intact, and a full-scan of a shape must return exactly that shape's rows.
//! This guards the shape-scoping of `scan_shape_by_*`, `delete_expired`, `delete_keys`, and range bounds -
//! a scoping bug there would bleed rows across tables, which the per-shape oracle and cross-config checks
//! both catch. Reads are taken at the current version (TTL/delete remove by age, like `lifecycle`).

use std::collections::{BTreeMap, HashMap};

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::{
		catalog::{id::TableId, shape::ShapeId},
		store::{EntryKind, MultiVersionCommit, MultiVersionGet},
	},
	key::row::RowKey,
	row::{Ttl, TtlAnchor, TtlCleanupMode},
};
use reifydb_store_multi::{
	MultiVersionScope,
	gc::row::{
		ScanStats,
		scanner::{ScanResult, drop_expired_keys, scan_shape_by_created_at, scan_shape_by_updated_at},
	},
	store::StandardMultiStore,
	tier::{RangeCursor, TierStorage},
};
use reifydb_value::util::cowvec::CowVec;

use crate::{
	fixtures::{build_row, flush, sync_persistent_store},
	workload::distinct_rows,
};

const SHAPES: [ShapeId; 3] = [ShapeId::Table(TableId(1)), ShapeId::Table(TableId(2)), ShapeId::Table(TableId(3))];

fn shape(idx: usize) -> ShapeId {
	SHAPES[idx]
}

/// Per-(shape, row) current value/version plus the header timestamps that decide TTL eligibility.
#[derive(Default)]
struct MsOracle {
	current: BTreeMap<(usize, u64), (Vec<u8>, u64)>,
	ts: BTreeMap<(usize, u64), (u64, u64)>,
}

impl MsOracle {
	fn set(&mut self, s: usize, row: u64, value: Vec<u8>, version: u64, created: u64, updated: u64) {
		self.current.insert((s, row), (value, version));
		self.ts.insert((s, row), (created, updated));
	}

	fn remove(&mut self, s: usize, row: u64) {
		self.current.remove(&(s, row));
		self.ts.remove(&(s, row));
	}

	fn get(&self, s: usize, row: u64) -> Option<(Vec<u8>, u64)> {
		self.current.get(&(s, row)).cloned()
	}

	fn scan(&self, s: usize, reverse: bool) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
		let mut rows: Vec<(Vec<u8>, Vec<u8>, u64)> = self
			.current
			.iter()
			.filter(|((shape_idx, _), _)| *shape_idx == s)
			.map(|((_, row), (value, version))| {
				(RowKey::encoded(shape(s), *row).to_vec(), value.clone(), *version)
			})
			.collect();
		rows.sort_by(|a, b| a.0.cmp(&b.0));
		if reverse {
			rows.reverse();
		}
		rows
	}
}

/// Shape-scoped row-TTL sweep mirroring `gc/row/actor.rs` (buffer scan->invalidate->drop, then persistent
/// delete_expired -> clear_read on a hit). Scoped to a single shape so the test can assert isolation.
fn ttl_sweep_shape(store: &StandardMultiStore, shape_id: ShapeId, ttl: &Ttl, now_nanos: u64) {
	if let Some(buffer) = store.commit() {
		loop {
			let mut cursor = RangeCursor::new();
			let mut stats = ScanStats::default();
			let mut removed_any = false;
			loop {
				let (expired, result) = match ttl.anchor {
					TtlAnchor::Created => scan_shape_by_created_at(
						buffer,
						shape_id,
						ttl,
						now_nanos,
						64,
						&mut cursor,
					)
					.unwrap(),
					TtlAnchor::Updated => scan_shape_by_updated_at(
						buffer,
						shape_id,
						ttl,
						now_nanos,
						64,
						&mut cursor,
					)
					.unwrap(),
				};
				if !expired.is_empty() {
					removed_any = true;
					for e in &expired {
						store.invalidate_read_key(&e.key);
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
		let deleted = persistent.delete_expired(EntryKind::Source(shape_id), ttl.anchor, cutoff, None).unwrap();
		if !deleted.is_empty() {
			store.clear_read();
		}
	}
}

fn physical_delete_shape(store: &StandardMultiStore, shape_id: ShapeId, rows: &[u64]) {
	let kind = EntryKind::Source(shape_id);
	let keys: Vec<EncodedKey> = rows.iter().map(|&r| RowKey::encoded(shape_id, r)).collect();
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

fn check_get_ms(configs: &[(&str, StandardMultiStore)], oracle: &MsOracle, s: usize, row: u64, read: u64, step: u32) {
	let key = RowKey::encoded(shape(s), row);
	let expected = oracle.get(s, row);
	for (name, store) in configs {
		let got = store.get(&key, CommitVersion(read)).unwrap().map(|r| (r.row.to_vec(), r.version.0));
		assert_eq!(
			got, expected,
			"MS GET mismatch: config={name} step={step} shape={s} row={row} read={read} store={got:?} oracle={expected:?}"
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
		store.range_rev(RowKey::full_scan(shape(s)), scope, batch).collect::<Result<Vec<_>, _>>().unwrap()
	} else {
		store.range(RowKey::full_scan(shape(s)), scope, batch).collect::<Result<Vec<_>, _>>().unwrap()
	};
	rows.into_iter().map(|r| (r.key.to_vec(), r.row.to_vec(), r.version.0)).collect()
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
			"MS RANGE fwd mismatch: config={name} step={step} shape={s} batch={batch} (store {} vs oracle {} rows)",
			fwd.len(),
			expected_fwd.len()
		);
		assert_eq!(
			rev,
			expected_rev,
			"MS RANGE rev mismatch: config={name} step={step} shape={s} batch={batch} (store {} vs oracle {} rows)",
			rev.len(),
			expected_rev.len()
		);
		let mut rev_reversed = rev.clone();
		rev_reversed.reverse();
		assert_eq!(
			fwd, rev_reversed,
			"MS RANGE fwd != rev-reversed: config={name} step={step} shape={s} batch={batch}"
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
	pub max_time_step: u64,
	pub max_ttl: u64,
}

pub fn drive(seed: u64, p: Params) {
	let mut rng = StdRng::seed_from_u64(seed);
	let mut oracle = MsOracle::default();

	let memory = StandardMultiStore::testing_memory();
	let (persistent, _g1) = sync_persistent_store();
	let (tiny, _g2) = sync_persistent_store();
	let page_rows = [256u64, 512][rng.random_range(0u32..2) as usize];
	tiny.configure_read_buffer(2, page_rows);
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
		let s = rng.random_range(0u32..SHAPES.len() as u32) as usize;

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
					let created = oracle.ts.get(&(s, row)).map(|(c, _)| *c).unwrap_or(now);
					let payload = format!("s{s}r{row}@v{version}").into_bytes();
					let bytes = build_row(&payload, created, now).0.to_vec();
					oracle.set(s, row, bytes.clone(), version, created, now);
					values.push((row, Some(bytes)));
				}
			}
			for (_, store) in &configs {
				let deltas: Vec<Delta> = values
					.iter()
					.map(|(row, value)| match value {
						Some(bytes) => Delta::Set {
							key: RowKey::encoded(shape(s), *row),
							row: EncodedRow(CowVec::new(bytes.clone())),
						},
						None => Delta::Remove {
							key: RowKey::encoded(shape(s), *row),
						},
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
			for ((shape_idx, row), (created, updated)) in &oracle.ts {
				if *shape_idx != s {
					continue;
				}
				let anchor_ts = match anchor {
					TtlAnchor::Created => *created,
					TtlAnchor::Updated => *updated,
				};
				if anchor_ts <= cutoff {
					expired.push(*row);
				}
			}
			for (_, store) in &configs {
				ttl_sweep_shape(store, shape(s), &ttl, now);
			}
			for row in expired {
				oracle.remove(s, row);
			}
		} else if roll < delete_hi {
			let count = rng.random_range(1u64..=4);
			let rows = distinct_rows(&mut rng, count, p.keyspace);
			for (_, store) in &configs {
				physical_delete_shape(store, shape(s), &rows);
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

	// Final sweep: after the whole run, every shape must still match the oracle exactly in every config -
	// the strongest isolation check, since any cross-shape bleed accumulated over the run shows up here.
	for (s, _) in SHAPES.iter().enumerate() {
		check_range_ms(&configs, &oracle, s, version, 16, steps);
	}
}
