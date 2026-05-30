// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Operator-state (FlowNodeState) lifecycle chaos.
//!
//! Operator state is single-version (only the latest value matters) and is never read-cached, so the
//! differential is memory vs commit+persistent. Exercises Set, `Delta::Drop` (synchronous
//! `evict_operator_state`), flush, and operator TTL; reads are taken at the current version.

use std::collections::BTreeMap;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::{
		catalog::flow::FlowNodeId,
		store::{EntryKind, MultiVersionCommit, MultiVersionGet},
	},
	key::flow_node_state::FlowNodeStateKey,
	row::{Ttl, TtlAnchor, TtlCleanupMode},
};
use reifydb_store_multi::{
	MultiVersionScope,
	gc::{
		operator::{
			OperatorScanStats,
			scanner::{drop_expired_operator_keys, scan_operator_by_created_at, scan_operator_by_updated_at},
		},
		row::scanner::ScanResult,
	},
	store::StandardMultiStore,
	tier::RangeCursor,
};
use reifydb_value::util::cowvec::CowVec;

use crate::{
	fixtures::{build_row, flush, sync_persistent_store},
	workload::distinct_rows,
};

const NODE: FlowNodeId = FlowNodeId(1);

fn op_key(id: u64) -> EncodedKey {
	FlowNodeStateKey::encoded(NODE, id.to_be_bytes().to_vec())
}

/// Single-version model: the latest (value, version) per key, plus the current header timestamps for TTL.
#[derive(Default)]
struct OpOracle {
	current: BTreeMap<u64, (Vec<u8>, u64)>,
	ts: BTreeMap<u64, (u64, u64)>,
}

impl OpOracle {
	fn set(&mut self, id: u64, value: Vec<u8>, version: u64, created: u64, updated: u64) {
		self.current.insert(id, (value, version));
		self.ts.insert(id, (created, updated));
	}

	fn remove(&mut self, id: u64) {
		self.current.remove(&id);
		self.ts.remove(&id);
	}

	fn get(&self, id: u64) -> Option<(Vec<u8>, u64)> {
		self.current.get(&id).cloned()
	}

	fn scan(&self, reverse: bool) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
		let mut rows: Vec<(Vec<u8>, Vec<u8>, u64)> = self
			.current
			.iter()
			.map(|(id, (value, version))| (op_key(*id).to_vec(), value.clone(), *version))
			.collect();
		rows.sort_by(|a, b| a.0.cmp(&b.0));
		if reverse {
			rows.reverse();
		}
		rows
	}
}

/// Deterministic operator-TTL sweep mirroring `gc/operator/actor.rs`: drop expired operator-state keys
/// from the buffer (invalidate-then-drop), then remove them from the persistent tier and clear the cache.
fn ttl_sweep_op(store: &StandardMultiStore, ttl: &Ttl, now_nanos: u64) {
	if let Some(buffer) = store.commit() {
		loop {
			let mut cursor = RangeCursor::new();
			let mut stats = OperatorScanStats::default();
			let mut removed_any = false;
			loop {
				let (expired, result) = match ttl.anchor {
					TtlAnchor::Created => {
						scan_operator_by_created_at(buffer, NODE, ttl, now_nanos, 64, &mut cursor).unwrap()
					}
					TtlAnchor::Updated => {
						scan_operator_by_updated_at(buffer, NODE, ttl, now_nanos, 64, &mut cursor).unwrap()
					}
				};
				if !expired.is_empty() {
					removed_any = true;
					for e in &expired {
						store.invalidate_read_key(&e.key);
					}
					drop_expired_operator_keys(buffer, &expired, &mut stats).unwrap();
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
		persistent.delete_expired(EntryKind::Operator(NODE), ttl.anchor, cutoff, None).unwrap();
		store.clear_read();
	}
}

fn check_get_op(configs: &[(&str, StandardMultiStore)], oracle: &OpOracle, id: u64, read: u64, step: u32) {
	let key = op_key(id);
	let expected = oracle.get(id);
	for (name, store) in configs {
		let got = store.get(&key, CommitVersion(read)).unwrap().map(|r| (r.row.to_vec(), r.version.0));
		assert_eq!(
			got, expected,
			"OP GET mismatch: config={name} step={step} id={id} read={read} store={got:?} oracle={expected:?}"
		);
	}
}

fn collect_range_op(store: &StandardMultiStore, read: u64, batch: usize, reverse: bool) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
	let scope = MultiVersionScope::AsOf {
		read: CommitVersion(read),
	};
	let rows = if reverse {
		store.range_rev(FlowNodeStateKey::node_range(NODE), scope, batch).collect::<Result<Vec<_>, _>>().unwrap()
	} else {
		store.range(FlowNodeStateKey::node_range(NODE), scope, batch).collect::<Result<Vec<_>, _>>().unwrap()
	};
	rows.into_iter().map(|r| (r.key.to_vec(), r.row.to_vec(), r.version.0)).collect()
}

fn check_range_op(configs: &[(&str, StandardMultiStore)], oracle: &OpOracle, read: u64, batch: usize, step: u32) {
	let expected_fwd = oracle.scan(false);
	let expected_rev = oracle.scan(true);
	for (name, store) in configs {
		let fwd = collect_range_op(store, read, batch, false);
		let rev = collect_range_op(store, read, batch, true);
		assert_eq!(
			fwd, expected_fwd,
			"OP RANGE fwd mismatch: config={name} step={step} batch={batch} (store {} vs oracle {} rows)",
			fwd.len(),
			expected_fwd.len()
		);
		assert_eq!(
			rev, expected_rev,
			"OP RANGE rev mismatch: config={name} step={step} batch={batch} (store {} vs oracle {} rows)",
			rev.len(),
			expected_rev.len()
		);
		let mut rev_reversed = rev.clone();
		rev_reversed.reverse();
		assert_eq!(fwd, rev_reversed, "OP RANGE fwd != rev-reversed: config={name} step={step} batch={batch}");
	}
}

pub struct Params {
	pub keyspace: u64,
	pub min_steps: u32,
	pub max_steps: u32,
	pub commit_pct: u32,
	pub flush_pct: u32,
	pub ttl_pct: u32,
	pub drop_pct: u32,
	pub max_deltas: u64,
	pub max_batch: u64,
	pub max_time_step: u64,
	pub max_ttl: u64,
}

pub fn drive(seed: u64, p: Params) {
	let mut rng = StdRng::seed_from_u64(seed);
	let mut oracle = OpOracle::default();

	let memory = StandardMultiStore::testing_memory();
	let (persistent, _g1) = sync_persistent_store();
	let configs: Vec<(&str, StandardMultiStore)> = vec![("memory", memory), ("persistent", persistent)];

	let mut version: u64 = 0;
	let mut now: u64 = 1000;

	let steps = rng.random_range(p.min_steps..=p.max_steps);
	for step in 0..steps {
		let roll = rng.random_range(0u32..100);
		let flush_hi = p.commit_pct + p.flush_pct;
		let ttl_hi = flush_hi + p.ttl_pct;
		let drop_hi = ttl_hi + p.drop_pct;

		if version == 0 || roll < p.commit_pct {
			version += 1;
			let count = rng.random_range(1..=p.max_deltas);
			let ids = distinct_rows(&mut rng, count, p.keyspace);
			let mut values: Vec<(u64, Vec<u8>)> = Vec::new();
			for id in ids {
				let created = oracle.ts.get(&id).map(|(c, _)| *c).unwrap_or(now);
				let payload = format!("op{id}@v{version}").into_bytes();
				let bytes = build_row(&payload, created, now).0.to_vec();
				oracle.set(id, bytes.clone(), version, created, now);
				values.push((id, bytes));
			}
			for (_, store) in &configs {
				let deltas: Vec<Delta> = values
					.iter()
					.map(|(id, bytes)| Delta::Set {
						key: op_key(*id),
						row: EncodedRow(CowVec::new(bytes.clone())),
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
			for (id, (created, updated)) in &oracle.ts {
				let anchor_ts = match anchor {
					TtlAnchor::Created => *created,
					TtlAnchor::Updated => *updated,
				};
				if anchor_ts <= cutoff {
					expired.push(*id);
				}
			}
			for (_, store) in &configs {
				ttl_sweep_op(store, &ttl, now);
			}
			for id in expired {
				oracle.remove(id);
			}
		} else if roll < drop_hi {
			version += 1;
			let count = rng.random_range(1u64..=4);
			let ids = distinct_rows(&mut rng, count, p.keyspace);
			for (_, store) in &configs {
				let deltas: Vec<Delta> = ids.iter().map(|id| Delta::Drop { key: op_key(*id) }).collect();
				MultiVersionCommit::commit(store, CowVec::new(deltas), CommitVersion(version)).unwrap();
			}
			for id in ids {
				oracle.remove(id);
			}
		} else {
			if rng.random_range(0u32..2) == 0 {
				let id = rng.random_range(1..=p.keyspace);
				check_get_op(&configs, &oracle, id, version, step);
			} else {
				let batch = rng.random_range(1..=p.max_batch) as usize;
				check_range_op(&configs, &oracle, version, batch, step);
			}
		}
	}
}
