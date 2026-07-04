// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Operator-state (FlowNodeState) lifecycle chaos.
//!
//! Operator state is single-version (only the latest value matters) and is never read-cached, so the
//! differential is memory vs commit+persistent. Exercises Set, `Delta::Drop` (synchronous
//! `evict_operator_state`), flush, and operator TTL; reads are taken at the current version.

use std::collections::{BTreeMap, BTreeSet};

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{
		catalog::flow::FlowNodeId,
		store::{EntryKind, MultiVersionCommit, MultiVersionGet},
	},
	key::flow_node_state::FlowNodeStateKey,
};
use reifydb_store_multi::{
	MultiVersionScope,
	gc::{
		operator::{
			OperatorScanStats,
			scanner::{drop_expired_operator_keys, scan_operator_expired},
		},
		row::scanner::ScanResult,
	},
	store::StandardMultiStore,
	tier::RangeCursor,
};
use reifydb_value::util::cowvec::CowVec;

use crate::{
	fixtures::{build_row, flush, pump_pending_drops, sync_persistent_store},
	workload::distinct_rows,
};

pub const NODE: FlowNodeId = FlowNodeId(1);

pub fn op_key(id: u64) -> EncodedKey {
	FlowNodeStateKey::encoded(NODE, id.to_be_bytes().to_vec())
}

/// Per-config reference model. Operator state is single-version in intent but cleaned up lazily:
/// superseded buffer versions linger until the drop actor's supersession batches, flushes, or TTL
/// sweeps prune them, and the drop-actor timing is thread-scheduling dependent. Pinned reads below
/// a key's newest version can therefore legitimately answer with the superseded value, the flushed
/// base, or nothing, depending on config and timing.
///
/// The oracle is consequently exact wherever determinism holds and an invariant elsewhere:
/// - exact whenever the key has no version above the read and no in-flight drop: every tier must serve the latest value
///   (or nothing, if the key never existed / was TTL-expired / was fully purged);
/// - otherwise a returned row must be plausible: a value the key actually held (tracked in `history`), at a version <=
///   read, and never from a dropped generation that is visible at this read (the PendingDrops mask contract).
/// TTL expiry is key-level (a key dies once its latest version is <= the cutoff); old versions
/// beneath live keys are pruned lazily and stay merely plausible. Reads are only issued at
/// versions >= the max flush cutoff W (see tests/chaos.rs).
pub struct OpOracle {
	persistent: bool,
	keys: BTreeMap<u64, KeyState>,
}

#[derive(Default)]
struct KeyState {
	latest_set: Option<u64>,
	history: BTreeMap<u64, Vec<u8>>,
	pending_drop: Option<u64>,
	flush_covered: BTreeSet<u64>,
	uncertain: bool,
}

impl OpOracle {
	pub fn new(persistent: bool) -> Self {
		Self {
			persistent,
			keys: BTreeMap::new(),
		}
	}

	pub fn set(&mut self, id: u64, value: Vec<u8>, version: u64) {
		let state = self.keys.entry(id).or_default();
		state.latest_set = Some(version);
		state.uncertain = false;
		state.history.insert(version, value);
	}

	pub fn flush(&mut self, cutoff: u64) {
		if !self.persistent {
			return;
		}
		for state in self.keys.values_mut() {
			let covered: Vec<u64> = state.history.range(..=cutoff).map(|(v, _)| *v).collect();
			state.flush_covered.extend(covered);
		}
	}

	pub fn ttl(&mut self, cutoff: u64) {
		self.keys.retain(|_, state| {
			if state.latest_set.is_some_and(|latest| latest <= cutoff) {
				return false;
			}
			if state.uncertain && {
				state.history.retain(|v, _| *v > cutoff);
				state.flush_covered.retain(|v| *v > cutoff);
				state.history.is_empty() && state.latest_set.is_none()
			} {
				return false;
			}
			state.flush_covered.retain(|v| *v > cutoff);
			true
		});
	}

	pub fn drop_key(&mut self, id: u64, version: u64) {
		let Some(state) = self.keys.get_mut(&id) else {
			return;
		};
		if !self.persistent {
			self.keys.remove(&id);
			return;
		}
		state.latest_set = None;
		state.pending_drop = Some(state.pending_drop.map_or(version, |d| d.max(version)));
	}

	pub fn pump(&mut self) {
		if !self.persistent {
			return;
		}
		self.keys.retain(|_, state| {
			if let Some(dropped) = state.pending_drop.take() {
				state.flush_covered.retain(|v| *v >= dropped);
				if state.latest_set.is_none() {
					let dead: Vec<u64> = state.history.range(..dropped).map(|(v, _)| *v).collect();
					for v in dead {
						state.history.remove(&v);
					}
				}
			}
			state.latest_set.is_some() || !state.history.is_empty()
		});
	}

	/// Models a process restart over the surviving SQLite file: the commit buffer, read cache,
	/// and PendingDrops overlay are all gone. What survives is at most the flushed base per key,
	/// so every flush-covered value stays plausible (including dropped-but-unpurged rows, which
	/// legitimately resurface: the recovery contract), keys never covered become exactly absent,
	/// and covered keys stay uncertain until their next Set re-establishes an exact expectation.
	pub fn restart(&mut self) {
		self.keys.retain(|_, state| {
			let covered = std::mem::take(&mut state.flush_covered);
			state.history.retain(|v, _| covered.contains(v));
			state.flush_covered = covered;
			state.latest_set = None;
			state.pending_drop = None;
			state.uncertain = true;
			!state.history.is_empty()
		});
	}

	pub fn is_exact(&self, id: u64, read: u64) -> bool {
		match self.keys.get(&id) {
			None => true,
			Some(state) => {
				if state.pending_drop.is_some() || state.uncertain {
					return false;
				}
				let newest = state
					.latest_set
					.into_iter()
					.chain(state.history.keys().next_back().copied())
					.max();
				newest.is_none_or(|v| v <= read)
			}
		}
	}

	pub fn exact_at(&self, id: u64, read: u64) -> Option<(Vec<u8>, u64)> {
		let state = self.keys.get(&id)?;
		let latest = state.latest_set?;
		if latest > read {
			return None;
		}
		Some((state.history.get(&latest).expect("latest set is recorded").clone(), latest))
	}

	pub fn row_is_plausible(&self, id: u64, read: u64, value: &[u8], version: u64) -> bool {
		let Some(state) = self.keys.get(&id) else {
			return false;
		};
		if version > read {
			return false;
		}
		if state.history.get(&version).map(|held| held.as_slice()) != Some(value) {
			return false;
		}
		!state.pending_drop.is_some_and(|dropped| version < dropped && dropped <= read)
	}
}

/// Deterministic operator-TTL sweep mirroring `gc/operator/actor.rs`: drop expired operator-state keys
/// from the buffer (invalidate-then-drop), then remove them from the persistent tier and clear the cache.
pub fn ttl_sweep_op(store: &StandardMultiStore, cutoff_version: CommitVersion) {
	if let Some(buffer) = store.commit() {
		loop {
			let mut cursor = RangeCursor::new();
			let mut stats = OperatorScanStats::default();
			let mut removed_any = false;
			loop {
				let (expired, result) =
					scan_operator_expired(buffer, NODE, cutoff_version, 64, &mut cursor).unwrap();
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
		persistent.delete_below_version(EntryKind::Operator(NODE), cutoff_version, None).unwrap();
		store.clear_read();
	}
}

pub fn collect_range_op(
	store: &StandardMultiStore,
	read: u64,
	batch: usize,
	reverse: bool,
) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
	let scope = MultiVersionScope::AsOf {
		read: CommitVersion(read),
	};
	let rows = if reverse {
		store.range_rev(FlowNodeStateKey::node_range(NODE), scope, batch)
			.collect::<Result<Vec<_>, _>>()
			.unwrap()
	} else {
		store.range(FlowNodeStateKey::node_range(NODE), scope, batch).collect::<Result<Vec<_>, _>>().unwrap()
	};
	rows.into_iter().map(|r| (r.key.to_vec(), r.row.to_vec(), r.version.0)).collect()
}

pub fn check_get_op(configs: &[(&str, StandardMultiStore, OpOracle)], id: u64, read: u64, step: u32) {
	let key = op_key(id);
	for (name, store, oracle) in configs {
		let got = store.get(&key, CommitVersion(read)).unwrap().map(|r| (r.row.to_vec(), r.version.0));
		assert_row(name, oracle, id, read, &got, "GET", step);
	}
}

pub fn check_get_many_op(configs: &[(&str, StandardMultiStore, OpOracle)], ids: &[u64], read: u64, step: u32) {
	let keys: Vec<EncodedKey> = ids.iter().map(|id| op_key(*id)).collect();
	for (name, store, oracle) in configs {
		let got = store.get_many(&keys, CommitVersion(read)).unwrap();
		for id in ids {
			let row = got.get(&op_key(*id)).map(|r| (r.row.to_vec(), r.version.0));
			assert_row(name, oracle, *id, read, &row, "GET_MANY", step);
		}
	}
}

pub fn assert_row(
	name: &str,
	oracle: &OpOracle,
	id: u64,
	read: u64,
	got: &Option<(Vec<u8>, u64)>,
	op: &str,
	step: u32,
) {
	if oracle.is_exact(id, read) {
		let expected = oracle.exact_at(id, read);
		assert_eq!(
			*got, expected,
			"OP {op} mismatch: config={name} step={step} id={id} read={read} store={got:?} oracle={expected:?}"
		);
		return;
	}
	if let Some((value, version)) = got {
		assert!(
			oracle.row_is_plausible(id, read, value, *version),
			"OP {op} leak: config={name} step={step} id={id} read={read} store returned version \
			 {version} which the key never legitimately held at this read"
		);
	}
}

pub fn check_range_op(configs: &[(&str, StandardMultiStore, OpOracle)], read: u64, batch: usize, step: u32) {
	// Forward and reverse are validated independently: they are two separate reads, and the
	// drop actor's message-driven supersession cleanup runs concurrently even under sync_only
	// pools, so a superseded version can legitimately vanish between the two scans. Per-key
	// exactness and per-row plausibility are scheduling-invariant; a cross-scan equality
	// assertion is not.
	for (name, store, oracle) in configs {
		for reverse in [false, true] {
			let dir = if reverse {
				"rev"
			} else {
				"fwd"
			};
			let mut rows = collect_range_op(store, read, batch, reverse);
			if reverse {
				rows.reverse();
			}
			let mut sorted = rows.clone();
			sorted.sort_by(|a, b| a.0.cmp(&b.0));
			sorted.dedup_by(|a, b| a.0 == b.0);
			assert_eq!(
				rows, sorted,
				"OP RANGE {dir} unsorted or duplicated keys: config={name} step={step} read={read}"
			);

			let mut by_key: BTreeMap<Vec<u8>, (Vec<u8>, u64)> = BTreeMap::new();
			for (key, value, version) in &rows {
				by_key.insert(key.clone(), (value.clone(), *version));
			}
			for id in oracle.keys.keys().copied().collect::<Vec<u64>>() {
				let got = by_key.remove(op_key(id).as_slice() as &[u8]);
				assert_row(name, oracle, id, read, &got, "RANGE", step);
			}
			assert!(
				by_key.is_empty(),
				"OP RANGE {dir} fabricated keys: config={name} step={step} read={read} extra={:?}",
				by_key.keys().collect::<Vec<_>>()
			);
		}
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
	pub purge_pct: u32,
	pub wipe_pct: u32,
	pub max_deltas: u64,
	pub max_batch: u64,
}

pub fn drive(seed: u64, p: Params) {
	let mut rng = StdRng::seed_from_u64(seed);

	let memory = StandardMultiStore::testing_memory();
	let (persistent, _g1) = sync_persistent_store();
	let mut configs: Vec<(&str, StandardMultiStore, OpOracle)> =
		vec![("memory", memory, OpOracle::new(false)), ("persistent", persistent, OpOracle::new(true))];

	let mut version: u64 = 0;
	// The soundness floor for pinned reads: the max flush cutoff issued so far (see tests/chaos.rs).
	let mut watermark: u64 = 0;

	let steps = rng.random_range(p.min_steps..=p.max_steps);
	for step in 0..steps {
		let roll = rng.random_range(0u32..100);
		let flush_hi = p.commit_pct + p.flush_pct;
		let ttl_hi = flush_hi + p.ttl_pct;
		let drop_hi = ttl_hi + p.drop_pct;
		let purge_hi = drop_hi + p.purge_pct;
		let wipe_hi = purge_hi + p.wipe_pct;

		if version == 0 || roll < p.commit_pct {
			version += 1;
			let count = rng.random_range(1..=p.max_deltas);
			let ids = distinct_rows(&mut rng, count, p.keyspace);
			let mut values: Vec<(u64, Vec<u8>)> = Vec::new();
			for id in ids {
				let payload = format!("op{id}@v{version}").into_bytes();
				let bytes = build_row(&payload).0.to_vec();
				values.push((id, bytes));
			}
			for (_, store, oracle) in &mut configs {
				let deltas: Vec<Delta> = values
					.iter()
					.map(|(id, bytes)| Delta::Set {
						key: op_key(*id),
						row: EncodedRow(CowVec::new(bytes.clone())),
					})
					.collect();
				MultiVersionCommit::commit(store, CowVec::new(deltas), CommitVersion(version)).unwrap();
				for (id, bytes) in &values {
					oracle.set(*id, bytes.clone(), version);
				}
			}
		} else if roll < flush_hi {
			let cutoff = rng.random_range(1..=version);
			for (_, store, oracle) in &mut configs {
				if store.persistent().is_some() {
					flush(store, CommitVersion(cutoff));
					oracle.flush(cutoff);
				}
			}
			watermark = watermark.max(cutoff);
		} else if roll < ttl_hi {
			// Version-anchored operator-state TTL: evict keys whose current version is at or below a
			// random cutoff version.
			let cutoff_version = rng.random_range(1..=version);
			for (_, store, oracle) in &mut configs {
				ttl_sweep_op(store, CommitVersion(cutoff_version));
				oracle.ttl(cutoff_version);
			}
		} else if roll < drop_hi {
			version += 1;
			let count = rng.random_range(1u64..=4);
			let ids = distinct_rows(&mut rng, count, p.keyspace);
			for (_, store, oracle) in &mut configs {
				let deltas: Vec<Delta> = ids
					.iter()
					.map(|id| Delta::Drop {
						key: op_key(*id),
					})
					.collect();
				MultiVersionCommit::commit(store, CowVec::new(deltas), CommitVersion(version)).unwrap();
				for id in &ids {
					oracle.drop_key(*id, version);
				}
			}
		} else if roll < purge_hi {
			// Deterministic stand-in for the drop actor's cadence: settle pending drops at a
			// seed-chosen point relative to drops, recreates, flushes, TTL sweeps, and reads.
			for (_, store, oracle) in &mut configs {
				pump_pending_drops(store);
				oracle.pump();
			}
		} else if roll < wipe_hi {
			// The read cache is reconstructible by contract: wiping it at any moment must have
			// zero semantic effect (the invariant the v3a cache-resident drop mask violated).
			if rng.random_range(0u32..2) == 0 {
				for (_, store, _) in &configs {
					store.clear_read();
				}
			} else {
				let id = rng.random_range(1..=p.keyspace);
				for (_, store, _) in &configs {
					store.invalidate_read_key(&op_key(id));
				}
			}
		} else {
			let read = if rng.random_range(0u32..2) == 0 {
				version
			} else {
				rng.random_range(watermark.max(1)..=version)
			};
			match rng.random_range(0u32..3) {
				0 => {
					let id = rng.random_range(1..=p.keyspace);
					check_get_op(&configs, id, read, step);
				}
				1 => {
					let batch = rng.random_range(1..=p.max_batch) as usize;
					check_range_op(&configs, read, batch, step);
				}
				_ => {
					let count = rng.random_range(1..=8);
					let ids = distinct_rows(&mut rng, count, p.keyspace);
					check_get_many_op(&configs, &ids, read, step);
				}
			}
		}
	}
}
