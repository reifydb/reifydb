// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Operator-state (FlowNodeState) lifecycle chaos.
//!
//! Operator state is single-version (only the latest value matters) and is never read-cached, so the
//! differential is memory vs commit+persistent. Exercises Set, silent removal (synchronous
//! `evict_dropped_state`), flush, and operator TTL; reads are taken at the current version.

use std::collections::BTreeMap;

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
	lifecycle::{operator::OperatorScanMetrics, progress::Progress},
};
use reifydb_store_multi::{
	MultiVersionScope,
	store::StandardMultiStore,
	tier::{
		RangeCursor, TierStorage,
		operator::{compact_expired_operator_keys, scan_operator_expired},
	},
};
use reifydb_value::util::cowvec::CowVec;

use crate::{
	fixtures::{build_row, flush, pump_compaction, sync_persistent_store},
	workload::distinct_rows,
};

pub const NODE: FlowNodeId = FlowNodeId(1);

pub fn op_key(id: u64) -> EncodedKey {
	FlowNodeStateKey::encoded(NODE, id.to_be_bytes().to_vec())
}

/// A deliberately naive MVCC store, applied the same operations as the real one.
///
/// This replaces a bespoke model that summarised expected behaviour per key (latest_set,
/// pending_drop, flush_covered). A summary model is a second implementation of the semantics, so it
/// has to be re-derived whenever the first one changes - and when it is not, it drifts silently. That
/// is exactly what happened through the deletion unification: the model kept drop-era erasure
/// behaviour under a removal-era name and stopped agreeing with the store, in a suite that was
/// feature-gated and therefore never run.
///
/// Here there is nothing to re-derive. Every write is one versioned entry, a read resolves the newest
/// entry at or below the read version, and a tombstone resolves to absent. The only judgement the
/// model makes is about what is PINNED versus merely LEGAL, which is genuinely necessary because
/// reclamation is asynchronous - see `exact` and `permits`.
pub struct RefStore {
	has_persistent: bool,
	/// The commit buffer: multi-version, id -> version -> Some(value) for a write, None for a tombstone.
	buffer: BTreeMap<u64, BTreeMap<u64, Option<Vec<u8>>>>,
	/// The persistent tier is single-version-per-key (`key BLOB PRIMARY KEY` with a version-guarded
	/// upsert), so it holds exactly one generation per key rather than a history.
	persistent: BTreeMap<u64, (u64, Option<Vec<u8>>)>,
	/// Every generation ever written, never pruned. The two tiers above are an UPPER BOUND on what
	/// the store holds - compaction collapses superseded generations at a moment the model cannot
	/// observe, which also changes which generation a later flush moves down - so `permits` checks
	/// against this instead of against the tiers.
	ever: BTreeMap<u64, BTreeMap<u64, Option<Vec<u8>>>>,
}

impl RefStore {
	pub fn new(has_persistent: bool) -> Self {
		Self {
			has_persistent,
			buffer: BTreeMap::new(),
			persistent: BTreeMap::new(),
			ever: BTreeMap::new(),
		}
	}

	pub fn set(&mut self, id: u64, value: Vec<u8>, version: u64) {
		self.buffer.entry(id).or_default().insert(version, Some(value.clone()));
		self.ever.entry(id).or_default().insert(version, Some(value));
	}

	/// A silent removal is a tombstone, not an erasure: it hides the key from reads at or above its
	/// own version and leaves every earlier version resolvable.
	pub fn remove_silent(&mut self, id: u64, version: u64) {
		self.buffer.entry(id).or_default().insert(version, None);
		self.ever.entry(id).or_default().insert(version, None);
	}

	/// Mirrors `fixtures::flush`: the newest generation at or below the cutoff moves into the
	/// persistent tier under a version guard, and every collected version leaves the buffer.
	pub fn flush(&mut self, cutoff: u64) {
		if !self.has_persistent {
			return;
		}
		self.buffer.retain(|id, versions| {
			let collected: Vec<(u64, Option<Vec<u8>>)> =
				versions.range(..=cutoff).map(|(v, e)| (*v, e.clone())).collect();
			if let Some((version, entry)) = collected.last() {
				let guard_passes = self.persistent.get(id).is_none_or(|(held, _)| *version >= *held);
				if guard_passes {
					self.persistent.insert(*id, (*version, entry.clone()));
				}
			}
			versions.retain(|v, _| *v > cutoff);
			!versions.is_empty()
		});
	}

	/// The TTL sweep is NOT one rule; the tiers expire on different terms and a merged model cannot
	/// express the difference, which is why this is split.
	///
	/// - Buffer: `scan_operator_expired` reads at `AsOf { read: u64::MAX }`, so it only ever sees each key's
	///   CURRENT generation and expires the key when that generation is at or below the cutoff.
	///   `compact_expired_operator_keys` then takes every version at or beneath it, so the key goes entirely. A key
	///   whose current generation is above the cutoff is untouched, including its older versions.
	/// - Persistent: `delete_below_version` is a blanket `WHERE version <= ?1`.
	///
	/// Both bounds are inclusive of the cutoff, despite the "below" in the persistent name.
	pub fn ttl(&mut self, cutoff: u64) {
		self.buffer.retain(|_, versions| {
			let expired = versions.keys().next_back().is_some_and(|newest| *newest <= cutoff);
			if expired {
				versions.clear();
			}
			!versions.is_empty()
		});
		if self.has_persistent {
			self.persistent.retain(|_, (version, _)| *version > cutoff);
		}
	}

	/// Draining compaction changes nothing the model can pin down: it only collapses superseded
	/// versions of a single-version-semantics key, and it is message-driven, so whether a given
	/// superseded version is still present is a scheduling detail. `exact` already declines to pin
	/// reads below a key's newest generation for exactly that reason.
	pub fn compact(&mut self) {}

	/// A restart over the surviving SQLite file: the commit buffer is gone, the persistent tier is
	/// what comes back. The chaos configs use sync_only pools so the real flush engine never fires on
	/// its own, which means the flush stand-in is the only thing that moved data down and the model
	/// stays exact across the restart rather than degrading to guesswork.
	pub fn restart(&mut self) {
		self.buffer.clear();
	}

	pub fn ids(&self) -> Vec<u64> {
		let mut ids: Vec<u64> = self.buffer.keys().copied().collect();
		ids.extend(self.persistent.keys().copied());
		ids.sort_unstable();
		ids.dedup();
		ids
	}

	/// Every generation either tier holds for this key, buffer shadowing persistent.
	fn merged(&self, id: u64) -> BTreeMap<u64, Option<Vec<u8>>> {
		let mut merged: BTreeMap<u64, Option<Vec<u8>>> = BTreeMap::new();
		if let Some((version, entry)) = self.persistent.get(&id) {
			merged.insert(*version, entry.clone());
		}
		if let Some(versions) = self.buffer.get(&id) {
			for (version, entry) in versions {
				merged.insert(*version, entry.clone());
			}
		}
		merged
	}

	/// The store probes the commit buffer first and only falls through to the persistent tier when
	/// the buffer holds nothing at or below the read.
	fn resolve(&self, id: u64, read: u64) -> Option<(Vec<u8>, u64)> {
		if let Some(versions) = self.buffer.get(&id)
			&& let Some((version, entry)) = versions.range(..=read).next_back()
		{
			return entry.as_ref().map(|value| (value.clone(), *version));
		}
		match self.persistent.get(&id) {
			Some((version, entry)) if *version <= read => {
				entry.as_ref().map(|value| (value.clone(), *version))
			}
			_ => None,
		}
	}

	fn newest(&self, id: u64) -> Option<u64> {
		self.merged(id).keys().next_back().copied()
	}

	/// The answer the store MUST give, or `None` when the answer is not pinned.
	///
	/// Two conditions, and both are necessary:
	///
	/// 1. The read sits at or above every generation of the key. Compaction always keeps the newest, so nothing
	///    reclamation may do can change the answer. Below that, a superseded generation may or may not have been
	///    collected yet.
	/// 2. The answer is served by the BUFFER. The buffer shadows the persistent tier, and its newest generation is
	///    never collapsed, so a buffer-served answer is invariant. A persistent-served answer is not: the model's
	///    persistent tier is an upper bound, because compaction may remove a generation before the flush would have
	///    moved it down, so the flush persists an older one or none at all. Pinning those would make the suite
	///    reject correct behaviour, which is the failure mode that matters - a test that rejects a correct store
	///    gets weakened until it passes, and then it guards nothing.
	///
	/// Reads that fall through to the persistent tier - every read in the restart config, whose buffer
	/// starts empty - are therefore checked by `permits` rather than pinned. That is a real loss of
	/// strength on that path, and it is the honest consequence of compaction having no observable
	/// moment.
	pub fn exact(&self, id: u64, read: u64) -> Option<Option<(Vec<u8>, u64)>> {
		if self.newest(id).is_some_and(|newest| newest > read) {
			return None;
		}
		let buffer_serves =
			self.buffer.get(&id).is_some_and(|versions| versions.range(..=read).next_back().is_some());
		match (buffer_serves, self.buffer.contains_key(&id) || self.persistent.contains_key(&id)) {
			(true, _) => Some(self.resolve(id, read)),
			// Nothing at all recorded for this key: the store must likewise have nothing.
			(false, false) => Some(None),
			(false, true) => None,
		}
	}

	/// Whether `got` is a legal answer at `read`, used only where `exact` declines to pin one.
	///
	/// Absent is always legal: anything still held may already have been reclaimed. A returned row
	/// must be a generation this key genuinely held, at or below the read.
	///
	/// It is tempting to also require that nothing newer at or below the read shadows it, and that is
	/// wrong. Operator state is single-version-semantics, so every write enqueues a compaction that
	/// collapses the key to its newest generation, asynchronously. Once superseded generations start
	/// disappearing, a read below the newest can legitimately fall through to an older one - including
	/// through a tombstone, because a tombstone that is itself superseded is collapsed away like any
	/// other generation, re-exposing the value beneath it. Every generation strictly between `version`
	/// and `read` is by definition superseded and therefore collapsible, so that extra condition is
	/// vacuous in principle and merely wrong in practice.
	///
	/// The teeth of this suite are in `exact`, which pins every read at or above a key's newest
	/// generation - precisely the reads compaction cannot disturb, since it always keeps the newest.
	pub fn permits(&self, id: u64, read: u64, got: &Option<(Vec<u8>, u64)>) -> bool {
		let Some((value, version)) = got else {
			return true;
		};
		if *version > read {
			return false;
		}
		self.ever.get(&id).and_then(|held| held.get(version)).map(|entry| entry.as_deref())
			== Some(Some(value.as_slice()))
	}

	/// Every generation the model believes this key holds, per tier, for failure diagnosis.
	pub fn dump(&self, id: u64) -> String {
		let render = |versions: Vec<(u64, Option<Vec<u8>>)>| -> String {
			versions.iter()
				.map(|(v, entry)| match entry {
					Some(value) => format!("v{v}={}", String::from_utf8_lossy(value)),
					None => format!("v{v}=<tombstone>"),
				})
				.collect::<Vec<String>>()
				.join(", ")
		};
		let buffer = self
			.buffer
			.get(&id)
			.map(|versions| render(versions.iter().map(|(v, e)| (*v, e.clone())).collect()))
			.unwrap_or_default();
		let persistent =
			self.persistent.get(&id).map(|(v, e)| render(vec![(*v, e.clone())])).unwrap_or_default();
		format!("buffer=[{buffer}] persistent=[{persistent}]")
	}
}

/// Deterministic operator-TTL sweep mirroring `gc/operator/actor.rs`: erase expired operator-state
/// versions from the buffer (invalidate-then-compact), then remove them from the persistent tier and
/// clear the cache. Both paths are inclusive of the cutoff.
pub fn ttl_sweep_op(store: &StandardMultiStore, cutoff_version: CommitVersion) {
	{
		let buffer = store.commit();
		loop {
			let mut cursor = RangeCursor::new();
			let mut stats = OperatorScanMetrics::default();
			let mut removed_any = false;
			loop {
				let (expired, result) =
					scan_operator_expired(buffer, NODE, cutoff_version, 64, &mut cursor).unwrap();
				if !expired.is_empty() {
					removed_any = true;
					for e in &expired {
						store.invalidate_read_key(&e.key);
					}
					compact_expired_operator_keys(buffer, &expired, &mut stats).unwrap();
				}
				if matches!(result, Progress::Exhausted) {
					break;
				}
			}
			if !removed_any {
				break;
			}
		}
	}
	if let Some(persistent) = store.persistent() {
		persistent
			.delete_below_version(EntryKind::Operator(NODE), cutoff_version, None, None, usize::MAX)
			.unwrap();
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

pub fn check_get_op(configs: &[(&str, StandardMultiStore, RefStore)], id: u64, read: u64, step: u32) {
	let key = op_key(id);
	for (name, store, oracle) in configs {
		let got = store.get(&key, CommitVersion(read)).unwrap().map(|r| (r.row.to_vec(), r.version.0));
		assert_row(name, store, oracle, id, read, &got, "GET", step);
	}
}

pub fn check_get_many_op(configs: &[(&str, StandardMultiStore, RefStore)], ids: &[u64], read: u64, step: u32) {
	let keys: Vec<EncodedKey> = ids.iter().map(|id| op_key(*id)).collect();
	for (name, store, oracle) in configs {
		let got = store.get_many(&keys, CommitVersion(read)).unwrap();
		for id in ids {
			let row = got.get(&op_key(*id)).map(|r| (r.row.to_vec(), r.version.0));
			assert_row(name, store, oracle, *id, read, &row, "GET_MANY", step);
		}
	}
}

/// Every generation the real store holds for this key, per tier, for failure diagnosis.
fn dump_store(store: &StandardMultiStore, id: u64) -> String {
	let key = op_key(id);
	let table = EntryKind::Operator(NODE);
	let render = |versions: Vec<(CommitVersion, Option<CowVec<u8>>)>| -> String {
		versions.iter()
			.map(|(v, value)| match value {
				Some(bytes) => format!("v{}={}", v.0, String::from_utf8_lossy(bytes)),
				None => format!("v{}=<tombstone>", v.0),
			})
			.collect::<Vec<String>>()
			.join(", ")
	};
	let buffer = store.commit().get_all_versions(table, key.as_ref()).map(render).unwrap_or_default();
	let persistent = match store.persistent() {
		Some(p) => p.get_all_versions(table, key.as_ref()).map(render).unwrap_or_default(),
		None => "<none>".to_string(),
	};
	format!("buffer=[{buffer}] persistent=[{persistent}]")
}

pub fn assert_row(
	name: &str,
	store: &StandardMultiStore,
	model: &RefStore,
	id: u64,
	read: u64,
	got: &Option<(Vec<u8>, u64)>,
	op: &str,
	step: u32,
) {
	if let Some(expected) = model.exact(id, read) {
		assert_eq!(
			*got,
			expected,
			"OP {op} mismatch: config={name} step={step} id={id} read={read}\n  store returned {got:?}\n  model expected {expected:?}\n  model  {}\n  store  {}",
			model.dump(id),
			dump_store(store, id)
		);
		return;
	}
	assert!(
		model.permits(id, read, got),
		"OP {op} leak: config={name} step={step} id={id} read={read}\n  store returned {got:?}, which \
		 the model says this key never held at this read\n  model  {}\n  store  {}",
		model.dump(id),
		dump_store(store, id)
	);
}

pub fn check_range_op(configs: &[(&str, StandardMultiStore, RefStore)], read: u64, batch: usize, step: u32) {
	// Forward and reverse are validated independently: they are two separate reads, and the
	// compaction engine's message-driven supersession cleanup runs concurrently even under sync_only
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
			for id in oracle.ids() {
				let got = by_key.remove(op_key(id).as_slice() as &[u8]);
				assert_row(name, store, oracle, id, read, &got, "RANGE", step);
			}
			assert!(
				by_key.is_empty(),
				"OP RANGE {dir} fabricated keys: config={name} step={step} read={read}\n{}",
				by_key.keys()
					.map(|k| {
						let id = u64::from_be_bytes(
							k[k.len() - 8..].try_into().expect("an 8 byte id suffix"),
						);
						format!(
							"  id={id}\n    model  {}\n    store  {}",
							oracle.dump(id),
							dump_store(store, id)
						)
					})
					.collect::<Vec<String>>()
					.join("\n")
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
	let mut configs: Vec<(&str, StandardMultiStore, RefStore)> =
		vec![("memory", memory, RefStore::new(false)), ("persistent", persistent, RefStore::new(true))];

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
				let deltas: Vec<Delta> =
					ids.iter().map(|id| Delta::remove_silent(op_key(*id))).collect();
				MultiVersionCommit::commit(store, CowVec::new(deltas), CommitVersion(version)).unwrap();
				for id in &ids {
					oracle.remove_silent(*id, version);
				}
			}
		} else if roll < purge_hi {
			// Deterministic stand-in for the compaction engine's cadence: drain compaction at a
			// seed-chosen point relative to drops, recreates, flushes, TTL sweeps, and reads.
			for (_, store, oracle) in &mut configs {
				pump_compaction(store);
				oracle.compact();
			}
		} else if roll < wipe_hi {
			// The read cache is reconstructible by contract: wiping it at any moment must have
			// zero semantic effect.
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
