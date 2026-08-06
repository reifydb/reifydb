// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, ops::Bound, sync::Arc};

use reifydb_codec::{
	encoded::row::EncodedRow,
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	internal_error,
	metrics::{collect::MetricsCollector, sample::MetricsSample},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_store_operator::{
	OperatorStore,
	snapshot::{DEFAULT_SNAPSHOT_CHUNK_BYTES, LoadedSnapshot, SnapshotStore, SnapshotWrite},
};
use reifydb_store_single::SingleStore;
use reifydb_transaction::dictionary::{DictionaryAllocatorRegistry, store::durable_max_index_id};
use reifydb_value::{Result, value::dictionary::DictionaryId};
use tracing::{error, warn};

#[derive(Clone)]
pub struct FlowSnapshots {
	store: SnapshotStore,
	single: SingleStore,
	dictionaries: DictionaryAllocatorRegistry,
}

impl FlowSnapshots {
	pub fn new(store: SnapshotStore, single: SingleStore, dictionaries: DictionaryAllocatorRegistry) -> Self {
		Self {
			store,
			single,
			dictionaries,
		}
	}

	pub fn write_flow(
		&self,
		operators: &OperatorStore,
		ids: &[OperatorId],
		flow_cursor: CommitVersion,
	) -> Option<CommitVersion> {
		let stateful: Vec<OperatorId> =
			ids.iter().copied().filter(|id| operators.upper(*id) > CommitVersion(0)).collect();
		if stateful.is_empty() {
			return None;
		}

		if !self.single.flush_pending_blocking() {
			warn!("operator snapshot aborted: the single store dictionary flush did not complete");
			return None;
		}
		let dictionary_max = match self.durable_dictionary_maxes() {
			Ok(maxes) => maxes,
			Err(e) => {
				warn!(error = %e, "operator snapshot aborted: durable dictionary state is unreadable");
				return None;
			}
		};

		let mut written: Vec<(OperatorId, u64)> = Vec::with_capacity(stateful.len());
		for id in stateful {
			let upper = operators.upper(id);
			operators.freeze(id);
			match self.write_operator(operators, id, upper, flow_cursor, &dictionary_max) {
				Ok(generation) => written.push((id, generation)),
				Err(e) => {
					warn!(operator = id.0, error = %e, "operator snapshot aborted; previous generation stays valid");
					self.discard_all(&written);
					return None;
				}
			}
		}
		Some(flow_cursor)
	}

	fn discard_all(&self, written: &[(OperatorId, u64)]) {
		for (id, generation) in written {
			if let Err(e) = self.store.discard(*id, *generation) {
				error!(operator = id.0, generation, error = %e, "failed to roll back a partial snapshot pass");
			}
		}
	}

	fn write_operator(
		&self,
		operators: &OperatorStore,
		id: OperatorId,
		upper: CommitVersion,
		flow_cursor: CommitVersion,
		dictionary_max: &[(u64, u128)],
	) -> Result<u64> {
		let mut entries = ArenaScan::new(operators, id);
		self.store.write(
			SnapshotWrite {
				operator: id,
				upper,
				flow_cursor,
				dictionary_max,
				chunk_bytes: DEFAULT_SNAPSHOT_CHUNK_BYTES,
			},
			&mut entries,
		)
	}

	fn durable_dictionary_maxes(&self) -> Result<Vec<(u64, u128)>> {
		let mut maxes = Vec::new();
		for dictionary in self.dictionaries.dictionary_ids() {
			if let Some(max) = durable_max_index_id(&self.single, dictionary)? {
				maxes.push((dictionary.0, max));
			}
		}
		Ok(maxes)
	}

	pub fn load_flow(
		&self,
		operators: &OperatorStore,
		ids: impl Iterator<Item = OperatorId>,
		truncated_before: CommitVersion,
	) -> FlowSnapshotLoad {
		let ids: Vec<OperatorId> = ids.collect();
		let mut expected: Option<Vec<OperatorId>> = None;
		loop {
			let catalog = match self.generation_catalog(&ids) {
				Ok(catalog) => catalog,
				Err(e) => {
					error!(error = %e, "operator snapshot generations unreadable");
					return FlowSnapshotLoad::Inconsistent;
				}
			};
			let present: Vec<OperatorId> = catalog.iter().map(|(id, _)| *id).collect();
			match &expected {
				None if present.is_empty() => return FlowSnapshotLoad::Empty,
				None => expected = Some(present),
				Some(first) if *first != present => {
					error!(
						"an operator lost every retained snapshot generation; its state cannot \
						 be restored at any cursor the rest of the flow agrees on"
					);
					return FlowSnapshotLoad::Inconsistent;
				}
				Some(_) => {}
			}
			let Some((cursor, picks)) = consistent_set(&catalog) else {
				error!(
					"no consistent operator snapshot set across the flow; every retained generation \
					 disagrees on the cursor it was taken at"
				);
				return FlowSnapshotLoad::Inconsistent;
			};
			match self.load_set(operators, &picks, cursor, truncated_before) {
				SetLoad::Restored => return FlowSnapshotLoad::Restored(cursor),
				SetLoad::Retry => continue,
			}
		}
	}

	fn generation_catalog(&self, ids: &[OperatorId]) -> Result<Vec<(OperatorId, Vec<(u64, CommitVersion)>)>> {
		let mut catalog = Vec::new();
		for id in ids {
			let generations = self.store.generation_cursors(*id)?;
			if !generations.is_empty() {
				catalog.push((*id, generations));
			}
		}
		Ok(catalog)
	}

	fn load_set(
		&self,
		operators: &OperatorStore,
		picks: &[(OperatorId, u64)],
		cursor: CommitVersion,
		truncated_before: CommitVersion,
	) -> SetLoad {
		let mut loaded: Vec<OperatorId> = Vec::with_capacity(picks.len());
		for (id, generation) in picks {
			match self.validate(*id, *generation, cursor, truncated_before) {
				Ok(snapshot) => {
					for (key, row) in snapshot.entries {
						operators.set(*id, key, row);
					}
					operators.set_upper(*id, snapshot.manifest.upper);
					loaded.push(*id);
				}
				Err(e) => {
					error!(
						operator = id.0,
						generation,
						error = %e,
						"discarding invalid operator snapshot generation"
					);
					if let Err(e) = self.store.discard(*id, *generation) {
						error!(operator = id.0, generation, error = %e, "failed to discard invalid snapshot generation");
					}
					for id in loaded {
						operators.drop_arena(id);
					}
					return SetLoad::Retry;
				}
			}
		}
		SetLoad::Restored
	}

	fn validate(
		&self,
		id: OperatorId,
		generation: u64,
		cursor: CommitVersion,
		truncated_before: CommitVersion,
	) -> Result<LoadedSnapshot> {
		let loaded = self.store.load(id, generation)?;
		for (dictionary, recorded) in &loaded.manifest.dictionary_max {
			let durable = durable_max_index_id(&self.single, DictionaryId(*dictionary))?.unwrap_or(0);
			if *recorded > durable {
				return Err(internal_error!(
					"snapshot references dictionary {} up to id {} but only {} is durable; interned values were lost",
					dictionary,
					recorded,
					durable
				));
			}
		}
		if loaded.manifest.flow_cursor != cursor {
			return Err(internal_error!(
				"snapshot cursor {} does not match the set cursor {} it was selected for",
				loaded.manifest.flow_cursor.0,
				cursor.0
			));
		}
		if loaded.manifest.flow_cursor < truncated_before {
			return Err(internal_error!(
				"snapshot cursor {} predates the cdc truncation floor {}; replay from it is impossible",
				loaded.manifest.flow_cursor.0,
				truncated_before.0
			));
		}
		Ok(loaded)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowSnapshotLoad {
	Empty,
	Restored(CommitVersion),
	Inconsistent,
}

enum SetLoad {
	Restored,
	Retry,
}

fn consistent_set(
	catalog: &[(OperatorId, Vec<(u64, CommitVersion)>)],
) -> Option<(CommitVersion, Vec<(OperatorId, u64)>)> {
	let mut cursors: Vec<CommitVersion> =
		catalog.iter().flat_map(|(_, generations)| generations.iter().map(|(_, cursor)| *cursor)).collect();
	cursors.sort_unstable();
	cursors.dedup();
	for cursor in cursors.into_iter().rev() {
		let picks: Option<Vec<(OperatorId, u64)>> = catalog
			.iter()
			.map(|(id, generations)| {
				generations
					.iter()
					.find(|(_, candidate)| *candidate == cursor)
					.map(|(generation, _)| (*id, *generation))
			})
			.collect();
		if let Some(picks) = picks {
			return Some((cursor, picks));
		}
	}
	None
}

struct ArenaScan<'a> {
	operators: &'a OperatorStore,
	id: OperatorId,
	pending: std::vec::IntoIter<(EncodedKey, EncodedRow)>,
	resume: Option<Bound<EncodedKey>>,
}

const ARENA_SCAN_BATCH: u64 = 1024;

impl<'a> ArenaScan<'a> {
	fn new(operators: &'a OperatorStore, id: OperatorId) -> Self {
		Self {
			operators,
			id,
			pending: Vec::new().into_iter(),
			resume: Some(Bound::Unbounded),
		}
	}
}

impl Iterator for ArenaScan<'_> {
	type Item = Result<(EncodedKey, EncodedRow)>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			if let Some(entry) = self.pending.next() {
				return Some(Ok(entry));
			}
			let start = self.resume.take()?;
			let batch = self.operators.range_batch(
				self.id,
				EncodedKeyRange::new(start, Bound::Unbounded),
				ARENA_SCAN_BATCH,
			);
			if batch.has_more {
				let last = batch.items.last().expect("a batch with more entries cannot be empty");
				self.resume = Some(Bound::Excluded(last.0.clone()));
			}
			if batch.items.is_empty() {
				return None;
			}
			self.pending = batch.items.into_iter();
		}
	}
}

#[derive(Clone, Default)]
pub struct SnapshotPinTracker {
	inner: Arc<Mutex<BTreeMap<FlowId, PinState>>>,
}

#[derive(Clone, Copy)]
struct PinState {
	checkpoint: CommitVersion,
	pin: Option<CommitVersion>,
}

impl Default for PinState {
	fn default() -> Self {
		Self {
			checkpoint: CommitVersion(0),
			pin: None,
		}
	}
}

impl SnapshotPinTracker {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn record_checkpoint(&self, flow: FlowId, version: CommitVersion) {
		self.inner.lock().entry(flow).or_default().checkpoint = version;
	}

	pub fn record_pin(&self, flow: FlowId, version: CommitVersion) {
		self.inner.lock().entry(flow).or_default().pin = Some(version);
	}

	pub fn forget(&self, flow: FlowId) {
		self.inner.lock().remove(&flow);
	}

	pub fn lags(&self) -> Vec<(FlowId, u64)> {
		self.inner
			.lock()
			.iter()
			.filter_map(|(flow, state)| {
				state.pin.map(|pin| (*flow, state.checkpoint.0.saturating_sub(pin.0)))
			})
			.collect()
	}
}

impl MetricsCollector for SnapshotPinTracker {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		for (flow, lag) in self.lags() {
			out.push(MetricsSample::count(format!("flow::{}", flow.0), "snapshot_pin_lag", lag));
		}
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use reifydb_codec::key::encoded::EncodedKeyRange;
	use reifydb_core::{delta::Delta, interface::store::SingleVersionCommit};
	use reifydb_runtime::shutdown::Shutdown;
	use reifydb_sqlite::SqliteConfig;
	use reifydb_store_operator::snapshot::SnapshotWrite;
	use reifydb_value::util::cowvec::CowVec;

	use super::*;

	const OP_A: OperatorId = OperatorId(1);
	const OP_B: OperatorId = OperatorId(2);

	fn snapshot_fixture() -> (FlowSnapshots, SnapshotStore, reifydb_sqlite::SqliteTempPathGuard) {
		let (config, guard) = SqliteConfig::test();
		let store = SnapshotStore::sqlite(config);
		let snapshots = FlowSnapshots::new(
			store.clone(),
			SingleStore::testing_memory(),
			DictionaryAllocatorRegistry::default(),
		);
		(snapshots, store, guard)
	}

	fn key(bytes: &[u8]) -> EncodedKey {
		EncodedKey::new(bytes)
	}

	fn row(bytes: &[u8]) -> EncodedRow {
		EncodedRow(CowVec::new(bytes.to_vec()))
	}

	fn scan(operators: &OperatorStore, id: OperatorId) -> Vec<(EncodedKey, EncodedRow)> {
		operators.range_batch(id, EncodedKeyRange::new(Bound::Unbounded, Bound::Unbounded), 1024).items
	}

	#[test]
	fn write_then_load_restores_arena_content_and_upper_and_pins_at_the_flow_cursor() {
		// The full round trip. The pin is the FLOW CURSOR, not any arena upper: a flow commits
		// after it consumes, so every arena upper sits ABOVE the cursor, and pinning CDC at an
		// upper would permit truncating exactly the records catch-up still needs. Per-operator
		// uppers are still restored, because they are what the next snapshot pass stamps.
		// Falsified by returning min(upper) as the pin, by stamping the manifests with upper
		// instead of the cursor, or by skipping set_upper at load.
		let (snapshots, _store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();
		source.set(OP_A, key(b"a1"), row(b"va1"));
		source.set(OP_A, key(b"a2"), row(b"va2"));
		source.set_upper(OP_A, CommitVersion(9));
		source.set(OP_B, key(b"b1"), row(b"vb1"));
		source.set_upper(OP_B, CommitVersion(5));

		let pin = snapshots.write_flow(&source, &[OP_A, OP_B, OperatorId(99)], CommitVersion(4));
		assert_eq!(pin, Some(CommitVersion(4)), "the pin must be the flow cursor, below every arena upper");

		let restored = OperatorStore::default();
		assert_eq!(
			snapshots.load_flow(&restored, [OP_A, OP_B].into_iter(), CommitVersion(0)),
			FlowSnapshotLoad::Restored(CommitVersion(4)),
			"load must report the cursor replay has to resume from"
		);
		assert_eq!(scan(&restored, OP_A), scan(&source, OP_A));
		assert_eq!(scan(&restored, OP_B), scan(&source, OP_B));
		assert_eq!(restored.upper(OP_A), CommitVersion(9));
		assert_eq!(restored.upper(OP_B), CommitVersion(5));
	}

	#[test]
	fn a_stateless_flow_writes_nothing_and_returns_no_pin() {
		// Operators that never carried committed state (upper == 0) have nothing to snapshot
		// and must not drag the flow pin to version zero, which would block CDC truncation
		// forever. Falsified by dropping the upper-zero filter: empty generations appear below
		// and a pin is returned for a flow that has nothing to restore.
		let (snapshots, store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();
		source.set(OP_A, key(b"a"), row(b"v"));

		assert_eq!(snapshots.write_flow(&source, &[OP_A, OP_B], CommitVersion(7)), None);
		assert!(store.generations(OP_A).expect("generations").is_empty());
		assert!(store.generations(OP_B).expect("generations").is_empty());
	}

	#[test]
	fn load_refuses_a_snapshot_behind_the_cdc_truncation_floor() {
		// CDC coverage check, now against the FLOW CURSOR: replay resumes at the cursor, so a
		// snapshot whose cursor predates truncated_before cannot be caught up and must be
		// discarded rather than silently resumed from stale state. Comparing the arena upper
		// here would pass while the needed records are already gone, which is the exact hole
		// this version-space split closes. Falsified by inverting the comparison: the accepting
		// case below then fails while the refusing case loads.
		let (snapshots, store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();
		source.set(OP_A, key(b"a"), row(b"v"));
		source.set_upper(OP_A, CommitVersion(9));
		assert_eq!(snapshots.write_flow(&source, &[OP_A], CommitVersion(5)), Some(CommitVersion(5)));

		let refused = OperatorStore::default();
		assert_eq!(
			snapshots.load_flow(&refused, [OP_A].into_iter(), CommitVersion(6)),
			FlowSnapshotLoad::Inconsistent,
			"cursor 5 < truncated_before 6 must be refused, and a flow whose only generation is \
			 uncovered has no set to fall back to"
		);
		assert_eq!(refused.upper(OP_A), CommitVersion(0));
		assert!(scan(&refused, OP_A).is_empty());
		assert!(
			store.generations(OP_A).expect("generations").is_empty(),
			"the uncovered generation must be discarded"
		);

		assert_eq!(snapshots.write_flow(&source, &[OP_A], CommitVersion(5)), Some(CommitVersion(5)));
		let accepted = OperatorStore::default();
		assert_eq!(
			snapshots.load_flow(&accepted, [OP_A].into_iter(), CommitVersion(5)),
			FlowSnapshotLoad::Restored(CommitVersion(5)),
			"cursor == truncated_before is still replayable"
		);
		assert_eq!(accepted.upper(OP_A), CommitVersion(9));
		assert_eq!(scan(&accepted, OP_A), scan(&source, OP_A));
	}

	#[test]
	fn a_flow_that_never_snapshotted_loads_empty_rather_than_inconsistent() {
		// Boot with no generations at all is the ordinary cold start and must stay a silent
		// empty boot; only a flow that HAS generations it cannot reconcile may poison. Falsified
		// by returning Inconsistent whenever no cursor is found, which would poison every flow
		// created after the last snapshot pass.
		let (snapshots, _store, _guard) = snapshot_fixture();
		let restored = OperatorStore::default();

		assert_eq!(
			snapshots.load_flow(&restored, [OP_A, OP_B].into_iter(), CommitVersion(0)),
			FlowSnapshotLoad::Empty
		);
	}

	#[test]
	fn a_crash_between_two_operators_falls_back_to_the_older_consistent_set() {
		// A snapshot pass writes one sqlite transaction per operator, so process death between
		// them leaves one operator a generation ahead of another. Loading the newest generation
		// of each would put the flow's operators at DIFFERENT cursors, and any single resume
		// point would then either skip or double-apply a window for one of them. The load must
		// step back to the newest cursor every operator can supply. Falsified by picking each
		// operator's newest generation independently: OP_A then carries "second" while the
		// reported cursor is 4.
		let (snapshots, store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();
		source.set(OP_A, key(b"a"), row(b"first"));
		source.set_upper(OP_A, CommitVersion(5));
		source.set(OP_B, key(b"b"), row(b"first"));
		source.set_upper(OP_B, CommitVersion(5));
		assert_eq!(snapshots.write_flow(&source, &[OP_A, OP_B], CommitVersion(4)), Some(CommitVersion(4)));

		// The interrupted pass: OP_A reaches disk at cursor 8, OP_B never does.
		let ahead = OperatorStore::default();
		ahead.set(OP_A, key(b"a"), row(b"second"));
		ahead.set_upper(OP_A, CommitVersion(9));
		assert_eq!(snapshots.write_flow(&ahead, &[OP_A], CommitVersion(8)), Some(CommitVersion(8)));
		assert_eq!(store.generations(OP_A).expect("generations"), vec![2, 1]);

		let restored = OperatorStore::default();
		assert_eq!(
			snapshots.load_flow(&restored, [OP_A, OP_B].into_iter(), CommitVersion(0)),
			FlowSnapshotLoad::Restored(CommitVersion(4)),
			"the newest cursor both operators can supply is 4, not OP_A's 8"
		);
		assert_eq!(scan(&restored, OP_A), scan(&source, OP_A));
		assert_eq!(scan(&restored, OP_B), scan(&source, OP_B));
		assert_eq!(restored.upper(OP_A), CommitVersion(5), "the fallback generation's upper must load too");
	}

	#[test]
	fn a_flow_with_no_shared_cursor_left_reports_inconsistent() {
		// Only two generations are retained, so a second interrupted pass can push the shared
		// cursor off the end. There is then no version the whole flow's state agrees on and no
		// safe resume point; the caller must poison rather than boot a half-old arena. Falsified
		// by falling back to loading whatever the newest generations are.
		let (snapshots, _store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();
		source.set(OP_A, key(b"a"), row(b"v"));
		source.set_upper(OP_A, CommitVersion(5));
		assert_eq!(snapshots.write_flow(&source, &[OP_A], CommitVersion(4)), Some(CommitVersion(4)));
		let other = OperatorStore::default();
		other.set(OP_B, key(b"b"), row(b"v"));
		other.set_upper(OP_B, CommitVersion(5));
		assert_eq!(snapshots.write_flow(&other, &[OP_B], CommitVersion(6)), Some(CommitVersion(6)));

		let restored = OperatorStore::default();
		assert_eq!(
			snapshots.load_flow(&restored, [OP_A, OP_B].into_iter(), CommitVersion(0)),
			FlowSnapshotLoad::Inconsistent
		);
		assert_eq!(restored.total_bytes(), 0, "nothing may be left in the arena from a refused set");
	}

	#[test]
	fn load_refuses_a_dictionary_regression_and_falls_back_to_the_previous_generation() {
		// Dictionary barrier at load: a manifest recording interned ids beyond what is
		// durable means a crash lost interns the snapshot references; decoding them after the
		// counter reseeds would be silently wrong, so that generation must be discarded and
		// the previous one used instead. Falsified by dropping the load-time dictionary check
		// (the poisoned newest generation then seeds the arena) or by not falling back.
		let (snapshots, store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();
		source.set(OP_A, key(b"old"), row(b"generation-1"));
		source.set_upper(OP_A, CommitVersion(5));
		assert_eq!(snapshots.write_flow(&source, &[OP_A], CommitVersion(4)), Some(CommitVersion(4)));

		store.write(
			SnapshotWrite {
				operator: OP_A,
				upper: CommitVersion(8),
				flow_cursor: CommitVersion(7),
				dictionary_max: &[(7, 100)],
				chunk_bytes: 1024,
			},
			&mut vec![Ok((key(b"new"), row(b"generation-2")))].into_iter(),
		)
		.expect("write a generation referencing undurable interns");
		assert_eq!(store.generations(OP_A).expect("generations"), vec![2, 1]);

		let restored = OperatorStore::default();
		assert_eq!(
			snapshots.load_flow(&restored, [OP_A].into_iter(), CommitVersion(0)),
			FlowSnapshotLoad::Restored(CommitVersion(4))
		);
		assert_eq!(
			scan(&restored, OP_A),
			scan(&source, OP_A),
			"the arena must carry generation 1, not the poisoned generation 2"
		);
		assert_eq!(restored.upper(OP_A), CommitVersion(5));
		assert_eq!(
			store.generations(OP_A).expect("generations"),
			vec![1],
			"the poisoned generation must be discarded"
		);
	}

	#[test]
	fn a_failed_dictionary_flush_aborts_the_snapshot_pass() {
		// Dictionary barrier at write: if the single store cannot make pending interns
		// durable, completing a snapshot would record state whose dictionary ids may not
		// survive a crash, so the whole pass must abort with the old generations untouched.
		// Falsified by ignoring the flush_pending_blocking return value.
		let (config, _db_guard) = SqliteConfig::test();
		let store = SnapshotStore::sqlite(config);
		let (mut single, _single_guard) = SingleStore::testing_memory_with_persistent_sqlite();
		SingleVersionCommit::commit(
			&mut single,
			CowVec::new(vec![Delta::Set {
				key: key(b"pending-intern"),
				row: row(b"v"),
			}]),
		)
		.expect("commit a pending single-store write");
		single.persistent().expect("persistent tier configured").shutdown();

		let snapshots =
			FlowSnapshots::new(store.clone(), single, DictionaryAllocatorRegistry::default());
		let source = OperatorStore::default();
		source.set(OP_A, key(b"a"), row(b"v"));
		source.set_upper(OP_A, CommitVersion(5));

		assert_eq!(
			snapshots.write_flow(&source, &[OP_A], CommitVersion(4)),
			None,
			"a failed flush barrier must abort"
		);
		assert!(
			store.generations(OP_A).expect("generations").is_empty(),
			"no generation may complete past a failed dictionary barrier"
		);
	}

	#[test]
	fn pin_lag_is_checkpoint_minus_pin_per_flow() {
		// The pin-lag metric is (flow checkpoint - snapshot pin): how far CDC must be
		// retained beyond the flow's durable position for a snapshot restore to catch up.
		// Falsified by inverting the subtraction, by recording the pin into the checkpoint
		// slot, or by keeping deleted flows in the collector output.
		let tracker = SnapshotPinTracker::new();
		tracker.record_checkpoint(FlowId(1), CommitVersion(9));
		tracker.record_pin(FlowId(1), CommitVersion(7));
		tracker.record_checkpoint(FlowId(2), CommitVersion(4));
		assert_eq!(tracker.lags(), vec![(FlowId(1), 2)], "a flow without a pin reports no lag");

		let mut out = Vec::new();
		tracker.collect(&mut out);
		assert_eq!(out.len(), 1);
		assert_eq!(out[0], MetricsSample::count("flow::1".to_string(), "snapshot_pin_lag", 2));

		tracker.forget(FlowId(1));
		assert!(tracker.lags().is_empty(), "a retired flow must leave the metric surface");
	}
}
