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

	pub fn write_flow(&self, operators: &OperatorStore, ids: &[OperatorId]) -> Option<CommitVersion> {
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

		let mut pin: Option<CommitVersion> = None;
		for id in stateful {
			let upper = operators.upper(id);
			operators.freeze(id);
			if let Err(e) = self.write_operator(operators, id, upper, &dictionary_max) {
				warn!(operator = id.0, error = %e, "operator snapshot aborted; previous generation stays valid");
				return None;
			}
			pin = Some(pin.map_or(upper, |current: CommitVersion| current.min(upper)));
		}
		pin
	}

	fn write_operator(
		&self,
		operators: &OperatorStore,
		id: OperatorId,
		upper: CommitVersion,
		dictionary_max: &[(u64, u128)],
	) -> Result<()> {
		let mut entries = ArenaScan::new(operators, id);
		self.store
			.write(
				SnapshotWrite {
					operator: id,
					upper,
					dictionary_max,
					chunk_bytes: DEFAULT_SNAPSHOT_CHUNK_BYTES,
				},
				&mut entries,
			)
			.map(|_| ())
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
	) {
		for id in ids {
			self.load_operator(operators, id, truncated_before);
		}
	}

	fn load_operator(&self, operators: &OperatorStore, id: OperatorId, truncated_before: CommitVersion) {
		let generations = match self.store.generations(id) {
			Ok(generations) => generations,
			Err(e) => {
				error!(operator = id.0, error = %e, "operator snapshot generations unreadable; booting empty");
				return;
			}
		};
		for generation in generations {
			match self.validate(id, generation, truncated_before) {
				Ok(loaded) => {
					for (key, row) in loaded.entries {
						operators.set(id, key, row);
					}
					operators.set_upper(id, loaded.manifest.upper);
					return;
				}
				Err(e) => {
					error!(
						operator = id.0,
						generation,
						error = %e,
						"discarding invalid operator snapshot generation"
					);
					if let Err(e) = self.store.discard(id, generation) {
						error!(operator = id.0, generation, error = %e, "failed to discard invalid snapshot generation");
					}
				}
			}
		}
	}

	fn validate(&self, id: OperatorId, generation: u64, truncated_before: CommitVersion) -> Result<LoadedSnapshot> {
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
		if loaded.manifest.upper < truncated_before {
			return Err(internal_error!(
				"snapshot upper {} predates the cdc truncation floor {}; replay from it is impossible",
				loaded.manifest.upper.0,
				truncated_before.0
			));
		}
		Ok(loaded)
	}
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
	fn write_then_load_restores_arena_content_and_upper_and_pins_at_min_upper() {
		// The full round trip: write_flow must persist every stateful operator, return the
		// MINIMUM upper across them (the version CDC replay must still cover for the whole
		// flow), and load_flow into an empty arena must reproduce the exact content and
		// per-operator uppers. Falsified by pinning at the max upper instead of the min, by
		// skipping set_upper at load, or by seeding the wrong operator's arena.
		let (snapshots, _store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();
		source.set(OP_A, key(b"a1"), row(b"va1"));
		source.set(OP_A, key(b"a2"), row(b"va2"));
		source.set_upper(OP_A, CommitVersion(9));
		source.set(OP_B, key(b"b1"), row(b"vb1"));
		source.set_upper(OP_B, CommitVersion(5));

		let pin = snapshots.write_flow(&source, &[OP_A, OP_B, OperatorId(99)]);
		assert_eq!(pin, Some(CommitVersion(5)), "the pin must be the minimum upper across stateful operators");

		let restored = OperatorStore::default();
		snapshots.load_flow(&restored, [OP_A, OP_B].into_iter(), CommitVersion(0));
		assert_eq!(scan(&restored, OP_A), scan(&source, OP_A));
		assert_eq!(scan(&restored, OP_B), scan(&source, OP_B));
		assert_eq!(restored.upper(OP_A), CommitVersion(9));
		assert_eq!(restored.upper(OP_B), CommitVersion(5));
	}

	#[test]
	fn a_stateless_flow_writes_nothing_and_returns_no_pin() {
		// Operators that never carried committed state (upper == 0) have nothing to snapshot
		// and must not drag the flow pin to version zero, which would block CDC truncation
		// forever. Falsified by including upper-zero operators in the min computation or by
		// writing empty generations for them.
		let (snapshots, store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();
		source.set(OP_A, key(b"a"), row(b"v"));

		assert_eq!(snapshots.write_flow(&source, &[OP_A, OP_B]), None);
		assert!(store.generations(OP_A).expect("generations").is_empty());
		assert!(store.generations(OP_B).expect("generations").is_empty());
	}

	#[test]
	fn load_refuses_a_snapshot_behind_the_cdc_truncation_floor() {
		// CDC coverage check: a snapshot whose upper predates truncated_before cannot be
		// caught up (the changes between its upper and the truncation floor are gone), so it
		// must be discarded and the arena boot empty rather than silently stale. Falsified by
		// inverting the comparison: the accepting case below then fails while the refusing
		// case loads.
		let (snapshots, store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();
		source.set(OP_A, key(b"a"), row(b"v"));
		source.set_upper(OP_A, CommitVersion(5));
		assert_eq!(snapshots.write_flow(&source, &[OP_A]), Some(CommitVersion(5)));

		let refused = OperatorStore::default();
		snapshots.load_flow(&refused, [OP_A].into_iter(), CommitVersion(6));
		assert_eq!(refused.upper(OP_A), CommitVersion(0), "upper 5 < truncated_before 6 must be refused");
		assert!(scan(&refused, OP_A).is_empty());
		assert!(
			store.generations(OP_A).expect("generations").is_empty(),
			"the uncovered generation must be discarded"
		);

		assert_eq!(snapshots.write_flow(&source, &[OP_A]), Some(CommitVersion(5)));
		let accepted = OperatorStore::default();
		snapshots.load_flow(&accepted, [OP_A].into_iter(), CommitVersion(5));
		assert_eq!(accepted.upper(OP_A), CommitVersion(5), "upper == truncated_before is still replayable");
		assert_eq!(scan(&accepted, OP_A), scan(&source, OP_A));
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
		assert_eq!(snapshots.write_flow(&source, &[OP_A]), Some(CommitVersion(5)));

		store.write(
			SnapshotWrite {
				operator: OP_A,
				upper: CommitVersion(8),
				dictionary_max: &[(7, 100)],
				chunk_bytes: 1024,
			},
			&mut vec![Ok((key(b"new"), row(b"generation-2")))].into_iter(),
		)
		.expect("write a generation referencing undurable interns");
		assert_eq!(store.generations(OP_A).expect("generations"), vec![2, 1]);

		let restored = OperatorStore::default();
		snapshots.load_flow(&restored, [OP_A].into_iter(), CommitVersion(0));
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

		assert_eq!(snapshots.write_flow(&source, &[OP_A]), None, "a failed flush barrier must abort");
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
