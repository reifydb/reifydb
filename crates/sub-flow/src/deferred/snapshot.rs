// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};
#[cfg(not(target_arch = "wasm32"))]
use std::{collections::BTreeSet, ops::Bound, result::Result as StdResult, vec::IntoIter};

#[cfg(not(target_arch = "wasm32"))]
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	operator::EncodedOperatorRow,
};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::FlowId,
	metrics::{collect::MetricsCollector, sample::MetricsSample},
};
use reifydb_runtime::sync::mutex::Mutex;
#[cfg(not(target_arch = "wasm32"))]
use reifydb_store_operator::{
	snapshot::{DEFAULT_SNAPSHOT_CHUNK_BYTES, LoadedSnapshot, SnapshotStore, SnapshotWrite},
	store::OperatorStore,
};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_store_single::SingleStore;
#[cfg(not(target_arch = "wasm32"))]
use reifydb_transaction::dictionary::{DictionaryAllocatorRegistry, store::durable_max_index_id};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_value::{Result, value::dictionary::DictionaryId};
#[cfg(not(target_arch = "wasm32"))]
use tracing::{error, info, warn};

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct FlowSnapshots {
	store: SnapshotStore,
	single: SingleStore,
	dictionaries: DictionaryAllocatorRegistry,
}

#[cfg(not(target_arch = "wasm32"))]
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
		if ids.is_empty() {
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

		let mut written: Vec<(OperatorId, u64)> = Vec::with_capacity(ids.len());
		for id in ids.iter().copied() {
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
		let catalog = match self.generation_catalog(&ids) {
			Ok(catalog) => catalog,
			Err(e) => {
				error!(error = %e, "operator snapshot generations unreadable");
				return FlowSnapshotLoad::Inconsistent(SnapshotRejection::Unreadable);
			}
		};
		if catalog.is_empty() {
			return FlowSnapshotLoad::Empty;
		}
		let mut rejected: BTreeSet<(OperatorId, u64)> = BTreeSet::new();
		let mut first: Option<SnapshotRejection> = None;
		loop {
			let Some((cursor, picks)) = consistent_set(&catalog, &rejected) else {
				let rejection = first.unwrap_or(SnapshotRejection::CursorDisagreement);
				error!(
					reason = rejection.reason(),
					"no operator snapshot set is left to resume this flow from"
				);
				return FlowSnapshotLoad::Inconsistent(rejection);
			};
			match self.load_set(operators, &picks, cursor, truncated_before) {
				SetLoad::Restored => return FlowSnapshotLoad::Restored(cursor),
				SetLoad::Rejected(pick, rejection) => {
					rejected.insert(pick);
					first.get_or_insert(rejection);
				}
			}
		}
	}

	pub fn sweep_orphans(&self, live: &BTreeSet<OperatorId>) {
		if live.is_empty() {
			return;
		}
		let stored = match self.store.operators() {
			Ok(stored) => stored,
			Err(e) => {
				error!(error = %e, "could not list operators holding snapshot generations; skipping orphan sweep");
				return;
			}
		};
		let mut discarded = 0usize;
		for operator in stored {
			if live.contains(&operator) {
				continue;
			}
			let generations = match self.store.generations(operator) {
				Ok(generations) => generations,
				Err(e) => {
					error!(operator = operator.0, error = %e, "could not list generations of an orphaned operator");
					continue;
				}
			};
			for generation in generations {
				match self.store.discard(operator, generation) {
					Ok(()) => discarded += 1,
					Err(e) => {
						error!(operator = operator.0, generation, error = %e, "failed to discard an orphaned snapshot generation")
					}
				}
			}
		}
		if discarded > 0 {
			info!(discarded, "discarded snapshot generations of operators no live flow owns");
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
				Err(rejection) => {
					if rejection.is_undecodable()
						&& let Err(e) = self.store.discard(*id, *generation)
					{
						error!(operator = id.0, generation, error = %e, "failed to discard an undecodable snapshot generation");
					}
					for id in loaded {
						operators.drop_arena(id);
					}
					return SetLoad::Rejected((*id, *generation), rejection);
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
	) -> StdResult<LoadedSnapshot, SnapshotRejection> {
		let loaded = match self.store.load(id, generation) {
			Ok(loaded) => loaded,
			Err(e) => {
				error!(operator = id.0, generation, error = %e, "operator snapshot generation is unreadable");
				return Err(SnapshotRejection::Unreadable);
			}
		};
		for (dictionary, recorded) in &loaded.manifest.dictionary_max {
			let durable = match durable_max_index_id(&self.single, DictionaryId(*dictionary)) {
				Ok(durable) => durable.unwrap_or(0),
				Err(e) => {
					error!(operator = id.0, generation, error = %e, "durable dictionary state is unreadable");
					return Err(SnapshotRejection::Unreadable);
				}
			};
			if *recorded > durable {
				error!(
					operator = id.0,
					generation,
					dictionary = *dictionary,
					recorded = %recorded,
					durable = %durable,
					"snapshot references interned values that did not survive; its rows can no longer be decoded"
				);
				return Err(SnapshotRejection::DictionaryLoss);
			}
		}
		if loaded.manifest.flow_cursor != cursor {
			error!(
				operator = id.0,
				generation,
				manifest = loaded.manifest.flow_cursor.0,
				set = cursor.0,
				"snapshot cursor does not match the set cursor it was selected for"
			);
			return Err(SnapshotRejection::ManifestMismatch);
		}
		if loaded.manifest.flow_cursor < truncated_before {
			error!(
				operator = id.0,
				generation,
				cursor = loaded.manifest.flow_cursor.0,
				truncated_before = truncated_before.0,
				"snapshot cursor predates the cdc truncation floor; replay from it is impossible"
			);
			return Err(SnapshotRejection::TruncatedBeyondSnapshot);
		}
		Ok(loaded)
	}
}

#[cfg_attr(target_arch = "wasm32", expect(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotRejection {
	Unreadable,
	DictionaryLoss,
	ManifestMismatch,
	TruncatedBeyondSnapshot,
	CursorDisagreement,
}

impl SnapshotRejection {
	#[cfg(not(target_arch = "wasm32"))]
	fn is_undecodable(&self) -> bool {
		match self {
			Self::DictionaryLoss | Self::ManifestMismatch => true,
			Self::Unreadable | Self::TruncatedBeyondSnapshot | Self::CursorDisagreement => false,
		}
	}

	pub fn reason(&self) -> &'static str {
		match self {
			Self::Unreadable => {
				"the flow's operator snapshots could not be read; the snapshot store is damaged or unavailable"
			}
			Self::DictionaryLoss => {
				"the flow's operator snapshots reference interned values that did not survive; their rows can no longer be decoded"
			}
			Self::ManifestMismatch => {
				"the flow's operator snapshot manifests disagree with the cursors recorded for them"
			}
			Self::TruncatedBeyondSnapshot => {
				"cdc is truncated past every operator snapshot this flow has; the versions needed to rebuild the gap are gone"
			}
			Self::CursorDisagreement => {
				"the flow's operator snapshots carry no cursor every operator agrees on; resuming would mix state from different versions"
			}
		}
	}
}

#[cfg_attr(target_arch = "wasm32", expect(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowSnapshotLoad {
	Empty,
	Restored(CommitVersion),
	Inconsistent(SnapshotRejection),
}

#[cfg(not(target_arch = "wasm32"))]
enum SetLoad {
	Restored,
	Rejected((OperatorId, u64), SnapshotRejection),
}

#[cfg(not(target_arch = "wasm32"))]
fn consistent_set(
	catalog: &[(OperatorId, Vec<(u64, CommitVersion)>)],
	rejected: &BTreeSet<(OperatorId, u64)>,
) -> Option<(CommitVersion, Vec<(OperatorId, u64)>)> {
	let mut cursors: Vec<CommitVersion> = Vec::new();
	for (id, generations) in catalog {
		for (generation, cursor) in generations {
			if !rejected.contains(&(*id, *generation)) {
				cursors.push(*cursor);
			}
		}
	}
	cursors.sort_unstable();
	cursors.dedup();
	for cursor in cursors.into_iter().rev() {
		let picks: Option<Vec<(OperatorId, u64)>> = catalog
			.iter()
			.map(|(id, generations)| {
				generations
					.iter()
					.find(|(generation, candidate)| {
						*candidate == cursor && !rejected.contains(&(*id, *generation))
					})
					.map(|(generation, _)| (*id, *generation))
			})
			.collect();
		if let Some(picks) = picks {
			return Some((cursor, picks));
		}
	}
	None
}

#[cfg(not(target_arch = "wasm32"))]
struct ArenaScan<'a> {
	operators: &'a OperatorStore,
	id: OperatorId,
	pending: IntoIter<(EncodedKey, EncodedOperatorRow)>,
	resume: Option<Bound<EncodedKey>>,
}

#[cfg(not(target_arch = "wasm32"))]
const ARENA_SCAN_BATCH: u64 = 1024;

#[cfg(not(target_arch = "wasm32"))]
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

#[cfg(not(target_arch = "wasm32"))]
impl Iterator for ArenaScan<'_> {
	type Item = Result<(EncodedKey, EncodedOperatorRow)>;

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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
	use std::ops::Bound;

	use reifydb_codec::{encoded::bytes::EncodedBytes, key::encoded::EncodedKeyRange};
	use reifydb_core::{delta::Delta, interface::store::SingleVersionCommit};
	use reifydb_runtime::shutdown::Shutdown;
	use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
	use reifydb_store_operator::snapshot::SnapshotWrite;
	use reifydb_value::util::cowvec::CowVec;

	use super::*;

	const OP_A: OperatorId = OperatorId(1);
	const OP_B: OperatorId = OperatorId(2);

	fn snapshot_fixture() -> (FlowSnapshots, SnapshotStore, SqliteTempPathGuard) {
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

	fn row(bytes: &[u8]) -> EncodedOperatorRow {
		EncodedOperatorRow::timeless(bytes)
	}

	fn scan(operators: &OperatorStore, id: OperatorId) -> Vec<(EncodedKey, EncodedOperatorRow)> {
		operators.range_batch(id, EncodedKeyRange::new(Bound::Unbounded, Bound::Unbounded), 1024).items
	}

	#[test]
	fn write_then_load_restores_arena_content_and_upper_and_pins_at_the_flow_cursor() {
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
	fn the_sweep_discards_generations_no_live_flow_owns() {
		let (snapshots, store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();
		source.set(OP_A, key(b"a"), row(b"live"));
		source.set_upper(OP_A, CommitVersion(9));
		source.set(OP_B, key(b"b"), row(b"orphan"));
		source.set_upper(OP_B, CommitVersion(9));
		snapshots.write_flow(&source, &[OP_A, OP_B], CommitVersion(4));

		snapshots.sweep_orphans(&BTreeSet::from([OP_A]));

		assert_eq!(store.generations(OP_A), Ok(vec![1]), "the live operator's generation must survive");
		assert_eq!(store.generations(OP_B), Ok(vec![]), "the orphan's generation must be discarded");
	}

	#[test]
	fn an_empty_live_set_sweeps_nothing() {
		let (snapshots, store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();
		source.set(OP_A, key(b"a"), row(b"live"));
		source.set_upper(OP_A, CommitVersion(9));
		snapshots.write_flow(&source, &[OP_A], CommitVersion(4));

		snapshots.sweep_orphans(&BTreeSet::new());

		assert_eq!(store.generations(OP_A), Ok(vec![1]), "an unknown live set must not erase anything");
	}

	#[test]
	fn an_operator_that_never_accumulated_state_still_yields_a_pin() {
		let (snapshots, store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();

		assert_eq!(
			snapshots.write_flow(&source, &[OP_A, OP_B], CommitVersion(7)),
			Some(CommitVersion(7)),
			"an empty operator set must still pin at the flow cursor"
		);
		assert_eq!(store.generations(OP_A), Ok(vec![1]), "the empty operator must still have a generation");
		assert_eq!(store.generations(OP_B), Ok(vec![1]), "the empty operator must still have a generation");
	}

	#[test]
	fn repeated_snapshots_of_an_idle_flow_keep_advancing_the_pin() {
		let (snapshots, _store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();

		assert_eq!(snapshots.write_flow(&source, &[OP_A], CommitVersion(10)), Some(CommitVersion(10)));
		assert_eq!(snapshots.write_flow(&source, &[OP_A], CommitVersion(20)), Some(CommitVersion(20)));
		assert_eq!(snapshots.write_flow(&source, &[OP_A], CommitVersion(30)), Some(CommitVersion(30)));
	}

	#[test]
	fn an_empty_snapshot_round_trips_without_inventing_state() {
		let (snapshots, _store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();
		assert_eq!(snapshots.write_flow(&source, &[OP_A], CommitVersion(6)), Some(CommitVersion(6)));

		let target = OperatorStore::default();
		let loaded = snapshots.load_flow(&target, [OP_A].into_iter(), CommitVersion(0));

		assert_eq!(loaded, FlowSnapshotLoad::Restored(CommitVersion(6)));
		assert!(scan(&target, OP_A).is_empty(), "an empty snapshot must restore no rows");
		assert_eq!(target.upper(OP_A), CommitVersion(0), "an empty snapshot must restore the zero upper");
	}

	#[test]
	fn a_flow_with_no_operators_at_all_yields_no_pin() {
		let (snapshots, _store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();

		assert_eq!(snapshots.write_flow(&source, &[], CommitVersion(7)), None);
	}

	#[test]
	fn load_refuses_a_snapshot_behind_the_cdc_truncation_floor() {
		let (snapshots, store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();
		source.set(OP_A, key(b"a"), row(b"v"));
		source.set_upper(OP_A, CommitVersion(9));
		assert_eq!(snapshots.write_flow(&source, &[OP_A], CommitVersion(5)), Some(CommitVersion(5)));

		let refused = OperatorStore::default();
		assert_eq!(
			snapshots.load_flow(&refused, [OP_A].into_iter(), CommitVersion(6)),
			FlowSnapshotLoad::Inconsistent(SnapshotRejection::TruncatedBeyondSnapshot),
			"cursor 5 < truncated_before 6 must be refused, and a flow whose only generation is \
			 uncovered has no set to fall back to"
		);
		assert_eq!(refused.upper(OP_A), CommitVersion(0));
		assert!(scan(&refused, OP_A).is_empty());
		assert_eq!(
			store.generations(OP_A).expect("generations"),
			vec![1],
			"refusing to resume from a generation is not a reason to destroy it"
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
	fn a_snapshot_the_cdc_floor_outran_stays_on_disk_for_recovery() {
		let (snapshots, store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();
		source.set(OP_A, key(b"a"), row(b"survivor"));
		source.set_upper(OP_A, CommitVersion(9));
		assert_eq!(snapshots.write_flow(&source, &[OP_A], CommitVersion(5)), Some(CommitVersion(5)));

		let refused = OperatorStore::default();
		assert_eq!(
			snapshots.load_flow(&refused, [OP_A].into_iter(), CommitVersion(6)),
			FlowSnapshotLoad::Inconsistent(SnapshotRejection::TruncatedBeyondSnapshot),
			"precondition: the floor is above the snapshot cursor, so the load must refuse"
		);

		assert_eq!(
			store.generations(OP_A).expect("generations"),
			vec![1],
			"the refused generation must survive"
		);
		let recovered = store.load(OP_A, 1).expect("the refused generation must still be readable");
		assert_eq!(recovered.manifest.flow_cursor, CommitVersion(5));
		assert_eq!(recovered.manifest.upper, CommitVersion(9));
		assert_eq!(
			recovered.entries,
			vec![(key(b"a"), row(b"survivor"))],
			"the state itself was never in question and must come back byte for byte"
		);
	}

	#[test]
	fn a_flow_that_never_snapshotted_loads_empty_rather_than_inconsistent() {
		let (snapshots, _store, _guard) = snapshot_fixture();
		let restored = OperatorStore::default();

		assert_eq!(
			snapshots.load_flow(&restored, [OP_A, OP_B].into_iter(), CommitVersion(0)),
			FlowSnapshotLoad::Empty
		);
	}

	#[test]
	fn a_crash_between_two_operators_falls_back_to_the_older_consistent_set() {
		let (snapshots, store, _guard) = snapshot_fixture();
		let source = OperatorStore::default();
		source.set(OP_A, key(b"a"), row(b"first"));
		source.set_upper(OP_A, CommitVersion(5));
		source.set(OP_B, key(b"b"), row(b"first"));
		source.set_upper(OP_B, CommitVersion(5));
		assert_eq!(snapshots.write_flow(&source, &[OP_A, OP_B], CommitVersion(4)), Some(CommitVersion(4)));

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
			FlowSnapshotLoad::Inconsistent(SnapshotRejection::CursorDisagreement)
		);
		assert_eq!(restored.total_bytes(), 0, "nothing may be left in the arena from a refused set");
	}

	#[test]
	fn the_load_reports_the_cause_that_stopped_it_not_the_shape_it_left_behind() {
		let (snapshots, store, _guard) = snapshot_fixture();
		store.write(
			SnapshotWrite {
				operator: OP_A,
				upper: CommitVersion(8),
				flow_cursor: CommitVersion(7),
				dictionary_max: &[(7, 100)],
				chunk_bytes: 1024,
			},
			&mut vec![Ok((key(b"a"), row(b"v")))].into_iter(),
		)
		.expect("write a generation referencing undurable interns");

		let restored = OperatorStore::default();
		assert_eq!(
			snapshots.load_flow(&restored, [OP_A].into_iter(), CommitVersion(0)),
			FlowSnapshotLoad::Inconsistent(SnapshotRejection::DictionaryLoss)
		);
	}

	#[test]
	fn load_refuses_a_dictionary_regression_and_falls_back_to_the_previous_generation() {
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
		let (config, _db_guard) = SqliteConfig::test();
		let store = SnapshotStore::sqlite(config);
		let (mut single, _single_guard) = SingleStore::testing_memory_with_persistent_sqlite();
		SingleVersionCommit::commit(
			&mut single,
			CowVec::new(vec![Delta::Set {
				key: key(b"pending-intern"),
				bytes: EncodedBytes(CowVec::new(b"v".to_vec())),
			}]),
		)
		.expect("commit a pending single-store write");
		single.persistent().expect("persistent tier configured").shutdown();

		let snapshots = FlowSnapshots::new(store.clone(), single, DictionaryAllocatorRegistry::default());
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
