// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	borrow::Cow,
	collections::{BTreeMap, BTreeSet},
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::util::budget::MemoryBudget;
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{byte_size::ByteSize, error::TypeError};

use crate::tier::{
	commit::{CommitCensus, CommitDomain, Settlement, Slice},
	range::RowBytes,
};

pub(super) type Kind = u8;

pub(super) const ENTRY_OVERHEAD: ByteSize = ByteSize::from_bytes(32);

pub(super) const FLOOR: ByteSize = ByteSize::from_bytes(256);

pub(super) type PersistHook = Box<dyn Fn(&TestState) + Send + Sync>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TestEntry {
	pub row: Option<EncodedPodRow>,
	pub version: u64,
}

pub(super) fn footprint(key: &EncodedKey, row: &Option<EncodedPodRow>) -> ByteSize {
	ENTRY_OVERHEAD
		.saturating_add(ByteSize::from_bytes(key.heap_bytes() as u64))
		.saturating_add(row_bytes(row))
}

fn row_bytes(row: &Option<EncodedPodRow>) -> ByteSize {
	ByteSize::from_bytes(row.as_ref().map_or(0, RowBytes::row_bytes) as u64)
}

#[derive(Default)]
pub(super) struct Live {
	pub kinds: BTreeMap<Kind, BTreeMap<EncodedKey, TestEntry>>,
	pub bytes: ByteSize,
}

pub(super) struct TestBatch {
	pub kind: Kind,
	pub entries: Vec<(EncodedKey, TestEntry)>,
	pub bytes: ByteSize,
}

pub(super) struct TestState {
	budget: Arc<MemoryBudget>,
	live: Mutex<Live>,
	persistent: Mutex<BTreeMap<(Kind, EncodedKey), Option<EncodedPodRow>>>,
	in_flight: Mutex<ByteSize>,
	watermark: Mutex<Option<u64>>,
	refused: Mutex<BTreeSet<EncodedKey>>,
	failing: AtomicBool,
	persists: AtomicU64,
	selects: AtomicU64,
	hook: Mutex<Option<PersistHook>>,
}

impl TestState {
	pub fn new(budget: Arc<MemoryBudget>) -> Self {
		Self {
			budget,
			live: Mutex::new(Live::default()),
			persistent: Mutex::new(BTreeMap::new()),
			in_flight: Mutex::new(ByteSize::ZERO),
			watermark: Mutex::new(Some(u64::MAX)),
			refused: Mutex::new(BTreeSet::new()),
			failing: AtomicBool::new(false),
			persists: AtomicU64::new(0),
			selects: AtomicU64::new(0),
			hook: Mutex::new(None),
		}
	}

	pub fn record(&self, kind: Kind, key: EncodedKey, row: Option<EncodedPodRow>, version: u64) {
		let incoming = row_bytes(&row);
		let mut live = self.live.lock();
		let entries = live.kinds.entry(kind).or_default();
		let released: ByteSize;
		let charged: ByteSize;
		match entries.get_mut(&key) {
			Some(entry) => {
				released = row_bytes(&entry.row);
				charged = incoming;
				entry.row = row;
				entry.version = version;
			}
			None => {
				released = ByteSize::ZERO;
				charged = footprint(&key, &row);
				entries.insert(
					key,
					TestEntry {
						row,
						version,
					},
				);
			}
		}
		live.bytes = live.bytes.saturating_sub(released).saturating_add(charged);
		self.budget.release(released);
		self.budget.charge(charged);
	}

	pub fn remove(&self, kind: Kind, key: EncodedKey, version: u64) {
		self.record(kind, key, None, version);
	}

	/// Drops every entry of `kind` the predicate rejects, refunding each one's whole footprint; a
	/// `retain` that forgot the refund would leak the key and the overhead forever.
	pub fn drop_where(&self, kind: Kind, keep: impl Fn(&EncodedKey) -> bool) {
		let mut live = self.live.lock();
		let Some(entries) = live.kinds.get_mut(&kind) else {
			return;
		};
		let victims: Vec<EncodedKey> =
			entries.keys().filter(|&key| !keep(key)).cloned().collect();
		let mut refunded = ByteSize::ZERO;
		for key in victims {
			let entry = entries.remove(&key).expect("a victim key was just observed present");
			refunded = refunded.saturating_add(footprint(&key, &entry.row));
		}
		live.bytes = live.bytes.saturating_sub(refunded);
		self.budget.release(refunded);
	}

	pub fn budget_used(&self) -> ByteSize {
		self.budget.used()
	}

	pub fn live_bytes(&self) -> ByteSize {
		self.live.lock().bytes
	}

	/// The whole resident set recomputed by walking it, so a drifting incremental counter is a test
	/// failure rather than a silent unbounded-RAM bug.
	pub fn census_bytes(&self) -> ByteSize {
		let live = self.live.lock();
		Self::walk(&live)
	}

	fn walk(live: &Live) -> ByteSize {
		let mut total = ByteSize::ZERO;
		for entries in live.kinds.values() {
			for (key, entry) in entries.iter() {
				total = total.saturating_add(footprint(key, &entry.row));
			}
		}
		total
	}

	/// Both measures taken under the live lock, so a writer cannot land between them; the in-flight
	/// charge is added from its counter because the entries themselves are owned by the batch.
	pub fn census(&self) -> CommitCensus {
		let live = self.live.lock();
		let walked = Self::walk(&live).saturating_add(*self.in_flight.lock());
		CommitCensus {
			counted: self.budget.used(),
			walked,
		}
	}

	pub fn in_flight_bytes(&self) -> ByteSize {
		*self.in_flight.lock()
	}

	pub fn live_entries(&self, kind: Kind) -> usize {
		self.live.lock().kinds.get(&kind).map_or(0, BTreeMap::len)
	}

	pub fn live_keys(&self, kind: Kind) -> Vec<EncodedKey> {
		self.live.lock().kinds.get(&kind).map_or_else(Vec::new, |entries| entries.keys().cloned().collect())
	}

	pub fn persistent_entries(&self) -> usize {
		self.persistent.lock().len()
	}

	pub fn persisted(&self, kind: Kind, key: &EncodedKey) -> Option<Option<EncodedPodRow>> {
		self.persistent.lock().get(&(kind, key.clone())).cloned()
	}

	pub fn set_watermark(&self, watermark: Option<u64>) {
		*self.watermark.lock() = watermark;
	}

	pub fn refuse(&self, key: EncodedKey) {
		self.refused.lock().insert(key);
	}

	pub fn fail_persist(&self, failing: bool) {
		self.failing.store(failing, Ordering::SeqCst);
	}

	pub fn persists(&self) -> u64 {
		self.persists.load(Ordering::SeqCst)
	}

	pub fn selects(&self) -> u64 {
		self.selects.load(Ordering::SeqCst)
	}

	pub fn set_hook(&self, hook: PersistHook) {
		*self.hook.lock() = Some(hook);
	}

	fn take(&self, kind: Kind, cutoff: u64, budget: ByteSize) -> Option<(TestBatch, bool)> {
		self.selects.fetch_add(1, Ordering::SeqCst);
		let mut live = self.live.lock();
		let entries = live.kinds.get_mut(&kind)?;

		let mut taken = ByteSize::ZERO;
		let mut chosen = Vec::new();
		let mut more = false;
		for (key, entry) in entries.iter() {
			if entry.version > cutoff {
				continue;
			}
			let cost = footprint(key, &entry.row);
			if taken != ByteSize::ZERO && taken.saturating_add(cost) > budget {
				more = true;
				break;
			}
			taken = taken.saturating_add(cost);
			chosen.push(key.clone());
		}
		if chosen.is_empty() {
			return None;
		}

		let mut batch = Vec::with_capacity(chosen.len());
		for key in chosen {
			let entry = entries.remove(&key).expect("a chosen key was just observed present");
			batch.push((key, entry));
		}
		let drained = entries.is_empty();
		if drained {
			live.kinds.remove(&kind);
		}
		live.bytes = live.bytes.saturating_sub(taken);
		drop(live);

		let mut in_flight = self.in_flight.lock();
		*in_flight = in_flight.saturating_add(taken);
		drop(in_flight);

		Some((
			TestBatch {
				kind,
				entries: batch,
				bytes: taken,
			},
			more,
		))
	}

	fn write(&self, batch: &TestBatch) -> reifydb_value::Result<Vec<EncodedKey>> {
		if let Some(hook) = self.hook.lock().as_ref() {
			hook(self);
		}
		if self.failing.load(Ordering::SeqCst) {
			return Err(TypeError::IntegerConversion {
				message: "test persist refused".to_string(),
			}
			.into());
		}
		self.persists.fetch_add(1, Ordering::SeqCst);
		let refused = self.refused.lock();
		let mut persistent = self.persistent.lock();
		let mut accepted = Vec::with_capacity(batch.entries.len());
		for (key, entry) in batch.entries.iter() {
			if refused.contains(key) {
				continue;
			}
			persistent.insert((batch.kind, key.clone()), entry.row.clone());
			accepted.push(key.clone());
		}
		Ok(accepted)
	}

	fn dispose(&self, batch: TestBatch, ack: Vec<EncodedKey>) -> Settlement {
		let accepted: BTreeSet<EncodedKey> = ack.into_iter().collect();
		let mut released = ByteSize::ZERO;
		let mut entries = 0u64;
		let mut returned = ByteSize::ZERO;

		let mut live = self.live.lock();
		for (key, entry) in batch.entries {
			let cost = footprint(&key, &entry.row);
			if accepted.contains(&key) {
				released = released.saturating_add(cost);
				entries += 1;
				continue;
			}
			live.kinds.entry(batch.kind).or_default().insert(key, entry);
			returned = returned.saturating_add(cost);
		}
		live.bytes = live.bytes.saturating_add(returned);
		drop(live);

		let mut in_flight = self.in_flight.lock();
		*in_flight = in_flight.saturating_sub(batch.bytes);

		Settlement {
			released,
			entries,
			reclaimed: entries,
		}
	}

	fn pending_kinds(&self) -> Vec<Kind> {
		self.live.lock().kinds.keys().copied().collect()
	}
}

#[derive(Clone, Copy, Debug)]
pub(super) struct TestDomain;

impl CommitDomain for TestDomain {
	type State = TestState;
	type Batch = TestBatch;
	type Ack = Vec<EncodedKey>;
	type Cutoff = u64;
	type Kind = Kind;

	const SCOPE: &'static str = "test_commit";

	const MAX_SLICES_PER_TICK: usize = 4;


	fn cutoff(state: &Self::State) -> Option<Self::Cutoff> {
		*state.watermark.lock()
	}

	fn cutoff_all() -> Self::Cutoff {
		u64::MAX
	}

	fn kinds(state: &Self::State) -> Vec<Self::Kind> {
		state.pending_kinds()
	}

	fn select(state: &Self::State, kind: Self::Kind, cutoff: Self::Cutoff, budget: ByteSize) -> Option<Slice<Self>> {
		let (batch, more) = state.take(kind, cutoff, budget)?;
		Some(Slice {
			bytes: batch.bytes,
			batch,
			more,
		})
	}

	fn persist(state: &Self::State, batch: &Self::Batch) -> reifydb_value::Result<Self::Ack> {
		state.write(batch)
	}

	fn settle(state: &Self::State, batch: Self::Batch, ack: Self::Ack) -> Settlement {
		state.dispose(batch, ack)
	}

	fn resident_bytes(state: &Self::State) -> ByteSize {
		state.budget.used()
	}

	fn kind_name(kind: Self::Kind) -> Cow<'static, str> {
		Cow::Owned(format!("k{kind}"))
	}

	fn census(state: &Self::State) -> CommitCensus {
		state.census()
	}
}

#[derive(Clone, Copy, Debug)]
pub(super) struct FloorDomain;

impl CommitDomain for FloorDomain {
	type State = TestState;
	type Batch = TestBatch;
	type Ack = Vec<EncodedKey>;
	type Cutoff = u64;
	type Kind = Kind;

	const SCOPE: &'static str = "floor_commit";

	const MAX_SLICES_PER_TICK: usize = 4;


	fn cutoff(state: &Self::State) -> Option<Self::Cutoff> {
		*state.watermark.lock()
	}

	fn cutoff_all() -> Self::Cutoff {
		u64::MAX
	}

	fn kinds(state: &Self::State) -> Vec<Self::Kind> {
		state.pending_kinds()
	}

	fn select(state: &Self::State, kind: Self::Kind, cutoff: Self::Cutoff, budget: ByteSize) -> Option<Slice<Self>> {
		let (batch, more) = state.take(kind, cutoff, budget)?;
		Some(Slice {
			bytes: batch.bytes,
			batch,
			more,
		})
	}

	fn persist(state: &Self::State, batch: &Self::Batch) -> reifydb_value::Result<Self::Ack> {
		state.write(batch)
	}

	fn settle(state: &Self::State, batch: Self::Batch, ack: Self::Ack) -> Settlement {
		state.dispose(batch, ack)
	}

	fn resident_bytes(state: &Self::State) -> ByteSize {
		state.budget.used()
	}

	fn kind_name(kind: Self::Kind) -> Cow<'static, str> {
		Cow::Owned(format!("k{kind}"))
	}

	fn census(state: &Self::State) -> CommitCensus {
		state.census()
	}

	fn worth_persisting(bytes: ByteSize) -> bool {
		bytes >= FLOOR
	}

	fn admits_over_budget_writes() -> bool {
		false
	}
}
