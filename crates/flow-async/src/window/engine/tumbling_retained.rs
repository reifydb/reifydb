// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	fmt::Debug,
	hash::Hash,
	marker::PhantomData,
};

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::operator::state::{OperatorState, StateCodec, decode, encode},
};
use reifydb_core::{
	internal_err,
	key::operator::{
		keyspace::expiry::TumblingExpiry,
		state::{GroupId, GroupStateKey, IntoGroupStateKey, KeyspaceId, KeyspaceMask},
	},
	metrics::heap::HeapSize,
	state::{batch::StateBatch, timer::StateStore},
};
use reifydb_macro::operator_state;
use reifydb_value::{Result, reifydb_assertions, value::row_number::RowNumber};

use crate::{
	operator::state::{
		expiry::{ExpiryIndex, expiry_drop, tumbling_expiry_key},
		reaper::Reaper,
	},
	window::{
		engine::{
			AccumulatorEvent, BatchMeta, EmitKind, GroupMeta, KeyspaceFamily, MetaSweep, RetainedEntryKey,
			WindowReindex, WindowStateKey,
			config::WindowEngineConfig,
			group_hash, meta_key_for, note_when_expiry_capped,
			tumbling::{ExpiredWindow, TumblingBuckets, TumblingIndexEntry},
		},
		span::{WindowAnchor, WindowSpan},
	},
};

#[derive(Clone)]
struct ResolvedSlot {
	group: GroupId,
	key: EncodedKey,
}

type SlotResolved<G, S> = HashMap<(G, WindowSpan<S>), ResolvedSlot>;

struct PendingEmit<G, S> {
	group_id: GroupId,
	group: G,
	span: WindowSpan<S>,
	count: u64,
}

struct WindowSlots<K> {
	group: GroupId,
	header: usize,
	entries: BTreeMap<K, usize>,
}

struct RetainedSession<G, S, K> {
	batch: StateBatch,
	metas: BTreeMap<G, (usize, BatchMeta<S>)>,
	windows: HashMap<(G, WindowSpan<S>), WindowSlots<K>>,
}

fn slot_value<T: OperatorState>(batch: &StateBatch, slot: usize) -> Result<Option<T>> {
	match batch.value(slot) {
		Some(row) => Ok(Some(decode::<T>(row)?)),
		None => Ok(None),
	}
}

pub struct RetainedResult<G, S> {
	pub row_number: RowNumber,
	pub group: G,
	pub group_id: GroupId,
	pub span: WindowSpan<S>,
	pub kind: EmitKind,
	pub count: u64,
}

#[operator_state]
#[derive(Debug, Clone, Default, PartialEq, HeapSize)]
struct RetainedHeader {
	count: u64,
}

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct RetainedEntry<K, V> {
	key: K,
	value: V,
}

impl<K: HeapSize, V: HeapSize> HeapSize for RetainedEntry<K, V> {
	fn heap_size(&self) -> usize {
		self.key.heap_size() + self.value.heap_size()
	}
}

impl<G, S, K, V> Reaper for RetainedTumblingEngine<G, S, K, V>
where
	S: WindowAnchor + Hash,
{
	fn reap(&mut self, store: &mut dyn StateStore, keys: &[GroupStateKey]) -> Result<()> {
		store.state_remove_many(keys)
	}
}

pub struct RetainedTumblingEngine<G, S, K, V> {
	meta_sweep: MetaSweep,
	expire_batch: usize,
	expiry: ExpiryIndex<TumblingExpiry>,
	_pd: PhantomData<(G, S, K, V)>,
}

impl<G, S, K, V> RetainedTumblingEngine<G, S, K, V>
where
	G: Clone + Eq + Ord + Hash + Debug,
	S: WindowAnchor + Hash,
	K: Ord + Clone + Debug + HeapSize,
	V: Clone + Debug + PartialEq + HeapSize,
	G: StateCodec,
	K: StateCodec,
	GroupMeta<S>: OperatorState,
	TumblingIndexEntry<G, S>: OperatorState,
	RetainedEntry<K, V>: OperatorState,
{
	pub fn new(config: WindowEngineConfig) -> Self {
		Self {
			meta_sweep: MetaSweep::default(),
			expire_batch: config.expire_batch(),
			expiry: ExpiryIndex::default(),
			_pd: PhantomData,
		}
	}

	pub fn reindex_windows(
		&mut self,
		store: &mut dyn StateStore,
		entries: &[WindowReindex<'_, G, S>],
	) -> Result<()> {
		let mut drops = Vec::new();
		let mut sets = Vec::new();
		for entry in entries.iter().filter(|entry| entry.prior != entry.new) {
			let owner = group_hash(entry.group)?;
			let order = entry.window_start.order_key().to_order();
			if let Some(old) = entry.prior {
				drops.push(tumbling_expiry_key(old, owner, order));
			}
			if let Some(new) = entry.new {
				sets.push((
					tumbling_expiry_key(new, owner, order),
					TumblingIndexEntry {
						group: entry.group.clone(),
						window_start: entry.window_start,
						group_id: entry.id,
						slot_key: entry.slot_key.as_bytes().to_vec(),
					},
				));
			}
		}
		if !drops.is_empty() {
			store.state_remove_many(&drops)?;
		}
		if !sets.is_empty() {
			self.expiry.set_many(store, sets)?;
		}
		Ok(())
	}

	pub fn apply<SK>(
		&mut self,
		store: &mut dyn StateStore,
		buckets: TumblingBuckets<G, S, (K, V)>,
		order: &[(G, WindowSpan<S>)],
		slot_key: SK,
	) -> Result<Vec<RetainedResult<G, S>>>
	where
		SK: Fn(&G, S) -> (GroupId, EncodedKey),
	{
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		let slot_resolved = Self::resolve_slots(order, &slot_key);
		reifydb_assertions! {
			let ordered = slot_resolved.len();
			let bucketed = buckets.len();
			assert!(
				ordered == bucketed,
				"the resolution order must name every bucket exactly once; a bucket missing from it \
				 gets no row number and would be dropped from this batch, while a duplicate silently \
				 renumbers a window that already published under another row \
				 (order={ordered}, buckets={bucketed})"
			);
		}
		let mut session = Self::open_session(store, &buckets, &slot_resolved)?;
		let results = self.apply_events(store, &mut session, buckets, order)?;
		let RetainedSession {
			mut batch,
			metas,
			..
		} = session;
		for (slot, meta) in metas.into_values() {
			if let Some(bumped) = meta.bumped {
				batch.set(
					slot,
					GroupMeta {
						high_water: Some(bumped),
					}
					.encode_state()?,
				);
			}
		}
		store.state_write_batch(batch)?;
		Ok(results)
	}

	pub fn load_entries(
		&self,
		store: &mut dyn StateStore,
		windows: &[(GroupId, u64)],
	) -> Result<HashMap<GroupId, BTreeMap<K, V>>> {
		if windows.is_empty() {
			return Ok(HashMap::new());
		}
		let mut read: HashMap<GroupId, BTreeMap<K, V>> = HashMap::with_capacity(windows.len());
		let mut total: u64 = 0;
		for (group, count) in windows {
			if read.insert(*group, BTreeMap::new()).is_some() {
				return internal_err!("a retained entry read names window {:?} twice", group);
			}
			total = match total.checked_add(*count) {
				Some(total) => total,
				None => return internal_err!("retained window counts overflow the entry read"),
			};
		}
		let Ok(limit) = usize::try_from(total) else {
			return internal_err!("retained window counts overflow the entry read (total={})", total);
		};
		let ids: Vec<GroupId> = windows.iter().map(|(group, _)| *group).collect();
		let sweep =
			store.group_sweep_many(&ids, limit, KeyspaceMask::of([KeyspaceId::GUEST_RETAINED_ENTRY]))?;
		if !sweep.complete {
			return internal_err!(
				"retained windows hold more entries than their headers count (limit={})",
				limit
			);
		}
		for (key, row) in sweep.rows {
			let Some(entries) = key.group().and_then(|group| read.get_mut(&group)) else {
				return internal_err!(
					"a retained entry read returned a row of a window it did not name"
				);
			};
			let entry: RetainedEntry<K, V> = decode(&row)?;
			entries.insert(entry.key, entry.value);
		}
		for (group, count) in windows {
			let held = read[group].len() as u64;
			if held != *count {
				return internal_err!(
					"retained window {:?} holds {} entries but its header counts {}",
					group,
					held,
					count
				);
			}
		}
		Ok(read)
	}

	pub fn window_counts(&self, store: &mut dyn StateStore, windows: &[(GroupId, EncodedKey)]) -> Result<Vec<u64>> {
		if windows.is_empty() {
			return Ok(Vec::new());
		}
		let keys: Vec<GroupStateKey> = windows
			.iter()
			.map(|(group, slot)| {
				(&WindowStateKey::new(KeyspaceFamily::Guest, *group, slot.clone()))
					.into_group_state_key()
			})
			.collect();
		let mut counts = Vec::with_capacity(keys.len());
		for row in store.state_get_many(&keys)? {
			let count = match row {
				Some(row) => decode::<RetainedHeader>(&row)?.count,
				None => 0,
			};
			counts.push(count);
		}
		Ok(counts)
	}

	fn entry_key(group: GroupId, key: &K) -> Result<RetainedEntryKey> {
		Ok(RetainedEntryKey::new(group, EncodedKey::new(encode(key)?.body())))
	}

	fn open_session(
		store: &mut dyn StateStore,
		buckets: &TumblingBuckets<G, S, (K, V)>,
		slot_resolved: &SlotResolved<G, S>,
	) -> Result<RetainedSession<G, S, K>> {
		let mut keys: Vec<GroupStateKey> = Vec::new();
		let mut meta_slots: BTreeMap<G, usize> = BTreeMap::new();
		for (group, _) in buckets.keys() {
			if !meta_slots.contains_key(group) {
				meta_slots.insert(group.clone(), keys.len());
				keys.push((&meta_key_for(group_hash(group)?)).into_group_state_key());
			}
		}
		let mut windows: HashMap<(G, WindowSpan<S>), WindowSlots<K>> = HashMap::with_capacity(buckets.len());
		for (window, events) in buckets {
			let Some(ResolvedSlot {
				group: id,
				key,
			}) = slot_resolved.get(window)
			else {
				continue;
			};
			let header = keys.len();
			keys.push(
				(&WindowStateKey::new(KeyspaceFamily::Guest, *id, key.clone())).into_group_state_key()
			);
			let mut entries: BTreeMap<K, usize> = BTreeMap::new();
			for event in events {
				let (AccumulatorEvent::Add((k, _)) | AccumulatorEvent::Remove((k, _))) = event;
				entries.entry(k.clone()).or_default();
			}
			for (k, slot) in entries.iter_mut() {
				*slot = keys.len();
				keys.push((&Self::entry_key(*id, k)?).into_group_state_key());
			}
			windows.insert(
				window.clone(),
				WindowSlots {
					group: *id,
					header,
					entries,
				},
			);
		}
		let batch = store.state_batch(keys)?;
		let mut metas: BTreeMap<G, (usize, BatchMeta<S>)> = BTreeMap::new();
		for (group, slot) in meta_slots {
			let initial = slot_value::<GroupMeta<S>>(&batch, slot)?.and_then(|meta| meta.high_water);
			metas.insert(
				group,
				(
					slot,
					BatchMeta {
						initial,
						bumped: None,
					},
				),
			);
		}
		Ok(RetainedSession {
			batch,
			metas,
			windows,
		})
	}

	fn resolve_slots<SK>(order: &[(G, WindowSpan<S>)], slot_key: &SK) -> SlotResolved<G, S>
	where
		SK: Fn(&G, S) -> (GroupId, EncodedKey),
	{
		let mut resolved: SlotResolved<G, S> = HashMap::with_capacity(order.len());
		for (group, span) in order {
			let (id, key) = slot_key(group, span.start);
			resolved.insert(
				(group.clone(), *span),
				ResolvedSlot {
					group: id,
					key,
				},
			);
		}
		resolved
	}

	fn entry_slot(slots: &WindowSlots<K>, key: &K) -> usize {
		*slots.entries.get(key).expect("every event key of a window holds a slot in the session it opened")
	}

	fn current(
		batch: &StateBatch,
		slots: &WindowSlots<K>,
		folded: &BTreeMap<K, Option<V>>,
		key: &K,
	) -> Result<Option<V>> {
		match folded.get(key) {
			Some(value) => Ok(value.clone()),
			None => Ok(slot_value::<RetainedEntry<K, V>>(batch, Self::entry_slot(slots, key))?
				.map(|entry| entry.value)),
		}
	}

	fn fold_window(
		batch: &mut StateBatch,
		slots: &WindowSlots<K>,
		events: Vec<AccumulatorEvent<(K, V)>>,
	) -> Result<(u64, u64)> {
		let mut header: RetainedHeader = slot_value(batch, slots.header)?.unwrap_or_default();
		let before = header.count;
		let mut folded: BTreeMap<K, Option<V>> = BTreeMap::new();
		for event in events {
			match event {
				AccumulatorEvent::Add((k, v)) => {
					if Self::current(batch, slots, &folded, &k)?.is_none() {
						header.count += 1;
					}
					folded.insert(k, Some(v));
				}
				AccumulatorEvent::Remove((k, v)) => {
					if header.count == 0 {
						continue;
					}
					if Self::current(batch, slots, &folded, &k)?.as_ref() == Some(&v) {
						folded.insert(k, None);
						header.count -= 1;
					}
				}
			}
		}
		for (key, value) in folded {
			let slot = Self::entry_slot(slots, &key);
			match value {
				Some(value) => batch.set(
					slot,
					RetainedEntry {
						key,
						value,
					}
					.encode_state()?,
				),
				None => batch.remove(slot),
			}
		}
		let after = header.count;
		match after == 0 {
			true => batch.remove(slots.header),
			false => batch.set(slots.header, header.encode_state()?),
		}
		Ok((before, after))
	}

	fn apply_events(
		&mut self,
		store: &mut dyn StateStore,
		session: &mut RetainedSession<G, S, K>,
		mut buckets: TumblingBuckets<G, S, (K, V)>,
		order: &[(G, WindowSpan<S>)],
	) -> Result<Vec<RetainedResult<G, S>>> {
		let mut pending: Vec<PendingEmit<G, S>> = Vec::new();

		for ordered in order {
			let Some(events) = buckets.remove(ordered) else {
				continue;
			};
			let (group, span) = ordered.clone();
			session.metas
				.get_mut(&group)
				.expect("every bucket group holds a meta slot in the session it opened")
				.1
				.observe(span.start);

			let Some(slots) = session.windows.get(ordered) else {
				continue;
			};
			let (before, after) = Self::fold_window(&mut session.batch, slots, events)?;

			if before > 0 || after > 0 {
				pending.push(PendingEmit {
					group_id: slots.group,
					group,
					span,
					count: after,
				});
			}
		}
		reifydb_assertions! {
			assert!(
				buckets.is_empty(),
				"the resolution order must drain every bucket; a leftover bucket's events were \
				 silently dropped and its window never gets a row number (leftovers={})",
				buckets.len()
			);
		}

		if pending.is_empty() {
			return Ok(Vec::new());
		}
		let groups: Vec<GroupId> = pending.iter().map(|p| p.group_id).collect();
		let rows = store.get_or_create_row_numbers_for_groups(&groups)?;
		reifydb_assertions! {
			let requested = groups.len();
			let returned = rows.len();
			assert!(
				returned == requested,
				"the identity batch must return one row per publishing window; a short batch makes the \
				 zip below drop the tail, so those windows publish nothing while their entries \
				 already advanced (requested={requested}, returned={returned})"
			);
		}

		let mut withdrawn: Vec<GroupId> = Vec::new();
		let mut results: Vec<RetainedResult<G, S>> = Vec::with_capacity(pending.len());
		for (emit, (row_number, is_new)) in pending.into_iter().zip(rows) {
			let kind = if emit.count == 0 {
				reifydb_assertions! {
					let group_id = emit.group_id;
					assert!(
						!is_new,
						"a window holding published entries must already own its mapping; minting \
						 one here means the identity was released while the row it addresses \
						 was still live, and this withdrawal names a row no sink can find \
						 (group={group_id:?}, row={row_number:?})"
					);
				}
				withdrawn.push(emit.group_id);
				EmitKind::Remove
			} else if is_new {
				EmitKind::Insert
			} else {
				EmitKind::Update
			};
			results.push(RetainedResult {
				row_number,
				group: emit.group,
				group_id: emit.group_id,
				span: emit.span,
				kind,
				count: emit.count,
			});
		}
		if !withdrawn.is_empty() {
			store.remove_row_numbers_for_groups(&withdrawn)?;
		}
		Ok(results)
	}

	pub fn expire(&mut self, store: &mut dyn StateStore, threshold: u64) -> Result<Vec<ExpiredWindow<G, S>>> {
		let due: Vec<(GroupStateKey, TumblingIndexEntry<G, S>)> =
			self.expiry.due(store, threshold, self.expire_batch)?;

		let mut out: Vec<ExpiredWindow<G, S>> = Vec::new();
		for (index_key, entry) in due {
			expiry_drop(store, &index_key)?;
			out.push(ExpiredWindow {
				group: entry.group,
				group_id: entry.group_id,
				window_start: entry.window_start,
			});
		}
		self.expiry.settle(store)?;
		reifydb_assertions! {
			for entry in crate::operator::state::expiry::expiry_all::<TumblingExpiry, TumblingIndexEntry<G, S>>(store)? {
				assert!(
					!out.iter().any(|window| window.group_id == entry.group_id),
					"the expiry index still holds a row for a group that is about to be queued for \
					 reaping; a window group must own exactly one index row, or reaping it orphans \
					 the rows left behind under a group id nothing resolves again"
				);
			}
		}
		note_when_expiry_capped(out.len(), self.expire_batch);
		Ok(out)
	}

	pub fn earliest_expiry(&mut self, store: &mut dyn StateStore) -> Result<Option<u64>> {
		self.expiry.earliest(store)
	}

	pub fn expire_meta(&mut self, store: &mut dyn StateStore, threshold: u64) -> Result<usize> {
		self.meta_sweep.sweep::<GroupMeta<S>>(store, threshold)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::key::operator::state::GroupId;
	use reifydb_value::{Result, factory::time::at_millis, util::hash::Hash128, value::datetime::DateTime};

	use super::{RetainedHeader, RetainedResult, RetainedTumblingEngine};
	use crate::{
		operator::{
			state::{
				mock::MockStore,
				reaper::{StoreReaper, enqueue, reap_group},
			},
			state_access::get,
		},
		window::{
			accumulator::invertible::retained_map::RetainedAccumulator,
			engine::{
				AccumulatorEvent::{self, Add, Remove},
				EmitKind, KeyspaceFamily, WindowReindex, WindowResult, WindowStateKey,
				config::WindowEngineConfig,
				tumbling::{TumblingBuckets, TumblingEngine},
			},
			span::WindowSpan,
		},
	};

	type Engine = RetainedTumblingEngine<u32, DateTime, u64, i64>;
	type Blob = TumblingEngine<u32, DateTime, RetainedAccumulator<u64, i64>>;
	type Event = AccumulatorEvent<(u64, i64)>;

	fn test_config() -> WindowEngineConfig {
		WindowEngineConfig::builder().family(KeyspaceFamily::Guest).build()
	}

	fn group_slot(group: &u32, window_start: DateTime) -> (GroupId, EncodedKey) {
		(GroupId::window(Hash128(u128::from(*group)), window_start.to_order()), EncodedKey::new(Vec::new()))
	}

	fn bucket(events: Vec<Event>) -> TumblingBuckets<u32, DateTime, (u64, i64)> {
		BTreeMap::from([((1u32, WindowSpan::new(at_millis(0), at_millis(10))), events)])
	}

	fn apply(engine: &mut Engine, store: &mut MockStore, events: Vec<Event>) -> Vec<RetainedResult<u32, DateTime>> {
		let buckets = bucket(events);
		let order: Vec<_> = buckets.keys().cloned().collect();
		engine.apply(store, buckets, &order, group_slot).expect("apply")
	}

	fn apply_blob(
		engine: &mut Blob,
		store: &mut MockStore,
		events: Vec<Event>,
	) -> Vec<WindowResult<u32, DateTime, BTreeMap<u64, i64>>> {
		let buckets = bucket(events);
		let order: Vec<_> = buckets.keys().cloned().collect();
		engine.apply(store, buckets, &order, group_slot, RetainedAccumulator::default).expect("apply")
	}

	fn kinds(results: &[RetainedResult<u32, DateTime>]) -> Vec<EmitKind> {
		results.iter().map(|r| r.kind).collect()
	}

	fn stored(engine: &Engine, store: &mut MockStore, window: (GroupId, EncodedKey)) -> Result<BTreeMap<u64, i64>> {
		let id = window.0;
		let count = engine.window_counts(store, &[window])?[0];
		Ok(engine.load_entries(store, &[(id, count)])?.remove(&id).expect("a read window always answers"))
	}

	#[test]
	fn parity_with_blob_engine_over_adds_updates_removes() {
		// The per-entry engine replaces the blob engine, so any drift in row number, emit kind or entries is a
		// wrong row downstream.
		let batches: Vec<fn() -> Vec<Event>> = vec![
			|| vec![Add((1, 10)), Add((2, 20))],
			|| vec![Add((1, 11)), Remove((2, 20))],
			|| vec![Remove((1, 11))],
			|| vec![Add((3, 30))],
		];
		let (mut entries_store, mut blob_store) = (MockStore::default(), MockStore::default());
		let mut engine = Engine::new(test_config());
		let mut blob = Blob::new(test_config());
		for (step, batch) in batches.iter().enumerate() {
			let ours = apply(&mut engine, &mut entries_store, batch());
			let theirs = apply_blob(&mut blob, &mut blob_store, batch());
			assert_eq!(ours.len(), 1, "step {step}: one window publishes");
			assert_eq!(theirs.len(), 1, "step {step}: one window publishes");
			assert_eq!(ours[0].row_number, theirs[0].row_number, "step {step}");
			assert_eq!(ours[0].kind, theirs[0].kind, "step {step}");
			let expected = match theirs[0].kind {
				EmitKind::Remove => BTreeMap::new(),
				_ => theirs[0].value.clone(),
			};
			assert_eq!(
				stored(&engine, &mut entries_store, group_slot(&1, at_millis(0))).unwrap(),
				expected,
				"step {step}"
			);
		}
	}

	#[test]
	fn overwriting_a_key_keeps_count() {
		// Counting an overwrite as a new entry keeps an emptied window alive, so its removal never publishes.
		let mut store = MockStore::default();
		let mut engine = Engine::new(test_config());
		assert_eq!(kinds(&apply(&mut engine, &mut store, vec![Add((1, 10))])), vec![EmitKind::Insert]);
		assert_eq!(kinds(&apply(&mut engine, &mut store, vec![Add((1, 99))])), vec![EmitKind::Update]);
		assert_eq!(kinds(&apply(&mut engine, &mut store, vec![Remove((1, 99))])), vec![EmitKind::Remove]);
	}

	#[test]
	fn removing_a_superseded_value_keeps_the_entry() {
		// A retraction of an overwritten value must not delete the newer value that replaced it.
		let mut store = MockStore::default();
		let mut engine = Engine::new(test_config());
		apply(&mut engine, &mut store, vec![Add((1, 10))]);
		apply(&mut engine, &mut store, vec![Add((1, 99))]);
		let out = apply(&mut engine, &mut store, vec![Remove((1, 10))]);
		assert_eq!(kinds(&out), vec![EmitKind::Update]);
		assert_eq!(
			stored(&engine, &mut store, group_slot(&1, at_millis(0))).unwrap(),
			BTreeMap::from([(1, 99)])
		);
	}

	#[test]
	fn a_retraction_that_matches_nothing_is_never_applied() {
		// A retraction into an empty window has nothing to undo; applying it corrupts the count so the window's
		// later removal never publishes.
		let mut store = MockStore::default();
		let mut engine = Engine::new(test_config());
		let out = apply(&mut engine, &mut store, vec![Remove((1, 10))]);
		assert!(out.is_empty(), "an empty window publishes nothing");
		assert!(
			stored(&engine, &mut store, group_slot(&1, at_millis(0))).unwrap().is_empty(),
			"nothing is stored"
		);
		assert_eq!(kinds(&apply(&mut engine, &mut store, vec![Add((1, 10))])), vec![EmitKind::Insert]);
		assert_eq!(
			stored(&engine, &mut store, group_slot(&1, at_millis(0))).unwrap(),
			BTreeMap::from([(1, 10)])
		);
		assert_eq!(kinds(&apply(&mut engine, &mut store, vec![Remove((1, 10))])), vec![EmitKind::Remove]);
	}

	#[test]
	fn apply_never_range_reads() {
		// A range read per batch brings back the whole-window cost this engine exists to remove.
		let mut store = MockStore::default();
		let mut engine = Engine::new(test_config());
		apply(&mut engine, &mut store, vec![Add((1, 10)), Add((2, 20)), Add((3, 30))]);
		assert_eq!(
			stored(&engine, &mut store, group_slot(&1, at_millis(0))).unwrap().len(),
			3,
			"precondition: entries exist"
		);
		let visited = store.rows_visited();
		apply(&mut engine, &mut store, vec![Add((4, 40)), Remove((1, 10))]);
		assert_eq!(store.rows_visited(), visited, "apply must touch entries by point reads only");
	}

	#[test]
	fn entries_and_header_are_reaped_with_the_window() {
		// Entries left behind after the reap leak one row per retained key per closed window.
		let mut store = MockStore::default();
		let mut engine = Engine::new(test_config());
		let out = apply(&mut engine, &mut store, vec![Add((1, 10)), Add((2, 20))]);
		let (group, slot) = group_slot(&1, at_millis(0));
		assert_eq!(out[0].group_id, group);
		assert_eq!(
			stored(&engine, &mut store, (group, slot.clone())).unwrap().len(),
			2,
			"precondition: entries exist"
		);
		engine.reindex_windows(
			&mut store,
			&[WindowReindex {
				group: &1,
				window_start: at_millis(0),
				id: group,
				slot_key: &slot,
				prior: None,
				new: Some(10),
			}],
		)
		.unwrap();

		let expired = engine.expire(&mut store, 10).unwrap();
		assert_eq!(expired.len(), 1);
		enqueue(&mut store, expired[0].group_id).unwrap();
		reap_group(&mut store, expired[0].group_id, &mut StoreReaper, 256).unwrap();

		assert_eq!(
			stored(&engine, &mut store, (group, slot.clone())).expect("the reaped window reads"),
			BTreeMap::new(),
			"every entry is reaped"
		);
		let header: Option<RetainedHeader> =
			get(&mut store, &WindowStateKey::new(KeyspaceFamily::Guest, group, slot)).unwrap();
		assert!(header.is_none(), "the header is reaped");
	}
}
