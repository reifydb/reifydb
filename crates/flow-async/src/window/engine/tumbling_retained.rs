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
	row::{
		operator::state::{OperatorState, StateCodec, decode, encode},
		pod::EncodedPodRow,
	},
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
use reifydb_value::{
	Result, reifydb_assertions,
	value::{datetime::DateTime, row_number::RowNumber},
};

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

struct PendingEmit<G, S, O> {
	group_id: GroupId,
	group: G,
	span: WindowSpan<S>,
	header_key: GroupStateKey,
	header: RetainedHeader<O>,
}

struct WindowSlots<K> {
	group: GroupId,
	header: usize,
	header_key: GroupStateKey,
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

pub struct RetainedResult<G, S, O> {
	pub row_number: RowNumber,
	pub group: G,
	pub group_id: GroupId,
	pub span: WindowSpan<S>,
	pub kind: EmitKind,
	pub value: Option<O>,
	pub prior: Option<O>,
	pub first_time: DateTime,
}

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct RetainedHeader<O> {
	count: u64,
	row: Option<RowNumber>,
	first_time: DateTime,
	last_output: Option<O>,
}

impl<O: HeapSize> HeapSize for RetainedHeader<O> {
	fn heap_size(&self) -> usize {
		self.last_output.heap_size()
	}
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

impl<G, S, K, V, O> Reaper for RetainedTumblingEngine<G, S, K, V, O>
where
	S: WindowAnchor + Hash,
	RetainedHeader<O>: OperatorState,
{
	fn reap(&mut self, store: &mut dyn StateStore, rows: &[(GroupStateKey, EncodedPodRow)]) -> Result<()> {
		let header_keyspace =
			KeyspaceFamily::Guest.keyspace(KeyspaceId::ACCUMULATOR, KeyspaceId::GUEST_ACCUMULATOR);
		let mut kept: Vec<(GroupId, RowNumber)> = Vec::new();
		for (key, row) in rows {
			if key.keyspace() != Some(header_keyspace) {
				continue;
			}
			let header: RetainedHeader<O> = decode(row)?;
			if let (Some(group), Some(number)) = (key.group(), header.row) {
				kept.push((group, number));
			}
		}
		if !kept.is_empty() {
			store.set_row_numbers_for_groups(&kept)?;
		}
		let keys: Vec<GroupStateKey> = rows.iter().map(|(key, _)| key.clone()).collect();
		store.state_remove(&keys)
	}
}

pub struct RetainedTumblingEngine<G, S, K, V, O> {
	meta_sweep: MetaSweep,
	expire_batch: usize,
	expiry: ExpiryIndex<TumblingExpiry>,
	_pd: PhantomData<(G, S, K, V, fn() -> O)>,
}

impl<G, S, K, V, O> RetainedTumblingEngine<G, S, K, V, O>
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
	O: Clone,
	RetainedHeader<O>: OperatorState,
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
			store.state_remove(&drops)?;
		}
		if !sets.is_empty() {
			self.expiry.set(store, sets)?;
		}
		Ok(())
	}

	pub fn apply<SK, BO>(
		&mut self,
		store: &mut dyn StateStore,
		buckets: TumblingBuckets<G, S, (K, V)>,
		order: &[(G, WindowSpan<S>)],
		slot_key: SK,
		build_output: BO,
	) -> Result<Vec<RetainedResult<G, S, O>>>
	where
		SK: Fn(&G, S) -> (GroupId, EncodedKey),
		BO: Fn(&G, WindowSpan<S>, &BTreeMap<K, V>) -> Option<O>,
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
		let pending = self.apply_events(store, &mut session, buckets, order)?;
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
		self.publish(store, pending, build_output)
	}

	fn publish<BO>(
		&self,
		store: &mut dyn StateStore,
		pending: Vec<PendingEmit<G, S, O>>,
		build_output: BO,
	) -> Result<Vec<RetainedResult<G, S, O>>>
	where
		BO: Fn(&G, WindowSpan<S>, &BTreeMap<K, V>) -> Option<O>,
	{
		if pending.is_empty() {
			return Ok(Vec::new());
		}
		let unnumbered: Vec<usize> = pending
			.iter()
			.enumerate()
			.filter(|(_, emit)| emit.header.count > 0 && emit.header.row.is_none())
			.map(|(index, _)| index)
			.collect();
		let mut assigned: HashMap<usize, (RowNumber, bool)> = HashMap::with_capacity(unnumbered.len());
		if !unnumbered.is_empty() {
			let groups: Vec<GroupId> = unnumbered.iter().map(|&index| pending[index].group_id).collect();
			let surviving = store.get_row_numbers_for_groups(&groups)?;
			reifydb_assertions! {
				let requested = groups.len();
				let returned = surviving.len();
				assert!(
					returned == requested,
					"the identity lookup must answer every unnumbered window; a short answer leaves a \
					 publishing window without a row (requested={requested}, returned={returned})"
				);
			}
			let fresh = surviving.iter().filter(|row| row.is_none()).count();
			let mut next = match fresh {
				0 => 0,
				count => store.mint_row_numbers(count)?.0,
			};
			for (&index, row) in unnumbered.iter().zip(surviving) {
				let numbered = match row {
					Some(row) => (row, false),
					None => {
						let row = RowNumber(next);
						next += 1;
						(row, true)
					}
				};
				assigned.insert(index, numbered);
			}
		}

		let due: Vec<(GroupId, u64)> = pending
			.iter()
			.filter(|emit| emit.header.count > 0)
			.map(|emit| (emit.group_id, emit.header.count))
			.collect();
		let mut read = self.load_entries(store, &due)?;

		let mut withdrawn: Vec<GroupId> = Vec::new();
		let mut headers: Vec<(GroupStateKey, EncodedPodRow)> = Vec::with_capacity(pending.len());
		let mut results: Vec<RetainedResult<G, S, O>> = Vec::with_capacity(pending.len());
		for (index, emit) in pending.into_iter().enumerate() {
			let PendingEmit {
				group_id,
				group,
				span,
				header_key,
				mut header,
			} = emit;
			let prior = header.last_output.take();
			if header.count == 0 {
				let Some(row_number) = header.row else {
					return internal_err!(
						"a retained window holding published entries must already own its row; \
						 withdrawing it without one would leave its published row behind (group={:?})",
						group_id
					);
				};
				withdrawn.push(group_id);
				results.push(RetainedResult {
					row_number,
					group,
					group_id,
					span,
					kind: EmitKind::Remove,
					value: None,
					prior,
					first_time: header.first_time,
				});
				continue;
			}
			let (row_number, kind) = match (header.row, assigned.remove(&index)) {
				(Some(row_number), _) => (row_number, EmitKind::Update),
				(None, Some((row_number, true))) => (row_number, EmitKind::Insert),
				(None, Some((row_number, false))) => (row_number, EmitKind::Update),
				(None, None) => {
					return internal_err!(
						"a publishing retained window was left without a row (group={:?})",
						group_id
					);
				}
			};
			let Some(entries) = read.remove(&group_id) else {
				return internal_err!(
					"the entry read skipped publishing retained window {:?}",
					group_id
				);
			};
			let value = build_output(&group, span, &entries);
			header.row = Some(row_number);
			header.last_output = value.clone();
			headers.push((header_key, header.encode_state()?));
			results.push(RetainedResult {
				row_number,
				group,
				group_id,
				span,
				kind,
				value,
				prior,
				first_time: header.first_time,
			});
		}
		if !headers.is_empty() {
			store.state_set(headers)?;
		}
		if !withdrawn.is_empty() {
			store.remove_row_number_for_group(&withdrawn)?;
		}
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
		let sweep = store.group_sweep(&ids, limit, KeyspaceMask::of([KeyspaceId::GUEST_RETAINED_ENTRY]))?;
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
			let header_key =
				(&WindowStateKey::new(KeyspaceFamily::Guest, *id, key.clone())).into_group_state_key();
			keys.push(header_key.clone());
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
					header_key,
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
		first_time: DateTime,
	) -> Result<(u64, RetainedHeader<O>)> {
		let mut header: RetainedHeader<O> =
			slot_value(batch, slots.header)?.unwrap_or_else(|| RetainedHeader {
				count: 0,
				row: None,
				first_time,
				last_output: None,
			});
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
		if header.count == 0 {
			batch.remove(slots.header);
		}
		Ok((before, header))
	}

	fn apply_events(
		&mut self,
		store: &mut dyn StateStore,
		session: &mut RetainedSession<G, S, K>,
		mut buckets: TumblingBuckets<G, S, (K, V)>,
		order: &[(G, WindowSpan<S>)],
	) -> Result<Vec<PendingEmit<G, S, O>>> {
		let now = store.batch_time();
		let mut pending: Vec<PendingEmit<G, S, O>> = Vec::new();

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
			let first_time = span.start.as_time().unwrap_or(now);
			let (before, header) = Self::fold_window(&mut session.batch, slots, events, first_time)?;

			if before > 0 || header.count > 0 {
				pending.push(PendingEmit {
					group_id: slots.group,
					group,
					span,
					header_key: slots.header_key.clone(),
					header,
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
		Ok(pending)
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
	use std::{collections::BTreeMap, slice::from_ref};

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{
		key::operator::state::{GroupId, KeyspaceMask},
		state::timer::StateStore,
	};
	use reifydb_value::{Result, util::hash::Hash128};

	use super::{RetainedHeader, RetainedResult, RetainedTumblingEngine};
	use crate::{
		factory::coord::event_coord_at_millis,
		operator::{
			state::{
				mock::MockStore,
				reaper::{Reaper, StoreReaper, enqueue, reap_group},
				seal::coord::Coord,
			},
			state_access::get,
		},
		window::{
			accumulator::invertible::retained_map::RetainedAccumulator,
			coord::DateTimeCoord,
			engine::{
				AccumulatorEvent::{self, Add, Remove},
				EmitKind, KeyspaceFamily, WindowReindex, WindowResult, WindowStateKey,
				config::WindowEngineConfig,
				tumbling::{TumblingBuckets, TumblingEngine},
			},
			span::{WindowAnchor, WindowSpan},
		},
	};

	type Out = BTreeMap<u64, i64>;
	type Engine = RetainedTumblingEngine<u32, DateTimeCoord, u64, i64, Out>;
	type Blob = TumblingEngine<u32, DateTimeCoord, RetainedAccumulator<u64, i64>>;
	type Event = AccumulatorEvent<(u64, i64)>;

	fn test_config() -> WindowEngineConfig {
		WindowEngineConfig::builder().family(KeyspaceFamily::Guest).build()
	}

	fn group_slot(group: &u32, window_start: DateTimeCoord) -> (GroupId, EncodedKey) {
		(GroupId::window(Hash128(u128::from(*group)), window_start.to_order()), EncodedKey::new(Vec::new()))
	}

	fn bucket(events: Vec<Event>) -> TumblingBuckets<u32, DateTimeCoord, (u64, i64)> {
		BTreeMap::from([((1u32, WindowSpan::new(event_coord_at_millis(0), event_coord_at_millis(10))), events)])
	}

	fn apply(
		engine: &mut Engine,
		store: &mut MockStore,
		events: Vec<Event>,
	) -> Vec<RetainedResult<u32, DateTimeCoord, Out>> {
		let buckets = bucket(events);
		let order: Vec<_> = buckets.keys().cloned().collect();
		engine.apply(store, buckets, &order, group_slot, |_, _, entries| Some(entries.clone())).expect("apply")
	}

	fn apply_blob(
		engine: &mut Blob,
		store: &mut MockStore,
		events: Vec<Event>,
	) -> Vec<WindowResult<u32, DateTimeCoord, BTreeMap<u64, i64>>> {
		let buckets = bucket(events);
		let order: Vec<_> = buckets.keys().cloned().collect();
		engine.apply(store, buckets, &order, group_slot, RetainedAccumulator::default).expect("apply")
	}

	fn kinds(results: &[RetainedResult<u32, DateTimeCoord, Out>]) -> Vec<EmitKind> {
		results.iter().map(|r| r.kind).collect()
	}

	fn stored(engine: &Engine, store: &mut MockStore, window: (GroupId, EncodedKey)) -> Result<BTreeMap<u64, i64>> {
		let (id, slot) = window;
		let header: Option<RetainedHeader<Out>> =
			get(store, from_ref(&WindowStateKey::new(KeyspaceFamily::Guest, id, slot)))?.remove(0);
		let count = header.map_or(0, |header| header.count);
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
			assert_eq!(
				ours[0].value,
				(theirs[0].kind != EmitKind::Remove).then(|| theirs[0].value.clone()),
				"step {step}"
			);
			let expected = match theirs[0].kind {
				EmitKind::Remove => BTreeMap::new(),
				_ => theirs[0].value.clone(),
			};
			assert_eq!(
				stored(&engine, &mut entries_store, group_slot(&1, event_coord_at_millis(0))).unwrap(),
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
			stored(&engine, &mut store, group_slot(&1, event_coord_at_millis(0))).unwrap(),
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
			stored(&engine, &mut store, group_slot(&1, event_coord_at_millis(0))).unwrap().is_empty(),
			"nothing is stored"
		);
		assert_eq!(kinds(&apply(&mut engine, &mut store, vec![Add((1, 10))])), vec![EmitKind::Insert]);
		assert_eq!(
			stored(&engine, &mut store, group_slot(&1, event_coord_at_millis(0))).unwrap(),
			BTreeMap::from([(1, 10)])
		);
		assert_eq!(kinds(&apply(&mut engine, &mut store, vec![Remove((1, 10))])), vec![EmitKind::Remove]);
	}

	#[test]
	fn apply_reads_only_the_publishing_windows_own_entries() {
		// A range read past the window's own entries brings back the whole-window cost this engine exists to
		// remove.
		let mut store = MockStore::default();
		let mut engine = Engine::new(test_config());
		apply(&mut engine, &mut store, vec![Add((1, 10)), Add((2, 20)), Add((3, 30))]);
		assert_eq!(
			stored(&engine, &mut store, group_slot(&1, event_coord_at_millis(0))).unwrap().len(),
			3,
			"precondition: entries exist"
		);
		let visited = store.rows_visited();
		apply(&mut engine, &mut store, vec![Add((4, 40)), Remove((1, 10))]);
		assert_eq!(
			store.rows_visited(),
			visited + 3,
			"apply reads only the publishing window's own entries, once"
		);
	}

	#[test]
	fn a_retained_update_carries_the_output_published_before_it_as_prior() {
		// The driver emits prior as the update's pre-image, so a lost or stale prior corrupts every retained
		// sink row.
		let mut store = MockStore::default();
		let mut engine = Engine::new(test_config());
		let start = event_coord_at_millis(0).as_time().unwrap();

		let inserted = apply(&mut engine, &mut store, vec![Add((1, 10))]);
		let updated = apply(&mut engine, &mut store, vec![Add((2, 20))]);
		let removed = apply(&mut engine, &mut store, vec![Remove((1, 10)), Remove((2, 20))]);

		assert_eq!(kinds(&inserted), vec![EmitKind::Insert]);
		assert_eq!(inserted[0].prior, None, "an insert has no pre-image");
		assert_eq!(inserted[0].value, Some(BTreeMap::from([(1, 10)])));

		assert_eq!(kinds(&updated), vec![EmitKind::Update]);
		assert_eq!(updated[0].prior, Some(BTreeMap::from([(1, 10)])));
		assert_eq!(updated[0].value, Some(BTreeMap::from([(1, 10), (2, 20)])));

		assert_eq!(kinds(&removed), vec![EmitKind::Remove]);
		assert_eq!(
			removed[0].prior,
			Some(BTreeMap::from([(1, 10), (2, 20)])),
			"a remove withdraws what was published"
		);
		assert_eq!(removed[0].value, None);

		for (step, result) in [&inserted[0], &updated[0], &removed[0]].into_iter().enumerate() {
			assert_eq!(
				result.first_time, start,
				"step {step}: the first time is fixed at the window start"
			);
			assert_eq!(
				result.row_number, inserted[0].row_number,
				"step {step}: one row for the window's life"
			);
		}
	}

	#[test]
	fn a_reaped_retained_window_hands_its_row_to_its_mapping() {
		// Phase 1 erases the header that holds the row; without the move a window woken before phase 2 inserts
		// a duplicate.
		let mut store = MockStore::default();
		let mut engine = Engine::new(test_config());
		let published = apply(&mut engine, &mut store, vec![Add((1, 10))]);
		let (group, slot) = group_slot(&1, event_coord_at_millis(0));
		assert!(!store.contains_row_mapping(group), "precondition: the hot path writes no mapping");
		engine.reindex_windows(
			&mut store,
			&[WindowReindex {
				group: &1,
				window_start: event_coord_at_millis(0),
				id: group,
				slot_key: &slot,
				prior: None,
				new: Some(10),
			}],
		)
		.unwrap();
		assert_eq!(engine.expire(&mut store, 10).unwrap().len(), 1, "precondition: the window expires");

		let swept = store.group_sweep(&[group], usize::MAX, KeyspaceMask::data()).unwrap().rows;
		assert!(!swept.is_empty(), "precondition: the window holds data to reap");
		Reaper::reap(&mut engine, &mut store, &swept).unwrap();

		assert!(store.contains_row_mapping(group), "the reap must move the header's row into the mapping");
		let woken = apply(&mut engine, &mut store, vec![Add((3, 30))]);
		assert_eq!(kinds(&woken), vec![EmitKind::Update], "the sink still holds the row, so this is an update");
		assert_eq!(woken[0].row_number, published[0].row_number, "the woken window keeps the row it published");
	}

	#[test]
	fn entries_and_header_are_reaped_with_the_window() {
		// Entries left behind after the reap leak one row per retained key per closed window.
		let mut store = MockStore::default();
		let mut engine = Engine::new(test_config());
		let out = apply(&mut engine, &mut store, vec![Add((1, 10)), Add((2, 20))]);
		let (group, slot) = group_slot(&1, event_coord_at_millis(0));
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
				window_start: event_coord_at_millis(0),
				id: group,
				slot_key: &slot,
				prior: None,
				new: Some(10),
			}],
		)
		.unwrap();

		let expired = engine.expire(&mut store, 10).unwrap();
		assert_eq!(expired.len(), 1);
		enqueue(&mut store, &[expired[0].group_id]).unwrap();
		reap_group(&mut store, expired[0].group_id, &mut StoreReaper, 256).unwrap();

		assert_eq!(
			stored(&engine, &mut store, (group, slot.clone())).expect("the reaped window reads"),
			BTreeMap::new(),
			"every entry is reaped"
		);
		let header: Option<RetainedHeader<Out>> =
			get(&mut store, from_ref(&WindowStateKey::new(KeyspaceFamily::Guest, group, slot)))
				.unwrap()
				.remove(0);
		assert!(header.is_none(), "the header is reaped");
	}
}
