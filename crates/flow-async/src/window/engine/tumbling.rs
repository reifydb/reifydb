// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	fmt::Debug,
	hash::Hash,
	marker::PhantomData,
};

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{
		operator::state::{OperatorState, StateCodec, decode},
		pod::EncodedPodRow,
	},
};
use reifydb_core::{
	internal_err,
	key::operator::{
		keyspace::expiry::TumblingExpiry,
		state::{GroupId, GroupStateKey, KeyspaceId},
	},
	metrics::heap::HeapSize,
	state::timer::StateStore,
};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result, reifydb_assertions,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::{
	operator::{
		state::{
			expiry::{ExpiryIndex, tumbling_expiry_key},
			reaper::Reaper,
		},
		state_access::batch,
	},
	window::{
		accumulator::{AccumulatorEvents, CountedAccumulator, WindowAccumulator},
		engine::{
			EmitKind, KeyspaceFamily, WindowReindex, WindowResult, WindowStateKey,
			config::WindowEngineConfig, group_hash, note_when_expiry_capped,
		},
		span::{WindowAnchor, WindowSpan},
	},
};

pub type TumblingBuckets<G, S, X> = BTreeMap<(G, WindowSpan<S>), AccumulatorEvents<X>>;

#[derive(Clone)]
struct ResolvedSlot {
	group: GroupId,
	key: EncodedKey,
}

type SlotResolved<G, S> = HashMap<(G, WindowSpan<S>), ResolvedSlot>;

struct PendingEmit<G, S, Output> {
	group_id: GroupId,
	group: G,
	span: WindowSpan<S>,
	value: Output,
	prior: Option<Output>,
	withdraw: bool,
}

pub struct ExpiredWindow<G, S> {
	pub group: G,
	pub group_id: GroupId,
	pub window_start: S,
}

#[operator_state]
#[derive(Debug, Clone)]
pub struct WindowSlot<A> {
	pub row: Option<RowNumber>,
	pub first_time: DateTime,
	pub acc: CountedAccumulator<A>,
}

impl<A: HeapSize> HeapSize for WindowSlot<A> {
	fn heap_size(&self) -> usize {
		self.acc.heap_size()
	}
}

#[operator_state]
#[derive(Clone)]
pub struct TumblingIndexEntry<G, S> {
	pub(crate) group: G,
	pub(crate) window_start: S,
	pub(crate) group_id: GroupId,
	pub(crate) slot_key: Vec<u8>,
}

impl<G, S, Accumulator> Reaper for TumblingEngine<G, S, Accumulator>
where
	S: WindowAnchor + Hash,
	Accumulator: WindowAccumulator,
{
	fn reap(&mut self, store: &mut dyn StateStore, rows: &[(GroupStateKey, EncodedPodRow)]) -> Result<()> {
		let window_keyspace = self.family.keyspace(KeyspaceId::ACCUMULATOR, KeyspaceId::GUEST_ACCUMULATOR);
		let mut kept: Vec<(GroupId, RowNumber)> = Vec::new();
		for (key, row) in rows {
			if key.keyspace() != Some(window_keyspace) {
				continue;
			}
			let window: WindowSlot<Accumulator> = decode(row)?;
			if let (Some(group), Some(number)) = (key.group(), window.row) {
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

pub struct TumblingEngine<G, S, Accumulator> {
	family: KeyspaceFamily,
	expire_batch: usize,
	dropped_retractions: u64,
	expiry: ExpiryIndex<TumblingExpiry>,
	earliest: Option<Option<u64>>,
	_pd: PhantomData<(G, S, Accumulator)>,
}

impl<G, S, Accumulator> TumblingEngine<G, S, Accumulator>
where
	G: Clone + Eq + Ord + Hash + Debug,
	S: WindowAnchor + Hash,
	Accumulator: WindowAccumulator,
	G: StateCodec,
	TumblingIndexEntry<G, S>: OperatorState,
{
	pub fn new(config: WindowEngineConfig) -> Self {
		Self {
			family: config.family(),
			expire_batch: config.expire_batch(),
			dropped_retractions: 0,
			expiry: ExpiryIndex::default(),
			earliest: None,
			_pd: PhantomData,
		}
	}

	pub fn dropped_retractions(&self) -> u64 {
		self.dropped_retractions
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
				if self.earliest.is_some_and(|known| known.is_none_or(|earliest| old <= earliest)) {
					self.earliest = None;
				}
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
				self.earliest = self
					.earliest
					.map(|known| Some(known.map_or(new, |earliest| earliest.min(new))));
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

	pub fn apply<K, NA>(
		&mut self,
		store: &mut dyn StateStore,
		buckets: TumblingBuckets<G, S, Accumulator::Contributions>,
		order: &[(G, WindowSpan<S>)],
		slot_key: K,
		new_accumulator: NA,
	) -> Result<Vec<WindowResult<G, S, Accumulator::Output>>>
	where
		K: Fn(&G, S) -> (GroupId, EncodedKey),
		NA: Fn() -> Accumulator,
	{
		self.dropped_retractions = 0;
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
		self.apply_events(store, buckets, order, &slot_resolved, &new_accumulator)
	}

	fn resolve_slots<K>(order: &[(G, WindowSpan<S>)], slot_key: &K) -> SlotResolved<G, S>
	where
		K: Fn(&G, S) -> (GroupId, EncodedKey),
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

	fn apply_events<NA>(
		&mut self,
		store: &mut dyn StateStore,
		mut buckets: TumblingBuckets<G, S, Accumulator::Contributions>,
		order: &[(G, WindowSpan<S>)],
		slot_resolved: &SlotResolved<G, S>,
		new_accumulator: &NA,
	) -> Result<Vec<WindowResult<G, S, Accumulator::Output>>>
	where
		NA: Fn() -> Accumulator,
	{
		let mut pending: Vec<PendingEmit<G, S, Accumulator::Output>> = Vec::new();

		let mut keys: Vec<WindowStateKey> = Vec::new();
		let mut slots: HashMap<(G, WindowSpan<S>), usize> = HashMap::new();
		let mut claimed: HashSet<WindowStateKey> = HashSet::new();
		for ordered in order {
			if !buckets.contains_key(ordered) {
				continue;
			}
			let Some(resolved) = slot_resolved.get(ordered) else {
				continue;
			};
			let state_key = WindowStateKey::new(self.family, resolved.group, resolved.key.clone());
			if !claimed.insert(state_key.clone()) {
				return internal_err!(
					"two windows of one batch resolve to one accumulator slot (group={:?})",
					resolved.group
				);
			}
			slots.insert(ordered.clone(), keys.len());
			keys.push(state_key);
		}
		let now = store.batch_time();
		let mut session = match keys.is_empty() {
			true => None,
			false => Some(batch::<_, WindowSlot<Accumulator>>(store, &keys)?),
		};
		let mut held: Vec<(usize, WindowSlot<Accumulator>)> = Vec::new();

		for ordered in order {
			let Some(events) = buckets.remove(ordered) else {
				continue;
			};
			let (group, span) = ordered.clone();

			let Some(ResolvedSlot {
				group: id,
				..
			}) = slot_resolved.get(&(group.clone(), span)).cloned()
			else {
				continue;
			};
			let (Some(session), Some(&slot)) = (session.as_mut(), slots.get(&(group.clone(), span))) else {
				continue;
			};

			let mut window = match session.value(slot)? {
				Some(window) => window,
				None => WindowSlot {
					row: None,
					first_time: span.start.as_time().unwrap_or(now),
					acc: CountedAccumulator::new(new_accumulator()),
				},
			};
			let was_empty_before = window.acc.is_empty();
			let prior = if was_empty_before {
				None
			} else {
				window.acc.finalize()
			};

			self.dropped_retractions += window.acc.apply(&events).dropped;

			let value = window.acc.finalize();

			match value {
				Some(value) => {
					pending.push(PendingEmit {
						group_id: id,
						group,
						span,
						value,
						prior,
						withdraw: false,
					});
					held.push((slot, window));
				}
				None => match prior.clone() {
					Some(p) => {
						pending.push(PendingEmit {
							group_id: id,
							group,
							span,
							value: p,
							prior,
							withdraw: true,
						});
						held.push((slot, window));
					}
					None => {
						session.set(slot, &window)?;
					}
				},
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

		let unnumbered: Vec<usize> = pending
			.iter()
			.zip(&held)
			.enumerate()
			.filter(|(_, (emit, (_, window)))| !emit.withdraw && window.row.is_none())
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

		let mut results: Vec<WindowResult<G, S, Accumulator::Output>> = Vec::with_capacity(pending.len());
		let mut withdrawn: Vec<GroupId> = Vec::new();
		for (index, (emit, (slot, mut window))) in pending.into_iter().zip(held).enumerate() {
			let (row_number, kind) = if emit.withdraw {
				let Some(row_number) = window.row else {
					return internal_err!(
						"a window holding a prior output must already own its row; withdrawing \
						 without one names a row no sink can find (group={:?})",
						emit.group_id
					);
				};
				withdrawn.push(emit.group_id);
				window.row = None;
				(row_number, EmitKind::Remove)
			} else {
				match (window.row, assigned.remove(&index)) {
					(Some(row_number), _) => (row_number, EmitKind::Update),
					(None, Some((row_number, true))) => (row_number, EmitKind::Insert),
					(None, Some((row_number, false))) => (row_number, EmitKind::Update),
					(None, None) => {
						return internal_err!(
							"a publishing window was left without a row (group={:?})",
							emit.group_id
						);
					}
				}
			};
			if !emit.withdraw {
				if window.row.is_none() {
					window.first_time = emit.span.start.as_time().unwrap_or(now);
				}
				window.row = Some(row_number);
			}
			let first_time = window.first_time;
			if let Some(session) = session.as_mut() {
				session.set(slot, &window)?;
			}
			results.push(WindowResult {
				row_number,
				group: emit.group,
				span: emit.span,
				value: emit.value,
				prior: emit.prior,
				kind,
				first_time: Some(first_time),
			});
		}
		if let Some(session) = session {
			session.commit(store)?;
		}
		if !withdrawn.is_empty() {
			store.remove_row_number_for_group(&withdrawn)?;
		}
		Ok(results)
	}

	pub fn expire_within(
		&mut self,
		store: &mut dyn StateStore,
		threshold: u64,
		budget: usize,
	) -> Result<Vec<ExpiredWindow<G, S>>> {
		let due: Vec<(GroupStateKey, TumblingIndexEntry<G, S>)> = self.expiry.due(store, threshold, budget)?;

		let mut out: Vec<ExpiredWindow<G, S>> = Vec::with_capacity(due.len());
		let mut index_keys: Vec<GroupStateKey> = Vec::with_capacity(due.len());
		for (index_key, entry) in due {
			index_keys.push(index_key);
			out.push(ExpiredWindow {
				group: entry.group,
				group_id: entry.group_id,
				window_start: entry.window_start,
			});
		}
		if !index_keys.is_empty() {
			store.state_remove(&index_keys)?;
		}
		self.expiry.settle(store)?;
		if !out.is_empty() {
			self.earliest = None;
		}
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
		if let Some(known) = self.earliest {
			reifydb_assertions! {
				let grounded = crate::operator::state::expiry::expiry_earliest::<TumblingExpiry>(store)?;
				assert!(
					known == grounded,
					"the earliest expiry held in memory is {known:?} but the index holds {grounded:?}; \
					 one above the index seals its earliest window late, one below seals early"
				);
			}
			return Ok(known);
		}
		let earliest = self.expiry.earliest(store)?;
		self.earliest = Some(earliest);
		Ok(earliest)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{
		key::operator::state::{GroupId, KeyspaceMask},
		state::timer::StateStore,
	};
	use reifydb_value::{Result, util::hash::Hash128, value::datetime::DateTime};

	use crate::{
		factory::coord::event_coord_at_millis,
		operator::state::{
			mock::MockStore,
			reaper::{Reaper, drain_group, enqueue},
			seal::coord::Coord,
		},
		window::{
			accumulator::{AccumulatorOp, mock::SumAccumulator, testkit::events},
			coord::{DateTimeCoord, U8Coord},
			engine::{
				EmitKind, WindowResult,
				config::WindowEngineConfig,
				tumbling::{TumblingBuckets, TumblingEngine},
			},
			span::{WindowAnchor, WindowSpan},
		},
	};

	fn test_config() -> WindowEngineConfig {
		WindowEngineConfig::builder().build()
	}

	fn row_key(group: &u32, window_start: DateTimeCoord) -> EncodedKey {
		EncodedKey::builder().u32(*group).u64(window_start.to_order()).build()
	}

	fn slot_key(group: &u32, window_start: DateTimeCoord) -> (GroupId, EncodedKey) {
		(GroupId::of(&row_key(group, window_start)), EncodedKey::new(Vec::new()))
	}

	fn order_of<Contribution>(
		buckets: &TumblingBuckets<u32, DateTimeCoord, Contribution>,
	) -> Vec<(u32, WindowSpan<DateTimeCoord>)> {
		buckets.keys().cloned().collect()
	}

	fn apply_sums(
		engine: &mut TumblingEngine<u32, DateTimeCoord, SumAccumulator>,
		store: &mut MockStore,
		buckets: TumblingBuckets<u32, DateTimeCoord, Vec<i64>>,
	) -> Result<Vec<WindowResult<u32, DateTimeCoord, i64>>> {
		let order = order_of(&buckets);
		engine.apply(store, buckets, &order, slot_key, SumAccumulator::default)
	}

	fn seed_window(
		store: &mut MockStore,
		window_start: i64,
		contribution: i64,
	) -> WindowResult<u32, DateTimeCoord, i64> {
		let mut engine = TumblingEngine::<u32, DateTimeCoord, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, DateTimeCoord, Vec<i64>> = BTreeMap::new();
		buckets.insert(
			(
				1u32,
				WindowSpan::new(
					event_coord_at_millis(window_start),
					event_coord_at_millis(window_start + 1),
				),
			),
			events([(AccumulatorOp::Add, contribution)]),
		);
		let mut results = apply_sums(&mut engine, store, buckets).expect("apply");
		results.pop().expect("one window")
	}

	fn group_slot(group: &u32, window_start: DateTimeCoord) -> (GroupId, EncodedKey) {
		// The shape a sub-flow window driver installs: every window interns as its own
		// (partition, slot) group sharing one empty row key, so the group alone separates them.
		(GroupId::window(Hash128(u128::from(*group)), window_start.to_order()), EncodedKey::new(Vec::new()))
	}

	fn apply_group_scoped(
		engine: &mut TumblingEngine<u32, DateTimeCoord, SumAccumulator>,
		store: &mut MockStore,
		buckets: TumblingBuckets<u32, DateTimeCoord, Vec<i64>>,
	) -> Vec<WindowResult<u32, DateTimeCoord, i64>> {
		let order = order_of(&buckets);
		engine.apply(store, buckets, &order, group_slot, SumAccumulator::default).expect("apply")
	}

	fn one_bucket(
		group: u32,
		window_start: i64,
		contribution: i64,
	) -> TumblingBuckets<u32, DateTimeCoord, Vec<i64>> {
		let mut buckets: TumblingBuckets<u32, DateTimeCoord, Vec<i64>> = BTreeMap::new();
		buckets.insert(
			(
				group,
				WindowSpan::new(
					event_coord_at_millis(window_start),
					event_coord_at_millis(window_start + 1),
				),
			),
			events([(AccumulatorOp::Add, contribution)]),
		);
		buckets
	}

	#[test]
	fn group_scoped_windows_keep_separate_state_under_one_shared_row_key() {
		// A group-scoped driver addresses every window with the same empty row key and leans on the
		// group to separate them. If the group fell out of the accumulator key or the row-number
		// lookup, every window of the operator would fold into one accumulator under one row.
		let mut store = MockStore::default();
		let mut engine = TumblingEngine::<u32, DateTimeCoord, SumAccumulator>::new(test_config());

		let mut buckets: TumblingBuckets<u32, DateTimeCoord, Vec<i64>> = BTreeMap::new();
		buckets.insert(
			(1u32, WindowSpan::new(event_coord_at_millis(0), event_coord_at_millis(1))),
			events([(AccumulatorOp::Add, 5)]),
		);
		buckets.insert(
			(1u32, WindowSpan::new(event_coord_at_millis(100), event_coord_at_millis(101))),
			events([(AccumulatorOp::Add, 7)]),
		);
		buckets.insert(
			(2u32, WindowSpan::new(event_coord_at_millis(0), event_coord_at_millis(1))),
			events([(AccumulatorOp::Add, 9)]),
		);
		let results = apply_group_scoped(&mut engine, &mut store, buckets);

		assert_eq!(results.len(), 3);
		let mut rows: Vec<u64> = results.iter().map(|r| r.row_number.0).collect();
		rows.sort_unstable();
		rows.dedup();
		assert_eq!(rows.len(), 3, "each window must own a row of its own despite the shared row key");
		let mut values: Vec<i64> = results.iter().map(|r| r.value).collect();
		values.sort_unstable();
		assert_eq!(values, vec![5, 7, 9], "no window may see another's contributions");

		// A restart reloads one window's accumulator; it must be that window's alone.
		let mut restarted = TumblingEngine::<u32, DateTimeCoord, SumAccumulator>::new(test_config());
		let out = apply_group_scoped(&mut restarted, &mut store, one_bucket(1, 0, 1));
		assert_eq!(out[0].value, 6, "the reloaded accumulator carries only window (1, 0)");
	}

	#[test]
	fn a_window_whose_accumulator_was_reclaimed_updates_its_row_rather_than_inserting_a_second() {
		// Phase-1 reclamation erases the accumulator and keeps the row-number mapping, so the
		// published row stays addressable and a later batch must update it in place. A second
		// insert lays a duplicate row over a live one, which a sink folding the stream cannot place.
		let mut store = MockStore::default();
		let published = seed_window(&mut store, 0, 5);
		let group = GroupId::of(&row_key(&1, event_coord_at_millis(0)));
		let swept = store.group_sweep(&[group], usize::MAX, KeyspaceMask::data()).unwrap().rows;
		assert!(!swept.is_empty(), "precondition: reclaim erased the accumulator");
		TumblingEngine::<u32, DateTimeCoord, SumAccumulator>::new(test_config())
			.reap(&mut store, &swept)
			.unwrap();
		assert!(
			store.contains_row_mapping(group),
			"precondition: the identity half must survive the data phase"
		);

		let mut engine = TumblingEngine::<u32, DateTimeCoord, SumAccumulator>::new(test_config());
		let results = apply_sums(&mut engine, &mut store, one_bucket(1, 0, 3)).expect("apply");

		assert_eq!(results.len(), 1);
		assert_eq!(results[0].row_number, published.row_number, "the woken window keeps the row it published");
		assert_eq!(
			results[0].kind,
			EmitKind::Update,
			"the published row survived the sweep, so this is an update and not a second insert"
		);
	}

	#[test]
	fn a_window_updated_twice_keeps_its_first_row_and_first_time_without_a_mapping() {
		// A live window must address one row for its whole life, read from its own record and never re-minted.
		let mut store = MockStore::default();
		let mut engine = TumblingEngine::<u32, DateTimeCoord, SumAccumulator>::new(test_config());
		let first = apply_sums(&mut engine, &mut store, one_bucket(1, 0, 5)).expect("apply");
		let second = apply_sums(&mut engine, &mut store, one_bucket(1, 0, 3)).expect("apply");
		let third = apply_sums(&mut engine, &mut store, one_bucket(1, 0, 2)).expect("apply");

		assert_eq!(first[0].kind, EmitKind::Insert);
		assert_eq!(second[0].kind, EmitKind::Update);
		assert_eq!(third[0].kind, EmitKind::Update);
		assert_eq!(second[0].row_number, first[0].row_number, "an update must address the inserted row");
		assert_eq!(third[0].row_number, first[0].row_number, "a second update must address the inserted row");
		assert_eq!(first[0].first_time, Some(event_coord_at_millis(0).as_time().unwrap()));
		assert_eq!(third[0].first_time, first[0].first_time, "first time is fixed when the window is created");
		assert!(
			!store.contains_row_mapping(GroupId::of(&row_key(&1, event_coord_at_millis(0)))),
			"a live window keeps its row in its record; a mapping here means the hot path wrote identity state"
		);

		let other = apply_sums(&mut engine, &mut store, one_bucket(2, 0, 1)).expect("apply");
		assert_eq!(
			other[0].row_number.0,
			first[0].row_number.0 + 1,
			"the two updates minted nothing, so the next window takes the next number"
		);
	}

	fn apply_row_window(
		store: &mut MockStore,
		at_millis: i64,
		event: (AccumulatorOp, i64),
	) -> WindowResult<u32, U8Coord, i64> {
		store.set_batch_time(DateTime::from_millis(at_millis));
		let mut engine = TumblingEngine::<u32, U8Coord, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, U8Coord, Vec<i64>> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(U8Coord(0), U8Coord(1))), events([event]));
		let order: Vec<(u32, WindowSpan<U8Coord>)> = buckets.keys().cloned().collect();
		let row_slot = |group: &u32, start: U8Coord| {
			let key = EncodedKey::builder().u32(*group).u64(start.0).build();
			(GroupId::of(&key), EncodedKey::new(Vec::new()))
		};
		let mut results =
			engine.apply(store, buckets, &order, row_slot, SumAccumulator::default).expect("apply");
		results.pop().expect("one window")
	}

	#[test]
	fn a_window_withdrawn_then_republished_takes_a_new_first_time() {
		// A reinsert is a new sink row; keeping the withdrawn row's time misstates when it was created.
		let mut store = MockStore::default();
		let inserted = apply_row_window(&mut store, 1000, (AccumulatorOp::Add, 5));
		let withdrawn = apply_row_window(&mut store, 2000, (AccumulatorOp::Remove, 5));
		let reinserted = apply_row_window(&mut store, 3000, (AccumulatorOp::Add, 7));

		assert_eq!(inserted.kind, EmitKind::Insert);
		assert_eq!(inserted.first_time, Some(DateTime::from_millis(1000)));
		assert_eq!(withdrawn.kind, EmitKind::Remove);
		assert_eq!(
			withdrawn.first_time,
			Some(DateTime::from_millis(1000)),
			"a withdraw retracts the row it published"
		);
		assert_eq!(reinserted.kind, EmitKind::Insert);
		assert_eq!(
			reinserted.first_time,
			Some(DateTime::from_millis(3000)),
			"a fresh row takes the time it was created"
		);
	}

	#[test]
	fn an_update_carries_the_value_published_before_it_as_prior() {
		// The driver rebuilds the pre-image from prior, so prior must be exactly the last published value.
		let mut store = MockStore::default();
		let mut engine = TumblingEngine::<u32, DateTimeCoord, SumAccumulator>::new(test_config());
		let first = apply_sums(&mut engine, &mut store, one_bucket(1, 0, 5)).expect("apply");
		let second = apply_sums(&mut engine, &mut store, one_bucket(1, 0, 3)).expect("apply");
		let third = apply_sums(&mut engine, &mut store, one_bucket(1, 0, 2)).expect("apply");

		assert_eq!(first[0].prior, None, "an insert has no pre-image");
		assert_eq!(second[0].prior, Some(first[0].value));
		assert_eq!(third[0].prior, Some(second[0].value));
		assert_eq!(third[0].value, 10);
	}

	#[test]
	fn a_fully_reaped_window_leaves_no_state_and_no_mapping_behind() {
		// The reap moves the row into the identity half; phase two must then erase it, or it leaks forever.
		let mut store = MockStore::default();
		seed_window(&mut store, 0, 5);
		let group = GroupId::of(&row_key(&1, event_coord_at_millis(0)));
		enqueue(&mut store, &[group]).unwrap();

		let mut engine = TumblingEngine::<u32, DateTimeCoord, SumAccumulator>::new(test_config());
		let drained = drain_group(&mut store, group, &mut engine, usize::MAX).unwrap();

		assert!(!drained.still_queued, "an unbounded drain finishes the group in one pass");
		assert!(
			store.group_sweep(&[group], usize::MAX, KeyspaceMask::data()).unwrap().rows.is_empty(),
			"no data row of the reaped window may survive"
		);
		assert!(
			!store.contains_row_mapping(group),
			"the row moved out by the reap must be reclaimed with the group"
		);

		let mut woken = TumblingEngine::<u32, DateTimeCoord, SumAccumulator>::new(test_config());
		let out = apply_sums(&mut woken, &mut store, one_bucket(1, 0, 4)).expect("apply");
		assert_eq!(out[0].kind, EmitKind::Insert, "a fully reclaimed window starts over as a new row");
	}

	#[test]
	fn a_window_that_publishes_nothing_mints_no_identity() {
		// Minting a row number before the publish decision leaves a mapping addressing a row the
		// view never held. The window then looks published, so its next value goes out as an update
		// whose pre-image is absent.
		let mut store = MockStore::default();
		let mut engine = TumblingEngine::<u32, DateTimeCoord, SumAccumulator>::new(test_config());

		let mut buckets: TumblingBuckets<u32, DateTimeCoord, Vec<i64>> = BTreeMap::new();
		buckets.insert(
			(1u32, WindowSpan::new(event_coord_at_millis(0), event_coord_at_millis(1))),
			events([(AccumulatorOp::Remove, 5)]),
		);
		let results = apply_sums(&mut engine, &mut store, buckets).expect("apply");

		assert!(results.is_empty(), "a window that finalizes to nothing publishes nothing");
		assert!(
			!store.contains_row_mapping(GroupId::of(&row_key(&1, event_coord_at_millis(0)))),
			"and must leave no identity behind for a row it never published"
		);

		let mut woken = TumblingEngine::<u32, DateTimeCoord, SumAccumulator>::new(test_config());
		let out = apply_sums(&mut woken, &mut store, one_bucket(1, 0, 4)).expect("apply");

		assert_eq!(out[0].kind, EmitKind::Insert, "the first row this window publishes is an insert");
	}

	#[test]
	fn accumulator_survives_restart() {
		// A window emptying under retraction emits a terminal Remove carrying the accumulator's
		// pre-batch finalize, read back from the store. Dropping the engine between publish and
		// retraction forces that read rather than serving it from the in-memory cache.
		let mut store = MockStore::default();

		let mut engine = TumblingEngine::<u32, DateTimeCoord, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, DateTimeCoord, Vec<i64>> = BTreeMap::new();
		buckets.insert(
			(1u32, WindowSpan::new(event_coord_at_millis(0), event_coord_at_millis(1))),
			events([(AccumulatorOp::Add, 5)]),
		);
		let published: Vec<WindowResult<u32, DateTimeCoord, i64>> =
			apply_sums(&mut engine, &mut store, buckets).unwrap();
		assert_eq!(published.len(), 1);
		assert!(matches!(published[0].kind, EmitKind::Insert));
		assert_eq!(published[0].value, 5);

		// A brand new engine with empty caches, forced to read the persisted accumulator back.
		let mut engine = TumblingEngine::<u32, DateTimeCoord, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, DateTimeCoord, Vec<i64>> = BTreeMap::new();
		buckets.insert(
			(1u32, WindowSpan::new(event_coord_at_millis(0), event_coord_at_millis(1))),
			events([(AccumulatorOp::Remove, 5)]),
		);
		let withdrawn: Vec<WindowResult<u32, DateTimeCoord, i64>> =
			apply_sums(&mut engine, &mut store, buckets).unwrap();

		assert_eq!(withdrawn.len(), 1, "emptying the window emits exactly one terminal diff");
		assert!(
			matches!(withdrawn[0].kind, EmitKind::Remove),
			"the window emptied under retraction, so the last published row must be withdrawn"
		);
		assert_eq!(withdrawn[0].value, 5, "the withdrawn value is the reloaded pre-batch accumulator output");
		assert_eq!(
			withdrawn[0].row_number, published[0].row_number,
			"the withdrawal targets the same row that was published"
		);
	}

	#[test]
	fn accumulator_survives_lru_eviction() {
		// The other way a read reaches the store is LRU eviction, with no restart: the accumulator
		// cache holds 8 windows, so more than that evicts the oldest and the next access re-reads it.
		let mut store = MockStore::default();
		let mut engine = TumblingEngine::<u32, DateTimeCoord, SumAccumulator>::new(test_config());

		let mut published_group_1: Vec<WindowResult<u32, DateTimeCoord, i64>> = Vec::new();
		for group in 1u32..=11u32 {
			let mut buckets: TumblingBuckets<u32, DateTimeCoord, Vec<i64>> = BTreeMap::new();
			buckets.insert(
				(group, WindowSpan::new(event_coord_at_millis(0), event_coord_at_millis(1))),
				events([(AccumulatorOp::Add, i64::from(group))]),
			);
			let out: Vec<WindowResult<u32, DateTimeCoord, i64>> =
				apply_sums(&mut engine, &mut store, buckets).unwrap();
			if group == 1 {
				published_group_1 = out;
			}
		}
		assert_eq!(published_group_1.len(), 1);
		assert!(matches!(published_group_1[0].kind, EmitKind::Insert));
		assert_eq!(published_group_1[0].value, 1);

		// Group 1's window was pushed out of the 8-slot cache by the later groups, so the same
		// engine must re-read its accumulator from the store to apply this retraction.
		let mut buckets: TumblingBuckets<u32, DateTimeCoord, Vec<i64>> = BTreeMap::new();
		buckets.insert(
			(1u32, WindowSpan::new(event_coord_at_millis(0), event_coord_at_millis(1))),
			events([(AccumulatorOp::Remove, 1)]),
		);
		let withdrawn: Vec<WindowResult<u32, DateTimeCoord, i64>> =
			apply_sums(&mut engine, &mut store, buckets).unwrap();

		assert_eq!(withdrawn.len(), 1, "emptying the evicted window emits exactly one terminal diff");
		assert!(
			matches!(withdrawn[0].kind, EmitKind::Remove),
			"the evicted window emptied under retraction, so the last published row must be withdrawn"
		);
		assert_eq!(withdrawn[0].value, 1, "the withdrawn value is the reloaded accumulator output for group 1");
		assert_eq!(
			withdrawn[0].row_number, published_group_1[0].row_number,
			"the withdrawal targets the same row that was published for group 1"
		);
	}
}
