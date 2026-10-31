// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	fmt::Debug,
	hash::Hash,
	marker::PhantomData,
	mem::take,
	ops::Bound::{self, Excluded, Included, Unbounded},
	slice::from_ref,
};

use reifydb_codec::{
	key::encode_u64_asc,
	row::operator::state::{OperatorState, StateCodec, decode},
};
use reifydb_core::{
	internal_err,
	key::operator::{
		keyspace::expiry::Expiry,
		state::{GroupId, GroupStateKey, GroupStateRange, IntoGroupStateKey, KeyspaceId},
	},
	metrics::heap::HeapSize,
	state::timer::StateStore,
};
use reifydb_macro::operator_state;
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::{
	operator::{
		state::{
			expiry::{ExpiryIndex, expiry_drop, rolling_expiry_key},
			seal::coord::Coord,
		},
		state_access::{get_classified, remove, set},
	},
	window::{
		accumulator::{UnmergeAccumulator, WindowAccumulator},
		engine::{
			AccumulatorEvent, EmitKind, PaneKey,
			config::WindowEngineConfig,
			daba::{DabaPanes, DabaQueue, Merge},
			group_hash, note_when_expiry_capped,
		},
		meta::{RollingMetaKey, RollingPublished},
	},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PaneFold {
	Running,
	Queue,
}

pub enum PaneEvent<C, X> {
	Insert(C, u64, X),
	Remove(C, u64, X),
	Update {
		row: u64,
		pre: (C, X),
		post: (C, X),
	},
}

impl<C: Copy, X> PaneEvent<C, X> {
	pub fn panes(&self) -> Vec<C> {
		match self {
			Self::Insert(pane, _, _) | Self::Remove(pane, _, _) => vec![*pane],
			Self::Update {
				pre,
				post,
				..
			} => vec![pre.0, post.0],
		}
	}
}

#[derive(Clone, Copy, Debug)]
pub struct PaneSpans<Span> {
	pub size: Span,
	pub lag: Span,
	pub lateness: Span,
	pub retract: Span,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PaneLines {
	pub output: Option<u64>,
	pub queue: Option<u64>,
	pub seal: u64,
	pub retract: u64,
}

impl PaneLines {
	pub fn of<C: Coord>(spans: &PaneSpans<C::Span>, newest: C) -> Self {
		let output = newest.checked_sub_span(spans.lag).map(Coord::to_order);
		let queue_below = newest.saturating_sub_span(spans.lateness.max(spans.retract)).to_order();
		Self {
			output,
			queue: queue_below.checked_sub(1).zip(output).map(|(queue, output)| queue.min(output)),
			seal: newest.saturating_sub_span(spans.lateness).to_order(),
			retract: newest.saturating_sub_span(spans.retract).to_order(),
		}
	}

	pub fn admits(lines: Option<&Self>, cutoff: Option<u64>, pane: u64, retraction: bool) -> bool {
		if cutoff.is_some_and(|cutoff| pane <= cutoff) {
			return false;
		}
		lines.is_none_or(|lines| {
			if retraction {
				pane >= lines.retract
			} else {
				pane >= lines.seal
			}
		})
	}
}

#[operator_state]
#[derive(Clone, Debug)]
pub struct RollingHeld<X> {
	pub held: Option<(u64, X)>,
	pub seen: u64,
}

impl<X: HeapSize> HeapSize for RollingHeld<X> {
	fn heap_size(&self) -> usize {
		self.held.heap_size()
	}
}

#[operator_state]
#[derive(Clone, Debug)]
pub struct RollingGroup<A, P, X> {
	pub newest: u64,
	pub oldest: u64,
	pub crossed: Option<u64>,
	pub running: Option<A>,
	pub queue: DabaQueue<A>,
	pub published: Option<P>,
	pub held: BTreeMap<u64, RollingHeld<X>>,
}

impl<A: HeapSize, P: HeapSize, X: HeapSize> HeapSize for RollingGroup<A, P, X> {
	fn heap_size(&self) -> usize {
		self.running.heap_size() + self.queue.heap_size() + self.published.heap_size() + self.held.heap_size()
	}
}

#[operator_state]
#[derive(Clone, Debug)]
pub struct RollingPane<A> {
	pub pane: A,
	pub partial: Option<A>,
}

impl<A: HeapSize> HeapSize for RollingPane<A> {
	fn heap_size(&self) -> usize {
		self.pane.heap_size() + self.partial.heap_size()
	}
}

#[operator_state]
#[derive(Clone)]
pub struct RollingIndexEntry<G> {
	group: G,
	group_id: GroupId,
}

pub struct RollingEmit<G, K, O> {
	pub group: G,
	pub group_id: GroupId,
	pub prior: Option<RollingPublished<K, O>>,
	pub post: Option<RollingPublished<K, O>>,
}

impl<G, K, O> RollingEmit<G, K, O> {
	pub fn kind(&self) -> Option<EmitKind> {
		match (&self.prior, &self.post) {
			(None, Some(_)) => Some(EmitKind::Insert),
			(Some(_), Some(_)) => Some(EmitKind::Update),
			(Some(_), None) => Some(EmitKind::Remove),
			(None, None) => None,
		}
	}
}

pub struct PaneApplied<E> {
	pub emits: Vec<E>,
	pub sealed_drops: u64,
	pub stale_retractions: u64,
	pub withdrawn: Vec<GroupId>,
	pub fresh: Vec<(GroupId, usize)>,
}

impl<E> Default for PaneApplied<E> {
	fn default() -> Self {
		Self {
			emits: Vec::new(),
			sealed_drops: 0,
			stale_retractions: 0,
			withdrawn: Vec::new(),
			fresh: Vec::new(),
		}
	}
}

pub type PaneGroups<G, C, X> = BTreeMap<G, (GroupId, Vec<PaneEvent<C, X>>)>;

pub trait PanePublisher<G, C, A> {
	type Published: Clone;
	type Emit;

	#[allow(clippy::too_many_arguments)]
	fn publish(
		&self,
		store: &mut dyn StateStore,
		group: &G,
		group_id: GroupId,
		newest: C,
		prior: Option<Self::Published>,
		value: Option<&A>,
		applied: &mut PaneApplied<Self::Emit>,
	) -> Result<Option<Self::Published>>;

	fn assign_row_number(
		&self,
		emit: &mut Self::Emit,
		published: &mut Self::Published,
		row: RowNumber,
	) -> Result<()>;
}

struct Panes<'s, A> {
	store: &'s mut dyn StateStore,
	group: GroupId,
	overlay: BTreeMap<u64, Option<RollingPane<A>>>,
	loaded: BTreeSet<u64>,
}

fn pane_range(group: GroupId, start: Bound<u64>, end: Bound<u64>) -> GroupStateRange {
	let bytes = |bound: Bound<u64>| bound.map(|pane| encode_u64_asc(pane).to_vec());
	GroupStateRange::Keyspace {
		group,
		keyspace: KeyspaceId::ROLLING_PANE,
		start: bytes(start),
		end: bytes(end),
	}
}

fn pane_of(key: &GroupStateKey) -> Result<u64> {
	match PaneKey::decode(key) {
		Some((_, pane)) => Ok(pane),
		None => internal_err!("a rolling pane row carries no pane order"),
	}
}

fn is_ordered(start: Bound<u64>, end: Bound<u64>) -> bool {
	match (start, end) {
		(Included(s), Included(e)) => s <= e,
		(Included(s), Excluded(e)) | (Excluded(s), Included(e)) => s < e,
		(Excluded(s), Excluded(e)) => s.saturating_add(1) < e,
		_ => true,
	}
}

impl<'s, A> Panes<'s, A>
where
	A: WindowAccumulator,
	RollingPane<A>: OperatorState,
{
	fn new(store: &'s mut dyn StateStore, group: GroupId) -> Self {
		Self {
			store,
			group,
			overlay: BTreeMap::new(),
			loaded: BTreeSet::new(),
		}
	}

	fn get(&mut self, pane: u64) -> Result<Option<RollingPane<A>>> {
		if let Some(row) = self.overlay.get(&pane) {
			return Ok(row.clone());
		}
		let row = get_classified::<_, RollingPane<A>>(self.store, from_ref(&PaneKey::new(self.group, pane)))?
			.remove(0);
		if row.is_some() {
			self.loaded.insert(pane);
		}
		Ok(row)
	}

	fn put(&mut self, pane: u64, row: Option<RollingPane<A>>) {
		self.overlay.insert(pane, row);
	}

	fn scan(&mut self, start: Bound<u64>, end: Bound<u64>) -> Result<BTreeMap<u64, RollingPane<A>>> {
		let mut out = BTreeMap::new();
		if !is_ordered(start, end) {
			return Ok(out);
		}
		for (key, row) in self.store.state_page(pane_range(self.group, start, end), None)? {
			let pane = pane_of(&key)?;
			if self.overlay.contains_key(&pane) {
				continue;
			}
			self.store.state_classify(&key, Some(row.byte_size()));
			self.loaded.insert(pane);
			out.insert(pane, decode::<RollingPane<A>>(&row)?);
		}
		for (pane, row) in self.overlay.range((start, end)) {
			if let Some(row) = row {
				out.insert(*pane, row.clone());
			}
		}
		Ok(out)
	}

	fn first(&mut self) -> Result<Option<u64>> {
		let deleted = self.overlay.values().filter(|row| row.is_none()).count();
		let mut stored = None;
		for (key, _) in
			self.store.state_page(pane_range(self.group, Unbounded, Unbounded), Some(deleted + 1))?
		{
			let pane = pane_of(&key)?;
			if !matches!(self.overlay.get(&pane), Some(None)) {
				stored = Some(pane);
				break;
			}
		}
		let staged = self.overlay.iter().find(|(_, row)| row.is_some()).map(|(pane, _)| *pane);
		Ok(match (stored, staged) {
			(Some(stored), Some(staged)) => Some(stored.min(staged)),
			(stored, staged) => stored.or(staged),
		})
	}

	fn flush(self) -> Result<&'s mut dyn StateStore> {
		let Self {
			store,
			group,
			overlay,
			loaded,
		} = self;
		let mut sets = Vec::new();
		let mut removes = Vec::new();
		for (pane, row) in overlay {
			let key = (&PaneKey::new(group, pane)).into_group_state_key();
			match row {
				Some(row) => {
					if !loaded.contains(&pane) {
						store.state_classify(&key, None);
					}
					sets.push((key, row.encode_state()?));
				}
				None if loaded.contains(&pane) => removes.push(key),
				None => {}
			}
		}
		if !sets.is_empty() {
			store.state_set(sets)?;
		}
		if !removes.is_empty() {
			store.state_remove(&removes)?;
		}
		Ok(store)
	}
}

impl<A> DabaPanes<A> for Panes<'_, A>
where
	A: WindowAccumulator,
	RollingPane<A>: OperatorState,
{
	fn value(&mut self, merge: Merge<A>, pane: u64) -> Result<A> {
		match self.get(pane)? {
			Some(mut row) => {
				(merge.compact)(&mut row.pane);
				Ok(row.pane)
			}
			None => internal_err!("the rolling queue names pane {pane}, which holds no row"),
		}
	}

	fn partial(&mut self, pane: u64) -> Result<A> {
		match self.get(pane)?.and_then(|row| row.partial) {
			Some(partial) => Ok(partial),
			None => internal_err!(
				"the rolling queue reads a partial of pane {pane} that was never converted"
			),
		}
	}

	fn set_partial(&mut self, merge: Merge<A>, pane: u64, partial: A) -> Result<()> {
		let Some(mut row) = self.get(pane)? else {
			return internal_err!("the rolling queue converts pane {pane}, which holds no row");
		};
		(merge.compact)(&mut row.pane);
		row.partial = Some(partial);
		self.put(pane, Some(row));
		Ok(())
	}

	fn last_below(&mut self, from: u64, below: u64) -> Result<Option<u64>> {
		if below <= from {
			return Ok(None);
		}
		let staged =
			self.overlay.range(from..below).rev().find(|(_, row)| row.is_some()).map(|(pane, _)| *pane);
		let mut bound = below;
		let stored = loop {
			if bound <= from {
				break None;
			}
			match self.store.state_last(pane_range(self.group, Included(from), Excluded(bound)))? {
				None => break None,
				Some((key, _)) => {
					let pane = pane_of(&key)?;
					if matches!(self.overlay.get(&pane), Some(None)) {
						bound = pane;
					} else {
						break Some(pane);
					}
				}
			}
		};
		Ok(staged.max(stored))
	}
}

pub struct PaneEngine<G, C: Coord, A> {
	fold: PaneFold,
	merge: Merge<A>,
	unmerge: Option<fn(&mut A, &A)>,
	spans: PaneSpans<C::Span>,
	expiry: ExpiryIndex<Expiry>,
	expire_batch: usize,
	_pd: PhantomData<G>,
}

fn unmerge_of<A: UnmergeAccumulator>(running: &mut A, other: &A) {
	running.unmerge(other);
}

impl<G, C, A> PaneEngine<G, C, A>
where
	G: Clone + Ord + Hash + Debug + StateCodec,
	C: Coord,
	A: UnmergeAccumulator,
	RollingPane<A>: OperatorState,
	RollingIndexEntry<G>: OperatorState,
{
	pub fn running(config: WindowEngineConfig, spans: PaneSpans<C::Span>) -> Self {
		let mut engine = Self::queue(config, spans, Merge::of());
		engine.fold = PaneFold::Running;
		engine.unmerge = Some(unmerge_of::<A>);
		engine
	}
}

impl<G, C, A> PaneEngine<G, C, A>
where
	G: Clone + Ord + Hash + Debug + StateCodec,
	C: Coord,
	A: WindowAccumulator,
	RollingPane<A>: OperatorState,
	RollingIndexEntry<G>: OperatorState,
{
	pub fn queue(config: WindowEngineConfig, spans: PaneSpans<C::Span>, merge: Merge<A>) -> Self {
		Self {
			fold: PaneFold::Queue,
			merge,
			unmerge: None,
			spans,
			expiry: ExpiryIndex::default(),
			expire_batch: config.expire_batch(),
			_pd: PhantomData,
		}
	}

	pub fn fold(&self) -> PaneFold {
		self.fold
	}

	pub fn lines(&self, newest: C) -> PaneLines {
		PaneLines::of::<C>(&self.spans, newest)
	}

	fn group_cutoff(&self, newest: u64, cutoff: Option<u64>) -> Option<u64> {
		let own = C::from_order(newest)
			.checked_sub_span(self.spans.lag)
			.and_then(|end| end.checked_sub_span(self.spans.size))
			.map(Coord::to_order);
		match (cutoff, own) {
			(Some(clock), Some(own)) => Some(clock.max(own)),
			(clock, own) => clock.or(own),
		}
	}

	pub fn earliest_expiry(&mut self, store: &mut dyn StateStore) -> Result<Option<u64>> {
		self.expiry.earliest(store)
	}

	pub fn state_keys(&self, groups: &PaneGroups<G, C, A::Contribution>) -> Vec<GroupStateKey> {
		let mut keys = Vec::new();
		for (group_id, events) in groups.values() {
			keys.push((&RollingMetaKey(*group_id)).into_group_state_key());
			let panes: BTreeSet<u64> =
				events.iter().flat_map(PaneEvent::panes).map(Coord::to_order).collect();
			for pane in panes {
				keys.push((&PaneKey::new(*group_id, pane)).into_group_state_key());
			}
		}
		keys
	}

	pub fn apply<P>(
		&mut self,
		store: &mut dyn StateStore,
		groups: PaneGroups<G, C, A::Contribution>,
		cutoff: Option<C>,
		new_accumulator: &dyn Fn() -> A,
		publisher: &P,
	) -> Result<PaneApplied<P::Emit>>
	where
		P: PanePublisher<G, C, A>,
		RollingGroup<A, P::Published, A::Contribution>: OperatorState + HeapSize,
	{
		let cutoff = cutoff.map(Coord::to_order);
		let mut applied = PaneApplied::default();
		for (group, (group_id, events)) in groups {
			self.apply_group(
				store,
				&group,
				group_id,
				events,
				cutoff,
				new_accumulator,
				publisher,
				&mut applied,
			)?;
		}
		if !applied.withdrawn.is_empty() {
			store.remove_row_number_for_group(&applied.withdrawn)?;
		}
		if !applied.fresh.is_empty() {
			let fresh = take(&mut applied.fresh);
			let ids: Vec<GroupId> = fresh.iter().map(|(group_id, _)| *group_id).collect();
			let numbers = store.get_or_create_row_numbers_for_groups(&ids)?;
			let keys: Vec<RollingMetaKey> = ids.iter().map(|group_id| RollingMetaKey(*group_id)).collect();
			let rows = get_classified::<_, RollingGroup<A, P::Published, A::Contribution>>(store, &keys)?;
			let mut numbered = Vec::with_capacity(rows.len());
			for (((group_id, at), (number, _)), row) in fresh.into_iter().zip(numbers).zip(rows) {
				let Some(mut row) = row else {
					return internal_err!("a freshly published rolling group has no group row");
				};
				let Some(published) = row.published.as_mut() else {
					return internal_err!(
						"a freshly published rolling group row holds no published row"
					);
				};
				publisher.assign_row_number(&mut applied.emits[at], published, number)?;
				numbered.push((RollingMetaKey(group_id), row));
			}
			set(store, numbered)?;
		}
		Ok(applied)
	}

	pub fn expire<P>(&mut self, store: &mut dyn StateStore, cutoff: C, publisher: &P) -> Result<Vec<P::Emit>>
	where
		P: PanePublisher<G, C, A>,
		RollingGroup<A, P::Published, A::Contribution>: OperatorState + HeapSize,
	{
		let due: Vec<(GroupStateKey, RollingIndexEntry<G>)> =
			self.expiry.due(store, cutoff.to_order(), self.expire_batch)?;
		let mut applied = PaneApplied::default();
		for (index_key, entry) in due {
			expiry_drop(store, &index_key)?;
			let Some(row) = get_classified::<_, RollingGroup<A, P::Published, A::Contribution>>(
				store,
				from_ref(&RollingMetaKey(entry.group_id)),
			)?
			.remove(0) else {
				return internal_err!("a rolling expiry row names a group with no group row");
			};
			let view = Panes::new(store, entry.group_id);
			self.settle(
				view,
				&entry.group,
				entry.group_id,
				row,
				true,
				None,
				Some(cutoff.to_order()),
				publisher,
				&mut applied,
			)?;
		}
		if !applied.withdrawn.is_empty() {
			store.remove_row_number_for_group(&applied.withdrawn)?;
		}
		self.expiry.settle(store)?;
		note_when_expiry_capped(applied.emits.len(), self.expire_batch);
		Ok(applied.emits)
	}

	#[allow(clippy::too_many_arguments)]
	fn apply_group<P>(
		&mut self,
		store: &mut dyn StateStore,
		group: &G,
		group_id: GroupId,
		events: Vec<PaneEvent<C, A::Contribution>>,
		cutoff: Option<u64>,
		new_accumulator: &dyn Fn() -> A,
		publisher: &P,
		applied: &mut PaneApplied<P::Emit>,
	) -> Result<()>
	where
		P: PanePublisher<G, C, A>,
		RollingGroup<A, P::Published, A::Contribution>: OperatorState + HeapSize,
	{
		let mut stored = get_classified::<_, RollingGroup<A, P::Published, A::Contribution>>(
			store,
			from_ref(&RollingMetaKey(group_id)),
		)?
		.remove(0);
		let lines = stored.as_ref().map(|row| self.lines(C::from_order(row.newest)));
		let admission = match &stored {
			Some(row) => self.group_cutoff(row.newest, cutoff),
			None => cutoff,
		};
		let mut held = stored.as_mut().map(|row| take(&mut row.held)).unwrap_or_default();
		let mut held_changed = false;
		let mut panes: BTreeMap<u64, Vec<AccumulatorEvent<A::Contribution>>> = BTreeMap::new();
		for event in events {
			match event {
				PaneEvent::Insert(pane, row_number, contribution) => {
					let pane = pane.to_order();
					if PaneLines::admits(lines.as_ref(), admission, pane, false) {
						panes.entry(pane)
							.or_default()
							.push(AccumulatorEvent::Add(contribution));
						held_changed |= held.remove(&row_number).is_some();
					} else {
						applied.sealed_drops += 1;
						if PaneLines::admits(lines.as_ref(), admission, pane, true) {
							held.entry(row_number)
								.or_insert(RollingHeld {
									held: None,
									seen: pane,
								})
								.seen = pane;
							held_changed = true;
						}
					}
				}
				PaneEvent::Remove(pane, row_number, contribution) => {
					let pane = pane.to_order();
					if !PaneLines::admits(lines.as_ref(), admission, pane, true) {
						applied.stale_retractions += 1;
					} else if let Some(record) = held.remove(&row_number) {
						held_changed = true;
						if let Some((at, value)) = record.held.filter(|(at, _)| {
							PaneLines::admits(lines.as_ref(), admission, *at, true)
						}) {
							panes.entry(at)
								.or_default()
								.push(AccumulatorEvent::Remove(value));
						}
					} else {
						panes.entry(pane)
							.or_default()
							.push(AccumulatorEvent::Remove(contribution));
					}
				}
				PaneEvent::Update {
					row: row_number,
					pre,
					post,
				} => {
					let (pre_pane, post_pane) = (pre.0.to_order(), post.0.to_order());
					if !PaneLines::admits(lines.as_ref(), admission, pre_pane, true) {
						applied.stale_retractions += 1;
						held.entry(row_number)
							.or_insert_with(|| RollingHeld {
								held: Some((pre_pane, pre.1)),
								seen: post_pane,
							})
							.seen = post_pane;
						held_changed = true;
					} else if !PaneLines::admits(lines.as_ref(), admission, post_pane, false) {
						applied.sealed_drops += 1;
						held.entry(row_number)
							.or_insert_with(|| RollingHeld {
								held: Some((pre_pane, pre.1)),
								seen: post_pane,
							})
							.seen = post_pane;
						held_changed = true;
					} else {
						match held.remove(&row_number) {
							Some(record) => {
								held_changed = true;
								if let Some((at, value)) =
									record.held.filter(|(at, _)| {
										PaneLines::admits(
											lines.as_ref(),
											admission,
											*at,
											true,
										)
									}) {
									panes.entry(at)
										.or_default()
										.push(AccumulatorEvent::Remove(value));
								}
							}
							None => panes
								.entry(pre_pane)
								.or_default()
								.push(AccumulatorEvent::Remove(pre.1)),
						}
						panes.entry(post_pane).or_default().push(AccumulatorEvent::Add(post.1));
					}
				}
			}
		}
		if panes.is_empty() {
			if let Some(mut row) = stored.filter(|_| held_changed) {
				row.held = held;
				set(store, vec![(RollingMetaKey(group_id), row)])?;
			}
			return Ok(());
		}
		let prior_oldest = stored.as_ref().map(|row| row.oldest);
		let mut row = stored.unwrap_or_else(|| RollingGroup {
			newest: 0,
			oldest: 0,
			crossed: None,
			running: None,
			queue: DabaQueue::default(),
			published: None,
			held: BTreeMap::new(),
		});
		row.held = held;
		let mut view = Panes::new(store, group_id);
		let mut touched = false;
		for (pane, events) in panes {
			let before = view.get(pane)?;
			let mut accumulator =
				before.as_ref().map(|row| row.pane.clone()).unwrap_or_else(new_accumulator);
			if !accumulator.apply(&events).touched {
				continue;
			}
			touched = true;
			if row.crossed.is_some_and(|crossed| pane <= crossed) {
				let Some(unmerge) = self.unmerge else {
					return internal_err!(
						"queued rolling pane {pane} changed after it entered the queue"
					);
				};
				if let Some(before) = before.as_ref().filter(|before| !before.pane.is_empty())
					&& let Some(running) = row.running.as_mut()
				{
					unmerge(running, &before.pane);
				}
				if !accumulator.is_empty() {
					row.running = Some(self.merge.merged(row.running.take(), &accumulator));
				}
			}
			row.newest = row.newest.max(pane);
			view.put(
				pane,
				(!accumulator.is_empty()).then_some(RollingPane {
					pane: accumulator,
					partial: None,
				}),
			);
		}
		if !touched {
			if held_changed {
				let store = view.flush()?;
				set(store, vec![(RollingMetaKey(group_id), row)])?;
			}
			return Ok(());
		}
		self.settle(
			view,
			group,
			group_id,
			row,
			prior_oldest.is_some(),
			prior_oldest,
			cutoff,
			publisher,
			applied,
		)
	}

	#[allow(clippy::too_many_arguments)]
	fn settle<P>(
		&mut self,
		mut view: Panes<'_, A>,
		group: &G,
		group_id: GroupId,
		mut row: RollingGroup<A, P::Published, A::Contribution>,
		existed: bool,
		prior_oldest: Option<u64>,
		cutoff: Option<u64>,
		publisher: &P,
		applied: &mut PaneApplied<P::Emit>,
	) -> Result<()>
	where
		P: PanePublisher<G, C, A>,
		RollingGroup<A, P::Published, A::Contribution>: OperatorState + HeapSize,
	{
		let lines = self.lines(C::from_order(row.newest));
		let admission = self.group_cutoff(row.newest, cutoff);
		row.held.retain(|_, record| {
			PaneLines::admits(Some(&lines), admission, record.seen, true)
				|| record
					.held
					.as_ref()
					.is_some_and(|(at, _)| PaneLines::admits(Some(&lines), admission, *at, true))
		});
		if let Some(cutoff) = admission {
			for (pane, value) in view.scan(Unbounded, Included(cutoff))? {
				view.put(pane, None);
				if row.crossed.is_some_and(|crossed| pane <= crossed) {
					match self.unmerge {
						Some(unmerge) => {
							if let Some(running) = row.running.as_mut() {
								unmerge(running, &value.pane);
							}
						}
						None => row.queue.pop(self.merge, &mut view)?,
					}
				}
			}
		}
		let through = match self.fold {
			PaneFold::Running => lines.output,
			PaneFold::Queue => lines.queue,
		};
		if let Some(through) = through
			&& row.crossed.is_none_or(|crossed| through > crossed)
		{
			let start = row.crossed.map_or(Unbounded, Excluded);
			for (pane, value) in view.scan(start, Included(through))? {
				match self.fold {
					PaneFold::Running => {
						row.running = Some(self.merge.merged(row.running.take(), &value.pane))
					}
					PaneFold::Queue => {
						let mut compacted = value.pane;
						(self.merge.compact)(&mut compacted);
						row.queue.push(self.merge, &mut view, pane, &compacted)?;
					}
				}
			}
			row.crossed = Some(through);
		}
		let oldest = view.first()?;
		let value = match oldest {
			None => None,
			Some(oldest) => match self.fold {
				PaneFold::Running => row.running.clone().filter(|running| !running.is_empty()),
				PaneFold::Queue => {
					let mut total = row.queue.query(self.merge, &mut view, Some(oldest))?;
					if let Some(output) = lines.output {
						let start = row.crossed.map_or(Unbounded, Excluded);
						for (_, open) in view.scan(start, Included(output))? {
							total = Some(self.merge.merged(total, &open.pane));
						}
					}
					total
				}
			},
		};
		let store = view.flush()?;
		let hash = group_hash(group)?;
		row.published = publisher.publish(
			store,
			group,
			group_id,
			C::from_order(row.newest),
			row.published.take(),
			value.as_ref(),
			applied,
		)?;
		let oldest = match oldest {
			Some(oldest) => oldest,
			None => {
				let free = row.newest;
				if cutoff.is_some_and(|cutoff| cutoff >= free) {
					if let Some(old) = prior_oldest {
						expiry_drop(store, &rolling_expiry_key(old, hash))?;
					}
					if existed {
						remove(store, from_ref(&RollingMetaKey(group_id)))?;
					}
					return Ok(());
				}
				free
			}
		};
		row.oldest = oldest;
		if prior_oldest != Some(oldest) {
			if let Some(old) = prior_oldest {
				expiry_drop(store, &rolling_expiry_key(old, hash))?;
			}
			self.expiry.set(
				store,
				vec![(
					rolling_expiry_key(oldest, hash),
					RollingIndexEntry {
						group: group.clone(),
						group_id,
					},
				)],
			)?;
		}
		set(store, vec![(RollingMetaKey(group_id), row)])
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::{BTreeMap, HashMap},
		ops::Bound::Unbounded,
	};

	use reifydb_codec::row::operator::state::decode;
	use reifydb_core::{key::operator::state::GroupId, metrics::heap::HeapSize, state::timer::StateStore};
	use reifydb_macro::operator_state;
	use reifydb_value::{
		util::hash::Hash128,
		value::{datetime::DateTime, duration::Duration},
	};

	use super::{PaneEngine, PaneEvent, PaneGroups, PaneSpans, RollingPane, pane_of, pane_range};
	use crate::{
		operator::state::mock::MockStore,
		window::{
			accumulator::{MergeAccumulator, UnmergeAccumulator, WindowAccumulator},
			engine::{config::WindowEngineConfig, daba::Merge, publish::RowPublisher},
			meta::NoKey,
		},
	};

	#[operator_state]
	#[derive(Clone, Debug, Default, PartialEq)]
	struct Bag {
		rows: BTreeMap<u64, i64>,
	}

	impl HeapSize for Bag {
		fn heap_size(&self) -> usize {
			0
		}
	}

	impl WindowAccumulator for Bag {
		type Contribution = (u64, i64);
		type Output = Vec<(u64, i64)>;

		fn add(&mut self, contribution: &(u64, i64)) {
			self.rows.insert(contribution.0, contribution.1);
		}

		fn remove(&mut self, contribution: &(u64, i64)) {
			self.rows.remove(&contribution.0);
		}

		fn finalize(&self) -> Option<Vec<(u64, i64)>> {
			(!self.rows.is_empty()).then(|| self.rows.iter().map(|(row, value)| (*row, *value)).collect())
		}

		fn is_empty(&self) -> bool {
			self.rows.is_empty()
		}
	}

	impl MergeAccumulator for Bag {
		fn merge(&mut self, other: &Self) {
			self.rows.extend(other.rows.iter().map(|(row, value)| (*row, *value)));
		}
	}

	impl UnmergeAccumulator for Bag {
		fn unmerge(&mut self, other: &Self) {
			for row in other.rows.keys() {
				self.rows.remove(row);
			}
		}
	}

	type Engine = PaneEngine<u32, DateTime, Bag>;

	struct Lcg(u64);

	impl Lcg {
		fn below(&mut self, bound: u64) -> u64 {
			self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			(self.0 >> 33) % bound
		}
	}

	fn ms(millis: u64) -> DateTime {
		DateTime::from_millis(i64::try_from(millis).unwrap())
	}

	fn span(millis: u64) -> Duration {
		Duration::from_milliseconds(millis as i64).unwrap()
	}

	#[derive(Clone, Copy)]
	struct Shape {
		size: u64,
		running: bool,
		lag: u64,
		lateness: u64,
		immutable: Option<u64>,
	}

	fn engine(shape: Shape) -> Engine {
		let spans = PaneSpans {
			size: span(shape.size),
			lag: span(shape.lag),
			lateness: span(shape.lateness),
			retract: span(shape.immutable.unwrap_or(shape.lateness)),
		};
		let config = WindowEngineConfig::builder().build();
		if shape.running {
			Engine::running(config, spans)
		} else {
			Engine::queue(config, spans, Merge::of())
		}
	}

	#[derive(Default)]
	struct Model {
		panes: BTreeMap<u32, BTreeMap<u64, BTreeMap<u64, i64>>>,
		newest: HashMap<u32, u64>,
	}

	impl Model {
		fn cutoff(&self, shape: Shape, group: u32, clock: Option<u64>) -> Option<u64> {
			let own = self.newest.get(&group).and_then(|newest| newest.checked_sub(shape.lag + shape.size));
			match (clock, own) {
				(Some(clock), Some(own)) => Some(clock.max(own)),
				(clock, own) => clock.or(own),
			}
		}

		fn admits(&self, shape: Shape, group: u32, pane: u64, cutoff: Option<u64>, retraction: bool) -> bool {
			if self.cutoff(shape, group, cutoff).is_some_and(|cutoff| pane <= cutoff) {
				return false;
			}
			let line = if retraction {
				shape.immutable.unwrap_or(shape.lateness)
			} else {
				shape.lateness
			};
			self.newest.get(&group).is_none_or(|newest| pane >= newest.saturating_sub(line))
		}

		fn expire(&mut self, shape: Shape, group: u32, clock: Option<u64>) {
			if let Some(cutoff) = self.cutoff(shape, group, clock)
				&& let Some(panes) = self.panes.get_mut(&group)
			{
				panes.retain(|pane, _| *pane > cutoff);
			}
			self.forget(group);
		}

		fn forget(&mut self, group: u32) {
			if self.panes.get(&group).is_some_and(|panes| panes.values().all(BTreeMap::is_empty)) {
				self.panes.remove(&group);
				self.newest.remove(&group);
			}
		}

		fn output(&self, shape: Shape, group: u32) -> Option<Vec<(u64, i64)>> {
			let output = self.newest.get(&group)?.checked_sub(shape.lag)?;
			let rows: BTreeMap<u64, i64> = self
				.panes
				.get(&group)?
				.range(..=output)
				.flat_map(|(_, rows)| rows.iter().map(|(row, value)| (*row, *value)))
				.collect();
			(!rows.is_empty()).then(|| rows.into_iter().collect())
		}
	}

	fn drive(shape: Shape, seed: u64) -> (usize, u64, usize) {
		let mut engine = engine(shape);
		let mut store = MockStore::default();
		let mut rng = Lcg(seed | 1);
		let mut model = Model::default();
		let mut published: HashMap<u32, Vec<(u64, i64)>> = HashMap::new();
		let mut live: Vec<(u32, u64, u64, i64)> = Vec::new();
		let (mut base, mut row, mut cutoff) = (1_000u64, 0u64, None::<u64>);
		let (mut emitted, mut dropped, mut expired) = (0usize, 0u64, 0usize);
		for round in 0..400u64 {
			let mut groups: PaneGroups<u32, DateTime, (u64, i64)> = BTreeMap::new();
			let mut admitted: Vec<(u32, u64, u64, i64, bool)> = Vec::new();
			let mut round_dropped = 0u64;
			let mut push = |model: &Model, group: u32, pane: u64, row: u64, value: i64, add: bool| {
				let event = if add {
					PaneEvent::Insert(ms(pane), row, (row, value))
				} else {
					PaneEvent::Remove(ms(pane), row, (row, value))
				};
				groups.entry(group)
					.or_insert_with(|| {
						(GroupId::hashed(Hash128(1_000 + group as u128)), Vec::new())
					})
					.1
					.push(event);
				if model.admits(shape, group, pane, cutoff, !add) {
					admitted.push((group, pane, row, value, add));
				} else {
					round_dropped += 1;
				}
			};
			if round % 2 == 1 && !live.is_empty() {
				let (group, pane, gone, value) =
					live.swap_remove(rng.below(live.len() as u64) as usize);
				push(&model, group, pane, gone, value, false);
			}
			for _ in 0..=rng.below(3) {
				row += 1;
				let group = rng.below(3) as u32;
				let pane = base.saturating_sub(rng.below(12));
				let value = rng.below(100) as i64;
				push(&model, group, pane, row, value, true);
				live.push((group, pane, row, value));
			}
			let touched: Vec<u32> = admitted.iter().map(|(group, ..)| *group).collect();
			for (group, pane, row, value, add) in admitted {
				let rows = model.panes.entry(group).or_default().entry(pane).or_default();
				if add {
					rows.insert(row, value);
				} else {
					rows.remove(&row);
				}
				let newest = model.newest.entry(group).or_insert(pane);
				*newest = (*newest).max(pane);
			}
			if round % 9 == 8 {
				cutoff = Some(base.saturating_sub(40));
			}
			for group in touched {
				model.expire(shape, group, cutoff);
			}
			let applied = engine
				.apply(
					&mut store,
					groups,
					cutoff.map(ms),
					&Bag::default,
					&RowPublisher {
						key: Some(&|_: &u32| NoKey),
						finalize: &|_: &u32, _: DateTime, bag: &Bag| bag.finalize(),
					},
				)
				.unwrap();
			assert_eq!(
				applied.sealed_drops + applied.stale_retractions,
				round_dropped,
				"seed {seed} round {round}: the engine dropped a different count than the lines allow"
			);
			dropped += round_dropped;
			let mut emits = applied.emits;
			if round % 13 == 12 {
				let at = base.saturating_sub(30);
				cutoff = Some(cutoff.map_or(at, |c| c.max(at)));
				let groups: Vec<u32> = model.panes.keys().copied().collect();
				for group in groups {
					model.expire(shape, group, cutoff);
				}
				let out = engine
					.expire(
						&mut store,
						ms(cutoff.unwrap()),
						&RowPublisher::<_, NoKey, _, _, _> {
							key: None,
							finalize: &|_: &u32, _: DateTime, bag: &Bag| bag.finalize(),
						},
					)
					.unwrap();
				expired += out.len();
				emits.extend(out);
			}
			for emit in emits {
				emitted += 1;
				match emit.post {
					Some(post) => published.insert(emit.group, post.last_value),
					None => published.remove(&emit.group),
				};
			}
			for group in 0..3u32 {
				assert_eq!(
					published.get(&group).cloned(),
					model.output(shape, group),
					"seed {seed} round {round}: group {group} publishes a window the panes disagree with"
				);
			}
			live.retain(|(group, pane, row, _)| {
				model.panes
					.get(group)
					.and_then(|panes| panes.get(pane))
					.is_some_and(|rows| rows.contains_key(row))
			});
			base += rng.below(3);
		}
		(emitted, dropped, expired)
	}

	fn shapes() -> Vec<Shape> {
		let mut shapes = Vec::new();
		for running in [true, false] {
			for lag in [0, 3] {
				for immutable in [None, Some(9)] {
					shapes.push(Shape {
						size: 25,
						running,
						lag,
						lateness: 4,
						immutable,
					});
				}
			}
		}
		shapes
	}

	#[test]
	fn both_folds_publish_exactly_the_panes_of_a_brute_force_window() {
		// A missed subtract, pop, crossing or drop line leaves a published row the panes do not support.
		for shape in shapes() {
			for seed in 0..12 {
				let (emitted, dropped, expired) = drive(shape, seed);
				assert!(emitted > 300, "the churn must publish often, emitted {emitted}");
				assert!(
					dropped > 10,
					"late rows must reach the seal and retract lines, dropped {dropped}"
				);
				assert!(expired > 0, "the expiry pass must emit, expired {expired}");
			}
		}
	}

	#[test]
	fn a_call_on_a_long_window_reads_a_bounded_number_of_pane_rows() {
		// Reading the sealed middle on every call is the cost this engine exists to remove.
		for running in [true, false] {
			let shape = Shape {
				size: 60_000,
				running,
				lag: 0,
				lateness: 4,
				immutable: None,
			};
			let mut engine = engine(shape);
			let mut store = MockStore::default();
			let group = GroupId::hashed(Hash128(1));
			let mut visited = Vec::new();
			for pane in 1_000..3_000u64 {
				let mut groups: PaneGroups<u32, DateTime, (u64, i64)> = BTreeMap::new();
				groups.insert(1, (group, vec![PaneEvent::Insert(ms(pane), pane, (pane, 1))]));
				let before = store.rows_visited();
				engine.apply(
					&mut store,
					groups,
					None,
					&Bag::default,
					&RowPublisher {
						key: Some(&|_: &u32| NoKey),
						finalize: &|_: &u32, _: DateTime, bag: &Bag| bag.finalize(),
					},
				)
				.unwrap();
				visited.push(store.rows_visited() - before);
			}
			assert_eq!(store.pane_count(), 2_000, "every pane must still be held");
			let worst = visited[100..].iter().max().copied().unwrap();
			assert!(worst <= 16, "running {running}: one call visited {worst} pane rows out of 2000");
		}
	}

	#[operator_state]
	#[derive(Clone, Debug, Default, PartialEq)]
	struct Tally {
		n: u64,
		sum: i64,
		raw: Vec<i64>,
	}

	impl HeapSize for Tally {
		fn heap_size(&self) -> usize {
			0
		}
	}

	impl WindowAccumulator for Tally {
		type Contribution = (u64, i64);
		type Output = i64;

		fn add(&mut self, contribution: &(u64, i64)) {
			self.n += 1;
			self.sum += contribution.1;
			self.raw.push(contribution.1);
		}

		fn remove(&mut self, contribution: &(u64, i64)) {
			self.n -= 1;
			self.sum -= contribution.1;
			if let Some(at) = self.raw.iter().position(|value| *value == contribution.1) {
				self.raw.remove(at);
			}
		}

		fn finalize(&self) -> Option<i64> {
			(self.n > 0).then_some(self.sum)
		}

		fn is_empty(&self) -> bool {
			self.n == 0
		}
	}

	impl MergeAccumulator for Tally {
		fn merge(&mut self, other: &Self) {
			self.n += other.n;
			self.sum += other.sum;
			self.raw.extend_from_slice(&other.raw);
		}

		fn compact(&mut self) {
			self.raw.clear();
		}
	}

	#[test]
	fn queued_panes_are_compacted_by_their_partial_write_and_never_rewritten_on_queue_entry() {
		// A raw partial means a missed compaction; no raw queued row means a queue entry rewrite.
		let spans = PaneSpans {
			size: span(60),
			lag: span(0),
			lateness: span(4),
			retract: span(4),
		};
		let mut engine: PaneEngine<u32, DateTime, Tally> =
			PaneEngine::queue(WindowEngineConfig::builder().build(), spans, Merge::of());
		let mut store = MockStore::default();
		let group = GroupId::hashed(Hash128(7));
		let mut rng = Lcg(5);
		let (mut partials, mut raw_queued) = (0usize, 0usize);
		for pane in 1_000..1_400u64 {
			let mut events = Vec::new();
			for row in 0..=rng.below(3) {
				events.push(PaneEvent::Insert(
					ms(pane),
					pane * 10 + row,
					(pane * 10 + row, rng.below(50) as i64),
				));
			}
			let mut groups: PaneGroups<u32, DateTime, (u64, i64)> = BTreeMap::new();
			groups.insert(1, (group, events));
			engine.apply(
				&mut store,
				groups,
				None,
				&Tally::default,
				&RowPublisher {
					key: Some(&|_: &u32| NoKey),
					finalize: &|_: &u32, _: DateTime, t: &Tally| t.finalize(),
				},
			)
			.unwrap();
			let queue_line = engine.lines(ms(pane)).queue.unwrap_or(0);
			for (key, row) in store.state_page(pane_range(group, Unbounded, Unbounded), None).unwrap() {
				let stored = pane_of(&key).unwrap();
				let row = decode::<RollingPane<Tally>>(&row).unwrap();
				match &row.partial {
					Some(partial) => {
						partials += 1;
						assert!(
							row.pane.raw.is_empty() && partial.raw.is_empty(),
							"pane {stored}: a stored partial sits next to raw samples"
						);
					}
					None if stored <= queue_line && !row.pane.raw.is_empty() => raw_queued += 1,
					None => {}
				}
			}
		}
		assert!(partials > 1_000, "the queue must convert panes often, saw {partials} partial rows");
		assert!(raw_queued > 0, "every queued pane was rewritten compacted before its partial write");
	}
}
