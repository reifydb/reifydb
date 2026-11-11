// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	fmt::Debug,
	hash::Hash,
};

use reifydb_codec::row::operator::state::{OperatorState, StateCodec};
use reifydb_core::{
	common::OnEmpty, interface::catalog::flow::OperatorId, internal_err, key::operator::state::GroupId,
	metrics::heap::HeapSize, state::timer::TimerKind,
};
use reifydb_value::Result;

use crate::{
	operator::{
		drops::DropCounter,
		state::seal::{
			coord::Coord,
			domain::SealDomain,
			ledger::{FiredAt, SealLedger},
		},
	},
	window::{
		accumulator::{Contribution, CountedAccumulator, WindowAccumulator},
		driver::{
			rows::{WindowColumns, WindowDiff, WindowRows},
			store::WindowStore,
		},
		engine::{
			pane::{
				PaneEngine, PaneEvent, PaneGroups, PanePublisher, RollingEmit, RollingGroup,
				RollingIndexEntry, RollingPane,
			},
			publish::RowPublisher,
		},
		meta::RollingPublished,
		prefetch::Prefetched,
		span::WindowAnchor,
	},
};

pub struct RoutedPanes<G, C, X> {
	pub groups: PaneGroups<G, C, X>,
	pub order: Vec<G>,
}

impl<G: Clone + Ord, C, X> RoutedPanes<G, C, X> {
	fn push(&mut self, group: &G, group_id: &dyn Fn(&G) -> GroupId, event: PaneEvent<C, X>) {
		if !self.groups.contains_key(group) {
			self.order.push(group.clone());
		}
		self.groups.entry(group.clone()).or_insert_with(|| (group_id(group), Vec::new())).1.push(event);
	}

	pub fn is_empty(&self) -> bool {
		self.groups.is_empty()
	}
}

pub trait RollingHooks {
	type Group;
	type Coord;
	type Accumulator: WindowAccumulator;
	type Published: Clone;
	type Emit;
	type Out;

	fn new_accumulator(&self) -> Self::Accumulator;

	fn publisher(
		&self,
	) -> impl PanePublisher<Self::Group, Self::Coord, Self::Accumulator, Published = Self::Published, Emit = Self::Emit>;

	fn emit(&mut self, emits: Vec<Self::Emit>) -> Result<Self::Out>;
}

pub struct RowHooks<'h, G, K, O, A, C> {
	pub new_accumulator: &'h dyn Fn() -> A,
	pub key: Option<&'h dyn Fn(&G) -> Result<K>>,
	pub finalize: &'h dyn Fn(&G, C, &A) -> Option<O>,
	pub on_empty: OnEmpty,
	pub empty: &'h dyn Fn(&O) -> Result<Option<O>>,
}

impl<G, K, O, A, C> RollingHooks for RowHooks<'_, G, K, O, A, C>
where
	G: Clone,
	K: Clone,
	O: Clone,
	A: WindowAccumulator,
	C: WindowAnchor,
{
	type Group = G;
	type Coord = C;
	type Accumulator = A;
	type Published = RollingPublished<K, O>;
	type Emit = RollingEmit<G, K, O>;
	type Out = Vec<RollingEmit<G, K, O>>;

	fn new_accumulator(&self) -> A {
		(self.new_accumulator)()
	}

	fn publisher(
		&self,
	) -> impl PanePublisher<G, C, A, Published = RollingPublished<K, O>, Emit = RollingEmit<G, K, O>> {
		RowPublisher {
			key: self.key,
			finalize: self.finalize,
			on_empty: self.on_empty,
			empty: self.empty,
		}
	}

	fn emit(&mut self, emits: Vec<RollingEmit<G, K, O>>) -> Result<Vec<RollingEmit<G, K, O>>> {
		Ok(emits)
	}
}

fn pane_of<C: Coord>(coord: C, pane: Option<C::Span>) -> C {
	match pane {
		Some(pane) => coord.floor_to(pane),
		None => coord,
	}
}

fn located<G, C: Coord, X>(columns: &WindowColumns<G, C, X>, row: usize, pane: Option<C::Span>) -> Result<(C, u64)> {
	let Some(coord) = columns.coords()[row] else {
		return internal_err!("a rolling row at batch position {} carries no coordinate", columns.index()[row]);
	};
	let Some(number) = columns.rows()[row] else {
		return internal_err!("a rolling row at batch position {} carries no row number", columns.index()[row]);
	};
	Ok((pane_of(coord, pane), number.0))
}

pub fn route_rolling<R>(
	rows: &mut R,
	pane: Option<<R::Coord as Coord>::Span>,
	group_id: &dyn Fn(&R::Group) -> GroupId,
) -> Result<RoutedPanes<R::Group, R::Coord, R::Contribution>>
where
	R: WindowRows,
	R::Group: Clone + Ord,
	R::Coord: Coord,
	R::Contribution: Clone,
{
	let mut routed = RoutedPanes {
		groups: BTreeMap::new(),
		order: Vec::new(),
	};
	for diff in 0..rows.diff_count() {
		match rows.diff(diff)? {
			WindowDiff::Insert(post) => {
				for row in 0..post.len() {
					let (at, number) = located(&post, row, pane)?;
					let event = PaneEvent::Insert(at, number, post.contributions()[row].clone());
					routed.push(&post.groups()[row], group_id, event);
				}
			}
			WindowDiff::Remove(pre) => {
				for row in 0..pre.len() {
					let (at, number) = located(&pre, row, pane)?;
					let event = PaneEvent::Remove(at, number, pre.contributions()[row].clone());
					routed.push(&pre.groups()[row], group_id, event);
				}
			}
			WindowDiff::Update {
				pre,
				post,
			} => {
				let mut before: HashMap<usize, usize> = pre
					.index()
					.iter()
					.enumerate()
					.map(|(row, &position)| (position, row))
					.collect();
				for row in 0..post.len() {
					let (post_at, number) = located(&post, row, pane)?;
					let group = &post.groups()[row];
					let contribution = post.contributions()[row].clone();
					let Some(prior) = before.remove(&post.index()[row]) else {
						routed.push(
							group,
							group_id,
							PaneEvent::Insert(post_at, number, contribution),
						);
						continue;
					};
					let (pre_at, pre_number) = located(&pre, prior, pane)?;
					let pre_group = &pre.groups()[prior];
					let pre_contribution = pre.contributions()[prior].clone();
					if pre_group == group {
						let event = PaneEvent::Update {
							row: number,
							pre: (pre_at, pre_contribution),
							post: (post_at, contribution),
						};
						routed.push(group, group_id, event);
					} else {
						let leave = PaneEvent::Leave(pre_at, pre_number, pre_contribution);
						routed.push(pre_group, group_id, leave);
						routed.push(
							group,
							group_id,
							PaneEvent::Join(post_at, number, contribution),
						);
					}
				}
				let mut removed: Vec<usize> = before.into_values().collect();
				removed.sort_unstable();
				for prior in removed {
					let (at, number) = located(&pre, prior, pane)?;
					let event = PaneEvent::Remove(at, number, pre.contributions()[prior].clone());
					routed.push(&pre.groups()[prior], group_id, event);
				}
			}
		}
	}
	Ok(routed)
}

pub fn rolling_cutoff<C: Coord>(clock: C, size: C::Span, lag: C::Span, pane: Option<C::Span>) -> Option<C> {
	clock.checked_sub_span(lag).and_then(|end| end.checked_sub_span(size)).map(|cutoff| pane_of(cutoff, pane))
}

pub struct RollingRunner<C: Coord> {
	size: C::Span,
	lag: C::Span,
	pane: Option<C::Span>,
	sealed_drops: DropCounter,
	stale_retractions: DropCounter,
}

type HookGroup<H, A> = RollingGroup<CountedAccumulator<A>, <H as RollingHooks>::Published, Contribution<A>>;

impl<C: SealDomain> RollingRunner<C> {
	pub fn new(operator: OperatorId, size: C::Span, lag: C::Span, pane: Option<C::Span>) -> Self {
		Self {
			size,
			lag,
			pane,
			sealed_drops: DropCounter::new(operator, "mutations targeting sealed windows"),
			stale_retractions: DropCounter::new(operator, "updates/removes past immutable"),
		}
	}

	pub fn pane(&self) -> Option<C::Span> {
		self.pane
	}

	pub fn route<R>(
		&self,
		rows: &mut R,
		group_id: &dyn Fn(&R::Group) -> GroupId,
	) -> Result<RoutedPanes<R::Group, C, R::Contribution>>
	where
		R: WindowRows<Coord = C>,
		R::Group: Clone + Ord,
		R::Contribution: Clone,
	{
		route_rolling(rows, self.pane, group_id)
	}

	fn cutoff(&self, clock: C) -> Option<C> {
		rolling_cutoff(clock, self.size, self.lag, self.pane)
	}

	pub fn apply<H>(
		&mut self,
		engine: &mut PaneEngine<H::Group, C, H::Accumulator>,
		store: &mut dyn WindowStore,
		routed: RoutedPanes<H::Group, C, Contribution<H::Accumulator>>,
		hooks: &mut H,
	) -> Result<H::Out>
	where
		H: RollingHooks<Coord = C>,
		H::Group: Clone + Ord + Hash + Debug + StateCodec,
		RollingPane<CountedAccumulator<H::Accumulator>>: OperatorState,
		RollingIndexEntry<H::Group>: OperatorState,
		HookGroup<H, H::Accumulator>: OperatorState + HeapSize,
	{
		let cutoff = self.cutoff(C::frontier(store)?);
		let before = engine.earliest_expiry(store)?;
		let keys = engine.state_keys(&routed.groups);
		let applied = {
			let mut prefetched = Prefetched::read(store, keys)?;
			let applied = engine.apply(
				&mut prefetched,
				routed.groups,
				&routed.order,
				cutoff,
				&|| hooks.new_accumulator(),
				&hooks.publisher(),
			)?;
			prefetched.write()?;
			applied
		};
		self.sealed_drops.note(applied.sealed_drops);
		self.stale_retractions.note(applied.stale_retractions);
		let after = engine.earliest_expiry(store)?;
		C::rearm_decay(store, self.size, Some(self.lag), before, after)?;
		hooks.emit(applied.emits)
	}

	pub fn on_timer<H>(
		&mut self,
		engine: &mut PaneEngine<H::Group, C, H::Accumulator>,
		store: &mut dyn WindowStore,
		kind: TimerKind,
		fired: FiredAt,
		hooks: &mut H,
	) -> Result<H::Out>
	where
		H: RollingHooks<Coord = C>,
		H::Group: Clone + Ord + Hash + Debug + StateCodec,
		RollingPane<CountedAccumulator<H::Accumulator>>: OperatorState,
		RollingIndexEntry<H::Group>: OperatorState,
		HookGroup<H, H::Accumulator>: OperatorState + HeapSize,
	{
		if kind != TimerKind::Seal {
			return internal_err!("a rolling window fired a {:?} timer it never arms", kind);
		}
		SealLedger::advance(store, fired)?;
		let before = engine.earliest_expiry(store)?;
		let emits = match self.cutoff(C::from_order(fired.at().to_order())) {
			Some(cutoff) => engine.expire(store, cutoff, &hooks.publisher())?,
			None => Vec::new(),
		};
		let after = engine.earliest_expiry(store)?;
		C::rearm_decay(store, self.size, Some(self.lag), before, after)?;
		hooks.emit(emits)
	}
}
