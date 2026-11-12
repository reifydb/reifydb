// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod carry;
pub mod guest_as_host;
pub mod hooks;
pub mod operator;
pub mod plain;
pub mod publish;
pub mod retained;
pub mod rows;
pub mod top_k;

use std::{
	collections::{BTreeMap, HashMap, btree_map::Entry},
	hash::Hash,
};

use reifydb_codec::{key::encoded::EncodedKey, row::operator::state::OperatorState};
use reifydb_core::key::operator::state::GroupId;
use reifydb_flow_async::{
	operator::state::seal::{
		coord::Coord,
		domain::SealDomain,
		ledger::{FiredAt, SealLedger},
	},
	timer::Timer as FlowTimer,
	window::{
		accumulator::{AccumulatorEvents, AccumulatorOp, Contributions, CountedAccumulator, WindowAccumulator},
		driver::store::WindowStore,
		engine::{
			KeyspaceFamily,
			config::WindowEngineConfig,
			pane::{PaneEngine, PaneSpans, RollingIndexEntry, RollingPane},
			tumbling::TumblingBuckets,
		},
		settings::WindowSettings,
		span::WindowSpan,
	},
};
use reifydb_value::{Result, config::ExtensionParams};

use crate::flow::operator::{
	timer::Timer,
	view::ColumnsView,
	windowed::operator::{Emit, KindSet, counted_merge, unmerge_counted},
};

pub(crate) fn operator_watermark<C: SealDomain>(store: &mut impl WindowStore) -> Result<C> {
	C::frontier(store)
}

pub(crate) fn observe_batch<C: SealDomain>(
	store: &mut impl WindowStore,
	newest: C,
	seal_span: C::SealSpan,
) -> Result<()> {
	C::observe(store, newest, seal_span)
}

pub(crate) fn all_rows(cols: &impl ColumnsView) -> Vec<usize> {
	(0..cols.row_count()).collect()
}

pub(crate) fn timer_frontier<C: SealDomain>(store: &mut impl WindowStore, timer: Timer<'_>) -> Result<Option<C>> {
	if !C::arms_timer() {
		return Ok(None);
	}
	SealLedger::advance(store, fired_at(&timer))?;
	C::frontier(store).map(Some)
}

pub(crate) fn fired_at(timer: &Timer<'_>) -> FiredAt {
	FiredAt::of(&FlowTimer {
		due: timer.due,
		kind: timer.kind,
		key: EncodedKey::new(timer.key),
	})
}

pub(crate) struct Routing<G, C, X> {
	buckets: TumblingBuckets<G, C, X>,
	order: Vec<(G, WindowSpan<C>)>,
}

impl<G, C, X> Default for Routing<G, C, X> {
	fn default() -> Self {
		Self {
			buckets: BTreeMap::new(),
			order: Vec::new(),
		}
	}
}

impl<G, C, X> Routing<G, C, X>
where
	G: Clone + Ord,
	X: Contributions,
	WindowSpan<C>: Clone + Ord,
{
	pub(crate) fn push(&mut self, group: G, span: WindowSpan<C>, op: AccumulatorOp, item: X::Item) {
		match self.buckets.entry((group, span)) {
			Entry::Occupied(mut slot) => slot.get_mut().push(op, item),
			Entry::Vacant(slot) => {
				self.order.push(slot.key().clone());
				slot.insert(AccumulatorEvents::new()).push(op, item);
			}
		}
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.buckets.is_empty()
	}

	pub(crate) fn finish(self) -> (TumblingBuckets<G, C, X>, Vec<(G, WindowSpan<C>)>) {
		(self.buckets, self.order)
	}
}

pub(crate) type WindowGroups<G, C> = HashMap<(G, C), (GroupId, EncodedKey)>;

pub(crate) fn intern_window_groups<G, C>(windows: impl IntoIterator<Item = ((G, C), EncodedKey)>) -> WindowGroups<G, C>
where
	G: Clone + Eq + Hash,
	C: Copy + Eq + Hash,
{
	windows.into_iter().map(|(window, key)| (window, (GroupId::of(&key), key))).collect()
}

pub(crate) fn group_of<G, C>(groups: &WindowGroups<G, C>, group: &G, coord: C) -> (GroupId, EncodedKey)
where
	G: Clone + Eq + Hash,
	C: Copy + Eq + Hash,
{
	groups.get(&(group.clone(), coord)).cloned().expect("every routed window is interned before the engine runs")
}

pub(crate) fn window_engine_config(_params: &ExtensionParams) -> WindowEngineConfig {
	WindowEngineConfig::builder().family(KeyspaceFamily::Guest).build()
}

pub(crate) fn rolling_engine<A>(
	config: WindowEngineConfig,
	spans: PaneSpans<<A::Coord as Coord>::Span>,
) -> PaneEngine<A::GroupKey, A::Coord, A::Accumulator>
where
	A: Emit,
	RollingPane<CountedAccumulator<A::Accumulator>>: OperatorState,
	RollingIndexEntry<A::GroupKey>: OperatorState,
{
	match <A::Kinds as KindSet<A>>::RUNNING {
		true => PaneEngine::running_by(config, spans, counted_merge::<A>(), unmerge_counted::<A>),
		false => PaneEngine::queue(config, spans, counted_merge::<A>()),
	}
}

pub(crate) fn rolling_output<A: Emit>(
	aggregator: &A,
	settings: &WindowSettings<A::Coord>,
	pane: <A::Coord as Coord>::Span,
	group: &A::GroupKey,
	newest: A::Coord,
	accumulator: &A::Accumulator,
) -> Option<A::Output> {
	let value = accumulator.finalize()?;
	let end = newest.saturating_sub_span(settings.lag.unwrap_or_default()).add_span(pane);
	let span = WindowSpan::new(end.saturating_sub_span(settings.fixed_size()), end);
	aggregator.build_output(group, span, &value)
}
