// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod carry;
pub mod guest_as_host;
pub mod operator;
pub mod plain;
pub mod publish;
pub mod retained;
pub mod top_k;

use std::{
	collections::{BTreeMap, HashMap},
	hash::Hash,
};

use reifydb_codec::key::encoded::{EncodedKey, IntoEncodedKey};
use reifydb_core::{
	key::operator::state::GroupId,
	state::timer::{StateStore, TimerStore},
};
use reifydb_flow_async::{
	operator::state::seal::{
		coord::Coord,
		domain::SealDomain,
		ledger::{FiredAt, SealLedger},
	},
	timer::Timer as FlowTimer,
	window::{
		accumulator::WindowAccumulator,
		engine::{
			KeyspaceFamily,
			config::WindowEngineConfig,
			pane::{PaneEvent, PaneGroups},
		},
		settings::WindowSettings,
		span::WindowSpan,
	},
};
use reifydb_value::{Result, config::ExtensionParams, value::diff_type::DiffType};

use crate::{
	error,
	flow::operator::{
		context::GuestContext,
		timer::Timer,
		view::{ChangeView, ColumnsView, DiffView},
		windowed::operator::{Contribution, Emit, Extracted, WindowedOperator, extract_rows},
	},
};

pub(crate) fn seal_frontier<C: SealDomain>(store: &mut (impl StateStore + TimerStore)) -> Result<C> {
	C::frontier(store)
}

pub(crate) fn observe_batch<C: SealDomain>(
	store: &mut (impl StateStore + TimerStore),
	newest: C,
	seal_span: C::SealSpan,
) -> Result<()> {
	C::observe(store, newest, seal_span)
}

pub(crate) fn timer_frontier<C: SealDomain>(
	store: &mut (impl StateStore + TimerStore),
	timer: Timer<'_>,
) -> Result<Option<C>> {
	if !C::arms_timer() {
		return Ok(None);
	}
	let fired = FiredAt::of(&FlowTimer {
		due: timer.due,
		kind: timer.kind,
		key: EncodedKey::new(timer.key),
	});
	SealLedger::advance(store, fired)?;
	C::frontier(store).map(Some)
}

pub(crate) fn bucket_of<C: Coord>(coord: C, size: C::Span) -> C {
	WindowSpan::for_coord(coord, size).start
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

pub(crate) type RollingGroups<A> =
	PaneGroups<<A as WindowedOperator>::GroupKey, <A as WindowedOperator>::Coord, Contribution<A>>;

pub(crate) fn partition_of<G>(group: &G) -> GroupId
where
	for<'a> &'a G: IntoEncodedKey,
{
	GroupId::of(&group.into_encoded_key())
}

fn extract_all<A: WindowedOperator, C: ColumnsView>(
	aggregator: &A,
	ctx: &mut impl GuestContext,
	cols: &C,
) -> error::Result<Vec<Extracted<A>>> {
	let rows: Vec<usize> = (0..cols.row_count()).collect();
	extract_rows(aggregator, ctx, cols, &rows)
}

pub(crate) fn route_rolling<A>(
	aggregator: &A,
	pane: <A::Coord as Coord>::Span,
	ctx: &mut impl GuestContext,
	change: &impl ChangeView,
) -> error::Result<RollingGroups<A>>
where
	A: WindowedOperator,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	let mut groups: RollingGroups<A> = BTreeMap::new();
	let mut push = |group: A::GroupKey, event: PaneEvent<A::Coord, Contribution<A>>| {
		groups.entry(group.clone()).or_insert_with(|| (partition_of(&group), Vec::new())).1.push(event);
	};
	for di in 0..change.diff_count() {
		let Some(diff) = change.diff(di) else {
			continue;
		};
		match diff.kind() {
			DiffType::Insert => {
				if let Some(cols) = diff.post() {
					for (_, coord, group, contribution) in extract_all(aggregator, ctx, &cols)? {
						push(group, PaneEvent::Insert(bucket_of(coord, pane), contribution));
					}
				}
			}
			DiffType::Remove => {
				if let Some(cols) = diff.pre() {
					for (_, coord, group, contribution) in extract_all(aggregator, ctx, &cols)? {
						push(group, PaneEvent::Remove(bucket_of(coord, pane), contribution));
					}
				}
			}
			DiffType::Update => {
				let (Some(pre), Some(post)) = (diff.pre(), diff.post()) else {
					continue;
				};
				let after = extract_all(aggregator, ctx, &post)?;
				if !A::UPDATE_PRE_RETRACTS {
					for (_, coord, group, contribution) in after {
						push(group, PaneEvent::Insert(bucket_of(coord, pane), contribution));
					}
					continue;
				}
				let mut before: BTreeMap<usize, (A::Coord, A::GroupKey, Contribution<A>)> =
					extract_all(aggregator, ctx, &pre)?
						.into_iter()
						.map(|(row, coord, group, contribution)| {
							(row, (coord, group, contribution))
						})
						.collect();
				for (row, coord, group, contribution) in after {
					let post_pane = bucket_of(coord, pane);
					match before.remove(&row) {
						Some((pre_coord, pre_group, pre_contribution))
							if pre_group == group =>
						{
							push(
								group,
								PaneEvent::Update {
									pre: (
										bucket_of(pre_coord, pane),
										pre_contribution,
									),
									post: (post_pane, contribution),
								},
							)
						}
						Some((pre_coord, pre_group, pre_contribution)) => {
							push(
								pre_group,
								PaneEvent::Remove(
									bucket_of(pre_coord, pane),
									pre_contribution,
								),
							);
							push(group, PaneEvent::Insert(post_pane, contribution));
						}
						None => push(group, PaneEvent::Insert(post_pane, contribution)),
					}
				}
				for (_, (pre_coord, pre_group, pre_contribution)) in before {
					push(
						pre_group,
						PaneEvent::Remove(bucket_of(pre_coord, pane), pre_contribution),
					);
				}
			}
		}
	}
	Ok(groups)
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
	let end = newest.add_span(pane);
	let span = WindowSpan::new(end.saturating_sub_span(settings.fixed_size()), end);
	aggregator.build_output(group, span, &value)
}

pub(crate) fn rolling_cutoff<A: WindowedOperator>(
	settings: &WindowSettings<A::Coord>,
	pane: <A::Coord as Coord>::Span,
	clock: A::Coord,
) -> Option<A::Coord> {
	clock.checked_sub_span(settings.fixed_size()).map(|cutoff| bucket_of(cutoff, pane))
}

#[cfg(test)]
mod tests {
	use reifydb_core::key::operator::state::{GroupId, group_data_of_inner, unmanaged_key};

	#[test]
	fn state_a_driver_addresses_without_a_group_can_never_be_reclaimed() {
		// A key built without a group must carry ROOT, never another group id, or reclaiming that group frees
		// it.
		let key = unmanaged_key(&[]).expect("an empty id fits the keyspace");

		assert_eq!(
			group_data_of_inner(key.as_ref().as_slice()),
			Some(GroupId::ROOT),
			"state addressed without a group must belong to the ROOT group"
		);
	}
}
