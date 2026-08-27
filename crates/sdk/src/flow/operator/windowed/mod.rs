// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod guest_as_host;
pub mod rolling;
pub mod rolling_incremental;
pub mod rolling_top_k;
pub mod tumbling;
pub mod tumbling_carry;

use std::{collections::HashMap, hash::Hash};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	key::operator_state::GroupId,
	state::timer::{StateStore, TimerStore},
};
use reifydb_flow::{
	operator::state::seal::{
		coord::Coord,
		domain::SealDomain,
		ledger::{FiredAt, SealLedger},
	},
	timer::Timer as FlowTimer,
	window::{engine::config::WindowEngineConfig, span::WindowSpan},
};
use reifydb_value::{Result, config::Config};

use crate::flow::operator::{context::GuestContext, timer::Timer};

pub(crate) fn seal_frontier<C: SealDomain>(store: &mut (impl StateStore + TimerStore)) -> Result<C> {
	C::frontier(store)
}

pub(crate) fn observe_batch<C: SealDomain>(
	store: &mut (impl StateStore + TimerStore),
	newest: C,
	lateness: C::Lateness,
) -> Result<()> {
	C::observe(store, newest, lateness)
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

pub(crate) type WindowGroups<G, C> = HashMap<(G, C), GroupId>;

pub(crate) fn intern_window_groups<G, C>(
	ctx: &mut impl GuestContext,
	windows: impl IntoIterator<Item = ((G, C), EncodedKey)>,
) -> Result<WindowGroups<G, C>>
where
	G: Clone + Eq + Hash,
	C: Copy + Eq + Hash,
{
	let (windows, keys): (Vec<(G, C)>, Vec<EncodedKey>) = windows.into_iter().unzip();
	if windows.is_empty() {
		return Ok(WindowGroups::new());
	}
	Ok(windows.into_iter().zip(ctx.intern_groups(&keys)?).map(|(window, (group, _))| (window, group)).collect())
}

pub(crate) fn group_of<G, C>(groups: &WindowGroups<G, C>, group: &G, coord: C) -> GroupId
where
	G: Clone + Eq + Hash,
	C: Copy + Eq + Hash,
{
	groups.get(&(group.clone(), coord)).copied().expect("every routed window is interned before the engine runs")
}

pub(crate) fn window_engine_config(_config: &Config) -> WindowEngineConfig {
	WindowEngineConfig::builder().build()
}

#[cfg(test)]
mod tests {
	use reifydb_core::key::operator_state::group_data_of_inner;

	use crate::flow::operator::state::utils::empty_key;

	#[test]
	fn state_a_driver_addresses_without_a_group_can_never_be_reclaimed() {
		// State addressed without a group carries no group id, so reclamation must never attribute or touch it.
		let key = empty_key();

		assert!(key.as_bytes().is_empty());
		assert_eq!(
			group_data_of_inner(key.as_bytes()),
			None,
			"state addressed without a group must not be attributable to any group"
		);
	}
}
