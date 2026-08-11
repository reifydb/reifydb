// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Windowed-aggregation authoring surface, one trait per window shape over a `WindowAccumulator`. Each trait's
//! driver owns diff routing, boundary math, late-event drop and state persistence in one place, so an operator
//! only describes its accumulator and how to build an output row.

pub mod bridge;
pub mod rolling;
pub mod rolling_incremental;
pub mod rolling_top_k;
pub mod tumbling;
pub mod tumbling_carry;

use std::{collections::HashMap, hash::Hash};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	key::operator_state::GroupId,
	state::store::{StateStore, TimerKind},
};
use reifydb_flow::window::{
	engine::config::WindowEngineConfig,
	ledger::{FiredAt, SealLedger},
	policy::SEAL_GATE_STEP,
	span::{WindowCoord, WindowSpan},
};
use reifydb_value::{
	Result,
	config::Config,
	value::{datetime::DateTime, duration::Duration},
};

use crate::flow::operator::context::OperatorContext;

pub(crate) fn seal_frontier(store: &mut impl StateStore) -> Result<DateTime> {
	let ledger = SealLedger::read_order(store)?.unwrap_or(0);
	let watermark = store.flow_watermark()?.map_or(0, |at| at.to_millis());
	Ok(<DateTime as WindowCoord>::from_order(ledger.max(watermark)))
}

pub(crate) fn advance_seal_frontier(store: &mut impl StateStore, fired: FiredAt) -> Result<DateTime> {
	SealLedger::advance(store, fired)?;
	seal_frontier(store)
}

pub(crate) fn bucket_of(coord: DateTime, size: Duration) -> DateTime {
	WindowSpan::for_coord(coord, size).start
}

pub(crate) fn seal_horizon_of(frontier: DateTime, seal_after: Duration) -> DateTime {
	frontier.saturating_sub(seal_after)
}

pub(crate) fn arm_seal_timer(store: &mut impl StateStore, newest_window: DateTime, seal_after: Duration) -> Result<()> {
	let at = newest_window.saturating_add(seal_after).saturating_add(SEAL_GATE_STEP);
	store.arm_timer(at, TimerKind::Seal, &EncodedKey::new(Vec::new()))
}

pub(crate) type WindowGroups<G, C> = HashMap<(G, C), GroupId>;

pub(crate) fn intern_window_groups<G, C>(
	ctx: &mut impl OperatorContext,
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
	Ok(windows.into_iter().zip(ctx.intern_groups(&keys)?).collect())
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
