// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Windowed-aggregation authoring surface, one trait per window shape over a `WindowAccumulator`. Each trait's
//! driver owns diff routing, boundary math, late-event drop and state persistence in one place, so an operator
//! only describes its accumulator and how to build an output row.

pub mod bridge;
pub mod multi_rolling;
pub mod rolling;
pub mod rolling_incremental;
pub mod tumbling;
pub mod tumbling_carry;

use std::{collections::HashMap, hash::Hash};

use reifydb_abi::operator::timer::TimerKind;
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	key::operator_group_state::GroupId,
	metrics::heap::StatePool,
	state::{budget::OperatorStateBudgetHandle, store::StateStore},
};
use reifydb_flow::window::{
	engine::config::WindowEngineConfig,
	ledger::{FiredAt, SealLedger},
	span::{WindowCoord, WindowSpan},
};
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	value::{datetime::DateTime, duration::Duration},
};

use crate::{config::Config, operator::context::OperatorContext};

pub(crate) fn seal_frontier<C: WindowCoord>(store: &mut impl StateStore) -> Result<C> {
	let ledger = SealLedger::read_order(store)?.unwrap_or(0);
	let watermark = store.flow_watermark()?.map_or(0, |at| at.to_millis());
	Ok(C::from_order(ledger.max(watermark)))
}

pub(crate) fn advance_seal_frontier<C: WindowCoord>(store: &mut impl StateStore, fired: FiredAt) -> Result<C> {
	SealLedger::advance(store, fired)?;
	seal_frontier(store)
}

pub(crate) fn bucket_of(coord: DateTime, size: Duration) -> DateTime {
	WindowSpan::for_coord(coord, size).start
}

pub(crate) fn seal_horizon_of<C: WindowCoord>(frontier: C, seal_after: Duration) -> C {
	C::from_order(
		frontier.to_order().saturating_sub(<DateTime as WindowCoord>::span_millis(seal_after).unwrap_or(0)),
	)
}

pub(crate) fn arm_seal_timer<C: WindowCoord>(
	store: &mut impl StateStore,
	newest_window: C,
	seal_after: Duration,
) -> Result<()> {
	let seal_after_ms = <DateTime as WindowCoord>::span_millis(seal_after).unwrap_or(0);
	let at = <DateTime as WindowCoord>::from_order(
		newest_window.to_order().saturating_add(seal_after_ms).saturating_add(1),
	);
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

pub(crate) fn window_engine_config(config: &Config) -> WindowEngineConfig {
	let budget = match config.usize("state_budget_bytes") {
		Some(bytes) => OperatorStateBudgetHandle::new(ByteSize::from_bytes(bytes as u64)),
		None => OperatorStateBudgetHandle::default(),
	};
	WindowEngineConfig::builder(budget).build()
}

pub(crate) struct WindowedBudget {
	handle: OperatorStateBudgetHandle,
	lease_governed: bool,
}

impl WindowedBudget {
	pub(crate) fn new(config: &Config, engine_config: &WindowEngineConfig) -> Self {
		Self {
			handle: engine_config.budget(),
			lease_governed: config.usize("state_budget_bytes").is_none(),
		}
	}

	pub(crate) fn sync_from_lease(&self, lease_bytes: u64) {
		if self.lease_governed && lease_bytes > 0 {
			self.handle.set_budget(ByteSize::from_bytes(lease_bytes));
		}
	}

	pub(crate) fn stat(&self) -> StatePool {
		StatePool {
			budget: self.handle.snapshot().budget,
			evictions: self.handle.evictions(),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_core::key::operator_group_state::group_data_of_inner;
	use reifydb_value::{byte_size::ByteSize, value::Value};

	use crate::{
		config::Config,
		operator::windowed::{WindowedBudget, window_engine_config},
		state::utils::empty_key,
	};

	#[test]
	fn state_a_driver_addresses_without_a_group_can_never_be_reclaimed() {
		// Node-scope state belongs to no group and reclamation must never touch it. The guarantee is
		// structural: its key is empty, carries no group id, and the drop predicate refuses it outright.
		let key = empty_key();

		assert!(key.as_bytes().is_empty());
		assert_eq!(
			group_data_of_inner(key.as_bytes()),
			None,
			"node-scope state must not be attributable to any group"
		);
	}

	#[test]
	fn lease_governs_the_budget_when_config_has_no_override() {
		// Without an override the guest budget must follow the host lease, or every guest self-governs on a
		// private 2 GiB pool and the shared pool bounds nothing.
		let config = Config::new("test", BTreeMap::new());
		let engine_config = window_engine_config(&config);
		let budget = WindowedBudget::new(&config, &engine_config);

		budget.sync_from_lease(64 * 1024 * 1024);

		assert_eq!(engine_config.budget().snapshot().budget, ByteSize::from_bytes(64 * 1024 * 1024));
	}

	#[test]
	fn missing_lease_keeps_the_default_budget() {
		// Lease 0 means no lease arrived at all (standalone or harness hosts), and collapsing the budget to
		// zero would evict everything on every apply.
		let config = Config::new("test", BTreeMap::new());
		let engine_config = window_engine_config(&config);
		let default_budget = engine_config.budget().snapshot().budget;
		let budget = WindowedBudget::new(&config, &engine_config);

		budget.sync_from_lease(0);

		assert_eq!(engine_config.budget().snapshot().budget, default_budget);
	}

	#[test]
	fn explicit_config_override_wins_over_the_lease() {
		// The apply config is the operator author's escape hatch, so the lease must never overwrite it.
		let mut values = BTreeMap::new();
		values.insert("state_budget_bytes".to_string(), Value::Uint8(512 * 1024));
		let config = Config::new("test", values);
		let engine_config = window_engine_config(&config);
		let budget = WindowedBudget::new(&config, &engine_config);

		budget.sync_from_lease(64 * 1024 * 1024);

		assert_eq!(engine_config.budget().snapshot().budget, ByteSize::from_bytes(512 * 1024));
	}
}
