// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Windowed-aggregation authoring surface.
//!
//! An operator implements one of the windowed authoring traits over a
//! `reifydb_core::window::accumulator::WindowAccumulator`:
//! - [`tumbling::TumblingOperator`] - non-overlapping windows.
//! - [`tumbling_carry::TumblingCarryOperator`] - tumbling windows that carry a value forward into the next window
//!   (EMA-family, prev-close, Heikin-Ashi).
//! - [`rolling::RollingOperator`] / [`rolling_incremental::RollingIncrementalOperator`]
//!   - overlapping rolling buffers of the last N windows.
//! - [`multi_rolling::MultiRollingOperator`] - rolling windows that emit multiple rows per group (top-K).
//!
//! The matching driver handles diff routing uniformly (`Insert -> add`,
//! `Update -> remove(pre) + add(post)`, `Remove -> remove(pre)`), window
//! boundary math, late-event drop, and state persistence in one place, so the
//! operator only describes its accumulator and how to build an output row.
//! Coordinate machinery lives in `reifydb_core::window::span`; the reusable
//! accumulator primitives in `reifydb_core::window::accumulator`.

pub mod bridge;
pub mod multi_rolling;
pub mod rolling;
pub mod rolling_incremental;
pub mod tumbling;
pub mod tumbling_carry;

use std::{collections::HashMap, hash::Hash};

use reifydb_codec::{
	key::encoded::EncodedKey,
	state::{OperatorState, decode_state},
};
use reifydb_core::{
	key::operator_state::GroupId,
	metrics::heap::StatePool,
	state::{budget::OperatorStateBudgetHandle, horizon::GroupPosition, store::StateStore},
	window::engine::config::WindowEngineConfig,
};
use reifydb_value::{Result, byte_size::ByteSize};

use crate::{config::Config, operator::context::OperatorContext};

const SEAL_WATERMARK_KEY: &[u8] = b"sdkwmk";

pub(crate) fn advance_seal_watermark(store: &mut impl StateStore, batch_max: u64) -> Result<u64> {
	let key = EncodedKey::new(SEAL_WATERMARK_KEY.to_vec());
	let current: u64 = match store.state_get(&key)? {
		Some(bytes) => decode_state(&bytes)?,
		None => 0,
	};
	if batch_max > current {
		store.state_set(&key, batch_max.encode_state(store.clock_now_nanos())?)?;
		Ok(batch_max)
	} else {
		Ok(current)
	}
}

pub(crate) type WindowGroups<G, C> = HashMap<(G, C), GroupId>;

pub(crate) fn intern_window_groups<G, C>(
	ctx: &mut impl OperatorContext,
	windows: impl IntoIterator<Item = ((G, C), EncodedKey)>,
	position: GroupPosition,
) -> Result<WindowGroups<G, C>>
where
	G: Clone + Eq + Hash,
	C: Copy + Eq + Hash,
{
	let (windows, keys): (Vec<(G, C)>, Vec<EncodedKey>) = windows.into_iter().unzip();
	if windows.is_empty() {
		return Ok(WindowGroups::new());
	}
	Ok(windows.into_iter().zip(ctx.intern_groups(&keys, position)?).collect())
}

pub(crate) fn group_of<G, C>(groups: &WindowGroups<G, C>, group: &G, coord: C) -> GroupId
where
	G: Clone + Eq + Hash,
	C: Copy + Eq + Hash,
{
	groups.get(&(group.clone(), coord)).copied().expect("every routed window is interned before the engine runs")
}

pub(crate) fn window_position(seal_after: Option<u64>, watermark: u64) -> GroupPosition {
	match seal_after {
		Some(_) => GroupPosition::Event(watermark),
		None => GroupPosition::Version,
	}
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

	use reifydb_value::{byte_size::ByteSize, value::Value};

	use crate::{
		config::Config,
		operator::windowed::{WindowedBudget, window_engine_config},
	};

	#[test]
	fn lease_governs_the_budget_when_config_has_no_override() {
		// Decision D2/D3: without an explicit state_budget_bytes the
		// guest budget must follow the host lease, otherwise every
		// guest self-governs on a private 2 GiB pool and the shared
		// pool bounds nothing.
		let config = Config::new("test", BTreeMap::new());
		let engine_config = window_engine_config(&config);
		let budget = WindowedBudget::new(&config, &engine_config);

		budget.sync_from_lease(64 * 1024 * 1024);

		assert_eq!(engine_config.budget().snapshot().budget, ByteSize::from_bytes(64 * 1024 * 1024));
	}

	#[test]
	fn missing_lease_keeps_the_default_budget() {
		// Decision D3: lease 0 means no lease arrived (standalone or
		// harness hosts); collapsing the budget to zero would evict
		// everything on every apply.
		let config = Config::new("test", BTreeMap::new());
		let engine_config = window_engine_config(&config);
		let default_budget = engine_config.budget().snapshot().budget;
		let budget = WindowedBudget::new(&config, &engine_config);

		budget.sync_from_lease(0);

		assert_eq!(engine_config.budget().snapshot().budget, default_budget);
	}

	#[test]
	fn explicit_config_override_wins_over_the_lease() {
		// Decision D2: state_budget_bytes in the apply config is the
		// operator author's escape hatch; the lease must never
		// overwrite it.
		let mut values = BTreeMap::new();
		values.insert("state_budget_bytes".to_string(), Value::Uint8(512 * 1024));
		let config = Config::new("test", values);
		let engine_config = window_engine_config(&config);
		let budget = WindowedBudget::new(&config, &engine_config);

		budget.sync_from_lease(64 * 1024 * 1024);

		assert_eq!(engine_config.budget().snapshot().budget, ByteSize::from_bytes(512 * 1024));
	}
}
