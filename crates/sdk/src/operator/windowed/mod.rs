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

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::window::{engine::config::WindowEngineConfig, store::WindowStore};
use reifydb_value::Result;

use crate::config::Config;

const SEAL_WATERMARK_KEY: &[u8] = b"sdkwmk";

pub(crate) fn advance_seal_watermark(store: &mut impl WindowStore, batch_max: u64) -> Result<u64> {
	let key = EncodedKey::new(SEAL_WATERMARK_KEY.to_vec());
	let current: u64 = store.internal_get(&key)?.unwrap_or(0);
	if batch_max > current {
		store.internal_set(&key, &batch_max)?;
		Ok(batch_max)
	} else {
		Ok(current)
	}
}

pub(crate) fn window_engine_config(config: &Config) -> WindowEngineConfig {
	let mut builder = WindowEngineConfig::builder();
	if let Some(capacity) = config.usize("state_cache_size") {
		builder = builder.state_cache_capacity(capacity);
	}
	if let Some(capacity) = config.usize("internal_state_cache_size") {
		builder = builder.internal_state_cache_capacity(capacity);
	}
	builder.build()
}
