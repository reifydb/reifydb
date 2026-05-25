// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Windowed-aggregation authoring surface.
//!
//! An operator implements one of the windowed authoring traits over a
//! [`accumulator::WindowAccumulator`]:
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
//! Coordinate machinery lives in [`span`]; the reusable accumulator primitives
//! in [`accumulator`].

pub mod accumulator;
pub mod multi_rolling;
pub mod rolling;
pub mod rolling_incremental;
pub mod span;
pub mod tumbling;
pub mod tumbling_carry;
