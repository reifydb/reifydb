// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Shared windowed-aggregation engine.
//!
//! Schema-agnostic core for both faces: the sdk drivers (static `Row` output)
//! and the flow Window/Aggregate operators (dynamic `Columns`/`RowShape`
//! output). [`accumulator`] holds the invertible [`accumulator::WindowAccumulator`]
//! trait and the reusable accumulator primitives; [`span`] holds the coordinate
//! machinery ([`span::Slot`], [`span::WindowSpan`]).

pub mod accumulator;
pub mod engine;
pub mod span;
pub mod state;
pub mod store;
