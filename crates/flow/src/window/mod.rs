// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Shared windowed-aggregation engine: one schema-agnostic core behind both faces, the sdk drivers (static `Row`
//! output) and the flow Window/Aggregate operators (dynamic `Columns`/`RowShape` output).

pub mod accumulator;
pub mod coord;
pub mod driver;
pub mod engine;
pub mod kind;
pub mod meta;
pub mod span;
