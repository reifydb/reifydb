// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[path = "operator/common.rs"]
mod common;

#[path = "operator/tumbling.rs"]
mod tumbling;

#[path = "operator/rolling.rs"]
mod rolling;

#[path = "operator/multi_rolling.rs"]
mod multi_rolling;

#[path = "operator/tumbling_carry.rs"]
mod tumbling_carry;

#[path = "operator/rolling_incremental.rs"]
mod rolling_incremental;

#[path = "operator/accumulator.rs"]
mod accumulator;
