// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_sdk::{operator::FFIOperatorAdapter, testing::harness::FFIOperatorHarness};

pub type Harness<C> = FFIOperatorHarness<FFIOperatorAdapter<C>>;

mod error_abort;
mod flush_cadence;
mod row_number_registry;
mod store_range;
mod window_count;
