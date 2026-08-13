// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_testing_flow::guest::GuestOperatorHarness;

pub type Harness<C> = GuestOperatorHarness<C>;

mod error_abort;
mod row_number_registry;
mod window_count;
