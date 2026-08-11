// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_sdk::operator::ExternCOperatorAdapter;
use reifydb_testing_sdk::harness::ExternCOperatorHarness;

pub type Harness<C> = ExternCOperatorHarness<ExternCOperatorAdapter<C>>;

mod error_abort;
mod flush_cadence;
mod row_number_registry;
mod store_range;
mod window_count;
