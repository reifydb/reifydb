// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_sdk::flow::operator::extern_c::binding::operator::ExternCOperatorAdapter;
use reifydb_testing_sdk::harness::ExternCOperatorHarness;

pub type Harness<C> = ExternCOperatorHarness<ExternCOperatorAdapter<C>>;

mod error_abort;
mod flush_cadence;
mod row_number_registry;
mod window_count;
