// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::common::harness::NativeOperatorHarness;

pub type Harness<C> = NativeOperatorHarness<C>;

mod error_abort;
mod flush_cadence;
mod lease_resize;
mod row_number_registry;
mod store_range;
mod window_count;
