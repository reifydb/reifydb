// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(crate) mod expiry;
pub mod operator;
pub mod partition;
pub(crate) mod store;

pub use operator::{LookupConfig, LookupOperator};
pub use partition::lookup_partition;
