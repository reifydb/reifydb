// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! DDL instruction handlers. Each operates on the catalog through an admin transaction and validates against
//! existing catalog state first, so a conflicting statement fails before the transaction commits.

pub mod alter;
pub mod create;
pub mod drop;
pub mod grant;
pub mod migrate;
pub mod revoke;
