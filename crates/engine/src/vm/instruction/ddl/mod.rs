// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! DDL instruction handlers. CREATE, ALTER, DROP, GRANT, REVOKE, and MIGRATE operate against the catalog tier
//! through admin transactions and emit catalog change events that the materialised view picks up. Each operation
//! validates against the existing catalog state before applying so a CREATE that conflicts with an existing
//! object fails before the transaction commits.

pub mod alter;
pub mod create;
pub mod drop;
pub mod grant;
pub mod migrate;
pub mod revoke;
