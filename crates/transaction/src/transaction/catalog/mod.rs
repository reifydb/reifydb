// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::change::CatalogTrackChangeOperations;

use crate::transaction::admin::AdminTransaction;

pub mod authentication;
pub mod config;
pub mod dictionary;
pub mod flow;
pub mod granted_role;
pub mod handler;
pub mod identity;
pub mod migration;
pub mod namespace;
pub mod policy;
pub mod procedure;
pub mod ringbuffer;
pub mod role;
pub mod series;
pub mod sink;
pub mod source;
pub mod sumtype;
pub mod table;
pub mod test;
pub mod view;

// Implement the umbrella trait for AdminTransaction
impl CatalogTrackChangeOperations for AdminTransaction {}
