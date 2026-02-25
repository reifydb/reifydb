// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::change::CatalogTrackChangeOperations;

use crate::transaction::admin::AdminTransaction;

pub mod dictionary;
pub mod flow;
pub mod handler;
pub mod namespace;
pub mod procedure;
pub mod ringbuffer;
pub mod role;
pub mod security_policy;
pub mod series;
pub mod subscription;
pub mod sumtype;
pub mod table;
pub mod user;
pub mod user_role;
pub mod view;

// Implement the umbrella trait for AdminTransaction
impl CatalogTrackChangeOperations for AdminTransaction {}
