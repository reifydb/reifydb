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
pub mod series;
pub mod subscription;
pub mod sumtype;
pub mod table;
pub mod view;

// Implement the umbrella trait for AdminTransaction
impl CatalogTrackChangeOperations for AdminTransaction {}
