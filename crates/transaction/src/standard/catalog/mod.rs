// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::change::CatalogTrackChangeOperations;

use crate::standard::StandardCommandTransaction;

pub mod dictionary;
pub mod flow;
pub mod namespace;
pub mod ringbuffer;
pub mod subscription;
pub mod table;
pub mod view;

// Implement the umbrella trait for StandardCommandTransaction
impl CatalogTrackChangeOperations for StandardCommandTransaction {}
