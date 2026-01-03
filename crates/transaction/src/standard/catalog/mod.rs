// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::CatalogTrackChangeOperations;

use crate::standard::StandardCommandTransaction;

mod dictionary;
mod flow;
mod namespace;
mod ringbuffer;
mod subscription;
mod table;
mod view;

// Implement the umbrella trait for StandardCommandTransaction
impl CatalogTrackChangeOperations for StandardCommandTransaction {}
