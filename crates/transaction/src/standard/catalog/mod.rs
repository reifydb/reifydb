// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::CatalogTrackChangeOperations;

use crate::standard::StandardCommandTransaction;

mod dictionary;
mod flow;
mod namespace;
mod ringbuffer;
mod table;
mod view;

// Implement the umbrella trait for StandardCommandTransaction
impl CatalogTrackChangeOperations for StandardCommandTransaction {}
