// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod memory;
mod registry;
pub mod table_scan;

pub use memory::{InMemorySource, empty, from_batches, from_columns, from_result};
pub use registry::{InMemorySourceRegistry, InMemoryTable, SourceRegistry, TableSource};
pub use table_scan::{create_pipeline_from_columns, create_table_scan_pipeline, scan_table};
