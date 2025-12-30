// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod memory;
mod registry;
pub mod table_scan;

pub use memory::{InMemorySource, empty, from_batches, from_columns, from_result};
pub use registry::{InMemorySourceRegistry, InMemoryTable, SourceRegistry, TableSource};
pub use table_scan::{create_pipeline_from_columns, create_table_scan_pipeline, scan_table};
