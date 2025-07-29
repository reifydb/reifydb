// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use registry::Functions;
use reifydb_core::Value;
use reifydb_core::frame::{ColumnValues, FrameColumn};
use std::collections::HashMap;
pub mod blob;
pub mod math;
mod registry;

pub trait ScalarFunction: Send + Sync {
    fn scalar(&self, columns: &[FrameColumn], row_count: usize) -> crate::Result<ColumnValues>;
}

pub trait AggregateFunction: Send + Sync {
    fn aggregate(
        &mut self,
        column: &FrameColumn,
        groups: &HashMap<Vec<Value>, Vec<usize>>,
    ) -> crate::Result<()>;

    fn finalize(&mut self) -> crate::Result<(Vec<Vec<Value>>, ColumnValues)>;
}
