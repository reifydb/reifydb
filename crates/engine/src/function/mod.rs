// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::{EngineColumn, EngineColumnData};
pub use registry::Functions;
use reifydb_core::Value;
use std::collections::HashMap;

pub mod blob;
pub mod math;
mod registry;

pub trait ScalarFunction: Send + Sync {
    fn scalar(&self, columns: &[EngineColumn], row_count: usize) -> crate::Result<EngineColumnData>;
}

pub trait AggregateFunction: Send + Sync {
    fn aggregate(
        &mut self,
        column: &EngineColumn,
        groups: &HashMap<Vec<Value>, Vec<usize>>,
    ) -> crate::Result<()>;

    fn finalize(&mut self) -> crate::Result<(Vec<Vec<Value>>, EngineColumnData)>;
}
