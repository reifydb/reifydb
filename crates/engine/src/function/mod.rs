// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{Column, ColumnValues};
pub use error::FunctionError;
use reifydb_core::{BitVec, Value};
use std::collections::HashMap;
use std::sync::Arc;

mod error;
pub mod math;

pub trait ScalarFunction {
    fn name(&self) -> &str;
    
    fn scalar(&self, _columns: &[Column], _row_count: usize)
    -> Result<ColumnValues, FunctionError>;
}

pub trait AggregateFunction {
    fn name(&self) -> &str;
    
    fn aggregate(
        &mut self,
        column: &Column,
        mask: &BitVec,
        groups: &HashMap<Vec<Value>, Vec<usize>>,
    ) -> Result<(), FunctionError>;

    fn finalize(&mut self) -> Result<(Vec<Vec<Value>>, ColumnValues), FunctionError>;
}

pub struct FunctionRegistry {
    scalars: HashMap<String, Arc<dyn ScalarFunction>>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self { scalars: HashMap::new() }
    }

    pub fn get_scalar(&self, name: &str) -> Option<Arc<dyn ScalarFunction>> {
        self.scalars.get(name).cloned()
    }

    pub fn register_scalar<F: ScalarFunction + 'static>(&mut self, func: F) {
        self.scalars.insert(func.name().to_string(), Arc::new(func));
    }
}
