// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub mod math;

pub use error::FunctionError;

mod error;

use reifydb_core::Value;
use reifydb_core::row::RowIter;
use reifydb_frame::{Column, ColumnValues};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionMode {
    // select abs(-1)
    Scalar,
    // from generate_series(1, 3)
    Generator,
    // from test.table select avg(num)
    Aggregate,
}

impl Display for FunctionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            FunctionMode::Scalar => "Scalar",
            FunctionMode::Generator => "Generator",
            FunctionMode::Aggregate => "Aggregate",
        };
        write!(f, "{}", label)
    }
}

pub trait Function: Send + Sync {
    fn name(&self) -> &str;
    fn modes(&self) -> &'static [FunctionMode];

    // fn prepare(&self, args: &[Expression]) -> Result<Box<dyn FunctionExecutor>, FunctionError>;
    fn prepare(&self) -> Result<Box<dyn FunctionExecutor>, FunctionError>;
}

pub trait FunctionExecutor: Send + Sync {
    fn name(&self) -> &str;

    /// For scalar input => scalar output
    fn eval_scalar(
        &self,
        _columns: &[Column],
        _row_count: usize,
    ) -> Result<ColumnValues, FunctionError> {
        Err(FunctionError::UnsupportedMode {
            function: self.name().to_string(),
            mode: FunctionMode::Scalar,
        })
    }

    /// For scalar inputs => output rows (like `generate_series`)
    fn eval_generator(&self, _args: &[Value]) -> Result<RowIter, FunctionError> {
        Err(FunctionError::UnsupportedMode {
            function: self.name().to_string(),
            mode: FunctionMode::Generator,
        })
    }

    /// For row streams => aggregated output (like `avg`)
    fn eval_aggregate(&mut self, _row: &[Value]) -> Result<(), FunctionError> {
        Err(FunctionError::UnsupportedMode {
            function: self.name().to_string(),
            mode: FunctionMode::Aggregate,
        })
    }

    fn finalize_aggregate(&self) -> Result<Value, FunctionError> {
        Err(FunctionError::UnsupportedMode {
            function: self.name().to_string(),
            mode: FunctionMode::Aggregate,
        })
    }
}

pub struct FunctionRegistry {
    functions: HashMap<String, Arc<dyn Function>>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self { functions: HashMap::new() }
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Function>> {
        self.functions.get(name).cloned()
    }

    pub fn register<F: Function + 'static>(&mut self, func: F) {
        self.functions.insert(func.name().to_string(), Arc::new(func));
    }
}
