// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::function::{Function, FunctionError, FunctionExecutor, FunctionMode};
use reifydb_frame::{Column, ColumnValues};

pub struct AbsFunction;

impl Function for AbsFunction {
    fn name(&self) -> &str {
        "abs"
    }

    fn modes(&self) -> &'static [FunctionMode] {
        &[FunctionMode::Scalar]
    }

    // fn prepare(&self, _args: &[Expression]) -> Result<Box<dyn FunctionExecutor>, FunctionError> {
    fn prepare(&self) -> Result<Box<dyn FunctionExecutor>, FunctionError> {
        Ok(Box::new(AbsExecutor))
    }
}

struct AbsExecutor;

impl FunctionExecutor for AbsExecutor {
    fn name(&self) -> &str {
        "abs"
    }

    fn eval_scalar(
        &self,
        columns: &[Column],
        row_count: usize,
    ) -> Result<ColumnValues, FunctionError> {
        let column = columns.get(0).unwrap();

        match &column.data {
            ColumnValues::Int2(vals, valid) => {
                let mut values = Vec::with_capacity(vals.len());

                for i in 0..row_count {
                    if valid.get(i).copied().unwrap_or(false) {
                        values.push(if vals[i] < 0 { vals[i] * -1 } else { vals[i] });
                    }
                }

                Ok(ColumnValues::int2_with_validity(values, valid.clone()))
            }
            _ => unimplemented!(),
        }
    }
}
