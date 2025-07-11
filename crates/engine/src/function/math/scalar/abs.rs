// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{Column, ColumnValues};
use crate::function::{FunctionError, ScalarFunction};

pub struct Abs;

impl Abs {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarFunction for Abs {
    fn scalar(&self, columns: &[Column], row_count: usize) -> Result<ColumnValues, FunctionError> {
        let column = columns.get(0).unwrap();

        match &column.values {
            ColumnValues::Int1(vals, valid) => {
                let mut values = Vec::with_capacity(vals.len());

                for i in 0..row_count {
                    if valid.get(i).copied().unwrap_or(false) {
                        values.push(if vals[i] < 0 { vals[i] * -1 } else { vals[i] });
                    }
                }

                Ok(ColumnValues::int1_with_validity(values, valid.clone()))
            }
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
