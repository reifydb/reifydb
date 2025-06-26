// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{Column, ColumnValues};
use crate::function::{FunctionError, ScalarFunction};

pub struct Avg {}

impl ScalarFunction for Avg {
    fn name(&self) -> &str {
        "avg"
    }

    fn scalar(&self, columns: &[Column], row_count: usize) -> Result<ColumnValues, FunctionError> {
        let mut sum = vec![0.0f64; row_count];
        let mut count = vec![0u32; row_count];

        for col in columns {
            match &col.data {
                ColumnValues::Int2(vals, valid) => {
                    for i in 0..row_count {
                        if valid.get(i).copied().unwrap_or(false) {
                            sum[i] += vals[i] as f64;
                            count[i] += 1;
                        }
                    }
                }
                ColumnValues::Float8(vals, valid) => {
                    for i in 0..row_count {
                        if valid.get(i).copied().unwrap_or(false) {
                            sum[i] += vals[i];
                            count[i] += 1;
                        }
                    }
                }
                values => unimplemented!("{values:?}"),
            }
        }

        let mut values = Vec::with_capacity(row_count);
        let mut valids = Vec::with_capacity(row_count);

        for i in 0..row_count {
            if count[i] > 0 {
                values.push(sum[i] / count[i] as f64);
                valids.push(true);
            } else {
                values.push(0.0);
                valids.push(false);
            }
        }

        Ok(ColumnValues::float8_with_validity(values, valids))
    }
}
