// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, FrameColumn};
use crate::function::ScalarFunction;

pub struct Avg {}

impl Avg {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarFunction for Avg {
    fn scalar(
        &self,
        columns: &[FrameColumn],
        row_count: usize,
    ) -> Result<ColumnValues, reifydb_core::Error> {
        let mut sum = vec![0.0f64; row_count];
        let mut count = vec![0u32; row_count];

        for col in columns {
            match &col.values {
                ColumnValues::Int2(vals, bitvec) => {
                    for i in 0..row_count {
                        if bitvec.get(i) {
                            sum[i] += vals[i] as f64;
                            count[i] += 1;
                        }
                    }
                }
                ColumnValues::Float8(vals, bitvec) => {
                    for i in 0..row_count {
                        if bitvec.get(i) {
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

        Ok(ColumnValues::float8_with_bitvec(values, valids))
    }
}
