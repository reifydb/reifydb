// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::{Column, ColumnData};
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
        columns: &[Column],
        row_count: usize,
    ) -> crate::Result<ColumnData> {
        let mut sum = vec![0.0f64; row_count];
        let mut count = vec![0u32; row_count];

        for col in columns {
            match &col.data() {
                ColumnData::Int2(container) => {
                    for i in 0..row_count {
                        if let Some(value) = container.get(i) {
                            sum[i] += *value as f64;
                            count[i] += 1;
                        }
                    }
                }
                ColumnData::Float8(container) => {
                    for i in 0..row_count {
                        if let Some(value) = container.get(i) {
                            sum[i] += *value;
                            count[i] += 1;
                        }
                    }
                }
                data => unimplemented!("{data:?}"),
            }
        }

        let mut data = Vec::with_capacity(row_count);
        let mut valids = Vec::with_capacity(row_count);

        for i in 0..row_count {
            if count[i] > 0 {
                data.push(sum[i] / count[i] as f64);
                valids.push(true);
            } else {
                data.push(0.0);
                valids.push(false);
            }
        }

        Ok(ColumnData::float8_with_bitvec(data, valids))
    }
}
