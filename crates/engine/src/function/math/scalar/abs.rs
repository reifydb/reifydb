// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, FrameColumn};
use crate::function::ScalarFunction;

pub struct Abs;

impl Abs {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarFunction for Abs {
    fn scalar(
        &self,
        columns: &[FrameColumn],
        row_count: usize,
    ) -> crate::Result<ColumnValues> {
        let column = columns.get(0).unwrap();

        match &column.values {
            ColumnValues::Int1(v, b) => {
                let mut values = Vec::with_capacity(v.len());

                for i in 0..row_count {
                    if b.get(i) {
                        values.push(if v[i] < 0 { v[i] * -1 } else { v[i] });
                    }
                }

                Ok(ColumnValues::int1_with_bitvec(values, b))
            }
            ColumnValues::Int2(v, b) => {
                let mut values = Vec::with_capacity(v.len());

                for i in 0..row_count {
                    if b.get(i) {
                        values.push(if v[i] < 0 { v[i] * -1 } else { v[i] });
                    }
                }

                Ok(ColumnValues::int2_with_bitvec(values, b))
            }
            _ => unimplemented!(),
        }
    }
}
