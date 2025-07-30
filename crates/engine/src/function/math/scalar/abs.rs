// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::{EngineColumn, EngineColumnData};
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
        columns: &[EngineColumn],
        row_count: usize,
    ) -> crate::Result<EngineColumnData> {
        let column = columns.get(0).unwrap();

        match &column.data() {
            EngineColumnData::Int1(container) => {
                let mut data = Vec::with_capacity(container.len());

                for i in 0..row_count {
                    if let Some(value) = container.get(i) {
                        data.push(if *value < 0 { *value * -1 } else { *value });
                    }
                }

                Ok(EngineColumnData::int1_with_bitvec(data, container.bitvec()))
            }
            EngineColumnData::Int2(container) => {
                let mut data = Vec::with_capacity(container.len());

                for i in 0..row_count {
                    if let Some(value) = container.get(i) {
                        data.push(if *value < 0 { *value * -1 } else { *value });
                    }
                }

                Ok(EngineColumnData::int2_with_bitvec(data, container.bitvec()))
            }
            _ => unimplemented!(),
        }
    }
}
