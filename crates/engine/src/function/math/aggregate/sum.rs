// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{Column, ColumnValues};
use crate::function::{AggregateFunction, FunctionError};
use reifydb_core::{BitVec, Value};
use std::collections::HashMap;

pub struct Sum {
    pub sums: HashMap<Vec<Value>, f64>,
}

impl AggregateFunction for Sum {
    fn name(&self) -> &str {
        "sum"
    }

    fn aggregate(
        &mut self,
        column: &Column,
        mask: &BitVec,
        groups: &HashMap<Vec<Value>, Vec<usize>>,
    ) -> Result<(), FunctionError> {
        match &column.data {
            ColumnValues::Float8(values, validity) => {
                for (group, indices) in groups {
                    let sum: f64 =
                        indices.iter().filter(|&&i| validity[i]).map(|&i| values[i]).sum();

                    self.sums.insert(group.clone(), sum);
                }
                return Ok(());
            }
            _ => unimplemented!(),
        }
    }

    fn finalize(&mut self) -> Result<(Vec<Vec<Value>>, ColumnValues), FunctionError> {
        let mut keys = Vec::with_capacity(self.sums.len());
        let mut values = ColumnValues::float8_with_capacity(self.sums.len());

        for (key, sum) in std::mem::take(&mut self.sums) {
            keys.push(key);
            values.push(sum);
        }

        Ok((keys, values))
    }
}
