// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::function::AggregateFunction;
use reifydb_core::Value;
use reifydb_core::frame::{ColumnValues, FrameColumn};
use std::collections::HashMap;

pub struct Min {
    pub mins: HashMap<Vec<Value>, f64>,
}

impl Min {
    pub fn new() -> Self {
        Self { mins: HashMap::new() }
    }
}

impl AggregateFunction for Min {
    fn aggregate(
        &mut self,
        column: &FrameColumn,
        groups: &HashMap<Vec<Value>, Vec<usize>>,
    ) -> crate::Result<()> {
        match &column.values() {
            ColumnValues::Float8(values, bitvec) => {
                for (group, indices) in groups {
                    let min_val = indices
                        .iter()
                        .filter(|&&i| bitvec.get(i))
                        .map(|&i| values[i])
                        .min_by(|a, b| a.partial_cmp(b).unwrap());

                    if let Some(min_val) = min_val {
                        self.mins
                            .entry(group.clone())
                            .and_modify(|v| *v = f64::min(*v, min_val))
                            .or_insert(min_val);
                    }
                }
                Ok(())
            }
            ColumnValues::Int2(values, bitvec) => {
                for (group, indices) in groups {
                    let min_val = indices
                        .iter()
                        .filter(|&&i| bitvec.get(i))
                        .map(|&i| values[i])
                        .min_by(|a, b| a.partial_cmp(b).unwrap());

                    if let Some(min_val) = min_val {
                        self.mins
                            .entry(group.clone())
                            .and_modify(|v| *v = f64::min(*v, min_val as f64))
                            .or_insert(min_val as f64);
                    }
                }
                Ok(())
            }
            _ => unimplemented!(),
        }
    }

    fn finalize(&mut self) -> crate::Result<(Vec<Vec<Value>>, ColumnValues)> {
        let mut keys = Vec::with_capacity(self.mins.len());
        let mut values = ColumnValues::float8_with_capacity(self.mins.len());

        for (key, min) in std::mem::take(&mut self.mins) {
            keys.push(key);
            values.push_value(Value::float8(min));
        }

        Ok((keys, values))
    }
}
