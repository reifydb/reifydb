// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::frame::{FrameColumn, ColumnValues};
use crate::function::AggregateFunction;
use reifydb_core::{BitVec, Value};
use std::collections::HashMap;

pub struct Max {
    pub maxs: HashMap<Vec<Value>, f64>,
}

impl Max {
    pub fn new() -> Self {
        Self { maxs: HashMap::new() }
    }
}

impl AggregateFunction for Max {
    fn aggregate(
		&mut self,
		column: &FrameColumn,
		mask: &BitVec,
		groups: &HashMap<Vec<Value>, Vec<usize>>,
    ) -> crate::Result<()> {
        match &column.values() {
            ColumnValues::Float8(values, bitvec) => {
                for (group, indices) in groups {
                    let max_val = indices
                        .iter()
                        .filter(|&&i| bitvec.get(i) && mask.get(i))
                        .map(|&i| values[i])
                        .max_by(|a, b| a.partial_cmp(b).unwrap());

                    if let Some(max_val) = max_val {
                        self.maxs
                            .entry(group.clone())
                            .and_modify(|v| *v = f64::max(*v, max_val))
                            .or_insert(max_val);
                    }
                }
                Ok(())
            }
            _ => unimplemented!(),
        }
    }

    fn finalize(&mut self) -> crate::Result<(Vec<Vec<Value>>, ColumnValues)> {
        let mut keys = Vec::with_capacity(self.maxs.len());
        let mut values = ColumnValues::float8_with_capacity(self.maxs.len());

        for (key, max) in std::mem::take(&mut self.maxs) {
            keys.push(key);
            values.push_value(Value::float8(max));
        }

        Ok((keys, values))
    }
}
