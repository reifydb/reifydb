// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::{EngineColumn, EngineColumnData};
use crate::function::AggregateFunction;
use reifydb_core::Value;
use std::collections::HashMap;

pub struct Sum {
    pub sums: HashMap<Vec<Value>, Value>,
}

impl Sum {
    pub fn new() -> Self {
        Self { sums: HashMap::new() }
    }
}

impl AggregateFunction for Sum {
    fn aggregate(
        &mut self,
        column: &EngineColumn,
        groups: &HashMap<Vec<Value>, Vec<usize>>,
    ) -> crate::Result<()> {
        match &column.data() {
            EngineColumnData::Float8(container) => {
                for (group, indices) in groups {
                    let sum: f64 = indices.iter().filter_map(|&i| container.get(i)).sum();

                    self.sums.insert(group.clone(), Value::float8(sum));
                }
                Ok(())
            }
            _ => unimplemented!(),
        }
    }

    fn finalize(&mut self) -> crate::Result<(Vec<Vec<Value>>, EngineColumnData)> {
        let mut keys = Vec::with_capacity(self.sums.len());
        let mut data = EngineColumnData::float8_with_capacity(self.sums.len());

        for (key, sum) in std::mem::take(&mut self.sums) {
            keys.push(key);
            data.push_value(sum);
        }

        Ok((keys, data))
    }
}
