// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{Column, ColumnValues};
use crate::function::{AggregateFunction, FunctionError};
use reifydb_core::{BitVec, Value};
use std::collections::HashMap;

pub struct Avg {
    pub sums: HashMap<Vec<Value>, f64>,
    pub counts: HashMap<Vec<Value>, u64>,
}

impl Avg {
    pub fn new() -> Self {
        Self { sums: HashMap::new(), counts: HashMap::new() }
    }
}

impl AggregateFunction for Avg {
    fn aggregate(
        &mut self,
        column: &Column,
        mask: &BitVec,
        groups: &HashMap<Vec<Value>, Vec<usize>>,
    ) -> Result<(), FunctionError> {
        match &column.values {
            ColumnValues::Float8(values, validity) => {
                for (group, indices) in groups {
                    let mut sum = 0.0;
                    let mut count = 0;

                    for &i in indices {
                        if mask.get(i) && validity[i] {
                            sum += values[i];
                            count += 1;
                        }
                    }

                    if count > 0 {
                        self.sums.entry(group.clone()).and_modify(|v| *v += sum).or_insert(sum);

                        self.counts
                            .entry(group.clone())
                            .and_modify(|c| *c += count)
                            .or_insert(count);
                    }
                }
                Ok(())
            }
            ColumnValues::Int2(values, validity) => {
                for (group, indices) in groups {
                    let mut sum = 0.0;
                    let mut count = 0;

                    for &i in indices {
                        if mask.get(i) && validity[i] {
                            sum += values[i] as f64;
                            count += 1;
                        }
                    }

                    if count > 0 {
                        self.sums.entry(group.clone()).and_modify(|v| *v += sum).or_insert(sum);

                        self.counts
                            .entry(group.clone())
                            .and_modify(|c| *c += count)
                            .or_insert(count);
                    }
                }
                Ok(())
            }
            _ => unimplemented!(),
        }
    }

    fn finalize(&mut self) -> Result<(Vec<Vec<Value>>, ColumnValues), FunctionError> {
        let mut keys = Vec::with_capacity(self.sums.len());
        let mut values = ColumnValues::float8_with_capacity(self.sums.len());

        for (key, sum) in std::mem::take(&mut self.sums) {
            let count = self.counts.remove(&key).unwrap_or(0);
            let avg = if count > 0 {
                sum / count as f64
            } else {
                f64::NAN // or return Value::Undefined if preferred
            };

            keys.push(key);
            values.push_value(Value::float8(avg));
        }

        Ok((keys, values))
    }
}
