// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{ColumnValues, DataFrame};
use base::Value;
use std::collections::HashMap;
use std::collections::hash_map::Iter;
use std::ops::Deref;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GroupByKey(pub Vec<Value>);

#[derive(Debug)]
pub struct GroupByView(pub HashMap<GroupByKey, Vec<usize>>);

impl Deref for GroupByView {
    type Target = HashMap<GroupByKey, Vec<usize>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl GroupByView {
    pub fn iter(&self) -> Iter<'_, GroupByKey, Vec<usize>> {
        self.0.iter()
    }
}

impl GroupByView {
    fn new() -> Self {
        Self(HashMap::new())
    }
}

impl DataFrame {
    pub fn group_by_view(&self, keys: &[&str]) -> crate::Result<GroupByView> {
        let row_count = self.columns.first().map_or(0, |c| c.data.len());

        let mut key_columns: Vec<&ColumnValues> = Vec::with_capacity(keys.len());

        for &key in keys {
            let column = self
                .columns
                .iter()
                .find(|c| c.name == key)
                .ok_or_else(|| format!("Column '{}' not found", key))?;
            key_columns.push(&column.data);
        }

        let mut result = GroupByView::new();

        for row_idx in 0..row_count {
            let mut values = Vec::with_capacity(keys.len());

            for col in &key_columns {
                let value = match col {
                    ColumnValues::Float8(data, valid) => {
                        if valid[row_idx] {
                            Value::float8(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Int2(data, valid) => {
                        if valid[row_idx] {
                            Value::Int2(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Text(data, valid) => {
                        if valid[row_idx] {
                            Value::Text(data[row_idx].clone())
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Bool(data, valid) => {
                        if valid[row_idx] {
                            Value::Bool(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Undefined(_) => Value::Undefined,
                };
                values.push(value);
            }

            result.0.entry(GroupByKey(values)).or_default().push(row_idx);
        }

        Ok(result)
    }
}



#[cfg(test)]
mod tests {
    #[test]
    fn test() {
        todo!()
    }
}
