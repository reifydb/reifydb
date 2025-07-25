// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, Frame};
use crate::Value;
use std::collections::HashMap;

pub type GroupByKey = Vec<Value>;

pub type GroupByView = HashMap<GroupByKey, Vec<usize>>;

impl Frame {
    pub fn group_by_view(&self, keys: &[&str]) -> crate::Result<GroupByView> {
        let row_count = self.columns.first().map_or(0, |c| c.values().len());

        let mut key_columns: Vec<&ColumnValues> = Vec::with_capacity(keys.len());

        for &key in keys {
            let column = self
                .columns
                .iter()
                .find(|c| c.qualified_name() == key || c.name() == key)
                .ok_or_else(|| crate::error!(crate::error::diagnostic::engine::frame_error(format!("Column '{}' not found", key))))?;
            key_columns.push(&column.values());
        }

        let mut result = GroupByView::new();

        for row_idx in 0..row_count {
            let mut values = Vec::with_capacity(keys.len());

            for col in &key_columns {
                let value = match col {
                    ColumnValues::Bool(data, bitvec) => {
                        if bitvec.get(row_idx) {
                            Value::Bool(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Float4(data, valid) => {
                        if valid.get(row_idx) {
                            Value::float4(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Float8(data, valid) => {
                        if valid.get(row_idx) {
                            Value::float8(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Int1(data, valid) => {
                        if valid.get(row_idx) {
                            Value::Int1(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Int2(data, valid) => {
                        if valid.get(row_idx) {
                            Value::Int2(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Int4(data, valid) => {
                        if valid.get(row_idx) {
                            Value::Int4(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Int8(data, valid) => {
                        if valid.get(row_idx) {
                            Value::Int8(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Int16(data, valid) => {
                        if valid.get(row_idx) {
                            Value::Int16(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Uint1(data, valid) => {
                        if valid.get(row_idx) {
                            Value::Uint1(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Uint2(data, valid) => {
                        if valid.get(row_idx) {
                            Value::Uint2(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Uint4(data, valid) => {
                        if valid.get(row_idx) {
                            Value::Uint4(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Uint8(data, valid) => {
                        if valid.get(row_idx) {
                            Value::Uint8(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Uint16(data, valid) => {
                        if valid.get(row_idx) {
                            Value::Uint16(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Utf8(data, valid) => {
                        if valid.get(row_idx) {
                            Value::Utf8(data[row_idx].clone())
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Date(data, valid) => {
                        if valid.get(row_idx) {
                            Value::Date(data[row_idx].clone())
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::DateTime(data, valid) => {
                        if valid.get(row_idx) {
                            Value::DateTime(data[row_idx].clone())
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Time(data, valid) => {
                        if valid.get(row_idx) {
                            Value::Time(data[row_idx].clone())
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Interval(data, valid) => {
                        if valid.get(row_idx) {
                            Value::Interval(data[row_idx].clone())
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Undefined(_) => Value::Undefined,
                    ColumnValues::RowId(data, bitvec) => {
                        if bitvec.get(row_idx) {
                            Value::RowId(data[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Uuid4(data, bitvec) => {
                        if bitvec.get(row_idx) {
                            Value::Uuid4(crate::value::uuid::Uuid4::from(data[row_idx]))
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Uuid7(data, bitvec) => {
                        if bitvec.get(row_idx) {
                            Value::Uuid7(crate::value::uuid::Uuid7::from(data[row_idx]))
                        } else {
                            Value::Undefined
                        }
                    }
                };

                values.push(value);
            }

            result.entry(values).or_default().push(row_idx);
        }

        Ok(result)
    }
}
