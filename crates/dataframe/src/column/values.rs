// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ValueRef;
use base::Value;
use base::ordered_float::OrderedF64;

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnValues {
    // value, is_valid
    Float8(Vec<f64>, Vec<bool>),
    Int2(Vec<i16>, Vec<bool>),
    Text(Vec<String>, Vec<bool>),
    Bool(Vec<bool>, Vec<bool>),

    // special case: all undefined
    Undefined(usize),
}

impl ColumnValues {
    pub fn bool(values: impl IntoIterator<Item = bool>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Bool(values, vec![true; len])
    }

    pub fn bool_with_validity(
        values: impl IntoIterator<Item = bool>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Bool(values, validity)
    }

    pub fn float8(values: impl IntoIterator<Item = f64>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Float8(values, vec![true; len])
    }

    pub fn float8_with_validity(
        values: impl IntoIterator<Item = f64>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Float8(values, validity)
    }

    pub fn int2(values: impl IntoIterator<Item = i16>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int2(values, vec![true; len])
    }

    pub fn int2_with_validity(
        values: impl IntoIterator<Item = i16>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Int2(values, validity)
    }

    pub fn text<'a>(values: impl IntoIterator<Item = impl ToString>) -> Self {
        let values = values.into_iter().map(|c| c.to_string()).collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Text(values, vec![true; len])
    }

    pub fn text_with_validity<'a>(
        values: impl IntoIterator<Item = &'a str>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().map(|c| c.to_string()).collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Text(values, validity)
    }

    pub fn undefined(len: usize) -> Self {
        ColumnValues::Undefined(len)
    }
}

impl ColumnValues {
    pub fn from_values(values: Vec<Value>, valid: Vec<bool>) -> Self {
        match values.get(0) {
            Some(Value::Float8(_)) => ColumnValues::Float8(
                values
                    .into_iter()
                    .map(|v| match v {
                        Value::Float8(f) => f,
                        _ => OrderedF64::zero(),
                    })
                    .map(|f| f.value())
                    .collect(),
                valid,
            ),
            Some(Value::Int2(_)) => ColumnValues::Int2(
                values
                    .into_iter()
                    .map(|v| match v {
                        Value::Int2(i) => i,
                        _ => 0,
                    })
                    .collect(),
                valid,
            ),
            Some(Value::Text(_)) => ColumnValues::Text(
                values
                    .into_iter()
                    .map(|v| match v {
                        Value::Text(s) => s,
                        _ => String::new(),
                    })
                    .collect(),
                valid,
            ),
            Some(Value::Bool(_)) => ColumnValues::Bool(
                values
                    .into_iter()
                    .map(|v| match v {
                        Value::Bool(b) => b,
                        _ => false,
                    })
                    .collect(),
                valid,
            ),
            _ => ColumnValues::Undefined(valid.len()),
        }
    }
}

impl ColumnValues {
    pub fn reorder(&mut self, indices: &[usize]) {
        match self {
            ColumnValues::Float8(v, valid) => {
                let new_v: Vec<_> = indices.iter().map(|&i| v[i]).collect();
                let new_valid: Vec<_> = indices.iter().map(|&i| valid[i]).collect();
                *v = new_v;
                *valid = new_valid;
            }
            ColumnValues::Int2(v, valid) => {
                let new_v: Vec<_> = indices.iter().map(|&i| v[i]).collect();
                let new_valid: Vec<_> = indices.iter().map(|&i| valid[i]).collect();
                *v = new_v;
                *valid = new_valid;
            }
            ColumnValues::Text(v, valid) => {
                let new_v: Vec<_> = indices.iter().map(|&i| v[i].clone()).collect();
                let new_valid: Vec<_> = indices.iter().map(|&i| valid[i]).collect();
                *v = new_v;
                *valid = new_valid;
            }
            ColumnValues::Bool(v, valid) => {
                let new_v: Vec<_> = indices.iter().map(|&i| v[i]).collect();
                let new_valid: Vec<_> = indices.iter().map(|&i| valid[i]).collect();
                *v = new_v;
                *valid = new_valid;
            }
            ColumnValues::Undefined(len) => {
                *len = indices.len();
            }
        }
    }
}

impl From<Value> for ColumnValues {
    fn from(value: Value) -> Self {
        match value {
            Value::Bool(v) => ColumnValues::bool([v]),
            Value::Float4(_) => unimplemented!(),
            Value::Float8(_) => unimplemented!(),
            Value::Int2(v) => ColumnValues::int2([v]),
            Value::Text(v) => ColumnValues::text([v]),
            Value::Uint2(_) => unimplemented!(),
            Value::Undefined => ColumnValues::undefined(1),
        }
    }
}

impl ColumnValues {
    pub fn get(&self, index: usize) -> ValueRef {
        match self {
            ColumnValues::Float8(v, b) => {
                if b[index] {
                    ValueRef::Float8(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Int2(v, b) => {
                if b[index] {
                    ValueRef::Int2(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Text(v, b) => {
                if b[index] {
                    ValueRef::Text(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Bool(v, b) => {
                if b[index] {
                    ValueRef::Bool(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Undefined(_) => ValueRef::Undefined,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnValues::Float8(_, b) => b.len(),
            ColumnValues::Int2(_, b) => b.len(),
            // ColumnValues::Float(_, b) => b.len(),
            ColumnValues::Text(_, b) => b.len(),
            ColumnValues::Bool(_, b) => b.len(),
            ColumnValues::Undefined(n) => *n,
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn implement() {
        todo!()
    }
}
