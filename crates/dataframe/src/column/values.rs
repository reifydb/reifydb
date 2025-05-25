// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::any::TypeId;
use crate::ValueRef;
use base::{CowVec, Value};

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnValues {
    // value, is_valid
    Float8(CowVec<f64>, CowVec<bool>),
    Int2(CowVec<i16>, CowVec<bool>),
    Text(CowVec<String>, CowVec<bool>),
    Bool(CowVec<bool>, CowVec<bool>),

    // special case: all undefined
    Undefined(usize),
}

impl ColumnValues {
    pub fn bool(values: impl IntoIterator<Item = bool>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Bool(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn bool_with_validity(
        values: impl IntoIterator<Item = bool>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Bool(CowVec::new(values), CowVec::new(validity))
    }

    pub fn float8(values: impl IntoIterator<Item = f64>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Float8(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn float8_with_validity(
        values: impl IntoIterator<Item = f64>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Float8(CowVec::new(values), CowVec::new(validity))
    }

    pub fn int2(values: impl IntoIterator<Item = i16>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int2(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn int2_with_validity(
        values: impl IntoIterator<Item = i16>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Int2(CowVec::new(values), CowVec::new(validity))
    }

    pub fn text<'a>(values: impl IntoIterator<Item = String>) -> Self {
        let values = values.into_iter().map(|c| c.to_string()).collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Text(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn text_with_validity<'a>(
        values: impl IntoIterator<Item = String>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().map(|c| c.to_string()).collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Text(CowVec::new(values), CowVec::new(validity))
    }

    pub fn undefined(len: usize) -> Self {
        ColumnValues::Undefined(len)
    }
}

impl ColumnValues {
    pub fn reorder(&mut self, indices: &[usize]) {
        match self {
            ColumnValues::Float8(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Int2(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Text(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Bool(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
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

impl ColumnValues {
    pub fn push(&mut self, value: Value) {
        match self {
            ColumnValues::Float8(values, validity) => match value {
                Value::Float8(v) => {
                    values.push(v.value());
                    validity.push(true);
                }
                _ => unimplemented!(),
            },
            ColumnValues::Int2(values, validity) => match value {
                Value::Int2(v) => {
                    values.push(v);
                    validity.push(true);
                }
                _ => unimplemented!(),
            },
            ColumnValues::Undefined(_) => match value {
                Value::Float8(v) => *self = ColumnValues::float8([v.value()]),
                _ => unimplemented!(),
            },
            v => unimplemented!("{v:?}"),
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
