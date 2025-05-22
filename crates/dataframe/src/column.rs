// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::Value;

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub data: ColumnValues,
}

#[derive(Debug, PartialEq)]
pub enum ColumnValues {
    // value, is_valid
    Int2(Vec<i16>, Vec<bool>),
    Text(Vec<String>, Vec<bool>),
    Bool(Vec<bool>, Vec<bool>),

    // special case: all undefined
    Undefined(usize),
}

impl From<Value> for ColumnValues {
    fn from(value: Value) -> Self {
        match value {
            Value::Bool(v) => ColumnValues::Bool(vec![v], vec![true]),
            Value::Float4(_) => unimplemented!(),
            Value::Float8(_) => unimplemented!(),
            Value::Int2(v) => ColumnValues::Int2(vec![v], vec![true]),
            Value::Text(v) => ColumnValues::Text(vec![v], vec![true]),
            Value::Uint2(_) => unimplemented!(),
            Value::Undefined => ColumnValues::Undefined(1),
        }
    }
}

impl ColumnValues {
    pub fn get(&self, index: usize) -> Value {
        match self {
            ColumnValues::Int2(v, b) => {
                if b[index] {
                    Value::Int2(v[index])
                } else {
                    Value::Undefined
                }
            }
            // ColumnValues::Float(v, b) => {
            //     if b[index] {
            //         Value::Float(OrderedF64(v[index]))
            //     } else {
            //         Value::Undefined
            //     }
            // }
            ColumnValues::Text(v, b) => {
                if b[index] {
                    Value::Text(v[index].clone())
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Bool(v, b) => {
                if b[index] {
                    Value::Bool(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Undefined(_) => Value::Undefined,
        }
    }

    pub fn push(&mut self, value: Value) {
        match (self, value) {
			(ColumnValues::Int2(v, b), Value::Int2(i)) => {
				v.push(i);
				b.push(true);
			}
			// (ColumnValues::Float(v, b), Value::Float(f)) => {
			//     v.push(f.0);
			//     b.push(true);
			// }
			(ColumnValues::Text(v, b), Value::Text(s)) => {
				v.push(s);
				b.push(true);
			}
			(ColumnValues::Bool(v, b), Value::Bool(x)) => {
				v.push(x);
				b.push(true);
			}
			(ColumnValues::Int2(_, b), Value::Undefined)
			// | (ColumnValues::Float(_, b), Value::Undefined)
			| (ColumnValues::Text(_, b), Value::Undefined)
			| (ColumnValues::Bool(_, b), Value::Undefined) => b.push(false),
			(ColumnValues::Undefined(n), Value::Undefined) => *n += 1,
			_ => panic!("Mismatched column type and value"),
		}
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnValues::Int2(_, b) => b.len(),
            // ColumnValues::Float(_, b) => b.len(),
            ColumnValues::Text(_, b) => b.len(),
            ColumnValues::Bool(_, b) => b.len(),
            ColumnValues::Undefined(n) => *n,
        }
    }

    pub fn is_undefined(&self, index: usize) -> bool {
        match self {
			ColumnValues::Int2(_, b)
			// | ColumnValues::Float(_, b)
			| ColumnValues::Text(_, b)
			| ColumnValues::Bool(_, b) => !b[index],
			ColumnValues::Undefined(_) => true,
		}
    }

    pub fn empty(&self) -> ColumnValues {
        match self {
            ColumnValues::Int2(_, _) => ColumnValues::Int2(Vec::new(), Vec::new()),
            // ColumnValues::Float(_, _) => ColumnValues::Float(Vec::new(), Vec::new()),
            ColumnValues::Text(_, _) => ColumnValues::Text(Vec::new(), Vec::new()),
            ColumnValues::Bool(_, _) => ColumnValues::Bool(Vec::new(), Vec::new()),
            ColumnValues::Undefined(_) => ColumnValues::Undefined(0),
        }
    }
}
