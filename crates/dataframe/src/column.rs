// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::Value;
use base::ordered_float::OrderedF64;

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub data: ColumnValues,
}

#[derive(Debug, PartialEq)]
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
            ColumnValues::Float8(v, b) => {
                if b[index] {
                    Value::Float8(v[index].try_into().unwrap())
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Int2(v, b) => {
                if b[index] {
                    Value::Int2(v[index])
                } else {
                    Value::Undefined
                }
            }
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
            ColumnValues::Float8(_, b) => b.len(),
            ColumnValues::Int2(_, b) => b.len(),
            // ColumnValues::Float(_, b) => b.len(),
            ColumnValues::Text(_, b) => b.len(),
            ColumnValues::Bool(_, b) => b.len(),
            ColumnValues::Undefined(n) => *n,
        }
    }

    pub fn is_undefined(&self, index: usize) -> bool {
        match self {
            ColumnValues::Float8(_, b)
            | ColumnValues::Int2(_, b)
            | ColumnValues::Text(_, b)
            | ColumnValues::Bool(_, b) => !b[index],
            ColumnValues::Undefined(_) => true,
        }
    }

    pub fn empty(&self) -> ColumnValues {
        match self {
            ColumnValues::Float8(_, _) => ColumnValues::Float8(Vec::new(), Vec::new()),
            ColumnValues::Int2(_, _) => ColumnValues::Int2(Vec::new(), Vec::new()),
            ColumnValues::Text(_, _) => ColumnValues::Text(Vec::new(), Vec::new()),
            ColumnValues::Bool(_, _) => ColumnValues::Bool(Vec::new(), Vec::new()),
            ColumnValues::Undefined(_) => ColumnValues::Undefined(0),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {
        todo!()
    }
}
