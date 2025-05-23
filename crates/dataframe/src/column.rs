// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::Value;
use base::ordered_float::OrderedF64;

#[derive(Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub data: ColumnValues,
}

impl Column {
    pub fn bool(name: &str, values: impl IntoIterator<Item = bool>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::bool(values) }
    }

    pub fn bool_with_validity(
        name: &str,
        values: impl IntoIterator<Item = bool>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::bool_with_validity(values, validity) }
    }

    pub fn float8(name: &str, values: impl IntoIterator<Item = f64>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::float8(values) }
    }

    pub fn float8_with_validity(
        name: &str,
        values: impl IntoIterator<Item = f64>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::float8_with_validity(values, validity) }
    }

    pub fn int2(name: &str, values: impl IntoIterator<Item = i16>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::int2(values) }
    }

    pub fn int2_with_validity(
        name: &str,
        values: impl IntoIterator<Item = i16>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::int2_with_validity(values, validity) }
    }

    pub fn text<'a>(name: &str, values: impl IntoIterator<Item = &'a str>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::text(values) }
    }

    pub fn text_with_validity<'a>(
        name: &str,
        values: impl IntoIterator<Item = &'a str>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::text_with_validity(values, validity) }
    }

    pub fn undefined(name: &str, len: usize) -> Self {
        Self { name: name.to_string(), data: ColumnValues::undefined(len) }
    }
}

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

    pub fn text<'a>(values: impl IntoIterator<Item = &'a str>) -> Self {
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
    pub fn get_as_value(&self, idx: usize) -> Value {
        match self {
            ColumnValues::Float8(v, valid) => {
                if valid[idx] {
                    Value::float8(v[idx])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Int2(v, valid) => {
                if valid[idx] {
                    Value::Int2(v[idx])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Text(v, valid) => {
                if valid[idx] {
                    Value::Text(v[idx].clone())
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Bool(v, valid) => {
                if valid[idx] {
                    Value::Bool(v[idx])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Undefined(_) => Value::Undefined,
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
    #[ignore]
    fn implement() {
        todo!()
    }
}
