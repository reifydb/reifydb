// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::num::IsNumber;
use reifydb_core::{CowVec, Value, ValueKind};

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnValues {
    // value, is_valid
    Bool(CowVec<bool>, CowVec<bool>),
    Float4(CowVec<f32>, CowVec<bool>),
    Float8(CowVec<f64>, CowVec<bool>),
    Int1(CowVec<i8>, CowVec<bool>),
    Int2(CowVec<i16>, CowVec<bool>),
    Int4(CowVec<i32>, CowVec<bool>),
    Int8(CowVec<i64>, CowVec<bool>),
    Int16(CowVec<i128>, CowVec<bool>),
    String(CowVec<String>, CowVec<bool>),
    Uint1(CowVec<u8>, CowVec<bool>),
    Uint2(CowVec<u16>, CowVec<bool>),
    Uint4(CowVec<u32>, CowVec<bool>),
    Uint8(CowVec<u64>, CowVec<bool>),
    Uint16(CowVec<u128>, CowVec<bool>),
    // special case: all undefined
    Undefined(usize),
}

impl ColumnValues {
    pub fn is_numeric(&self) -> bool {
        match self {
            ColumnValues::Float4(_, _)
            | ColumnValues::Float8(_, _)
            | ColumnValues::Int1(_, _)
            | ColumnValues::Int2(_, _)
            | ColumnValues::Int4(_, _)
            | ColumnValues::Int8(_, _)
            | ColumnValues::Int16(_, _)
            | ColumnValues::Uint1(_, _)
            | ColumnValues::Uint2(_, _)
            | ColumnValues::Uint4(_, _)
            | ColumnValues::Uint8(_, _)
            | ColumnValues::Uint16(_, _) => true,
            ColumnValues::String(_, _) | ColumnValues::Bool(_, _) | ColumnValues::Undefined(_) => {
                false
            }
        }
    }
}

impl ColumnValues {
    pub fn get_numeric_value(&self, index: usize) -> Option<impl IsNumber> {
        match self {
            ColumnValues::Int1(values, validity) => {
                if validity[index] {
                    Some(values[index])
                } else {
                    None
                }
            }
            _ => unimplemented!(),
        }
    }
}

impl ColumnValues {
    pub fn with_capacity(value: ValueKind, capacity: usize) -> Self {
        match value {
            ValueKind::Bool => Self::bool_with_capacity(capacity),
            ValueKind::Float4 => Self::float4_with_capacity(capacity),
            ValueKind::Float8 => Self::float4_with_capacity(capacity),
            ValueKind::Int1 => Self::int1_with_capacity(capacity),
            ValueKind::Int2 => Self::int2_with_capacity(capacity),
            ValueKind::Int4 => Self::int4_with_capacity(capacity),
            ValueKind::Int8 => Self::int8_with_capacity(capacity),
            ValueKind::Int16 => Self::int16_with_capacity(capacity),
            ValueKind::String => Self::string_with_capacity(capacity),
            ValueKind::Uint1 => Self::uint1_with_capacity(capacity),
            ValueKind::Uint2 => Self::uint2_with_capacity(capacity),
            ValueKind::Uint4 => Self::uint4_with_capacity(capacity),
            ValueKind::Uint8 => Self::uint8_with_capacity(capacity),
            ValueKind::Uint16 => Self::uint16_with_capacity(capacity),
            ValueKind::Undefined => Self::undefined(capacity),
        }
    }

    // FIXME wrapping and then later unwrapping a value feels pretty stupid -- FIXME
    pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Value> + 'a> {
        match self {
            ColumnValues::Bool(values, validity) => Box::new(
                values
                    .iter()
                    .zip(validity.iter())
                    .map(|(v, va)| if *va { Value::Bool(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Int1(values, validity) => Box::new(
                values
                    .iter()
                    .zip(validity.iter())
                    .map(|(v, va)| if *va { Value::Int1(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Int2(values, validity) => Box::new(
                values
                    .iter()
                    .zip(validity.iter())
                    .map(|(v, va)| if *va { Value::Int2(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Int4(values, validity) => Box::new(
                values
                    .iter()
                    .zip(validity.iter())
                    .map(|(v, va)| if *va { Value::Int4(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Int8(values, validity) => Box::new(
                values
                    .iter()
                    .zip(validity.iter())
                    .map(|(v, va)| if *va { Value::Int8(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Int16(values, validity) => Box::new(
                values
                    .iter()
                    .zip(validity.iter())
                    .map(|(v, va)| if *va { Value::Int16(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::String(values, validity) => Box::new(
                values
                    .iter()
                    .zip(validity.iter())
                    .map(|(v, va)| if *va { Value::String(v.clone()) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Uint1(values, validity) => Box::new(
                values
                    .iter()
                    .zip(validity.iter())
                    .map(|(v, va)| if *va { Value::Uint1(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Uint2(values, validity) => Box::new(
                values
                    .iter()
                    .zip(validity.iter())
                    .map(|(v, va)| if *va { Value::Uint2(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Uint4(values, validity) => Box::new(
                values
                    .iter()
                    .zip(validity.iter())
                    .map(|(v, va)| if *va { Value::Uint4(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Uint8(values, validity) => Box::new(
                values
                    .iter()
                    .zip(validity.iter())
                    .map(|(v, va)| if *va { Value::Uint8(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Uint16(values, validity) => Box::new(
                values
                    .iter()
                    .zip(validity.iter())
                    .map(|(v, va)| if *va { Value::Uint16(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            _ => unimplemented!(),
        }
    }
}

impl ColumnValues {
    pub fn bool(values: impl IntoIterator<Item = bool>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Bool(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn bool_with_capacity(capacity: usize) -> Self {
        ColumnValues::Bool(CowVec::with_capacity(capacity), CowVec::with_capacity(capacity))
    }

    pub fn bool_with_validity(
        values: impl IntoIterator<Item = bool>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        assert_eq!(validity.len(), values.len());
        ColumnValues::Bool(CowVec::new(values), CowVec::new(validity))
    }

    pub fn float4(values: impl IntoIterator<Item = f32>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Float4(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn float4_with_capacity(capacity: usize) -> Self {
        ColumnValues::Float4(CowVec::with_capacity(capacity), CowVec::with_capacity(capacity))
    }

    pub fn float4_with_validity(
        values: impl IntoIterator<Item = f32>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        assert_eq!(validity.len(), values.len());
        ColumnValues::Float4(CowVec::new(values), CowVec::new(validity))
    }

    pub fn float8(values: impl IntoIterator<Item = f64>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Float8(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn float8_with_capacity(capacity: usize) -> Self {
        ColumnValues::Float8(CowVec::with_capacity(capacity), CowVec::with_capacity(capacity))
    }

    pub fn float8_with_validity(
        values: impl IntoIterator<Item = f64>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        assert_eq!(validity.len(), values.len());
        ColumnValues::Float8(CowVec::new(values), CowVec::new(validity))
    }

    pub fn int1(values: impl IntoIterator<Item = i8>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int1(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn int1_with_capacity(capacity: usize) -> Self {
        ColumnValues::Int1(CowVec::with_capacity(capacity), CowVec::with_capacity(capacity))
    }

    pub fn int1_with_validity(
        values: impl IntoIterator<Item = i8>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        assert_eq!(validity.len(), values.len());
        ColumnValues::Int1(CowVec::new(values), CowVec::new(validity))
    }

    pub fn int2(values: impl IntoIterator<Item = i16>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int2(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn int2_with_capacity(capacity: usize) -> Self {
        ColumnValues::Int2(CowVec::with_capacity(capacity), CowVec::with_capacity(capacity))
    }

    pub fn int2_with_validity(
        values: impl IntoIterator<Item = i16>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        assert_eq!(validity.len(), values.len());
        ColumnValues::Int2(CowVec::new(values), CowVec::new(validity))
    }

    pub fn int4(values: impl IntoIterator<Item = i32>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int4(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn int4_with_capacity(capacity: usize) -> Self {
        ColumnValues::Int4(CowVec::with_capacity(capacity), CowVec::with_capacity(capacity))
    }

    pub fn int4_with_validity(
        values: impl IntoIterator<Item = i32>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        assert_eq!(validity.len(), values.len());
        ColumnValues::Int4(CowVec::new(values), CowVec::new(validity))
    }

    pub fn int8(values: impl IntoIterator<Item = i64>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int8(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn int8_with_capacity(capacity: usize) -> Self {
        ColumnValues::Int8(CowVec::with_capacity(capacity), CowVec::with_capacity(capacity))
    }

    pub fn int8_with_validity(
        values: impl IntoIterator<Item = i64>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        assert_eq!(validity.len(), values.len());
        ColumnValues::Int8(CowVec::new(values), CowVec::new(validity))
    }

    pub fn int16(values: impl IntoIterator<Item = i128>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int16(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn int16_with_capacity(capacity: usize) -> Self {
        ColumnValues::Int16(CowVec::with_capacity(capacity), CowVec::with_capacity(capacity))
    }

    pub fn int16_with_validity(
        values: impl IntoIterator<Item = i128>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        assert_eq!(validity.len(), values.len());
        ColumnValues::Int16(CowVec::new(values), CowVec::new(validity))
    }

    pub fn string<'a>(values: impl IntoIterator<Item = String>) -> Self {
        let values = values.into_iter().map(|c| c.to_string()).collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::String(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn string_with_capacity(capacity: usize) -> Self {
        ColumnValues::String(CowVec::with_capacity(capacity), CowVec::with_capacity(capacity))
    }

    pub fn string_with_validity<'a>(
        values: impl IntoIterator<Item = String>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().map(|c| c.to_string()).collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        assert_eq!(validity.len(), values.len());
        ColumnValues::String(CowVec::new(values), CowVec::new(validity))
    }

    pub fn uint1(values: impl IntoIterator<Item = u8>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uint1(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn uint1_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uint1(CowVec::with_capacity(capacity), CowVec::with_capacity(capacity))
    }

    pub fn uint1_with_validity(
        values: impl IntoIterator<Item = u8>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        assert_eq!(validity.len(), values.len());
        ColumnValues::Uint1(CowVec::new(values), CowVec::new(validity))
    }

    pub fn uint2(values: impl IntoIterator<Item = u16>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uint2(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn uint2_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uint2(CowVec::with_capacity(capacity), CowVec::with_capacity(capacity))
    }

    pub fn uint2_with_validity(
        values: impl IntoIterator<Item = u16>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        assert_eq!(validity.len(), values.len());
        ColumnValues::Uint2(CowVec::new(values), CowVec::new(validity))
    }

    pub fn uint4(values: impl IntoIterator<Item = u32>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uint4(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn uint4_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uint4(CowVec::with_capacity(capacity), CowVec::with_capacity(capacity))
    }

    pub fn uint4_with_validity(
        values: impl IntoIterator<Item = u32>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        assert_eq!(validity.len(), values.len());
        ColumnValues::Uint4(CowVec::new(values), CowVec::new(validity))
    }

    pub fn uint8(values: impl IntoIterator<Item = u64>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uint8(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn uint8_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uint8(CowVec::with_capacity(capacity), CowVec::with_capacity(capacity))
    }

    pub fn uint8_with_validity(
        values: impl IntoIterator<Item = u64>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        assert_eq!(validity.len(), values.len());
        ColumnValues::Uint8(CowVec::new(values), CowVec::new(validity))
    }

    pub fn uint16(values: impl IntoIterator<Item = u128>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uint16(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn uint16_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uint16(CowVec::with_capacity(capacity), CowVec::with_capacity(capacity))
    }

    pub fn uint16_with_validity(
        values: impl IntoIterator<Item = u128>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        assert_eq!(validity.len(), values.len());
        ColumnValues::Uint16(CowVec::new(values), CowVec::new(validity))
    }

    pub fn undefined(len: usize) -> Self {
        ColumnValues::Undefined(len)
    }
}

impl ColumnValues {
    pub fn from_many(value: Value, row_count: usize) -> Self {
        match value {
            Value::Bool(v) => ColumnValues::bool(vec![v; row_count]),
            Value::Float4(v) => ColumnValues::float4([v.value()]),
            Value::Float8(v) => ColumnValues::float8([v.value()]),
            Value::Int1(v) => ColumnValues::int1(vec![v; row_count]),
            Value::Int2(v) => ColumnValues::int2(vec![v; row_count]),
            Value::Int4(v) => ColumnValues::int4(vec![v; row_count]),
            Value::Int8(v) => ColumnValues::int8(vec![v; row_count]),
            Value::Int16(v) => ColumnValues::int16(vec![v; row_count]),
            Value::String(v) => ColumnValues::string(vec![v; row_count]),
            Value::Uint1(v) => ColumnValues::uint1(vec![v; row_count]),
            Value::Uint2(v) => ColumnValues::uint2(vec![v; row_count]),
            Value::Uint4(v) => ColumnValues::uint4(vec![v; row_count]),
            Value::Uint8(v) => ColumnValues::uint8(vec![v; row_count]),
            Value::Uint16(v) => ColumnValues::uint16(vec![v; row_count]),
            Value::Undefined => ColumnValues::undefined(row_count),
        }
    }
}

impl From<Value> for ColumnValues {
    fn from(value: Value) -> Self {
        Self::from_many(value, 1)
    }
}

impl ColumnValues {
    pub fn len(&self) -> usize {
        match self {
            ColumnValues::Bool(_, b) => b.len(),
            ColumnValues::Float4(_, b) => b.len(),
            ColumnValues::Float8(_, b) => b.len(),
            ColumnValues::Int1(_, b) => b.len(),
            ColumnValues::Int2(_, b) => b.len(),
            ColumnValues::Int4(_, b) => b.len(),
            ColumnValues::Int8(_, b) => b.len(),
            ColumnValues::Int16(_, b) => b.len(),
            ColumnValues::String(_, b) => b.len(),
            ColumnValues::Uint1(_, b) => b.len(),
            ColumnValues::Uint2(_, b) => b.len(),
            ColumnValues::Uint4(_, b) => b.len(),
            ColumnValues::Uint8(_, b) => b.len(),
            ColumnValues::Uint16(_, b) => b.len(),
            ColumnValues::Undefined(n) => *n,
        }
    }
}

impl ColumnValues {
    pub fn kind(&self) -> ValueKind {
        match self {
            ColumnValues::Bool(_, _) => ValueKind::Bool,
            ColumnValues::Float4(_, _) => ValueKind::Float4,
            ColumnValues::Float8(_, _) => ValueKind::Float8,
            ColumnValues::Int1(_, _) => ValueKind::Int1,
            ColumnValues::Int2(_, _) => ValueKind::Int2,
            ColumnValues::Int4(_, _) => ValueKind::Int4,
            ColumnValues::Int8(_, _) => ValueKind::Int8,
            ColumnValues::Int16(_, _) => ValueKind::Int16,
            ColumnValues::String(_, _) => ValueKind::String,
            ColumnValues::Uint1(_, _) => ValueKind::Uint1,
            ColumnValues::Uint2(_, _) => ValueKind::Uint2,
            ColumnValues::Uint4(_, _) => ValueKind::Uint4,
            ColumnValues::Uint8(_, _) => ValueKind::Uint8,
            ColumnValues::Uint16(_, _) => ValueKind::Uint16,
            ColumnValues::Undefined(_) => ValueKind::Undefined,
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
