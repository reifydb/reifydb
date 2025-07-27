// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::RowId;
use crate::value::uuid::{Uuid4, Uuid7};
use crate::{BitVec, CowVec, Type, Value};
use crate::{Date, DateTime, Interval, Time};

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnValues {
    Bool(CowVec<bool>, BitVec),
    Float4(CowVec<f32>, BitVec),
    Float8(CowVec<f64>, BitVec),
    Int1(CowVec<i8>, BitVec),
    Int2(CowVec<i16>, BitVec),
    Int4(CowVec<i32>, BitVec),
    Int8(CowVec<i64>, BitVec),
    Int16(CowVec<i128>, BitVec),
    Uint1(CowVec<u8>, BitVec),
    Uint2(CowVec<u16>, BitVec),
    Uint4(CowVec<u32>, BitVec),
    Uint8(CowVec<u64>, BitVec),
    Uint16(CowVec<u128>, BitVec),
    Utf8(CowVec<String>, BitVec),
    Date(CowVec<Date>, BitVec),
    DateTime(CowVec<DateTime>, BitVec),
    Time(CowVec<Time>, BitVec),
    Interval(CowVec<Interval>, BitVec),
    RowId(CowVec<RowId>, BitVec),
    Uuid4(CowVec<Uuid4>, BitVec),
    Uuid7(CowVec<Uuid7>, BitVec),
    // special case: all undefined
    Undefined(usize),
}

impl ColumnValues {
    pub fn get_type(&self) -> Type {
        match self {
            ColumnValues::Bool(_, _) => Type::Bool,
            ColumnValues::Float4(_, _) => Type::Float4,
            ColumnValues::Float8(_, _) => Type::Float8,
            ColumnValues::Int1(_, _) => Type::Int1,
            ColumnValues::Int2(_, _) => Type::Int2,
            ColumnValues::Int4(_, _) => Type::Int4,
            ColumnValues::Int8(_, _) => Type::Int8,
            ColumnValues::Int16(_, _) => Type::Int16,
            ColumnValues::Uint1(_, _) => Type::Uint1,
            ColumnValues::Uint2(_, _) => Type::Uint2,
            ColumnValues::Uint4(_, _) => Type::Uint4,
            ColumnValues::Uint8(_, _) => Type::Uint8,
            ColumnValues::Uint16(_, _) => Type::Uint16,
            ColumnValues::Utf8(_, _) => Type::Utf8,
            ColumnValues::Date(_, _) => Type::Date,
            ColumnValues::DateTime(_, _) => Type::DateTime,
            ColumnValues::Time(_, _) => Type::Time,
            ColumnValues::Interval(_, _) => Type::Interval,
            ColumnValues::RowId(_, _) => Type::RowId,
            ColumnValues::Uuid4(_, _) => Type::Uuid4,
            ColumnValues::Uuid7(_, _) => Type::Uuid7,
            ColumnValues::Undefined(_) => Type::Undefined,
        }
    }

    pub fn is_bool(&self) -> bool {
        self.get_type() == Type::Bool
    }

    pub fn is_float(&self) -> bool {
        self.get_type() == Type::Float4 || self.get_type() == Type::Float8
    }

    pub fn is_utf8(&self) -> bool {
        self.get_type() == Type::Utf8
    }

    pub fn is_number(&self) -> bool {
        matches!(
            self.get_type(),
            Type::Float4
                | Type::Float8
                | Type::Int1
                | Type::Int2
                | Type::Int4
                | Type::Int8
                | Type::Int16
                | Type::Uint1
                | Type::Uint2
                | Type::Uint4
                | Type::Uint8
                | Type::Uint16
        )
    }

    pub fn is_text(&self) -> bool {
        self.get_type() == Type::Utf8
    }

    pub fn is_temporal(&self) -> bool {
        matches!(self.get_type(), Type::Date | Type::DateTime | Type::Time | Type::Interval)
    }

    pub fn is_uuid(&self) -> bool {
        matches!(self.get_type(), Type::Uuid4 | Type::Uuid7)
    }
}

impl ColumnValues {
    pub fn bitvec(&self) -> &BitVec {
        match self {
            ColumnValues::Bool(_, bitvec) => bitvec,
            ColumnValues::Float4(_, bitvec) => bitvec,
            ColumnValues::Float8(_, bitvec) => bitvec,
            ColumnValues::Int1(_, bitvec) => bitvec,
            ColumnValues::Int2(_, bitvec) => bitvec,
            ColumnValues::Int4(_, bitvec) => bitvec,
            ColumnValues::Int8(_, bitvec) => bitvec,
            ColumnValues::Int16(_, bitvec) => bitvec,
            ColumnValues::Uint1(_, bitvec) => bitvec,
            ColumnValues::Uint2(_, bitvec) => bitvec,
            ColumnValues::Uint4(_, bitvec) => bitvec,
            ColumnValues::Uint8(_, bitvec) => bitvec,
            ColumnValues::Uint16(_, bitvec) => bitvec,
            ColumnValues::Utf8(_, bitvec) => bitvec,
            ColumnValues::Date(_, bitvec) => bitvec,
            ColumnValues::DateTime(_, bitvec) => bitvec,
            ColumnValues::Time(_, bitvec) => bitvec,
            ColumnValues::Interval(_, bitvec) => bitvec,
            ColumnValues::RowId(_, bitvec) => bitvec,
            ColumnValues::Uuid4(_, bitvec) => bitvec,
            ColumnValues::Uuid7(_, bitvec) => bitvec,
            ColumnValues::Undefined(_) => unreachable!(),
        }
    }
}

impl ColumnValues {
    pub fn with_capacity(target: Type, capacity: usize) -> Self {
        match target {
            Type::Bool => Self::bool_with_capacity(capacity),
            Type::Float4 => Self::float4_with_capacity(capacity),
            Type::Float8 => Self::float8_with_capacity(capacity),
            Type::Int1 => Self::int1_with_capacity(capacity),
            Type::Int2 => Self::int2_with_capacity(capacity),
            Type::Int4 => Self::int4_with_capacity(capacity),
            Type::Int8 => Self::int8_with_capacity(capacity),
            Type::Int16 => Self::int16_with_capacity(capacity),
            Type::Uint1 => Self::uint1_with_capacity(capacity),
            Type::Uint2 => Self::uint2_with_capacity(capacity),
            Type::Uint4 => Self::uint4_with_capacity(capacity),
            Type::Uint8 => Self::uint8_with_capacity(capacity),
            Type::Uint16 => Self::uint16_with_capacity(capacity),
            Type::Utf8 => Self::utf8_with_capacity(capacity),
            Type::Date => Self::date_with_capacity(capacity),
            Type::DateTime => Self::datetime_with_capacity(capacity),
            Type::Time => Self::time_with_capacity(capacity),
            Type::Interval => Self::interval_with_capacity(capacity),
            Type::RowId => Self::row_id_with_capacity(capacity),
            Type::Uuid4 => Self::uuid4_with_capacity(capacity),
            Type::Uuid7 => Self::uuid7_with_capacity(capacity),
            Type::Undefined => Self::undefined(capacity),
        }
    }

    // FIXME wrapping and then later unwrapping a value feels pretty stupid -- FIXME
    pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Value> + 'a> {
        match self {
            ColumnValues::Bool(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Bool(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Float4(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::float4(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Float8(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::float8(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Int1(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Int1(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Int2(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Int2(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Int4(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Int4(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Int8(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Int8(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Int16(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Int16(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Utf8(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Utf8(v.clone()) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Uint1(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Uint1(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Uint2(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Uint2(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Uint4(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Uint4(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Uint8(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Uint8(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Uint16(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Uint16(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Date(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Date(v.clone()) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::DateTime(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::DateTime(v.clone()) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Time(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Time(v.clone()) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Interval(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Interval(v.clone()) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::RowId(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::RowId(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Uuid4(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Uuid4(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Uuid7(values, bitvec) => Box::new(
                values
                    .iter()
                    .zip(bitvec.iter())
                    .map(|(v, b)| if b { Value::Uuid7(*v) } else { Value::Undefined })
                    .into_iter(),
            ),
            ColumnValues::Undefined(size) => {
                Box::new((0..*size).map(|_| Value::Undefined).collect::<Vec<Value>>().into_iter())
            }
        }
    }
}

impl ColumnValues {
    pub fn bool(values: impl IntoIterator<Item = bool>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Bool(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn bool_with_capacity(capacity: usize) -> Self {
        ColumnValues::Bool(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn bool_with_bitvec(
        values: impl IntoIterator<Item = bool>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Bool(CowVec::new(values), bitvec)
    }

    pub fn float4(values: impl IntoIterator<Item = f32>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Float4(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn float4_with_capacity(capacity: usize) -> Self {
        ColumnValues::Float4(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn float4_with_bitvec(
        values: impl IntoIterator<Item = f32>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Float4(CowVec::new(values), bitvec)
    }

    pub fn float8(values: impl IntoIterator<Item = f64>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Float8(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn float8_with_capacity(capacity: usize) -> Self {
        ColumnValues::Float8(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn float8_with_bitvec(
        values: impl IntoIterator<Item = f64>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Float8(CowVec::new(values), bitvec)
    }

    pub fn int1(values: impl IntoIterator<Item = i8>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int1(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn int1_with_capacity(capacity: usize) -> Self {
        ColumnValues::Int1(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn int1_with_bitvec(
        values: impl IntoIterator<Item = i8>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Int1(CowVec::new(values), bitvec)
    }

    pub fn int2(values: impl IntoIterator<Item = i16>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int2(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn int2_with_capacity(capacity: usize) -> Self {
        ColumnValues::Int2(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn int2_with_bitvec(
        values: impl IntoIterator<Item = i16>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Int2(CowVec::new(values), bitvec)
    }

    pub fn int4(values: impl IntoIterator<Item = i32>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int4(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn int4_with_capacity(capacity: usize) -> Self {
        ColumnValues::Int4(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn int4_with_bitvec(
        values: impl IntoIterator<Item = i32>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Int4(CowVec::new(values), bitvec)
    }

    pub fn int8(values: impl IntoIterator<Item = i64>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int8(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn int8_with_capacity(capacity: usize) -> Self {
        ColumnValues::Int8(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn int8_with_bitvec(
        values: impl IntoIterator<Item = i64>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Int8(CowVec::new(values), bitvec)
    }

    pub fn int16(values: impl IntoIterator<Item = i128>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int16(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn int16_with_capacity(capacity: usize) -> Self {
        ColumnValues::Int16(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn int16_with_bitvec(
        values: impl IntoIterator<Item = i128>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Int16(CowVec::new(values), bitvec)
    }

    pub fn utf8<'a>(values: impl IntoIterator<Item = String>) -> Self {
        let values = values.into_iter().map(|c| c.to_string()).collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Utf8(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn utf8_with_capacity(capacity: usize) -> Self {
        ColumnValues::Utf8(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn utf8_with_bitvec<'a>(
        values: impl IntoIterator<Item = String>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Utf8(CowVec::new(values), bitvec)
    }

    pub fn uint1(values: impl IntoIterator<Item = u8>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uint1(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn uint1_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uint1(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn uint1_with_bitvec(
        values: impl IntoIterator<Item = u8>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Uint1(CowVec::new(values), bitvec)
    }

    pub fn uint2(values: impl IntoIterator<Item = u16>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uint2(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn uint2_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uint2(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn uint2_with_bitvec(
        values: impl IntoIterator<Item = u16>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Uint2(CowVec::new(values), bitvec)
    }

    pub fn uint4(values: impl IntoIterator<Item = u32>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uint4(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn uint4_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uint4(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn uint4_with_bitvec(
        values: impl IntoIterator<Item = u32>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Uint4(CowVec::new(values), bitvec)
    }

    pub fn uint8(values: impl IntoIterator<Item = u64>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uint8(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn uint8_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uint8(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn uint8_with_bitvec(
        values: impl IntoIterator<Item = u64>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Uint8(CowVec::new(values), bitvec)
    }

    pub fn uint16(values: impl IntoIterator<Item = u128>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uint16(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn uint16_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uint16(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn uint16_with_bitvec(
        values: impl IntoIterator<Item = u128>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Uint16(CowVec::new(values), bitvec)
    }

    pub fn date(values: impl IntoIterator<Item = Date>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Date(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn date_with_capacity(capacity: usize) -> Self {
        ColumnValues::Date(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn date_with_bitvec(
        values: impl IntoIterator<Item = Date>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Date(CowVec::new(values), bitvec)
    }

    pub fn datetime(values: impl IntoIterator<Item = DateTime>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::DateTime(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn datetime_with_capacity(capacity: usize) -> Self {
        ColumnValues::DateTime(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn datetime_with_bitvec(
        values: impl IntoIterator<Item = DateTime>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::DateTime(CowVec::new(values), bitvec)
    }

    pub fn time(values: impl IntoIterator<Item = Time>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Time(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn time_with_capacity(capacity: usize) -> Self {
        ColumnValues::Time(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn time_with_bitvec(
        values: impl IntoIterator<Item = Time>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Time(CowVec::new(values), bitvec)
    }

    pub fn interval(values: impl IntoIterator<Item = Interval>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Interval(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn interval_with_capacity(capacity: usize) -> Self {
        ColumnValues::Interval(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn interval_with_bitvec(
        values: impl IntoIterator<Item = Interval>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Interval(CowVec::new(values), bitvec)
    }

    pub fn uuid4(values: impl IntoIterator<Item = Uuid4>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uuid4(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn uuid4_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uuid4(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn uuid4_with_bitvec(
        values: impl IntoIterator<Item = Uuid4>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Uuid4(CowVec::new(values), bitvec)
    }

    pub fn uuid7(values: impl IntoIterator<Item = Uuid7>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uuid7(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn uuid7_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uuid7(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn uuid7_with_bitvec(
        values: impl IntoIterator<Item = Uuid7>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Uuid7(CowVec::new(values), bitvec)
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
            Value::Utf8(v) => ColumnValues::utf8(vec![v; row_count]),
            Value::Uint1(v) => ColumnValues::uint1(vec![v; row_count]),
            Value::Uint2(v) => ColumnValues::uint2(vec![v; row_count]),
            Value::Uint4(v) => ColumnValues::uint4(vec![v; row_count]),
            Value::Uint8(v) => ColumnValues::uint8(vec![v; row_count]),
            Value::Uint16(v) => ColumnValues::uint16(vec![v; row_count]),
            Value::Date(v) => ColumnValues::date(vec![v; row_count]),
            Value::DateTime(v) => ColumnValues::datetime(vec![v; row_count]),
            Value::Time(v) => ColumnValues::time(vec![v; row_count]),
            Value::Interval(v) => ColumnValues::interval(vec![v; row_count]),
            Value::RowId(v) => ColumnValues::row_id(vec![v; row_count]),
            Value::Uuid4(v) => ColumnValues::uuid4(vec![v; row_count]),
            Value::Uuid7(v) => ColumnValues::uuid7(vec![v; row_count]),
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
            ColumnValues::Utf8(_, b) => b.len(),
            ColumnValues::Uint1(_, b) => b.len(),
            ColumnValues::Uint2(_, b) => b.len(),
            ColumnValues::Uint4(_, b) => b.len(),
            ColumnValues::Uint8(_, b) => b.len(),
            ColumnValues::Uint16(_, b) => b.len(),
            ColumnValues::Date(_, b) => b.len(),
            ColumnValues::DateTime(_, b) => b.len(),
            ColumnValues::Time(_, b) => b.len(),
            ColumnValues::Interval(_, b) => b.len(),
            ColumnValues::RowId(_, b) => b.len(),
            ColumnValues::Uuid4(_, b) => b.len(),
            ColumnValues::Uuid7(_, b) => b.len(),
            ColumnValues::Undefined(n) => *n,
        }
    }
}

impl ColumnValues {
    pub fn row_id(row_ids: impl IntoIterator<Item = RowId>) -> Self {
        let values = row_ids.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::RowId(CowVec::new(values), BitVec::new(len, true))
    }

    pub fn row_id_with_capacity(capacity: usize) -> Self {
        ColumnValues::RowId(CowVec::with_capacity(capacity), BitVec::with_capacity(capacity))
    }

    pub fn row_id_with_bitvec(
        row_ids: impl IntoIterator<Item = RowId>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = row_ids.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::RowId(CowVec::new(values), bitvec)
    }

    pub fn as_row_id(&self) -> &[RowId] {
        match self {
            ColumnValues::RowId(values, _) => values,
            _ => panic!("not a row id column"),
        }
    }

    pub fn as_row_id_mut(&mut self) -> &mut CowVec<RowId> {
        match self {
            ColumnValues::RowId(values, _) => values,
            _ => panic!("not a row id column"),
        }
    }
}
