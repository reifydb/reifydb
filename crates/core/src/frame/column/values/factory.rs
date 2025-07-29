// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::ColumnValues;
use crate::frame::column::container::{
    BlobContainer, BoolContainer, NumberContainer, RowIdContainer, StringContainer,
    TemporalContainer, UndefinedContainer, UuidContainer,
};
use crate::value::{Blob, Uuid4, Uuid7};
use crate::{BitVec, Date, DateTime, Interval, RowId, Time};

impl ColumnValues {
    pub fn bool(values: impl IntoIterator<Item = bool>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Bool(BoolContainer::from_vec(values))
    }

    pub fn bool_with_capacity(capacity: usize) -> Self {
        ColumnValues::Bool(BoolContainer::with_capacity(capacity))
    }

    pub fn bool_with_bitvec(
        values: impl IntoIterator<Item = bool>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Bool(BoolContainer::new(values, bitvec))
    }

    pub fn float4(values: impl IntoIterator<Item = f32>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Float4(NumberContainer::from_vec(values))
    }

    pub fn float4_with_capacity(capacity: usize) -> Self {
        ColumnValues::Float4(NumberContainer::with_capacity(capacity))
    }

    pub fn float4_with_bitvec(
        values: impl IntoIterator<Item = f32>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Float4(NumberContainer::new(values, bitvec))
    }

    pub fn float8(values: impl IntoIterator<Item = f64>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Float8(NumberContainer::from_vec(values))
    }

    pub fn float8_with_capacity(capacity: usize) -> Self {
        ColumnValues::Float8(NumberContainer::with_capacity(capacity))
    }

    pub fn float8_with_bitvec(
        values: impl IntoIterator<Item = f64>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Float8(NumberContainer::new(values, bitvec))
    }

    pub fn int1(values: impl IntoIterator<Item = i8>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Int1(NumberContainer::from_vec(values))
    }

    pub fn int1_with_capacity(capacity: usize) -> Self {
        ColumnValues::Int1(NumberContainer::with_capacity(capacity))
    }

    pub fn int1_with_bitvec(
        values: impl IntoIterator<Item = i8>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Int1(NumberContainer::new(values, bitvec))
    }

    pub fn int2(values: impl IntoIterator<Item = i16>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Int2(NumberContainer::from_vec(values))
    }

    pub fn int2_with_capacity(capacity: usize) -> Self {
        ColumnValues::Int2(NumberContainer::with_capacity(capacity))
    }

    pub fn int2_with_bitvec(
        values: impl IntoIterator<Item = i16>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Int2(NumberContainer::new(values, bitvec))
    }

    pub fn int4(values: impl IntoIterator<Item = i32>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Int4(NumberContainer::from_vec(values))
    }

    pub fn int4_with_capacity(capacity: usize) -> Self {
        ColumnValues::Int4(NumberContainer::with_capacity(capacity))
    }

    pub fn int4_with_bitvec(
        values: impl IntoIterator<Item = i32>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Int4(NumberContainer::new(values, bitvec))
    }

    pub fn int8(values: impl IntoIterator<Item = i64>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Int8(NumberContainer::from_vec(values))
    }

    pub fn int8_with_capacity(capacity: usize) -> Self {
        ColumnValues::Int8(NumberContainer::with_capacity(capacity))
    }

    pub fn int8_with_bitvec(
        values: impl IntoIterator<Item = i64>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Int8(NumberContainer::new(values, bitvec))
    }

    pub fn int16(values: impl IntoIterator<Item = i128>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Int16(NumberContainer::from_vec(values))
    }

    pub fn int16_with_capacity(capacity: usize) -> Self {
        ColumnValues::Int16(NumberContainer::with_capacity(capacity))
    }

    pub fn int16_with_bitvec(
        values: impl IntoIterator<Item = i128>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Int16(NumberContainer::new(values, bitvec))
    }

    pub fn utf8(values: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let values = values.into_iter().map(|c| c.into()).collect::<Vec<_>>();
        ColumnValues::Utf8(StringContainer::from_vec(values))
    }

    pub fn utf8_with_capacity(capacity: usize) -> Self {
        ColumnValues::Utf8(StringContainer::with_capacity(capacity))
    }

    pub fn utf8_with_bitvec<'a>(
        values: impl IntoIterator<Item = String>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Utf8(StringContainer::new(values, bitvec))
    }

    pub fn uint1(values: impl IntoIterator<Item = u8>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Uint1(NumberContainer::from_vec(values))
    }

    pub fn uint1_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uint1(NumberContainer::with_capacity(capacity))
    }

    pub fn uint1_with_bitvec(
        values: impl IntoIterator<Item = u8>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Uint1(NumberContainer::new(values, bitvec))
    }

    pub fn uint2(values: impl IntoIterator<Item = u16>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Uint2(NumberContainer::from_vec(values))
    }

    pub fn uint2_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uint2(NumberContainer::with_capacity(capacity))
    }

    pub fn uint2_with_bitvec(
        values: impl IntoIterator<Item = u16>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Uint2(NumberContainer::new(values, bitvec))
    }

    pub fn uint4(values: impl IntoIterator<Item = u32>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Uint4(NumberContainer::from_vec(values))
    }

    pub fn uint4_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uint4(NumberContainer::with_capacity(capacity))
    }

    pub fn uint4_with_bitvec(
        values: impl IntoIterator<Item = u32>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Uint4(NumberContainer::new(values, bitvec))
    }

    pub fn uint8(values: impl IntoIterator<Item = u64>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Uint8(NumberContainer::from_vec(values))
    }

    pub fn uint8_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uint8(NumberContainer::with_capacity(capacity))
    }

    pub fn uint8_with_bitvec(
        values: impl IntoIterator<Item = u64>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Uint8(NumberContainer::new(values, bitvec))
    }

    pub fn uint16(values: impl IntoIterator<Item = u128>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Uint16(NumberContainer::from_vec(values))
    }

    pub fn uint16_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uint16(NumberContainer::with_capacity(capacity))
    }

    pub fn uint16_with_bitvec(
        values: impl IntoIterator<Item = u128>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Uint16(NumberContainer::new(values, bitvec))
    }

    pub fn date(values: impl IntoIterator<Item = Date>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Date(TemporalContainer::from_vec(values))
    }

    pub fn date_with_capacity(capacity: usize) -> Self {
        ColumnValues::Date(TemporalContainer::with_capacity(capacity))
    }

    pub fn date_with_bitvec(
        values: impl IntoIterator<Item = Date>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Date(TemporalContainer::new(values, bitvec))
    }

    pub fn datetime(values: impl IntoIterator<Item = DateTime>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::DateTime(TemporalContainer::from_vec(values))
    }

    pub fn datetime_with_capacity(capacity: usize) -> Self {
        ColumnValues::DateTime(TemporalContainer::with_capacity(capacity))
    }

    pub fn datetime_with_bitvec(
        values: impl IntoIterator<Item = DateTime>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::DateTime(TemporalContainer::new(values, bitvec))
    }

    pub fn time(values: impl IntoIterator<Item = Time>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Time(TemporalContainer::from_vec(values))
    }

    pub fn time_with_capacity(capacity: usize) -> Self {
        ColumnValues::Time(TemporalContainer::with_capacity(capacity))
    }

    pub fn time_with_bitvec(
        values: impl IntoIterator<Item = Time>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Time(TemporalContainer::new(values, bitvec))
    }

    pub fn interval(values: impl IntoIterator<Item = Interval>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Interval(TemporalContainer::from_vec(values))
    }

    pub fn interval_with_capacity(capacity: usize) -> Self {
        ColumnValues::Interval(TemporalContainer::with_capacity(capacity))
    }

    pub fn interval_with_bitvec(
        values: impl IntoIterator<Item = Interval>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Interval(TemporalContainer::new(values, bitvec))
    }

    pub fn uuid4(values: impl IntoIterator<Item = Uuid4>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Uuid4(UuidContainer::from_vec(values))
    }

    pub fn uuid4_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uuid4(UuidContainer::with_capacity(capacity))
    }

    pub fn uuid4_with_bitvec(
        values: impl IntoIterator<Item = Uuid4>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Uuid4(UuidContainer::new(values, bitvec))
    }

    pub fn uuid7(values: impl IntoIterator<Item = Uuid7>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Uuid7(UuidContainer::from_vec(values))
    }

    pub fn uuid7_with_capacity(capacity: usize) -> Self {
        ColumnValues::Uuid7(UuidContainer::with_capacity(capacity))
    }

    pub fn uuid7_with_bitvec(
        values: impl IntoIterator<Item = Uuid7>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Uuid7(UuidContainer::new(values, bitvec))
    }

    pub fn blob(values: impl IntoIterator<Item = Blob>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        ColumnValues::Blob(BlobContainer::from_vec(values))
    }

    pub fn blob_with_capacity(capacity: usize) -> Self {
        ColumnValues::Blob(BlobContainer::with_capacity(capacity))
    }

    pub fn blob_with_bitvec(
        values: impl IntoIterator<Item = Blob>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::Blob(BlobContainer::new(values, bitvec))
    }

    pub fn row_id(row_ids: impl IntoIterator<Item = RowId>) -> Self {
        let values = row_ids.into_iter().collect::<Vec<_>>();
        ColumnValues::RowId(RowIdContainer::from_vec(values))
    }

    pub fn row_id_with_capacity(capacity: usize) -> Self {
        ColumnValues::RowId(RowIdContainer::with_capacity(capacity))
    }

    pub fn row_id_with_bitvec(
        row_ids: impl IntoIterator<Item = RowId>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        let values = row_ids.into_iter().collect::<Vec<_>>();
        let bitvec = bitvec.into();
        assert_eq!(bitvec.len(), values.len());
        ColumnValues::RowId(RowIdContainer::new(values, bitvec))
    }

    pub fn undefined(len: usize) -> Self {
        ColumnValues::Undefined(UndefinedContainer::new(len))
    }
}
