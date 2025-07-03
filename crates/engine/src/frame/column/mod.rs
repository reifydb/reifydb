// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use layout::ColumnLayout;
pub use push::Push;
use reifydb_core::Kind;
pub use values::ColumnValues;

mod adjust;
mod extend;
mod filter;
mod get;
mod layout;
mod push;
mod reorder;
mod slice;
mod validity;
mod values;

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub data: ColumnValues,
}

impl Column {
    pub fn kind(&self) -> Kind {
        self.data.kind()
    }

    pub fn is_numeric(&self) -> bool {
        self.data.is_numeric()
    }
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

    pub fn float4(name: &str, values: impl IntoIterator<Item = f32>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::float4(values) }
    }

    pub fn float4_with_validity(
        name: &str,
        values: impl IntoIterator<Item = f32>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::float4_with_validity(values, validity) }
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

    pub fn int1(name: &str, values: impl IntoIterator<Item = i8>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::int1(values) }
    }

    pub fn int1_with_validity(
        name: &str,
        values: impl IntoIterator<Item = i8>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::int1_with_validity(values, validity) }
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

    pub fn int4(name: &str, values: impl IntoIterator<Item = i32>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::int4(values) }
    }

    pub fn int4_with_validity(
        name: &str,
        values: impl IntoIterator<Item = i32>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::int4_with_validity(values, validity) }
    }

    pub fn int8(name: &str, values: impl IntoIterator<Item = i64>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::int8(values) }
    }

    pub fn int8_with_validity(
        name: &str,
        values: impl IntoIterator<Item = i64>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::int8_with_validity(values, validity) }
    }

    pub fn int16(name: &str, values: impl IntoIterator<Item = i128>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::int16(values) }
    }

    pub fn int16_with_validity(
        name: &str,
        values: impl IntoIterator<Item = i128>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::int16_with_validity(values, validity) }
    }

    pub fn uint1(name: &str, values: impl IntoIterator<Item = u8>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::uint1(values) }
    }

    pub fn uint1_with_validity(
        name: &str,
        values: impl IntoIterator<Item = u8>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::uint1_with_validity(values, validity) }
    }

    pub fn uint2(name: &str, values: impl IntoIterator<Item = u16>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::uint2(values) }
    }

    pub fn uint2_with_validity(
        name: &str,
        values: impl IntoIterator<Item = u16>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::uint2_with_validity(values, validity) }
    }

    pub fn uint4(name: &str, values: impl IntoIterator<Item = u32>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::uint4(values) }
    }

    pub fn uint4_with_validity(
        name: &str,
        values: impl IntoIterator<Item = u32>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::uint4_with_validity(values, validity) }
    }

    pub fn uint8(name: &str, values: impl IntoIterator<Item = u64>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::uint8(values) }
    }

    pub fn uint8_with_validity(
        name: &str,
        values: impl IntoIterator<Item = u64>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::uint8_with_validity(values, validity) }
    }

    pub fn uint16(name: &str, values: impl IntoIterator<Item = u128>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::uint16(values) }
    }

    pub fn uint16_with_validity(
        name: &str,
        values: impl IntoIterator<Item = u128>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::uint16_with_validity(values, validity) }
    }

    pub fn string<'a>(name: &str, values: impl IntoIterator<Item = &'a str>) -> Self {
        Self {
            name: name.to_string(),
            data: ColumnValues::string(values.into_iter().map(|s| s.to_string())),
        }
    }

    pub fn string_with_validity<'a>(
        name: &str,
        values: impl IntoIterator<Item = &'a str>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self {
            name: name.to_string(),
            data: ColumnValues::string_with_validity(
                values.into_iter().map(|s| s.to_string()),
                validity,
            ),
        }
    }

    pub fn undefined(name: &str, len: usize) -> Self {
        Self { name: name.to_string(), data: ColumnValues::undefined(len) }
    }
}
