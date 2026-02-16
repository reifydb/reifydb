// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Utility functions for marshalling

use reifydb_abi::data::column::ColumnTypeCode;
use reifydb_core::value::column::data::ColumnData;

/// Convert ColumnData variant to type code
pub(super) fn column_data_to_type_code(data: &ColumnData) -> ColumnTypeCode {
	match data {
		ColumnData::Bool(_) => ColumnTypeCode::Bool,
		ColumnData::Float4(_) => ColumnTypeCode::Float4,
		ColumnData::Float8(_) => ColumnTypeCode::Float8,
		ColumnData::Int1(_) => ColumnTypeCode::Int1,
		ColumnData::Int2(_) => ColumnTypeCode::Int2,
		ColumnData::Int4(_) => ColumnTypeCode::Int4,
		ColumnData::Int8(_) => ColumnTypeCode::Int8,
		ColumnData::Int16(_) => ColumnTypeCode::Int16,
		ColumnData::Uint1(_) => ColumnTypeCode::Uint1,
		ColumnData::Uint2(_) => ColumnTypeCode::Uint2,
		ColumnData::Uint4(_) => ColumnTypeCode::Uint4,
		ColumnData::Uint8(_) => ColumnTypeCode::Uint8,
		ColumnData::Uint16(_) => ColumnTypeCode::Uint16,
		ColumnData::Utf8 {
			..
		} => ColumnTypeCode::Utf8,
		ColumnData::Date(_) => ColumnTypeCode::Date,
		ColumnData::DateTime(_) => ColumnTypeCode::DateTime,
		ColumnData::Time(_) => ColumnTypeCode::Time,
		ColumnData::Duration(_) => ColumnTypeCode::Duration,
		ColumnData::IdentityId(_) => ColumnTypeCode::IdentityId,
		ColumnData::Uuid4(_) => ColumnTypeCode::Uuid4,
		ColumnData::Uuid7(_) => ColumnTypeCode::Uuid7,
		ColumnData::Blob {
			..
		} => ColumnTypeCode::Blob,
		ColumnData::Int {
			..
		} => ColumnTypeCode::Int,
		ColumnData::Uint {
			..
		} => ColumnTypeCode::Uint,
		ColumnData::Decimal {
			..
		} => ColumnTypeCode::Decimal,
		ColumnData::Any(_) => ColumnTypeCode::Any,
		ColumnData::DictionaryId(_) => ColumnTypeCode::DictionaryId,
		ColumnData::Option {
			..
		} => {
			unreachable!("Option columns cannot be marshalled to FFI yet")
		}
	}
}
