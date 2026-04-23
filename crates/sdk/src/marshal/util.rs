// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Utility functions for marshalling

use reifydb_abi::data::column::ColumnTypeCode;
use reifydb_core::value::column::buffer::ColumnBuffer;

/// Convert ColumnBuffer variant to type code
pub(crate) fn column_data_to_type_code(data: &ColumnBuffer) -> ColumnTypeCode {
	match data {
		ColumnBuffer::Bool(_) => ColumnTypeCode::Bool,
		ColumnBuffer::Float4(_) => ColumnTypeCode::Float4,
		ColumnBuffer::Float8(_) => ColumnTypeCode::Float8,
		ColumnBuffer::Int1(_) => ColumnTypeCode::Int1,
		ColumnBuffer::Int2(_) => ColumnTypeCode::Int2,
		ColumnBuffer::Int4(_) => ColumnTypeCode::Int4,
		ColumnBuffer::Int8(_) => ColumnTypeCode::Int8,
		ColumnBuffer::Int16(_) => ColumnTypeCode::Int16,
		ColumnBuffer::Uint1(_) => ColumnTypeCode::Uint1,
		ColumnBuffer::Uint2(_) => ColumnTypeCode::Uint2,
		ColumnBuffer::Uint4(_) => ColumnTypeCode::Uint4,
		ColumnBuffer::Uint8(_) => ColumnTypeCode::Uint8,
		ColumnBuffer::Uint16(_) => ColumnTypeCode::Uint16,
		ColumnBuffer::Utf8 {
			..
		} => ColumnTypeCode::Utf8,
		ColumnBuffer::Date(_) => ColumnTypeCode::Date,
		ColumnBuffer::DateTime(_) => ColumnTypeCode::DateTime,
		ColumnBuffer::Time(_) => ColumnTypeCode::Time,
		ColumnBuffer::Duration(_) => ColumnTypeCode::Duration,
		ColumnBuffer::IdentityId(_) => ColumnTypeCode::IdentityId,
		ColumnBuffer::Uuid4(_) => ColumnTypeCode::Uuid4,
		ColumnBuffer::Uuid7(_) => ColumnTypeCode::Uuid7,
		ColumnBuffer::Blob {
			..
		} => ColumnTypeCode::Blob,
		ColumnBuffer::Int {
			..
		} => ColumnTypeCode::Int,
		ColumnBuffer::Uint {
			..
		} => ColumnTypeCode::Uint,
		ColumnBuffer::Decimal {
			..
		} => ColumnTypeCode::Decimal,
		ColumnBuffer::Any(_) => ColumnTypeCode::Any,
		ColumnBuffer::DictionaryId(_) => ColumnTypeCode::DictionaryId,
		ColumnBuffer::Option {
			inner,
			..
		} => column_data_to_type_code(inner),
	}
}
