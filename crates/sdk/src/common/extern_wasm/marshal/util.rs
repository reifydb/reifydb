// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::tag::ValueKind;
use reifydb_core::value::column::buffer::ColumnBuffer;

pub(crate) fn column_data_to_type_code(data: &ColumnBuffer) -> ValueKind {
	match data {
		ColumnBuffer::Bool(_) => ValueKind::Boolean,
		ColumnBuffer::Float4(_) => ValueKind::Float4,
		ColumnBuffer::Float8(_) => ValueKind::Float8,
		ColumnBuffer::Int1(_) => ValueKind::Int1,
		ColumnBuffer::Int2(_) => ValueKind::Int2,
		ColumnBuffer::Int4(_) => ValueKind::Int4,
		ColumnBuffer::Int8(_) => ValueKind::Int8,
		ColumnBuffer::Int16(_) => ValueKind::Int16,
		ColumnBuffer::Uint1(_) => ValueKind::Uint1,
		ColumnBuffer::Uint2(_) => ValueKind::Uint2,
		ColumnBuffer::Uint4(_) => ValueKind::Uint4,
		ColumnBuffer::Uint8(_) => ValueKind::Uint8,
		ColumnBuffer::Uint16(_) => ValueKind::Uint16,
		ColumnBuffer::Utf8 {
			..
		} => ValueKind::Utf8,
		ColumnBuffer::Date(_) => ValueKind::Date,
		ColumnBuffer::DateTime(_) => ValueKind::DateTime,
		ColumnBuffer::Time(_) => ValueKind::Time,
		ColumnBuffer::Duration(_) => ValueKind::Duration,
		ColumnBuffer::IdentityId(_) => ValueKind::IdentityId,
		ColumnBuffer::Uuid4(_) => ValueKind::Uuid4,
		ColumnBuffer::Uuid7(_) => ValueKind::Uuid7,
		ColumnBuffer::Blob {
			..
		} => ValueKind::Blob,
		ColumnBuffer::Int {
			..
		} => ValueKind::Int,
		ColumnBuffer::Uint {
			..
		} => ValueKind::Uint,
		ColumnBuffer::Decimal {
			..
		} => ValueKind::Decimal,
		ColumnBuffer::Any(_) => ValueKind::Any,
		ColumnBuffer::DictionaryId(_) => ValueKind::DictionaryId,
		ColumnBuffer::Option {
			inner,
			..
		} => column_data_to_type_code(inner),
	}
}
