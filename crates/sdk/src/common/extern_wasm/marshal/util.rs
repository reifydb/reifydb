// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::tag::ValueKind;
use reifydb_core::{
	error::diagnostic::flow::extern_column_type_unsupported,
	value::column::{buffer::ColumnBuffer, columns::Columns},
};
use reifydb_value::{Result, error};

pub fn ensure_marshallable(columns: &Columns) -> Result<()> {
	for column in columns.iter() {
		if matches!(column_data_to_type_code(column.data()), ValueKind::Digest) {
			return Err(error!(extern_column_type_unsupported(
				column.name().text(),
				column.data().get_type()
			)));
		}
	}
	Ok(())
}

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
		ColumnBuffer::Decimal(_) => ValueKind::Decimal,
		ColumnBuffer::Any {
			..
		} => ValueKind::Any,
		ColumnBuffer::DictionaryId {
			..
		} => ValueKind::DictionaryId,
		ColumnBuffer::Digest {
			..
		} => ValueKind::Digest,
	}
}
