// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_codec::tag::ValueKind;
use reifydb_core::error::diagnostic::flow::extern_column_type_unsupported;
use reifydb_value::{
	Result, error,
	value::{
		column_view::{ColumnView, ViewData},
		system_columns::user_columns,
	},
};

pub fn ensure_marshallable(batch: &RecordBatch) -> Result<()> {
	for (field, array) in user_columns(batch) {
		ensure_view_marshallable(&ColumnView::try_from((array, field.as_ref()))?)?;
	}
	Ok(())
}

pub(crate) fn ensure_view_marshallable(view: &ColumnView<'_>) -> Result<()> {
	if matches!(column_data_to_type_code(view), ValueKind::Digest) {
		return Err(error!(extern_column_type_unsupported(view.field.name(), view.get_type())));
	}
	Ok(())
}

pub(crate) fn column_data_to_type_code(view: &ColumnView<'_>) -> ValueKind {
	match &view.data {
		ViewData::Bool(_) => ValueKind::Boolean,
		ViewData::Float4(_) => ValueKind::Float4,
		ViewData::Float8(_) => ValueKind::Float8,
		ViewData::Int1(_) => ValueKind::Int1,
		ViewData::Int2(_) => ValueKind::Int2,
		ViewData::Int4(_) => ValueKind::Int4,
		ViewData::Int8(_) => ValueKind::Int8,
		ViewData::Int16(_) => ValueKind::Int16,
		ViewData::Uint1(_) => ValueKind::Uint1,
		ViewData::Uint2(_) => ValueKind::Uint2,
		ViewData::Uint4(_) => ValueKind::Uint4,
		ViewData::Uint8(_) => ValueKind::Uint8,
		ViewData::Uint16(_) => ValueKind::Uint16,
		ViewData::Utf8 {
			..
		} => ValueKind::Utf8,
		ViewData::Date(_) => ValueKind::Date,
		ViewData::DateTime(_) => ValueKind::DateTime,
		ViewData::Time(_) => ValueKind::Time,
		ViewData::Duration(_) => ValueKind::Duration,
		ViewData::IdentityId(_) => ValueKind::IdentityId,
		ViewData::Uuid4(_) => ValueKind::Uuid4,
		ViewData::Uuid7(_) => ValueKind::Uuid7,
		ViewData::Blob {
			..
		} => ValueKind::Blob,
		ViewData::Decimal(_) => ValueKind::Decimal,
		ViewData::Any {
			..
		} => ValueKind::Any,
		ViewData::DictionaryId {
			..
		} => ValueKind::DictionaryId,
		ViewData::Digest {
			..
		} => ValueKind::Digest,
		ViewData::None {
			..
		} => ValueKind::None,
	}
}
