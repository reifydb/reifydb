// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	Frame, FrameColumn, FrameColumnData,
	value::column::{Column, ColumnData, Columns},
};

impl From<ColumnData> for FrameColumnData {
	fn from(value: ColumnData) -> Self {
		match value {
			ColumnData::Bool(container) => FrameColumnData::Bool(container),
			ColumnData::Float4(container) => FrameColumnData::Float4(container),
			ColumnData::Float8(container) => FrameColumnData::Float8(container),
			ColumnData::Int1(container) => FrameColumnData::Int1(container),
			ColumnData::Int2(container) => FrameColumnData::Int2(container),
			ColumnData::Int4(container) => FrameColumnData::Int4(container),
			ColumnData::Int8(container) => FrameColumnData::Int8(container),
			ColumnData::Int16(container) => FrameColumnData::Int16(container),
			ColumnData::Uint1(container) => FrameColumnData::Uint1(container),
			ColumnData::Uint2(container) => FrameColumnData::Uint2(container),
			ColumnData::Uint4(container) => FrameColumnData::Uint4(container),
			ColumnData::Uint8(container) => FrameColumnData::Uint8(container),
			ColumnData::Uint16(container) => FrameColumnData::Uint16(container),
			ColumnData::Utf8 {
				container,
				..
			} => FrameColumnData::Utf8(container),
			ColumnData::Date(container) => FrameColumnData::Date(container),
			ColumnData::DateTime(container) => FrameColumnData::DateTime(container),
			ColumnData::Time(container) => FrameColumnData::Time(container),
			ColumnData::Interval(container) => FrameColumnData::Interval(container),
			ColumnData::RowNumber(container) => FrameColumnData::RowNumber(container),
			ColumnData::IdentityId(container) => FrameColumnData::IdentityId(container),
			ColumnData::Uuid4(container) => FrameColumnData::Uuid4(container),
			ColumnData::Uuid7(container) => FrameColumnData::Uuid7(container),
			ColumnData::Blob {
				container,
				..
			} => FrameColumnData::Blob(container),
			ColumnData::Int {
				container,
				..
			} => FrameColumnData::Int(container),
			ColumnData::Uint {
				container,
				..
			} => FrameColumnData::Uint(container),
			ColumnData::Decimal {
				container,
				..
			} => FrameColumnData::Decimal(container),
			ColumnData::Undefined(container) => FrameColumnData::Undefined(container),
		}
	}
}

impl From<Column<'_>> for FrameColumn {
	fn from(value: Column) -> Self {
		// Since Column is now just name + data, we extract any qualification from the name
		let name_str = value.name.text();
		let parts: Vec<&str> = name_str.split('.').collect();
		let (namespace, store, name) = match parts.as_slice() {
			[ns, src, n] => (Some(ns.to_string()), Some(src.to_string()), n.to_string()),
			[src, n] => (None, Some(src.to_string()), n.to_string()),
			[n] => (None, None, n.to_string()),
			_ => (None, None, name_str.to_string()),
		};
		FrameColumn {
			namespace,
			store,
			name,
			data: value.data.into(),
		}
	}
}

impl From<Columns<'_>> for Frame {
	fn from(columns: Columns) -> Self {
		let frame_columns: Vec<FrameColumn> = columns.columns.into_iter().map(|col| col.into()).collect();
		if !columns.row_numbers.is_empty() {
			Frame::with_row_numbers(frame_columns, columns.row_numbers.to_vec())
		} else {
			Frame::new(frame_columns)
		}
	}
}
