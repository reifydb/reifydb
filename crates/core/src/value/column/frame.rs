// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	fragment::Fragment,
	util::cowvec::CowVec,
	value::{
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	},
};

use crate::value::column::{ColumnBuffer, ColumnWithName, columns::Columns};

impl From<ColumnBuffer> for FrameColumnData {
	fn from(value: ColumnBuffer) -> Self {
		match value {
			ColumnBuffer::Bool(container) => FrameColumnData::Bool(container),
			ColumnBuffer::Float4(container) => FrameColumnData::Float4(container),
			ColumnBuffer::Float8(container) => FrameColumnData::Float8(container),
			ColumnBuffer::Int1(container) => FrameColumnData::Int1(container),
			ColumnBuffer::Int2(container) => FrameColumnData::Int2(container),
			ColumnBuffer::Int4(container) => FrameColumnData::Int4(container),
			ColumnBuffer::Int8(container) => FrameColumnData::Int8(container),
			ColumnBuffer::Int16(container) => FrameColumnData::Int16(container),
			ColumnBuffer::Uint1(container) => FrameColumnData::Uint1(container),
			ColumnBuffer::Uint2(container) => FrameColumnData::Uint2(container),
			ColumnBuffer::Uint4(container) => FrameColumnData::Uint4(container),
			ColumnBuffer::Uint8(container) => FrameColumnData::Uint8(container),
			ColumnBuffer::Uint16(container) => FrameColumnData::Uint16(container),
			ColumnBuffer::Utf8 {
				container,
				..
			} => FrameColumnData::Utf8(container),
			ColumnBuffer::Date(container) => FrameColumnData::Date(container),
			ColumnBuffer::DateTime(container) => FrameColumnData::DateTime(container),
			ColumnBuffer::Time(container) => FrameColumnData::Time(container),
			ColumnBuffer::Duration(container) => FrameColumnData::Duration(container),
			ColumnBuffer::IdentityId(container) => FrameColumnData::IdentityId(container),
			ColumnBuffer::Uuid4(container) => FrameColumnData::Uuid4(container),
			ColumnBuffer::Uuid7(container) => FrameColumnData::Uuid7(container),
			ColumnBuffer::Blob {
				container,
				..
			} => FrameColumnData::Blob(container),
			ColumnBuffer::Int {
				container,
				..
			} => FrameColumnData::Int(container),
			ColumnBuffer::Uint {
				container,
				..
			} => FrameColumnData::Uint(container),
			ColumnBuffer::Decimal {
				container,
				..
			} => FrameColumnData::Decimal(container),
			ColumnBuffer::Any(container) => FrameColumnData::Any(container),
			ColumnBuffer::DictionaryId(container) => FrameColumnData::DictionaryId(container),
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => FrameColumnData::Option {
				inner: Box::new(FrameColumnData::from(*inner)),
				bitvec,
			},
		}
	}
}

impl From<ColumnWithName> for FrameColumn {
	fn from(value: ColumnWithName) -> Self {
		FrameColumn {
			name: value.name.to_string(),
			data: value.data.into(),
		}
	}
}

impl From<Columns> for Frame {
	fn from(columns: Columns) -> Self {
		let frame_columns: Vec<FrameColumn> = columns
			.names
			.iter()
			.zip(columns.columns.iter())
			.map(|(name, data)| FrameColumn {
				name: name.to_string(),
				data: data.clone().into(),
			})
			.collect();
		Frame {
			row_numbers: columns.row_numbers.to_vec(),
			created_at: columns.created_at.to_vec(),
			updated_at: columns.updated_at.to_vec(),
			columns: frame_columns,
		}
	}
}

impl From<FrameColumnData> for ColumnBuffer {
	fn from(value: FrameColumnData) -> Self {
		match value {
			FrameColumnData::Bool(container) => ColumnBuffer::Bool(container),
			FrameColumnData::Float4(container) => ColumnBuffer::Float4(container),
			FrameColumnData::Float8(container) => ColumnBuffer::Float8(container),
			FrameColumnData::Int1(container) => ColumnBuffer::Int1(container),
			FrameColumnData::Int2(container) => ColumnBuffer::Int2(container),
			FrameColumnData::Int4(container) => ColumnBuffer::Int4(container),
			FrameColumnData::Int8(container) => ColumnBuffer::Int8(container),
			FrameColumnData::Int16(container) => ColumnBuffer::Int16(container),
			FrameColumnData::Uint1(container) => ColumnBuffer::Uint1(container),
			FrameColumnData::Uint2(container) => ColumnBuffer::Uint2(container),
			FrameColumnData::Uint4(container) => ColumnBuffer::Uint4(container),
			FrameColumnData::Uint8(container) => ColumnBuffer::Uint8(container),
			FrameColumnData::Uint16(container) => ColumnBuffer::Uint16(container),
			FrameColumnData::Utf8(container) => ColumnBuffer::Utf8 {
				container,
				max_bytes: MaxBytes::MAX,
			},
			FrameColumnData::Date(container) => ColumnBuffer::Date(container),
			FrameColumnData::DateTime(container) => ColumnBuffer::DateTime(container),
			FrameColumnData::Time(container) => ColumnBuffer::Time(container),
			FrameColumnData::Duration(container) => ColumnBuffer::Duration(container),
			FrameColumnData::IdentityId(container) => ColumnBuffer::IdentityId(container),
			FrameColumnData::Uuid4(container) => ColumnBuffer::Uuid4(container),
			FrameColumnData::Uuid7(container) => ColumnBuffer::Uuid7(container),
			FrameColumnData::Blob(container) => ColumnBuffer::Blob {
				container,
				max_bytes: MaxBytes::MAX,
			},
			FrameColumnData::Int(container) => ColumnBuffer::Int {
				container,
				max_bytes: MaxBytes::MAX,
			},
			FrameColumnData::Uint(container) => ColumnBuffer::Uint {
				container,
				max_bytes: MaxBytes::MAX,
			},
			FrameColumnData::Decimal(container) => ColumnBuffer::Decimal {
				container,
				precision: Precision::MAX,
				scale: Scale::new(0),
			},
			FrameColumnData::Any(container) => ColumnBuffer::Any(container),
			FrameColumnData::DictionaryId(container) => ColumnBuffer::DictionaryId(container),
			FrameColumnData::Option {
				inner,
				bitvec,
			} => ColumnBuffer::Option {
				inner: Box::new(ColumnBuffer::from(*inner)),
				bitvec,
			},
		}
	}
}

impl From<FrameColumn> for ColumnWithName {
	fn from(value: FrameColumn) -> Self {
		ColumnWithName::new(Fragment::internal(value.name), value.data.into())
	}
}

impl From<Frame> for Columns {
	fn from(frame: Frame) -> Self {
		let columns: Vec<ColumnWithName> = frame.columns.into_iter().map(|col| col.into()).collect();
		let mut names = Vec::with_capacity(columns.len());
		let mut buffers = Vec::with_capacity(columns.len());
		for c in columns {
			names.push(c.name);
			buffers.push(c.data);
		}
		Columns {
			row_numbers: CowVec::new(frame.row_numbers),
			created_at: CowVec::new(frame.created_at),
			updated_at: CowVec::new(frame.updated_at),
			columns: CowVec::new(buffers),
			names: CowVec::new(names),
		}
	}
}
