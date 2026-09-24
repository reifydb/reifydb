// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::NullBuffer;
use reifydb_value::{
	fragment::Fragment,
	util::bitmap,
	value::{
		constraint::bytes::MaxBytes,
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	},
};

use crate::value::column::{ColumnBuffer, ColumnWithName, columns::Columns};

impl From<ColumnBuffer> for FrameColumnData {
	fn from(value: ColumnBuffer) -> Self {
		let value = match value.split_nulls() {
			(bare, Some(nulls)) => {
				return FrameColumnData::Option {
					inner: Box::new(FrameColumnData::from(bare)),
					bitvec: nulls.into_inner(),
				};
			}
			(bare, None) => bare,
		};
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
			ColumnBuffer::Int(container) => FrameColumnData::Int(container),
			ColumnBuffer::Uint(container) => FrameColumnData::Uint(container),
			ColumnBuffer::Decimal(container) => FrameColumnData::Decimal(container),
			ColumnBuffer::Any {
				container,
				declared_type,
			} => FrameColumnData::Any {
				container,
				declared_type,
			},
			ColumnBuffer::DictionaryId {
				container,
				dictionary_id,
			} => FrameColumnData::DictionaryId {
				container,
				dictionary_id,
			},
			ColumnBuffer::Digest {
				container,
				inner,
				accuracy,
			} => FrameColumnData::Digest {
				container,
				inner,
				accuracy,
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
			system: columns.system,
			columns: frame_columns,
			op: None,
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
			FrameColumnData::Int(container) => ColumnBuffer::Int(container),
			FrameColumnData::Uint(container) => ColumnBuffer::Uint(container),
			FrameColumnData::Decimal(container) => ColumnBuffer::Decimal(container),
			FrameColumnData::Any {
				container,
				declared_type,
			} => ColumnBuffer::Any {
				container,
				declared_type,
			},
			FrameColumnData::DictionaryId {
				container,
				dictionary_id,
			} => ColumnBuffer::DictionaryId {
				container,
				dictionary_id,
			},
			FrameColumnData::Option {
				inner,
				bitvec,
			} => {
				let inner = ColumnBuffer::from(*inner);
				let bits = bitmap::resize(&bitvec, inner.len());
				inner.with_nulls(NullBuffer::new(bits))
			}
			FrameColumnData::Digest {
				container,
				inner,
				accuracy,
			} => ColumnBuffer::Digest {
				container,
				inner,
				accuracy,
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
			system: frame.system,
			columns: buffers,
			names,
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{datetime::DateTime, row_number::RowNumber, system_columns::SystemColumns};

	use super::*;
	use crate::value::column::{ColumnWithName, buffer::ColumnBuffer};

	fn columns(time_nanos: [i64; 2]) -> Columns {
		Columns::with_system(
			vec![ColumnWithName::new(Fragment::internal("v"), ColumnBuffer::int4(vec![10, 20]))],
			SystemColumns::new(
				vec![RowNumber(1), RowNumber(2)],
				Vec::new(),
				vec![DateTime::from_nanos(900), DateTime::from_nanos(901)],
				vec![DateTime::from_nanos(950), DateTime::from_nanos(951)],
				time_nanos.map(DateTime::from_nanos).to_vec(),
				Vec::new(),
			),
		)
	}

	#[test]
	fn a_frame_round_trips_the_time_vector() {
		// Hydration feeds query results straight back into a flow, so a Frame that drops #time
		// yields Columns whose other system vectors are full-length while time is empty - a shape
		// the substrate cannot represent, since every row owns a #time.
		let before = columns([1_700_000_000, 1_700_000_001]);

		let after: Columns = Frame::from(before.clone()).into();

		assert_eq!(
			after.time().to_vec(),
			before.time().to_vec(),
			"#time must survive Columns -> Frame -> Columns"
		);
	}

	#[test]
	fn a_round_tripped_frame_keeps_every_system_vector_the_same_length() {
		// The failure mode is a length mismatch, not a wrong value, and it only surfaces far
		// downstream; pin the invariant here instead.
		let after: Columns = Frame::from(columns([5, 6])).into();

		let rows = after.row_count();
		assert_eq!(after.row_numbers().len(), rows, "row_numbers");
		assert_eq!(after.created_at().len(), rows, "created_at");
		assert_eq!(after.updated_at().len(), rows, "updated_at");
		assert_eq!(after.time().len(), rows, "time");
	}
}
