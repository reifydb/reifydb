// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::value::column::ColumnData;

impl ColumnData {
	pub fn take(&self, num: usize) -> ColumnData {
		match self {
			ColumnData::Bool(container) => ColumnData::Bool(container.take(num)),
			ColumnData::Float4(container) => ColumnData::Float4(container.take(num)),
			ColumnData::Float8(container) => ColumnData::Float8(container.take(num)),
			ColumnData::Int1(container) => ColumnData::Int1(container.take(num)),
			ColumnData::Int2(container) => ColumnData::Int2(container.take(num)),
			ColumnData::Int4(container) => ColumnData::Int4(container.take(num)),
			ColumnData::Int8(container) => ColumnData::Int8(container.take(num)),
			ColumnData::Int16(container) => ColumnData::Int16(container.take(num)),
			ColumnData::Utf8 {
				container,
				max_bytes,
			} => ColumnData::Utf8 {
				container: container.take(num),
				max_bytes: *max_bytes,
			},
			ColumnData::Uint1(container) => ColumnData::Uint1(container.take(num)),
			ColumnData::Uint2(container) => ColumnData::Uint2(container.take(num)),
			ColumnData::Uint4(container) => ColumnData::Uint4(container.take(num)),
			ColumnData::Uint8(container) => ColumnData::Uint8(container.take(num)),
			ColumnData::Uint16(container) => ColumnData::Uint16(container.take(num)),
			ColumnData::Date(container) => ColumnData::Date(container.take(num)),
			ColumnData::DateTime(container) => ColumnData::DateTime(container.take(num)),
			ColumnData::Time(container) => ColumnData::Time(container.take(num)),
			ColumnData::Duration(container) => ColumnData::Duration(container.take(num)),
			ColumnData::Undefined(container) => ColumnData::Undefined(container.take(num)),
			ColumnData::IdentityId(container) => ColumnData::IdentityId(container.take(num)),
			ColumnData::Uuid4(container) => ColumnData::Uuid4(container.take(num)),
			ColumnData::Uuid7(container) => ColumnData::Uuid7(container.take(num)),
			ColumnData::Blob {
				container,
				max_bytes,
			} => ColumnData::Blob {
				container: container.take(num),
				max_bytes: *max_bytes,
			},
			ColumnData::Int {
				container,
				max_bytes,
			} => ColumnData::Int {
				container: container.take(num),
				max_bytes: *max_bytes,
			},
			ColumnData::Uint {
				container,
				max_bytes,
			} => ColumnData::Uint {
				container: container.take(num),
				max_bytes: *max_bytes,
			},
			ColumnData::Decimal {
				container,
				precision,
				scale,
			} => ColumnData::Decimal {
				container: container.take(num),
				precision: *precision,
				scale: *scale,
			},
			ColumnData::Any(container) => ColumnData::Any(container.take(num)),
		}
	}
}
