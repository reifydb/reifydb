// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use crate::{
	Date, DateTime, Interval, Time, Type, Value,
	value::{
		container::{
			BlobContainer, BoolContainer, IdentityIdContainer, NumberContainer,
			RowIdContainer, StringContainer, TemporalContainer,
			UndefinedContainer, UuidContainer,
		},
		uuid::{Uuid4, Uuid7},
	},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FrameColumnData {
	Bool(BoolContainer),
	Float4(NumberContainer<f32>),
	Float8(NumberContainer<f64>),
	Int1(NumberContainer<i8>),
	Int2(NumberContainer<i16>),
	Int4(NumberContainer<i32>),
	Int8(NumberContainer<i64>),
	Int16(NumberContainer<i128>),
	Uint1(NumberContainer<u8>),
	Uint2(NumberContainer<u16>),
	Uint4(NumberContainer<u32>),
	Uint8(NumberContainer<u64>),
	Uint16(NumberContainer<u128>),
	Utf8(StringContainer),
	Date(TemporalContainer<Date>),
	DateTime(TemporalContainer<DateTime>),
	Time(TemporalContainer<Time>),
	Interval(TemporalContainer<Interval>),
	RowId(RowIdContainer),
	IdentityId(IdentityIdContainer),
	Uuid4(UuidContainer<Uuid4>),
	Uuid7(UuidContainer<Uuid7>),
	Blob(BlobContainer),
	// special case: all undefined
	Undefined(UndefinedContainer),
}

impl FrameColumnData {
	pub fn get_type(&self) -> Type {
		match self {
			FrameColumnData::Bool(_) => Type::Bool,
			FrameColumnData::Float4(_) => Type::Float4,
			FrameColumnData::Float8(_) => Type::Float8,
			FrameColumnData::Int1(_) => Type::Int1,
			FrameColumnData::Int2(_) => Type::Int2,
			FrameColumnData::Int4(_) => Type::Int4,
			FrameColumnData::Int8(_) => Type::Int8,
			FrameColumnData::Int16(_) => Type::Int16,
			FrameColumnData::Uint1(_) => Type::Uint1,
			FrameColumnData::Uint2(_) => Type::Uint2,
			FrameColumnData::Uint4(_) => Type::Uint4,
			FrameColumnData::Uint8(_) => Type::Uint8,
			FrameColumnData::Uint16(_) => Type::Uint16,
			FrameColumnData::Utf8(_) => Type::Utf8,
			FrameColumnData::Date(_) => Type::Date,
			FrameColumnData::DateTime(_) => Type::DateTime,
			FrameColumnData::Time(_) => Type::Time,
			FrameColumnData::Interval(_) => Type::Interval,
			FrameColumnData::RowId(_) => Type::RowId,
			FrameColumnData::IdentityId(_) => Type::IdentityId,
			FrameColumnData::Uuid4(_) => Type::Uuid4,
			FrameColumnData::Uuid7(_) => Type::Uuid7,
			FrameColumnData::Blob(_) => Type::Blob,
			FrameColumnData::Undefined(_) => Type::Undefined,
		}
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		match self {
			FrameColumnData::Bool(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Float4(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Float8(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Int1(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Int2(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Int4(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Int8(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Int16(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Uint1(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Uint2(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Uint4(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Uint8(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Uint16(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Utf8(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Date(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::DateTime(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Time(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Interval(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::RowId(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::IdentityId(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Uuid4(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Uuid7(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Blob(container) => {
				container.is_defined(idx)
			}
			FrameColumnData::Undefined(_) => false,
		}
	}

	pub fn is_bool(&self) -> bool {
		self.get_type() == Type::Bool
	}

	pub fn is_float(&self) -> bool {
		self.get_type() == Type::Float4
			|| self.get_type() == Type::Float8
	}

	pub fn is_utf8(&self) -> bool {
		self.get_type() == Type::Utf8
	}

	pub fn is_number(&self) -> bool {
		matches!(
			self.get_type(),
			Type::Float4
				| Type::Float8 | Type::Int1 | Type::Int2
				| Type::Int4 | Type::Int8 | Type::Int16
				| Type::Uint1 | Type::Uint2 | Type::Uint4
				| Type::Uint8 | Type::Uint16
		)
	}

	pub fn is_text(&self) -> bool {
		self.get_type() == Type::Utf8
	}

	pub fn is_temporal(&self) -> bool {
		matches!(
			self.get_type(),
			Type::Date
				| Type::DateTime | Type::Time
				| Type::Interval
		)
	}

	pub fn is_uuid(&self) -> bool {
		matches!(self.get_type(), Type::Uuid4 | Type::Uuid7)
	}

	pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Value> + 'a> {
		Box::new((0..self.len()).map(move |i| self.get_value(i)))
	}
}

impl FrameColumnData {
	pub fn len(&self) -> usize {
		match self {
			FrameColumnData::Bool(container) => container.len(),
			FrameColumnData::Float4(container) => container.len(),
			FrameColumnData::Float8(container) => container.len(),
			FrameColumnData::Int1(container) => container.len(),
			FrameColumnData::Int2(container) => container.len(),
			FrameColumnData::Int4(container) => container.len(),
			FrameColumnData::Int8(container) => container.len(),
			FrameColumnData::Int16(container) => container.len(),
			FrameColumnData::Uint1(container) => container.len(),
			FrameColumnData::Uint2(container) => container.len(),
			FrameColumnData::Uint4(container) => container.len(),
			FrameColumnData::Uint8(container) => container.len(),
			FrameColumnData::Uint16(container) => container.len(),
			FrameColumnData::Utf8(container) => container.len(),
			FrameColumnData::Date(container) => container.len(),
			FrameColumnData::DateTime(container) => container.len(),
			FrameColumnData::Time(container) => container.len(),
			FrameColumnData::Interval(container) => container.len(),
			FrameColumnData::RowId(container) => container.len(),
			FrameColumnData::IdentityId(container) => container.len(),
			FrameColumnData::Uuid4(container) => container.len(),
			FrameColumnData::Uuid7(container) => container.len(),
			FrameColumnData::Blob(container) => container.len(),
			FrameColumnData::Undefined(container) => {
				container.len()
			}
		}
	}

	pub fn as_string(&self, index: usize) -> String {
		match self {
			FrameColumnData::Bool(container) => {
				container.as_string(index)
			}
			FrameColumnData::Float4(container) => {
				container.as_string(index)
			}
			FrameColumnData::Float8(container) => {
				container.as_string(index)
			}
			FrameColumnData::Int1(container) => {
				container.as_string(index)
			}
			FrameColumnData::Int2(container) => {
				container.as_string(index)
			}
			FrameColumnData::Int4(container) => {
				container.as_string(index)
			}
			FrameColumnData::Int8(container) => {
				container.as_string(index)
			}
			FrameColumnData::Int16(container) => {
				container.as_string(index)
			}
			FrameColumnData::Uint1(container) => {
				container.as_string(index)
			}
			FrameColumnData::Uint2(container) => {
				container.as_string(index)
			}
			FrameColumnData::Uint4(container) => {
				container.as_string(index)
			}
			FrameColumnData::Uint8(container) => {
				container.as_string(index)
			}
			FrameColumnData::Uint16(container) => {
				container.as_string(index)
			}
			FrameColumnData::Utf8(container) => {
				container.as_string(index)
			}
			FrameColumnData::Date(container) => {
				container.as_string(index)
			}
			FrameColumnData::DateTime(container) => {
				container.as_string(index)
			}
			FrameColumnData::Time(container) => {
				container.as_string(index)
			}
			FrameColumnData::Interval(container) => {
				container.as_string(index)
			}
			FrameColumnData::RowId(container) => {
				container.as_string(index)
			}
			FrameColumnData::IdentityId(container) => {
				container.as_string(index)
			}
			FrameColumnData::Uuid4(container) => {
				container.as_string(index)
			}
			FrameColumnData::Uuid7(container) => {
				container.as_string(index)
			}
			FrameColumnData::Blob(container) => {
				container.as_string(index)
			}
			FrameColumnData::Undefined(container) => {
				container.as_string(index)
			}
		}
	}
}

impl FrameColumnData {
	pub fn get_value(&self, index: usize) -> Value {
		match self {
			FrameColumnData::Bool(container) => {
				container.get_value(index)
			}
			FrameColumnData::Float4(container) => {
				container.get_value(index)
			}
			FrameColumnData::Float8(container) => {
				container.get_value(index)
			}
			FrameColumnData::Int1(container) => {
				container.get_value(index)
			}
			FrameColumnData::Int2(container) => {
				container.get_value(index)
			}
			FrameColumnData::Int4(container) => {
				container.get_value(index)
			}
			FrameColumnData::Int8(container) => {
				container.get_value(index)
			}
			FrameColumnData::Int16(container) => {
				container.get_value(index)
			}
			FrameColumnData::Uint1(container) => {
				container.get_value(index)
			}
			FrameColumnData::Uint2(container) => {
				container.get_value(index)
			}
			FrameColumnData::Uint4(container) => {
				container.get_value(index)
			}
			FrameColumnData::Uint8(container) => {
				container.get_value(index)
			}
			FrameColumnData::Uint16(container) => {
				container.get_value(index)
			}
			FrameColumnData::Utf8(container) => {
				container.get_value(index)
			}
			FrameColumnData::Date(container) => {
				container.get_value(index)
			}
			FrameColumnData::DateTime(container) => {
				container.get_value(index)
			}
			FrameColumnData::Time(container) => {
				container.get_value(index)
			}
			FrameColumnData::Interval(container) => {
				container.get_value(index)
			}
			FrameColumnData::RowId(container) => {
				container.get_value(index)
			}
			FrameColumnData::IdentityId(container) => {
				container.get_value(index)
			}
			FrameColumnData::Uuid4(container) => {
				container.get_value(index)
			}
			FrameColumnData::Uuid7(container) => {
				container.get_value(index)
			}
			FrameColumnData::Blob(container) => {
				container.get_value(index)
			}
			FrameColumnData::Undefined(container) => {
				container.get_value(index)
			}
		}
	}
}
