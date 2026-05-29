// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use crate::value::{
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	duration::Duration,
	identity::IdentityId,
	int::Int,
	time::Time,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
	value_type::ValueType,
};

pub trait GetType {
	fn get_type() -> ValueType;
}

impl GetType for bool {
	fn get_type() -> ValueType {
		ValueType::Boolean
	}
}

impl GetType for f32 {
	fn get_type() -> ValueType {
		ValueType::Float4
	}
}

impl GetType for f64 {
	fn get_type() -> ValueType {
		ValueType::Float8
	}
}

impl GetType for i8 {
	fn get_type() -> ValueType {
		ValueType::Int1
	}
}

impl GetType for i16 {
	fn get_type() -> ValueType {
		ValueType::Int2
	}
}

impl GetType for i32 {
	fn get_type() -> ValueType {
		ValueType::Int4
	}
}

impl GetType for i64 {
	fn get_type() -> ValueType {
		ValueType::Int8
	}
}

impl GetType for i128 {
	fn get_type() -> ValueType {
		ValueType::Int16
	}
}

impl GetType for String {
	fn get_type() -> ValueType {
		ValueType::Utf8
	}
}

impl GetType for u8 {
	fn get_type() -> ValueType {
		ValueType::Uint1
	}
}

impl GetType for u16 {
	fn get_type() -> ValueType {
		ValueType::Uint2
	}
}

impl GetType for u32 {
	fn get_type() -> ValueType {
		ValueType::Uint4
	}
}

impl GetType for u64 {
	fn get_type() -> ValueType {
		ValueType::Uint8
	}
}

impl GetType for u128 {
	fn get_type() -> ValueType {
		ValueType::Uint16
	}
}

impl GetType for Date {
	fn get_type() -> ValueType {
		ValueType::Date
	}
}

impl GetType for Time {
	fn get_type() -> ValueType {
		ValueType::Time
	}
}

impl GetType for DateTime {
	fn get_type() -> ValueType {
		ValueType::DateTime
	}
}

impl GetType for Duration {
	fn get_type() -> ValueType {
		ValueType::Duration
	}
}

impl GetType for Uuid4 {
	fn get_type() -> ValueType {
		ValueType::Uuid4
	}
}

impl GetType for IdentityId {
	fn get_type() -> ValueType {
		ValueType::IdentityId
	}
}

impl GetType for Uuid7 {
	fn get_type() -> ValueType {
		ValueType::Uuid7
	}
}

impl GetType for Int {
	fn get_type() -> ValueType {
		ValueType::Int
	}
}

impl GetType for Uint {
	fn get_type() -> ValueType {
		ValueType::Uint
	}
}

impl GetType for Decimal {
	fn get_type() -> ValueType {
		ValueType::Decimal
	}
}
