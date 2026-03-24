// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::value::{
	Value,
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	duration::Duration,
	int::Int,
	ordered_f32::OrderedF32,
	ordered_f64::OrderedF64,
	time::Time,
	r#type::Type,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
};

pub trait ToValue {
	fn to_value(&self) -> Value;
}

impl ToValue for i8 {
	fn to_value(&self) -> Value {
		Value::Int1(*self)
	}
}

impl ToValue for i16 {
	fn to_value(&self) -> Value {
		Value::Int2(*self)
	}
}

impl ToValue for i32 {
	fn to_value(&self) -> Value {
		Value::Int4(*self)
	}
}

impl ToValue for i64 {
	fn to_value(&self) -> Value {
		Value::Int8(*self)
	}
}

impl ToValue for i128 {
	fn to_value(&self) -> Value {
		Value::Int16(*self)
	}
}

impl ToValue for u8 {
	fn to_value(&self) -> Value {
		Value::Uint1(*self)
	}
}

impl ToValue for u16 {
	fn to_value(&self) -> Value {
		Value::Uint2(*self)
	}
}

impl ToValue for u32 {
	fn to_value(&self) -> Value {
		Value::Uint4(*self)
	}
}

impl ToValue for u64 {
	fn to_value(&self) -> Value {
		Value::Uint8(*self)
	}
}

impl ToValue for u128 {
	fn to_value(&self) -> Value {
		Value::Uint16(*self)
	}
}

impl ToValue for f32 {
	fn to_value(&self) -> Value {
		OrderedF32::try_from(*self).map(Value::Float4).unwrap_or(Value::None {
			inner: Type::Float4,
		})
	}
}

impl ToValue for f64 {
	fn to_value(&self) -> Value {
		OrderedF64::try_from(*self).map(Value::Float8).unwrap_or(Value::None {
			inner: Type::Float8,
		})
	}
}

impl ToValue for Decimal {
	fn to_value(&self) -> Value {
		Value::Decimal(self.clone())
	}
}

impl ToValue for Int {
	fn to_value(&self) -> Value {
		Value::Int(self.clone())
	}
}

impl ToValue for Uint {
	fn to_value(&self) -> Value {
		Value::Uint(self.clone())
	}
}

impl ToValue for Date {
	fn to_value(&self) -> Value {
		Value::Date(*self)
	}
}

impl ToValue for DateTime {
	fn to_value(&self) -> Value {
		Value::DateTime(*self)
	}
}

impl ToValue for Time {
	fn to_value(&self) -> Value {
		Value::Time(*self)
	}
}

impl ToValue for Duration {
	fn to_value(&self) -> Value {
		Value::Duration(*self)
	}
}

impl ToValue for Uuid4 {
	fn to_value(&self) -> Value {
		Value::Uuid4(*self)
	}
}

impl ToValue for Uuid7 {
	fn to_value(&self) -> Value {
		Value::Uuid7(*self)
	}
}
