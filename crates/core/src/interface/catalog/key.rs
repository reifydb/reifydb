// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{Value, datetime::DateTime, value_type::ValueType};
use serde::{Deserialize, Serialize};

use crate::interface::catalog::{column::Column, id::PrimaryKeyId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrimaryKey {
	pub id: PrimaryKeyId,
	pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum TimestampPrecision {
	#[default]
	Millisecond = 0,
	Microsecond = 1,
	Nanosecond = 2,
	Second = 3,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum KeySpec {
	DateTime {
		column: String,
		precision: TimestampPrecision,
	},
	Integer {
		column: String,
	},
}

impl KeySpec {
	pub fn column(&self) -> &str {
		match self {
			KeySpec::DateTime {
				column,
				..
			} => column,
			KeySpec::Integer {
				column,
			} => column,
		}
	}

	pub fn decode(key_kind: u8, precision_raw: u8, column: String) -> Self {
		match key_kind {
			1 => KeySpec::Integer {
				column,
			},
			_ => {
				let precision = match precision_raw {
					1 => TimestampPrecision::Microsecond,
					2 => TimestampPrecision::Nanosecond,
					3 => TimestampPrecision::Second,
					_ => TimestampPrecision::Millisecond,
				};
				KeySpec::DateTime {
					column,
					precision,
				}
			}
		}
	}

	pub fn value_to_u64(&self, value: Value) -> Option<u64> {
		match value {
			Value::Int1(v) => u64::try_from(v).ok(),
			Value::Int2(v) => u64::try_from(v).ok(),
			Value::Int4(v) => u64::try_from(v).ok(),
			Value::Int8(v) => u64::try_from(v).ok(),
			Value::Int16(v) => u64::try_from(v).ok(),
			Value::Uint1(v) => Some(v as u64),
			Value::Uint2(v) => Some(v as u64),
			Value::Uint4(v) => Some(v as u64),
			Value::Uint8(v) => Some(v),
			Value::Uint16(v) => u64::try_from(v).ok(),
			Value::DateTime(dt) => {
				let nanos = dt.to_nanos();
				match self {
					KeySpec::DateTime {
						precision,
						..
					} => Some(match precision {
						TimestampPrecision::Second => nanos / 1_000_000_000,
						TimestampPrecision::Millisecond => nanos / 1_000_000,
						TimestampPrecision::Microsecond => nanos / 1_000,
						TimestampPrecision::Nanosecond => nanos,
					}),
					_ => Some(nanos),
				}
			}
			_ => None,
		}
	}

	pub fn value_from_u64(&self, key_type: Option<ValueType>, v: u64) -> Value {
		match key_type {
			Some(ValueType::Int1) => Value::Int1(v as i8),
			Some(ValueType::Int2) => Value::Int2(v as i16),
			Some(ValueType::Int4) => Value::Int4(v as i32),
			Some(ValueType::Int8) => Value::Int8(v as i64),
			Some(ValueType::Uint1) => Value::Uint1(v as u8),
			Some(ValueType::Uint2) => Value::Uint2(v as u16),
			Some(ValueType::Uint4) => Value::Uint4(v as u32),
			Some(ValueType::Uint8) => Value::Uint8(v),
			Some(ValueType::Uint16) => Value::Uint16(v as u128),
			Some(ValueType::Int16) => Value::Int16(v as i128),
			Some(ValueType::DateTime) => {
				let nanos: u64 = match self {
					KeySpec::DateTime {
						precision,
						..
					} => match precision {
						TimestampPrecision::Second => v * 1_000_000_000,
						TimestampPrecision::Millisecond => v * 1_000_000,
						TimestampPrecision::Microsecond => v * 1_000,
						TimestampPrecision::Nanosecond => v,
					},
					_ => v,
				};
				Value::DateTime(DateTime::from_nanos(nanos))
			}
			_ => Value::Uint8(v),
		}
	}
}
