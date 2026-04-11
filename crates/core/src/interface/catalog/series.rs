// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{Value, datetime::DateTime, sumtype::SumTypeId, r#type::Type};
use serde::{Deserialize, Serialize};

use crate::{
	interface::catalog::{
		column::Column,
		id::{NamespaceId, SeriesId},
		key::PrimaryKey,
	},
	value::column::data::ColumnData,
};

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
pub enum SeriesKey {
	DateTime {
		column: String,
		precision: TimestampPrecision,
	},
	Integer {
		column: String,
	},
}

impl SeriesKey {
	pub fn column(&self) -> &str {
		match self {
			SeriesKey::DateTime {
				column,
				..
			} => column,
			SeriesKey::Integer {
				column,
			} => column,
		}
	}

	/// Decode a `SeriesKey` from its stored representation.
	///
	/// `key_kind`: 1 = Integer, otherwise DateTime.
	/// `precision_raw`: only used for DateTime keys (0=ms, 1=us, 2=ns, 3=s).
	pub fn decode(key_kind: u8, precision_raw: u8, column: String) -> Self {
		match key_kind {
			1 => SeriesKey::Integer {
				column,
			},
			_ => {
				let precision = match precision_raw {
					1 => TimestampPrecision::Microsecond,
					2 => TimestampPrecision::Nanosecond,
					3 => TimestampPrecision::Second,
					_ => TimestampPrecision::Millisecond,
				};
				SeriesKey::DateTime {
					column,
					precision,
				}
			}
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Series {
	pub id: SeriesId,
	pub namespace: NamespaceId,
	pub name: String,
	pub columns: Vec<Column>,
	pub tag: Option<SumTypeId>,
	pub key: SeriesKey,
	pub primary_key: Option<PrimaryKey>,
	pub underlying: bool,
}

impl Series {
	pub fn name(&self) -> &str {
		&self.name
	}

	/// Returns the Type of the key column, if the key column is found in columns.
	pub fn key_column_type(&self) -> Option<Type> {
		let key_col_name = self.key.column();
		self.columns.iter().find(|c| c.name == key_col_name).map(|c| c.constraint.get_type())
	}

	/// Convert a Value to u64 for series key encoding.
	///
	/// Handles both integer and datetime key types. For datetime keys, the nanos-since-epoch
	/// value is divided by the precision factor to recover the original u64 key.
	/// Negative values (pre-1970 dates, negative integers) are rejected.
	pub fn key_to_u64(&self, value: Value) -> Option<u64> {
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
				match &self.key {
					SeriesKey::DateTime {
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

	/// Convert a u64 key value back to the appropriate Value type for encoding.
	pub fn key_from_u64(&self, v: u64) -> Value {
		let ty = self.key_column_type();
		match ty.as_ref() {
			Some(Type::Int1) => Value::Int1(v as i8),
			Some(Type::Int2) => Value::Int2(v as i16),
			Some(Type::Int4) => Value::Int4(v as i32),
			Some(Type::Int8) => Value::Int8(v as i64),
			Some(Type::Uint1) => Value::Uint1(v as u8),
			Some(Type::Uint2) => Value::Uint2(v as u16),
			Some(Type::Uint4) => Value::Uint4(v as u32),
			Some(Type::Uint8) => Value::Uint8(v),
			Some(Type::Uint16) => Value::Uint16(v as u128),
			Some(Type::Int16) => Value::Int16(v as i128),
			Some(Type::DateTime) => {
				let nanos: u64 = match &self.key {
					SeriesKey::DateTime {
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

	/// Build a ColumnData from u64 key values using the proper key column type.
	pub fn key_column_data(&self, keys: Vec<u64>) -> ColumnData {
		let key_type = self.key_column_type();
		match &key_type {
			Some(ty) => {
				let mut data = ColumnData::with_capacity(ty.clone(), keys.len());
				for k in keys {
					data.push_value(self.key_from_u64(k));
				}
				data
			}
			None => ColumnData::uint8(keys),
		}
	}

	/// Returns columns excluding the key column (data columns only).
	pub fn data_columns(&self) -> impl Iterator<Item = &Column> {
		let key_column = self.key.column().to_string();
		self.columns.iter().filter(move |c| c.name != key_column)
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesMetadata {
	pub id: SeriesId,
	pub row_count: u64,
	pub oldest_key: u64,
	pub newest_key: u64,
	pub sequence_counter: u64,
}

impl SeriesMetadata {
	pub fn new(series_id: SeriesId) -> Self {
		Self {
			id: series_id,
			row_count: 0,
			oldest_key: 0,
			newest_key: 0,
			sequence_counter: 0,
		}
	}
}
