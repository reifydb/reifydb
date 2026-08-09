// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_value::{
	Result,
	value::{Value, datetime::DateTime, sumtype::SumTypeId, value_type::ValueType},
};
use serde::{Deserialize, Serialize};

use crate::{
	common::TimeSource,
	interface::catalog::{
		column::Column,
		id::{NamespaceId, SeriesId},
		key::PrimaryKey,
	},
	return_internal_error,
	value::column::buffer::ColumnBuffer,
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
	pub partition_by: Vec<String>,
	pub underlying: bool,
	pub time: TimeSource,
}

impl Series {
	pub fn name(&self) -> &str {
		&self.name
	}

	pub fn key_column_type(&self) -> Option<ValueType> {
		let key_col_name = self.key.column();
		self.columns.iter().find(|c| c.name == key_col_name).map(|c| c.constraint.get_type())
	}

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

	pub fn key_from_u64(&self, v: u64) -> Value {
		let ty = self.key_column_type();
		match ty.as_ref() {
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

	pub fn key_column_data(&self, keys: Vec<u64>) -> ColumnBuffer {
		let key_type = self.key_column_type();
		match &key_type {
			Some(ty) => {
				let mut data = ColumnBuffer::with_capacity(ty.clone(), keys.len());
				for k in keys {
					data.push_value(self.key_from_u64(k));
				}
				data
			}
			None => ColumnBuffer::uint8(keys),
		}
	}

	pub fn data_columns(&self) -> impl Iterator<Item = &Column> {
		let key_column = self.key.column().to_string();
		self.columns.iter().filter(move |c| c.name != key_column)
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesMetadata {
	pub row_count: u64,
	pub oldest_key: u64,
	pub newest_key: u64,
	pub sequence_counter: u64,
}

impl SeriesMetadata {
	pub fn new() -> Self {
		Self {
			row_count: 0,
			oldest_key: 0,
			newest_key: 0,
			sequence_counter: 0,
		}
	}
}

impl Default for SeriesMetadata {
	fn default() -> Self {
		Self::new()
	}
}

const SERIES_METADATA_WIDTH: usize = 32;

pub fn encode_series_metadata(metadata: &SeriesMetadata) -> EncodedPodRow {
	let mut bytes = Vec::with_capacity(SERIES_METADATA_WIDTH);
	bytes.extend_from_slice(&metadata.row_count.to_be_bytes());
	bytes.extend_from_slice(&metadata.oldest_key.to_be_bytes());
	bytes.extend_from_slice(&metadata.newest_key.to_be_bytes());
	bytes.extend_from_slice(&metadata.sequence_counter.to_be_bytes());
	EncodedPodRow::new(&bytes)
}

pub fn decode_series_metadata(row: &EncodedPodRow) -> Result<SeriesMetadata> {
	let bytes = row.body();
	if bytes.len() != SERIES_METADATA_WIDTH {
		return_internal_error!(
			"Series metadata is {} bytes wide, expected {}. This indicates a corrupt metadata row.",
			bytes.len(),
			SERIES_METADATA_WIDTH
		)
	}
	Ok(SeriesMetadata {
		row_count: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
		oldest_key: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
		newest_key: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
		sequence_counter: u64::from_be_bytes(bytes[24..32].try_into().unwrap()),
	})
}

#[cfg(test)]
mod series_metadata_tests {
	use super::*;

	#[test]
	fn every_field_survives_a_round_trip_at_the_declared_width() {
		let metadata = SeriesMetadata {
			row_count: 42,
			oldest_key: 100,
			newest_key: 900,
			sequence_counter: 7,
		};

		let row = encode_series_metadata(&metadata);

		assert_eq!(row.len(), SERIES_METADATA_WIDTH);
		assert_eq!(decode_series_metadata(&row).unwrap(), metadata);
	}

	#[test]
	fn the_key_bounds_do_not_swap_because_they_select_which_buckets_materialise() {
		let metadata = SeriesMetadata {
			row_count: 1,
			oldest_key: 1,
			newest_key: u64::MAX,
			sequence_counter: 0,
		};

		let decoded = decode_series_metadata(&encode_series_metadata(&metadata)).unwrap();

		assert_eq!(decoded.oldest_key, 1);
		assert_eq!(decoded.newest_key, u64::MAX);
	}

	#[test]
	fn a_row_of_the_wrong_width_is_rejected_rather_than_rewinding_the_sequence_counter() {
		assert!(decode_series_metadata(&EncodedPodRow::new(&[0u8; 31])).is_err());
		assert!(decode_series_metadata(&EncodedPodRow::new(&[0u8; 33])).is_err());
		assert!(decode_series_metadata(&EncodedPodRow::new(&[0u8; 40])).is_err());
	}
}
