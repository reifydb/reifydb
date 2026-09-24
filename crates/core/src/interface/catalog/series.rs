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
	value::column::{buffer::ColumnBuffer, builder::ColumnBuilder, columns::Columns},
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

	pub fn extract_key(&self, columns: &Columns, row_idx: usize) -> Option<u64> {
		let key_column = self.column();
		columns.iter()
			.find(|col| col.name().text() == key_column)
			.and_then(|col| self.key_to_u64(col.data().get_value(row_idx)))
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
				match self {
					SeriesKey::DateTime {
						precision,
						..
					} => u64::try_from(match precision {
						TimestampPrecision::Second => nanos.div_euclid(1_000_000_000),
						TimestampPrecision::Millisecond => nanos.div_euclid(1_000_000),
						TimestampPrecision::Microsecond => nanos.div_euclid(1_000),
						TimestampPrecision::Nanosecond => nanos,
					})
					.ok(),
					_ => u64::try_from(nanos).ok(),
				}
			}
			_ => None,
		}
	}

	pub fn key_from_u64(&self, v: u64, key_type: Option<ValueType>) -> Value {
		match key_type.as_ref() {
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
				let nanos: i128 = match self {
					SeriesKey::DateTime {
						precision,
						..
					} => match precision {
						TimestampPrecision::Second => v as i128 * 1_000_000_000,
						TimestampPrecision::Millisecond => v as i128 * 1_000_000,
						TimestampPrecision::Microsecond => v as i128 * 1_000,
						TimestampPrecision::Nanosecond => v as i128,
					},
					_ => v as i128,
				};
				Value::DateTime(DateTime::from_nanos(
					i64::try_from(nanos).expect("series key past the datetime range"),
				))
			}
			_ => Value::Uint8(v),
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
		self.key.key_to_u64(value)
	}

	pub fn key_from_u64(&self, v: u64) -> Value {
		self.key.key_from_u64(v, self.key_column_type())
	}

	pub fn key_column_data(&self, keys: Vec<u64>) -> ColumnBuffer {
		let key_type = self.key_column_type();
		match &key_type {
			Some(ty) => {
				let mut builder = ColumnBuilder::with_capacity(ty.clone(), keys.len());
				for k in keys {
					builder.push_value(self.key_from_u64(k));
				}
				builder.finish()
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
pub struct SeriesPartitionMetadata {
	pub row_count: u64,
	pub oldest_key: u64,
	pub newest_key: u64,
	pub sequence_counter: u64,
	pub last_write_at: DateTime,
	pub dirty_from_key: u64,
	pub dirty_to_key: u64,
}

impl SeriesPartitionMetadata {
	pub fn new() -> Self {
		Self {
			row_count: 0,
			oldest_key: 0,
			newest_key: 0,
			sequence_counter: 0,
			last_write_at: DateTime::default(),
			dirty_from_key: u64::MAX,
			dirty_to_key: 0,
		}
	}
}

impl Default for SeriesPartitionMetadata {
	fn default() -> Self {
		Self::new()
	}
}

const SERIES_PARTITION_METADATA_WIDTH: usize = 56;

pub fn encode_series_partition_metadata(metadata: &SeriesPartitionMetadata) -> EncodedPodRow {
	let mut bytes = Vec::with_capacity(SERIES_PARTITION_METADATA_WIDTH);
	bytes.extend_from_slice(&metadata.row_count.to_be_bytes());
	bytes.extend_from_slice(&metadata.oldest_key.to_be_bytes());
	bytes.extend_from_slice(&metadata.newest_key.to_be_bytes());
	bytes.extend_from_slice(&metadata.sequence_counter.to_be_bytes());
	bytes.extend_from_slice(&metadata.last_write_at.to_order().to_be_bytes());
	bytes.extend_from_slice(&metadata.dirty_from_key.to_be_bytes());
	bytes.extend_from_slice(&metadata.dirty_to_key.to_be_bytes());
	EncodedPodRow::new(&bytes)
}

pub fn decode_series_partition_metadata(row: &EncodedPodRow) -> Result<SeriesPartitionMetadata> {
	let bytes = row.body();
	if bytes.len() != SERIES_PARTITION_METADATA_WIDTH {
		return_internal_error!(
			"Series partition metadata is {} bytes wide, expected {}. This indicates a corrupt metadata row.",
			bytes.len(),
			SERIES_PARTITION_METADATA_WIDTH
		)
	}
	Ok(SeriesPartitionMetadata {
		row_count: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
		oldest_key: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
		newest_key: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
		sequence_counter: u64::from_be_bytes(bytes[24..32].try_into().unwrap()),
		last_write_at: DateTime::from_order(u64::from_be_bytes(bytes[32..40].try_into().unwrap())),
		dirty_from_key: u64::from_be_bytes(bytes[40..48].try_into().unwrap()),
		dirty_to_key: u64::from_be_bytes(bytes[48..56].try_into().unwrap()),
	})
}

#[cfg(test)]
mod series_partition_metadata_tests {
	use super::*;

	#[test]
	fn every_field_survives_a_round_trip_at_the_declared_width() {
		let metadata = SeriesPartitionMetadata {
			row_count: 42,
			oldest_key: 100,
			newest_key: 900,
			sequence_counter: 7,
			last_write_at: DateTime::from_order(1_700_000_000_000_000_000),
			dirty_from_key: 512,
			dirty_to_key: 1024,
		};

		let row = encode_series_partition_metadata(&metadata);

		assert_eq!(row.len(), SERIES_PARTITION_METADATA_WIDTH);
		assert_eq!(decode_series_partition_metadata(&row).unwrap(), metadata);
	}

	#[test]
	fn the_key_bounds_do_not_swap_because_they_select_which_buckets_materialise() {
		let metadata = SeriesPartitionMetadata {
			row_count: 1,
			oldest_key: 1,
			newest_key: u64::MAX,
			sequence_counter: 0,
			last_write_at: DateTime::default(),
			dirty_from_key: u64::MAX,
			dirty_to_key: 0,
		};

		let decoded = decode_series_partition_metadata(&encode_series_partition_metadata(&metadata)).unwrap();

		assert_eq!(decoded.oldest_key, 1);
		assert_eq!(decoded.newest_key, u64::MAX);
	}

	#[test]
	fn a_row_of_the_wrong_width_is_rejected_rather_than_rewinding_the_sequence_counter() {
		assert!(decode_series_partition_metadata(&EncodedPodRow::new(&[0u8; 32])).is_err());
		assert!(decode_series_partition_metadata(&EncodedPodRow::new(&[0u8; 55])).is_err());
		assert!(decode_series_partition_metadata(&EncodedPodRow::new(&[0u8; 57])).is_err());
	}

	#[test]
	fn the_last_write_stamp_does_not_collide_with_the_sequence_counter() {
		// the two sit next to each other in the row; a width or offset slip would let a
		// sequence bump read back as a write stamp and strand a bucket forever
		let metadata = SeriesPartitionMetadata {
			row_count: 0,
			oldest_key: 0,
			newest_key: 0,
			sequence_counter: u64::MAX,
			last_write_at: DateTime::from_order(1),
			dirty_from_key: u64::MAX,
			dirty_to_key: 0,
		};

		let decoded = decode_series_partition_metadata(&encode_series_partition_metadata(&metadata)).unwrap();

		assert_eq!(decoded.sequence_counter, u64::MAX);
		assert_eq!(decoded.last_write_at, DateTime::from_order(1));
	}
}

#[cfg(test)]
mod series_key_tests {
	use super::*;

	#[test]
	fn a_negative_datetime_gives_no_series_key() {
		// nanos before the epoch must never wrap into a huge positive u64 key
		let key = SeriesKey::DateTime {
			column: "ts".to_string(),
			precision: TimestampPrecision::Nanosecond,
		};

		assert_eq!(key.key_to_u64(Value::DateTime(DateTime::from_nanos(-1))), None);
	}
}
