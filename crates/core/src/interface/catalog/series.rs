// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::sumtype::SumTypeId;
use serde::{Deserialize, Serialize};

use crate::interface::catalog::{
	column::ColumnDef,
	id::{NamespaceId, SeriesId},
	key::PrimaryKeyDef,
};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimestampPrecision {
	Millisecond = 0,
	Microsecond = 1,
	Nanosecond = 2,
	Second = 3,
}

impl Default for TimestampPrecision {
	fn default() -> Self {
		TimestampPrecision::Millisecond
	}
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
pub struct SeriesDef {
	pub id: SeriesId,
	pub namespace: NamespaceId,
	pub name: String,
	pub columns: Vec<ColumnDef>,
	pub tag: Option<SumTypeId>,
	pub key: SeriesKey,
	pub primary_key: Option<PrimaryKeyDef>,
}

impl SeriesDef {
	pub fn name(&self) -> &str {
		&self.name
	}

	/// Returns columns excluding the key column (data columns only).
	pub fn data_columns(&self) -> impl Iterator<Item = &ColumnDef> {
		let key_column = self.key.column().to_string();
		self.columns.iter().filter(move |c| c.name != key_column)
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesMetadata {
	pub id: SeriesId,
	pub row_count: u64,
	pub oldest_key: i64,
	pub newest_key: i64,
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
