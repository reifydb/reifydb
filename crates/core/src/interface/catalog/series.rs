// SPDX-License-Identifier: AGPL-3.0-or-later
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
}

impl Default for TimestampPrecision {
	fn default() -> Self {
		TimestampPrecision::Millisecond
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesDef {
	pub id: SeriesId,
	pub namespace: NamespaceId,
	pub name: String,
	pub columns: Vec<ColumnDef>,
	pub tag: Option<SumTypeId>,
	pub precision: TimestampPrecision,
	pub primary_key: Option<PrimaryKeyDef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesMetadata {
	pub id: SeriesId,
	pub row_count: u64,
	pub oldest_timestamp: i64,
	pub newest_timestamp: i64,
	pub sequence_counter: u64,
}

impl SeriesMetadata {
	pub fn new(series_id: SeriesId) -> Self {
		Self {
			id: series_id,
			row_count: 0,
			oldest_timestamp: 0,
			newest_timestamp: 0,
			sequence_counter: 0,
		}
	}
}
