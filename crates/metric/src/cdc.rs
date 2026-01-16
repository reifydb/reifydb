// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CDC (Change Data Capture) statistics types, reader, and writer.
//!
//! This module contains everything related to CDC metrics:
//! - `CdcStats` - statistics for CDC entries
//! - `CdcOperation` - operation type for CDC metrics processing
//! - `CdcStatsWriter` - single writer for CDC statistics
//! - `CdcStatsReader` - read-only access to CDC statistics

use std::ops::AddAssign;

use reifydb_core::{
	encoded::{encoded::EncodedValues, key::EncodedKey},
	interface::store::SingleVersionStore,
};
use reifydb_type::util::cowvec::CowVec;

use crate::{
	MetricId,
	encoding::{
		cdc_stats_key_prefix, decode_cdc_stats, decode_cdc_stats_key, encode_cdc_stats, encode_cdc_stats_key,
	},
	parser::parse_id,
};

/// CDC (Change Data Capture) statistics for a single object.
///
/// Tracks storage consumption from CDC entries without tiering.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CdcStats {
	/// Total bytes used by keys in CDC entries
	pub key_bytes: u64,
	/// Total bytes used by values in CDC entries
	pub value_bytes: u64,
	/// Number of CDC entries
	pub entry_count: u64,
}

impl CdcStats {
	/// Create new empty CDC stats.
	pub fn new() -> Self {
		Self::default()
	}

	/// Total bytes (keys + values).
	pub fn total_bytes(&self) -> u64 {
		self.key_bytes + self.value_bytes
	}

	/// Record CDC bytes for a change.
	pub fn record(&mut self, key_bytes: u64, value_bytes: u64) {
		self.key_bytes += key_bytes;
		self.value_bytes += value_bytes;
		self.entry_count += 1;
	}
}

impl AddAssign for CdcStats {
	fn add_assign(&mut self, rhs: Self) {
		self.key_bytes += rhs.key_bytes;
		self.value_bytes += rhs.value_bytes;
		self.entry_count += rhs.entry_count;
	}
}

/// CDC operation for metrics processing.
///
/// Represents a single CDC change to be recorded in statistics.
#[derive(Debug, Clone)]
pub struct CdcOperation {
	/// The key associated with this CDC change
	pub key: EncodedKey,
	/// Size of the value attributed to this change
	pub value_bytes: u64,
}

/// Writer for CDC statistics (single writer only, no tiering).
///
/// This should only be used by the MetricsWorker to maintain single-writer semantics.
pub struct CdcStatsWriter<S> {
	storage: S,
}

impl<S: SingleVersionStore> CdcStatsWriter<S> {
	/// Create a new writer.
	pub fn new(storage: S) -> Self {
		Self {
			storage,
		}
	}

	/// Record CDC bytes for a change.
	pub fn record_cdc(&mut self, key: &[u8], value_bytes: u64) -> reifydb_type::Result<()> {
		let id = parse_id(key);
		let key_bytes = key.len() as u64;

		let storage_key = EncodedKey::new(encode_cdc_stats_key(id));

		// Read current (or default)
		let mut stats = self
			.storage
			.get(&storage_key)?
			.and_then(|v| decode_cdc_stats(v.values.as_slice()))
			.unwrap_or_default();

		// Modify
		stats.record(key_bytes, value_bytes);

		// Write back
		self.storage.set(&storage_key, EncodedValues(CowVec::new(encode_cdc_stats(&stats))))
	}
}

/// Reader for CDC statistics (read-only).
#[derive(Clone)]
pub struct CdcStatsReader<S> {
	storage: S,
}

impl<S: SingleVersionStore> CdcStatsReader<S> {
	/// Create a new reader.
	pub fn new(storage: S) -> Self {
		Self {
			storage,
		}
	}

	/// Get stats for a specific object.
	pub fn get(&self, id: MetricId) -> reifydb_type::Result<Option<CdcStats>> {
		let key = EncodedKey::new(encode_cdc_stats_key(id));
		Ok(self.storage.get(&key)?.and_then(|v| decode_cdc_stats(v.values.as_slice())))
	}

	/// Scan all CDC stats entries.
	pub fn scan_all(&self) -> reifydb_type::Result<Vec<(MetricId, CdcStats)>> {
		let prefix = EncodedKey::new(cdc_stats_key_prefix());
		let batch = self.storage.prefix(&prefix)?;

		let mut results = Vec::new();
		for item in batch.items {
			if let Some(id) = decode_cdc_stats_key(item.key.as_slice()) {
				if let Some(stats) = decode_cdc_stats(item.values.as_slice()) {
					results.push((id, stats));
				}
			}
		}

		Ok(results)
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_cdc_stats() {
		let mut stats = CdcStats::new();
		stats.record(10, 100);
		stats.record(20, 200);

		assert_eq!(stats.key_bytes, 30);
		assert_eq!(stats.value_bytes, 300);
		assert_eq!(stats.entry_count, 2);
		assert_eq!(stats.total_bytes(), 330);
	}

	#[test]
	fn test_cdc_stats_add_assign() {
		let mut stats1 = CdcStats::new();
		stats1.record(10, 100);

		let mut stats2 = CdcStats::new();
		stats2.record(20, 200);

		stats1 += stats2;

		assert_eq!(stats1.key_bytes, 30);
		assert_eq!(stats1.value_bytes, 300);
		assert_eq!(stats1.entry_count, 2);
	}
}
