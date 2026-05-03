// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ops::AddAssign;

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::store::SingleVersionStore,
};
use reifydb_type::{Result, util::cowvec::CowVec};

use crate::{
	MetricId,
	storage::{
		encoding::{
			cdc_stats_key_prefix, decode_cdc_stats, decode_cdc_stats_key, encode_cdc_stats,
			encode_cdc_stats_key,
		},
		parser::parse_id,
	},
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CdcStats {
	pub key_bytes: u64,

	pub value_bytes: u64,

	pub entry_count: u64,
}

impl CdcStats {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn total_bytes(&self) -> u64 {
		self.key_bytes + self.value_bytes
	}

	pub fn record(&mut self, key_bytes: u64, value_bytes: u64) {
		self.key_bytes += key_bytes;
		self.value_bytes += value_bytes;
		self.entry_count += 1;
	}

	pub fn record_drop(&mut self, key_bytes: u64, value_bytes: u64) {
		self.key_bytes = self.key_bytes.saturating_sub(key_bytes);
		self.value_bytes = self.value_bytes.saturating_sub(value_bytes);
		self.entry_count = self.entry_count.saturating_sub(1);
	}
}

impl AddAssign for CdcStats {
	fn add_assign(&mut self, rhs: Self) {
		self.key_bytes += rhs.key_bytes;
		self.value_bytes += rhs.value_bytes;
		self.entry_count += rhs.entry_count;
	}
}

pub struct CdcStatsWriter<S> {
	storage: S,
}

impl<S: SingleVersionStore> CdcStatsWriter<S> {
	pub fn new(storage: S) -> Self {
		Self {
			storage,
		}
	}

	pub fn record_cdc(&mut self, key: &[u8], value_bytes: u64) -> Result<()> {
		let id = parse_id(key);
		let key_bytes = key.len() as u64;

		let storage_key = EncodedKey::new(encode_cdc_stats_key(id));

		let mut stats = self
			.storage
			.get(&storage_key)?
			.and_then(|v| decode_cdc_stats(v.row.as_slice()))
			.unwrap_or_default();

		stats.record(key_bytes, value_bytes);

		self.storage.set(&storage_key, EncodedRow(CowVec::new(encode_cdc_stats(&stats))))
	}

	pub fn record_drop(&mut self, key: &[u8], value_bytes: u64) -> Result<()> {
		let id = parse_id(key);
		let key_bytes = key.len() as u64;
		let storage_key = EncodedKey::new(encode_cdc_stats_key(id));

		let mut stats = self
			.storage
			.get(&storage_key)?
			.and_then(|v| decode_cdc_stats(v.row.as_slice()))
			.unwrap_or_default();

		stats.record_drop(key_bytes, value_bytes);
		self.storage.set(&storage_key, EncodedRow(CowVec::new(encode_cdc_stats(&stats))))
	}
}

#[derive(Clone)]
pub struct CdcStatsReader<S> {
	storage: S,
}

impl<S: SingleVersionStore> CdcStatsReader<S> {
	pub fn new(storage: S) -> Self {
		Self {
			storage,
		}
	}

	pub fn get(&self, id: MetricId) -> Result<Option<CdcStats>> {
		let key = EncodedKey::new(encode_cdc_stats_key(id));
		Ok(self.storage.get(&key)?.and_then(|v| decode_cdc_stats(v.row.as_slice())))
	}

	pub fn scan_all(&self) -> Result<Vec<(MetricId, CdcStats)>> {
		let prefix = EncodedKey::new(cdc_stats_key_prefix());
		let batch = self.storage.prefix(&prefix)?;

		let mut results = Vec::new();
		for item in batch.items {
			if let Some(id) = decode_cdc_stats_key(item.key.as_slice())
				&& let Some(stats) = decode_cdc_stats(item.row.as_slice())
			{
				results.push((id, stats));
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

	#[test]
	fn test_cdc_stats_record_drop() {
		let mut stats = CdcStats::new();
		stats.record(10, 100);
		stats.record(20, 200);

		assert_eq!(stats.entry_count, 2);

		// Drop one entry
		stats.record_drop(10, 100);

		assert_eq!(stats.key_bytes, 20);
		assert_eq!(stats.value_bytes, 200);
		assert_eq!(stats.entry_count, 1);
	}

	#[test]
	fn test_cdc_stats_record_drop_saturates() {
		let mut stats = CdcStats::new();
		stats.record(10, 100);

		// Drop more than recorded - should saturate at 0
		stats.record_drop(20, 200);

		assert_eq!(stats.key_bytes, 0);
		assert_eq!(stats.value_bytes, 0);
		assert_eq!(stats.entry_count, 0);
	}
}
