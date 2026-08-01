// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	ops::AddAssign,
};

use reifydb_codec::encoded::row::EncodedRow;
use reifydb_core::interface::{catalog::metrics::MetricsId, store::SingleVersionStore};
use reifydb_value::{Result, byte_size::ByteSize, count::Count, util::cowvec::CowVec};

use crate::metrics::storage::{
	encoding::{
		cdc_stats_key_prefix, decode_cdc_stats, decode_cdc_stats_key, encode_cdc_stats, encode_cdc_stats_key,
	},
	parser::parse_id,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CdcMetrics {
	pub key_bytes: ByteSize,

	pub value_bytes: ByteSize,

	pub entry_count: Count,
}

impl CdcMetrics {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn total_bytes(&self) -> ByteSize {
		self.key_bytes.saturating_add(self.value_bytes)
	}

	pub fn record(&mut self, key_bytes: ByteSize, value_bytes: ByteSize) {
		self.key_bytes = self.key_bytes.saturating_add(key_bytes);
		self.value_bytes = self.value_bytes.saturating_add(value_bytes);
		self.entry_count = self.entry_count.saturating_add(Count::new(1));
	}

	pub fn record_compaction(&mut self, key_bytes: ByteSize, value_bytes: ByteSize, count: Count) {
		self.key_bytes = self.key_bytes.saturating_sub(key_bytes);
		self.value_bytes = self.value_bytes.saturating_sub(value_bytes);
		self.entry_count = self.entry_count.saturating_sub(count);
	}
}

impl AddAssign for CdcMetrics {
	fn add_assign(&mut self, rhs: Self) {
		self.key_bytes = self.key_bytes.saturating_add(rhs.key_bytes);
		self.value_bytes = self.value_bytes.saturating_add(rhs.value_bytes);
		self.entry_count = self.entry_count.saturating_add(rhs.entry_count);
	}
}

pub struct CdcMetricsWriter<S> {
	storage: S,
	stats: HashMap<MetricsId, CdcMetrics>,
	dirty: HashSet<MetricsId>,
}

impl<S: SingleVersionStore> CdcMetricsWriter<S> {
	pub fn new(storage: S) -> Self {
		let mut stats = HashMap::new();

		if let Ok(batch) = storage.prefix(&cdc_stats_key_prefix()) {
			for item in batch.items {
				if let Some(id) = decode_cdc_stats_key(item.key.as_slice())
					&& let Some(s) = decode_cdc_stats(item.row.as_slice())
				{
					stats.insert(id, s);
				}
			}
		}
		Self {
			storage,
			stats,
			dirty: HashSet::new(),
		}
	}

	pub fn record_cdc(&mut self, key: &[u8], value_bytes: ByteSize) -> Result<()> {
		let id = parse_id(key);
		let key_bytes = ByteSize::from_bytes(key.len() as u64);
		self.stats.entry(id).or_default().record(key_bytes, value_bytes);
		self.dirty.insert(id);
		Ok(())
	}

	pub fn record_compaction(
		&mut self,
		id: MetricsId,
		key_bytes: ByteSize,
		value_bytes: ByteSize,
		count: Count,
	) -> Result<()> {
		self.stats.entry(id).or_default().record_compaction(key_bytes, value_bytes, count);
		self.dirty.insert(id);
		Ok(())
	}

	pub fn flush(&mut self) -> Result<()> {
		if self.dirty.is_empty() {
			return Ok(());
		}
		let dirty: Vec<MetricsId> = self.dirty.drain().collect();
		for id in dirty {
			if let Some(stats) = self.stats.get(&id) {
				let storage_key = encode_cdc_stats_key(id);
				self.storage.set(&storage_key, EncodedRow(CowVec::new(encode_cdc_stats(stats))))?;
			}
		}
		Ok(())
	}
}

#[derive(Clone)]
pub struct CdcMetricsReader<S> {
	storage: S,
}

impl<S: SingleVersionStore> CdcMetricsReader<S> {
	pub fn new(storage: S) -> Self {
		Self {
			storage,
		}
	}

	pub fn get(&self, id: MetricsId) -> Result<Option<CdcMetrics>> {
		let key = encode_cdc_stats_key(id);
		Ok(self.storage.get(&key)?.and_then(|v| decode_cdc_stats(v.row.as_slice())))
	}

	pub fn scan_all(&self) -> Result<Vec<(MetricsId, CdcMetrics)>> {
		let prefix = cdc_stats_key_prefix();
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
	use reifydb_value::{byte_size::ByteSize, count::Count};

	use super::*;

	fn bytes(n: u64) -> ByteSize {
		ByteSize::from_bytes(n)
	}

	#[test]
	fn test_cdc_stats() {
		let mut stats = CdcMetrics::new();
		stats.record(bytes(10), bytes(100));
		stats.record(bytes(20), bytes(200));

		assert_eq!(stats.key_bytes, bytes(30));
		assert_eq!(stats.value_bytes, bytes(300));
		assert_eq!(stats.entry_count, Count::new(2));
		assert_eq!(stats.total_bytes(), bytes(330));
	}

	#[test]
	fn test_cdc_stats_add_assign() {
		let mut stats1 = CdcMetrics::new();
		stats1.record(bytes(10), bytes(100));

		let mut stats2 = CdcMetrics::new();
		stats2.record(bytes(20), bytes(200));

		stats1 += stats2;

		assert_eq!(stats1.key_bytes, bytes(30));
		assert_eq!(stats1.value_bytes, bytes(300));
		assert_eq!(stats1.entry_count, Count::new(2));
	}

	#[test]
	fn test_cdc_stats_record_compaction() {
		let mut stats = CdcMetrics::new();
		stats.record(bytes(10), bytes(100));
		stats.record(bytes(20), bytes(200));

		assert_eq!(stats.entry_count, Count::new(2));

		stats.record_compaction(bytes(10), bytes(100), Count::new(1));

		assert_eq!(stats.key_bytes, bytes(20));
		assert_eq!(stats.value_bytes, bytes(200));
		assert_eq!(stats.entry_count, Count::new(1));
	}

	#[test]
	fn test_cdc_stats_record_compaction_saturates() {
		// Compacting more than was recorded must clamp; these are unsigned counters.
		let mut stats = CdcMetrics::new();
		stats.record(bytes(10), bytes(100));

		stats.record_compaction(bytes(20), bytes(200), Count::new(1));

		assert_eq!(stats.key_bytes, ByteSize::ZERO);
		assert_eq!(stats.value_bytes, ByteSize::ZERO);
		assert_eq!(stats.entry_count, Count::ZERO);
	}
}
