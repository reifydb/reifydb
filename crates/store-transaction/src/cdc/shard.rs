// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Sharded CDC worker for async CDC generation.
//!
//! Each shard owns a subset of partitions determined by hash(partition_id) % num_shards.
//! This allows scaling to 100K+ partitions with a fixed number of OS threads.

use std::{
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	thread::{self, JoinHandle},
	time::Duration,
};

use crossbeam_channel::{Receiver, Sender, bounded};
use reifydb_core::{CommitVersion, EncodedKey, key::Key};
use reifydb_type::CowVec;
use tracing::{error, info, trace};

use super::{
	InternalCdc, InternalCdcChange, InternalCdcSequencedChange,
	codec::encode_internal_cdc,
	exclude::should_exclude_from_cdc,
	log::{CommitEntry, CommitOp, CommitRecord},
};
use crate::{
	hot::HotStorage,
	stats::{StatsOp, StatsWorker, Tier},
	store::version::get_version_info_before,
	tier::{EntryKind, TierStorage},
};

/// Configuration for CDC shard workers.
#[derive(Debug, Clone)]
pub struct CdcShardConfig {
	/// Number of shard workers. Default: 1.
	pub num_shards: usize,
	/// How long to wait for more commits before processing a batch.
	pub batch_window: Duration,
	/// Maximum commits to batch before processing.
	pub max_batch_size: usize,
	/// Channel capacity per shard.
	pub channel_capacity: usize,
}

impl Default for CdcShardConfig {
	fn default() -> Self {
		Self {
			num_shards: 1,
			batch_window: Duration::from_millis(10),
			max_batch_size: 100,
			channel_capacity: 1_000,
		}
	}
}

/// Compute which shard owns a partition.
#[inline]
pub fn shard_for_partition(partition: EntryKind, num_shards: usize) -> usize {
	match partition {
		EntryKind::Source(id) => (id.to_u64() as usize) % num_shards,
		EntryKind::Operator(id) => (id.to_u64() as usize) % num_shards,
		// Multi, Single, Cdc tables go to shard 0
		_ => 0,
	}
}

/// Message to a CDC shard worker.
pub(crate) enum ShardMessage {
	/// Process a commit record (will be filtered to this shard's partitions).
	Process(CommitRecord),
	/// Shutdown the worker.
	Shutdown,
}

/// A CDC shard worker that processes commits for a subset of partitions.
pub struct CdcShardWorker {
	shard_id: usize,
	pub(crate) sender: Sender<ShardMessage>,
	watermark: Arc<AtomicU64>,
	handle: Option<JoinHandle<()>>,
}

impl CdcShardWorker {
	/// Spawn a new shard worker.
	pub fn spawn(
		shard_id: usize,
		num_shards: usize,
		storage: HotStorage,
		config: CdcShardConfig,
		stats_worker: Arc<StatsWorker>,
	) -> Self {
		let (sender, receiver) = bounded(config.channel_capacity);
		let watermark = Arc::new(AtomicU64::new(0));
		let worker_watermark = Arc::clone(&watermark);

		let handle = thread::Builder::new()
			.name(format!("cdc-shard-{}", shard_id))
			.spawn(move || {
				info!(shard_id, num_shards, "CDC shard worker started");
				Self::worker_loop(shard_id, num_shards, storage, receiver, worker_watermark, config, stats_worker);
				info!(shard_id, "CDC shard worker stopped");
			})
			.expect("Failed to spawn CDC shard worker");

		Self {
			shard_id,
			sender,
			watermark,
			handle: Some(handle),
		}
	}

	/// Send a commit record to this shard for processing.
	#[inline]
	pub fn send(&self, record: CommitRecord) {
		let _ = self.sender.try_send(ShardMessage::Process(record));
	}

	/// Get the current CDC watermark (last processed version) for this shard.
	pub fn watermark(&self) -> CommitVersion {
		CommitVersion(self.watermark.load(Ordering::Acquire))
	}

	/// Shutdown the worker gracefully.
	pub fn shutdown(&mut self) {
		let _ = self.sender.send(ShardMessage::Shutdown);
		if let Some(handle) = self.handle.take() {
			let _ = handle.join();
		}
	}

	fn worker_loop(
		shard_id: usize,
		num_shards: usize,
		storage: HotStorage,
		receiver: Receiver<ShardMessage>,
		watermark: Arc<AtomicU64>,
		config: CdcShardConfig,
		stats_worker: Arc<StatsWorker>,
	) {
		loop {
			let mut records = Vec::new();

			// Wait for first message
			match receiver.recv() {
				Ok(ShardMessage::Process(record)) => records.push(record),
				Ok(ShardMessage::Shutdown) | Err(_) => break,
			}

			// Collect more within batch window
			let deadline = std::time::Instant::now() + config.batch_window;
			while records.len() < config.max_batch_size {
				let timeout = deadline.saturating_duration_since(std::time::Instant::now());
				if timeout.is_zero() {
					break;
				}

				match receiver.recv_timeout(timeout) {
					Ok(ShardMessage::Process(record)) => records.push(record),
					Ok(ShardMessage::Shutdown) => {
						// Process remaining before shutdown
						Self::process_batch(
							shard_id, num_shards, &storage, &records, &watermark, &stats_worker,
						);
						return;
					}
					Err(_) => break, // Timeout, process what we have
				}
			}

			if !records.is_empty() {
				Self::process_batch(shard_id, num_shards, &storage, &records, &watermark, &stats_worker);
			}
		}
	}

	fn process_batch(
		shard_id: usize,
		num_shards: usize,
		storage: &HotStorage,
		records: &[CommitRecord],
		watermark: &AtomicU64,
		stats_worker: &StatsWorker,
	) {
		// Filter entries that belong to this shard
		let shard_entries: Vec<(CommitVersion, u64, &CommitEntry)> = records
			.iter()
			.flat_map(|r| {
				r.entries
					.iter()
					.filter(|e| shard_for_partition(e.table, num_shards) == shard_id)
					.map(move |e| (r.version, r.timestamp, e))
			})
			.collect();

		if shard_entries.is_empty() {
			// Update watermark even if no entries for this shard
			if let Some(last) = records.last() {
				watermark.fetch_max(last.version.0, Ordering::Release);
			}
			return;
		}

		trace!(shard_id, entry_count = shard_entries.len(), "Processing CDC batch");

		// Batch lookup previous versions (single pass per unique key+version)
		// We look up versions BEFORE the commit version, since the new version
		// has already been written to storage by the time we process the record.
		// Cache key includes version because the same key at different versions
		// needs different lookups (e.g., Insert at v1 finds nothing, Delete at v2 finds v1).
		let mut prev_versions: HashMap<(EntryKind, Vec<u8>, CommitVersion), Option<CommitVersion>> = HashMap::new();
		for (version, _, entry) in &shard_entries {
			let cache_key = (entry.table, entry.key.as_ref().to_vec(), *version);
			if !prev_versions.contains_key(&cache_key) {
				let prev = get_version_info_before(storage, entry.table, entry.key.as_ref(), *version)
					.ok()
					.flatten()
					.map(|info| info.version);
				prev_versions.insert(cache_key, prev);
			}
		}

		// Generate CDC grouped by version
		let mut cdc_by_version: HashMap<CommitVersion, (u64, Vec<InternalCdcSequencedChange>)> = HashMap::new();
		let mut seq_counters: HashMap<CommitVersion, u16> = HashMap::new();

		for (version, timestamp, entry) in shard_entries {
			// Check if this key kind should be excluded from CDC
			let encoded_key = EncodedKey(entry.key.clone());
			if let Some(kind) = Key::kind(&encoded_key) {
				if should_exclude_from_cdc(kind) {
					continue;
				}
			}

			let seq = seq_counters.entry(version).or_insert(0);
			*seq += 1;

			let cache_key = (entry.table, entry.key.as_ref().to_vec(), version);
			let pre_version = prev_versions.get(&cache_key).copied().flatten();
			let key = encoded_key;

			let change = match entry.op {
				CommitOp::Set => {
					if let Some(pre) = pre_version {
						InternalCdcChange::Update {
							key,
							pre_version: pre,
							post_version: version,
						}
					} else {
						InternalCdcChange::Insert {
							key,
							post_version: version,
						}
					}
				}
				CommitOp::Remove => {
					match pre_version {
						Some(pre) => InternalCdcChange::Delete {
							key,
							pre_version: pre,
						},
						None => continue, // Remove without existing = no-op
					}
				}
			};

			cdc_by_version.entry(version).or_insert_with(|| (timestamp, Vec::new())).1.push(
				InternalCdcSequencedChange {
					sequence: *seq,
					change,
				},
			);
		}

		// Write CDC entries to storage and track stats
		let mut batches: HashMap<EntryKind, Vec<(CowVec<u8>, Option<CowVec<u8>>)>> = HashMap::new();
		// Collect CDC entries with their changes for stats tracking
		let mut cdc_for_stats: Vec<(CommitVersion, Vec<InternalCdcSequencedChange>, u64)> = Vec::new();

		for (version, (timestamp, changes)) in cdc_by_version {
			if changes.is_empty() {
				continue;
			}

			let cdc = InternalCdc {
				version,
				timestamp,
				changes: changes.clone(),
			};

			match encode_internal_cdc(&cdc) {
				Ok(encoded) => {
					let encoded_size = encoded.as_slice().len() as u64;
					batches.entry(EntryKind::Cdc).or_default().push((
						CowVec::new(version.0.to_be_bytes().to_vec()),
						Some(CowVec::new(encoded.as_slice().to_vec())),
					));
					cdc_for_stats.push((version, changes, encoded_size));
				}
				Err(e) => {
					error!(shard_id, version = version.0, "Failed to encode CDC: {:?}", e);
				}
			}
		}

		if !batches.is_empty() {
			if let Err(e) = storage.set(batches) {
				error!(shard_id, "CDC shard write failed: {:?}", e);
			}
		}

		// Track CDC stats for each change
		for (version, changes, encoded_size) in cdc_for_stats {
			if changes.is_empty() {
				continue;
			}

			// Calculate value bytes excluding keys (keys are tracked separately in cdc_key_bytes)
			// The encoded_size includes serialized keys, so we subtract them to avoid double-counting
			let key_bytes_total: u64 = changes.iter().map(|c| c.change.key().len() as u64).sum();
			let value_only_size = encoded_size.saturating_sub(key_bytes_total);
			let bytes_per_change = value_only_size / changes.len().max(1) as u64;
			let mut stats_ops = Vec::with_capacity(changes.len());

			for change in &changes {
				stats_ops.push(StatsOp::Cdc {
					tier: Tier::Hot,
					key: change.change.key().as_ref().to_vec(),
					value_bytes: bytes_per_change,
					count: 1,
				});
			}

			stats_worker.record_batch(stats_ops, version);
		}

		// Update watermark to highest processed version
		if let Some(last) = records.last() {
			watermark.fetch_max(last.version.0, Ordering::Release);
		}
	}
}

impl Drop for CdcShardWorker {
	fn drop(&mut self) {
		self.shutdown();
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{FlowNodeId, PrimitiveId, TableId};

	use super::*;

	#[test]
	fn test_shard_for_partition_source() {
		let num_shards = 4;

		// Source IDs should distribute across shards
		assert_eq!(shard_for_partition(EntryKind::Source(PrimitiveId::table(0u64)), num_shards), 0);
		assert_eq!(shard_for_partition(EntryKind::Source(PrimitiveId::table(1u64)), num_shards), 1);
		assert_eq!(shard_for_partition(EntryKind::Source(PrimitiveId::table(4u64)), num_shards), 0);
		assert_eq!(shard_for_partition(EntryKind::Source(PrimitiveId::table(5u64)), num_shards), 1);
	}

	#[test]
	fn test_shard_for_partition_operator() {
		let num_shards = 4;

		// Operator IDs should distribute across shards
		assert_eq!(shard_for_partition(EntryKind::Operator(FlowNodeId(0)), num_shards), 0);
		assert_eq!(shard_for_partition(EntryKind::Operator(FlowNodeId(1)), num_shards), 1);
		assert_eq!(shard_for_partition(EntryKind::Operator(FlowNodeId(4)), num_shards), 0);
	}

	#[test]
	fn test_shard_for_partition_global_tables() {
		let num_shards = 4;

		// Multi, Single, Cdc always go to shard 0
		assert_eq!(shard_for_partition(EntryKind::Multi, num_shards), 0);
		assert_eq!(shard_for_partition(EntryKind::Single, num_shards), 0);
		assert_eq!(shard_for_partition(EntryKind::Cdc, num_shards), 0);
	}

	#[test]
	fn test_shard_distribution_evenness() {
		let num_shards = 8;
		let num_partitions = 1000u64;

		let mut shard_counts = vec![0usize; num_shards];

		for i in 0..num_partitions {
			let shard = shard_for_partition(EntryKind::Source(PrimitiveId::table(i)), num_shards);
			shard_counts[shard] += 1;
		}

		// Each shard should have roughly num_partitions / num_shards entries
		let expected = (num_partitions as usize) / num_shards;
		for count in shard_counts {
			// Allow 20% variance
			assert!(count >= expected * 80 / 100);
			assert!(count <= expected * 120 / 100);
		}
	}

	#[test]
	fn test_shard_consistency() {
		let num_shards = 4;

		// Same partition always maps to same shard
		let partition = EntryKind::Source(PrimitiveId::table(42u64));
		let shard1 = shard_for_partition(partition, num_shards);
		let shard2 = shard_for_partition(partition, num_shards);
		assert_eq!(shard1, shard2);
	}
}
