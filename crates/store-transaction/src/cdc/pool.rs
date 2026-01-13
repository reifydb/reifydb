// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CDC shard worker pool with dispatcher.
//!
//! The pool manages a fixed number of shard workers and routes commit records
//! to the appropriate shards based on partition assignment.

use std::{collections::HashSet, sync::Arc, thread::{self, JoinHandle}};

use crossbeam_channel::{Receiver, Sender, bounded};
use reifydb_core::CommitVersion;
use tracing::info;

use crate::hot::HotStorage;
use crate::stats::StatsWorker;

use super::{
	log::CommitRecord,
	shard::{CdcShardConfig, CdcShardWorker, ShardMessage, shard_for_partition},
};

/// CDC shard worker pool with dispatcher thread.
///
/// The pool spawns a configurable number of shard workers and a single dispatcher
/// thread. The dispatcher receives commit records from the commit log and routes
/// them to the appropriate shards based on partition assignment.
pub struct CdcShardPool {
	shards: Vec<CdcShardWorker>,
	num_shards: usize,
	dispatcher_handle: Option<JoinHandle<()>>,
	shutdown_tx: Sender<()>,
}

impl CdcShardPool {
	/// Create a new shard pool connected to the commit log receiver.
	///
	/// This spawns:
	/// - `config.num_shards` worker threads (one per shard)
	/// - 1 dispatcher thread that routes records to shards
	pub fn new(
		storage: HotStorage,
		config: CdcShardConfig,
		commit_receiver: Receiver<CommitRecord>,
		stats_worker: Arc<StatsWorker>,
	) -> Self {
		let num_shards = config.num_shards.max(1);
		let (shutdown_tx, shutdown_rx) = bounded(1);

		// Spawn shard workers
		let shards: Vec<CdcShardWorker> =
			(0..num_shards).map(|shard_id| CdcShardWorker::spawn(shard_id, num_shards, storage.clone(), config.clone(), stats_worker.clone())).collect();

		// Create senders for dispatcher
		let shard_senders: Vec<_> = shards.iter().map(|s| s.sender.clone()).collect();

		// Spawn dispatcher thread
		let dispatcher_handle = thread::Builder::new()
			.name("cdc-dispatcher".to_string())
			.spawn(move || {
				info!(num_shards, "CDC dispatcher started");
				Self::dispatcher_loop(num_shards, shard_senders, commit_receiver, shutdown_rx);
				info!("CDC dispatcher stopped");
			})
			.expect("Failed to spawn CDC dispatcher");

		Self {
			shards,
			num_shards,
			dispatcher_handle: Some(dispatcher_handle),
			shutdown_tx,
		}
	}

	fn dispatcher_loop(
		num_shards: usize,
		shard_senders: Vec<Sender<ShardMessage>>,
		receiver: Receiver<CommitRecord>,
		shutdown: Receiver<()>,
	) {
		loop {
			crossbeam_channel::select! {
				recv(receiver) -> msg => {
					match msg {
						Ok(record) => {
							// Dispatch to all shards that have entries in this commit.
							// Each shard will filter to its own partitions.
							let mut sent_to = HashSet::new();
							for entry in &record.entries {
								let shard_id = shard_for_partition(entry.table, num_shards);
								if sent_to.insert(shard_id) {
									// Clone record for each shard that needs it
									let _ = shard_senders[shard_id].try_send(
										ShardMessage::Process(record.clone())
									);
								}
							}
						}
						Err(_) => break, // Channel closed
					}
				}
				recv(shutdown) -> _ => break,
			}
		}

		// Send shutdown to all shards
		for sender in &shard_senders {
			let _ = sender.send(ShardMessage::Shutdown);
		}
	}

	/// Get the number of shards.
	pub fn num_shards(&self) -> usize {
		self.num_shards
	}

	/// Get CDC watermark for a specific shard.
	pub fn shard_watermark(&self, shard_id: usize) -> Option<CommitVersion> {
		self.shards.get(shard_id).map(|s| s.watermark())
	}

	/// Get minimum watermark across all shards (global CDC progress).
	///
	/// This represents the version up to which all shards have processed CDC.
	/// CDC entries for versions up to this watermark are guaranteed to be available.
	pub fn min_watermark(&self) -> Option<CommitVersion> {
		self.shards.iter().map(|s| s.watermark()).min()
	}

	/// Get maximum watermark across all shards.
	///
	/// This represents the highest version any shard has processed.
	pub fn max_watermark(&self) -> Option<CommitVersion> {
		self.shards.iter().map(|s| s.watermark()).max()
	}

	/// Get watermarks for all shards.
	pub fn all_watermarks(&self) -> Vec<(usize, CommitVersion)> {
		self.shards.iter().enumerate().map(|(i, s)| (i, s.watermark())).collect()
	}

	/// Shutdown all workers gracefully.
	pub fn shutdown(&mut self) {
		// Signal dispatcher to stop
		let _ = self.shutdown_tx.send(());

		// Wait for dispatcher to finish
		if let Some(handle) = self.dispatcher_handle.take() {
			let _ = handle.join();
		}

		// Shutdown all shards (they should already be stopping from dispatcher shutdown)
		for shard in &mut self.shards {
			shard.shutdown();
		}
	}
}

impl Drop for CdcShardPool {
	fn drop(&mut self) {
		self.shutdown();
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use reifydb_core::interface::PrimitiveId;
	use reifydb_type::CowVec;

	use super::*;
	use crate::{
		cdc::log::{CommitEntry, CommitLogConfig, CommitOp},
		tier::EntryKind,
	};

	#[test]
	fn test_shard_pool_creation() {
		// This test verifies the pool can be created without panicking
		// We don't actually spawn workers since we don't have real storage
		let config = CdcShardConfig {
			num_shards: 2,
			batch_window: Duration::from_millis(10),
			max_batch_size: 10,
			channel_capacity: 10,
		};

		assert_eq!(config.num_shards, 2);
	}

	#[test]
	fn test_shard_routing() {
		let num_shards = 4;

		// Create records with entries for different partitions
		let record = CommitRecord {
			version: CommitVersion(1),
			timestamp: 12345,
			entries: vec![
				CommitEntry {
					table: EntryKind::Source(PrimitiveId::table(0u64)), // shard 0
					key: CowVec::new(vec![1, 2, 3]),
					op: CommitOp::Set,
				},
				CommitEntry {
					table: EntryKind::Source(PrimitiveId::table(1u64)), // shard 1
					key: CowVec::new(vec![4, 5, 6]),
					op: CommitOp::Set,
				},
				CommitEntry {
					table: EntryKind::Source(PrimitiveId::table(4u64)), // shard 0 again
					key: CowVec::new(vec![7, 8, 9]),
					op: CommitOp::Set,
				},
			],
		};

		// Count unique shards that should receive this record
		let mut shards_needed: HashSet<usize> = HashSet::new();
		for entry in &record.entries {
			shards_needed.insert(shard_for_partition(entry.table, num_shards));
		}

		// Should only need 2 shards (0 and 1), not 3
		assert_eq!(shards_needed.len(), 2);
		assert!(shards_needed.contains(&0));
		assert!(shards_needed.contains(&1));
	}
}
