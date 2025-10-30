// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::mpsc;

use reifydb_core::{CommitVersion, interface::Key};
use reifydb_type::Result;

use super::{MemoryBackend, write::WriteCommand};
use crate::backend::{
	diagnostic::database_error,
	gc::{BackendGarbageCollect, GcStats},
};

impl BackendGarbageCollect for MemoryBackend {
	fn compact_operator_states(&self) -> Result<GcStats> {
		// Phase 1: Acquire read lock to identify operator keys that need compaction
		let operations = {
			let multi = self.multi.read();

			// Collect keys that are FlowNodeStateKeys with more than 1 version
			// Limit to ~1024 VERSIONS per batch (not keys!) to match SQLite behavior
			let mut ops = Vec::new();
			let mut keys_with_multiple_versions = 0;
			let mut total_versions_to_remove = 0;
			let mut keys_queued = 0;
			let mut keys_skipped = 0;
			const VERSION_LIMIT: usize = 1024;

			for (key, chain) in multi.iter() {
				// Check if this is a FlowNodeStateKey
				if matches!(Key::decode(key), Some(Key::FlowNodeState(_))) {
					// Only process keys with more than 1 version (need compaction)
					if chain.len() > 1 {
						keys_with_multiple_versions += 1;

						// Calculate how many versions this key will remove (all but the latest)
						let versions_for_this_key = chain.len() - 1;

						// Check if adding this key would exceed the 1024 version limit
						if total_versions_to_remove + versions_for_this_key <= VERSION_LIMIT {
							// Get the latest version for this key
							if let Some(latest_version) = chain.get_latest_version() {
								// Compact to keep only the latest version
								// Use a version number higher than latest to keep only
								// latest
								ops.push((
									key.clone(),
									CommitVersion(latest_version.0 + 1),
								));
								total_versions_to_remove += versions_for_this_key;
								keys_queued += 1;
							}
						} else {
							// Would exceed limit, stop batching
							keys_skipped += 1;
						}
					}
				}
			}

			println!(
				"[GC-Memory] Phase 1: Found {} operator keys with multiple versions",
				keys_with_multiple_versions
			);
			println!(
				"[GC-Memory] Phase 1: Batching {} keys (~{} versions to remove, limit={})",
				keys_queued, total_versions_to_remove, VERSION_LIMIT
			);
			if keys_skipped > 0 {
				println!(
					"[GC-Memory] Phase 1: Skipped {} keys (would exceed version limit)",
					keys_skipped
				);
			}

			ops
		};
		// Read lock is released here

		if operations.is_empty() {
			return Ok(GcStats::default());
		}

		// Phase 2: Send compaction list to writer actor via channel
		let (sender, receiver) = mpsc::channel();

		self.writer
			.send(WriteCommand::GarbageCollect {
				operations,
				respond_to: sender,
			})
			.map_err(|_| reifydb_type::Error(database_error("Failed to send GC command".to_string())))?;

		// Wait for completion
		let result = receiver.recv().map_err(|_| {
			reifydb_type::Error(database_error("Failed to receive GC response".to_string()))
		})?;

		if let Ok(ref stats) = result {
			println!(
				"[GC-Memory] Phase 2: Compacted {} keys, removed {} versions",
				stats.keys_processed, stats.versions_removed
			);
		}

		result
	}
}
