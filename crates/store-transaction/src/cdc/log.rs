// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Append-only commit record log for async CDC generation.
//!
//! The commit log captures lightweight metadata about each commit (keys and operation types)
//! without the actual values. This enables async CDC generation by shard workers.

use crossbeam_channel::{Receiver, Sender, bounded};
use reifydb_core::{CommitVersion, CowVec, delta::Delta};

use crate::{store::router::classify_key, tier::EntryKind};

/// Operation type for a commit entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOp {
	Set,
	Remove,
}

/// A single entry in a commit record.
#[derive(Debug, Clone)]
pub struct CommitEntry {
	pub table: EntryKind,
	pub key: CowVec<u8>,
	pub op: CommitOp,
}

/// A commit record capturing metadata for async processing.
#[derive(Debug, Clone)]
pub struct CommitRecord {
	pub version: CommitVersion,
	pub timestamp: u64,
	pub entries: Vec<CommitEntry>,
}

/// Configuration for the commit log.
#[derive(Debug, Clone)]
pub struct CommitLogConfig {
	/// Maximum records to buffer before blocking.
	pub buffer_capacity: usize,
}

impl Default for CommitLogConfig {
	fn default() -> Self {
		Self {
			buffer_capacity: 10_000,
		}
	}
}

/// Append-only commit log.
///
/// Captures commit metadata and sends it to CDC shard workers for async processing.
pub struct CommitLog {
	sender: Sender<CommitRecord>,
}

impl CommitLog {
	/// Create a new commit log, returning the log and a receiver for the dispatcher.
	pub fn new(config: CommitLogConfig) -> (Self, Receiver<CommitRecord>) {
		let (sender, receiver) = bounded(config.buffer_capacity);
		(Self { sender }, receiver)
	}

	/// Append a commit record. Non-blocking, drops if buffer full.
	#[inline]
	pub fn append(&self, record: CommitRecord) {
		let _ = self.sender.try_send(record);
	}

	/// Build a commit record from optimized deltas.
	pub fn build_record(version: CommitVersion, timestamp: u64, deltas: &[Delta]) -> CommitRecord {
		let entries = deltas
			.iter()
			.filter_map(|delta| {
				let (key, op) = match delta {
					Delta::Set {
						key,
						..
					} => (key.clone(), CommitOp::Set),
					Delta::Remove {
						key,
					} => (key.clone(), CommitOp::Remove),
					Delta::Drop {
						..
					} => return None, // Drops don't generate CDC
				};
				let table = classify_key(&key);
				Some(CommitEntry {
					table,
					key: CowVec::new(key.as_ref().to_vec()),
					op,
				})
			})
			.collect();

		CommitRecord {
			version,
			timestamp,
			entries,
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{CowVec, EncodedKey, value::encoded::EncodedValues};

	use super::*;

	fn make_key(bytes: &[u8]) -> EncodedKey {
		EncodedKey(CowVec::new(bytes.to_vec()))
	}

	#[test]
	fn test_commit_log_send_receive() {
		let config = CommitLogConfig {
			buffer_capacity: 10,
		};
		let (log, receiver) = CommitLog::new(config);

		let record = CommitRecord {
			version: CommitVersion(1),
			timestamp: 12345,
			entries: vec![CommitEntry {
				table: EntryKind::Multi,
				key: CowVec::new(vec![1, 2, 3]),
				op: CommitOp::Set,
			}],
		};

		log.append(record.clone());

		let received = receiver.try_recv().unwrap();
		assert_eq!(received.version.0, 1);
		assert_eq!(received.timestamp, 12345);
		assert_eq!(received.entries.len(), 1);
	}

	#[test]
	fn test_build_record_filters_drops() {
		let deltas = vec![
			Delta::Set {
				key: make_key(&[1, 2, 3]),
				values: EncodedValues(CowVec::new(vec![4, 5, 6])),
			},
			Delta::Remove {
				key: make_key(&[7, 8, 9]),
			},
			Delta::Drop {
				key: make_key(&[10, 11, 12]),
				up_to_version: None,
				keep_last_versions: Some(1),
			},
		];

		let record = CommitLog::build_record(CommitVersion(1), 12345, &deltas);

		// Should have 2 entries (Set and Remove), not 3 (Drop filtered)
		assert_eq!(record.entries.len(), 2);
		assert_eq!(record.entries[0].op, CommitOp::Set);
		assert_eq!(record.entries[1].op, CommitOp::Remove);
	}

	#[test]
	fn test_commit_log_drops_when_full() {
		let config = CommitLogConfig {
			buffer_capacity: 1,
		};
		let (log, _receiver) = CommitLog::new(config);

		let record = CommitRecord {
			version: CommitVersion(1),
			timestamp: 12345,
			entries: vec![],
		};

		// First should succeed
		log.append(record.clone());

		// Second should be dropped (buffer full, receiver not draining)
		log.append(CommitRecord {
			version: CommitVersion(2),
			..record
		});

		// No panic, just dropped
	}
}
