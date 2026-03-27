// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker

use std::any::Any;

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{key::EncodedKey, row::EncodedRow},
};
use reifydb_type::util::cowvec::CowVec;

use crate::{
	log::{Entry, Index},
	message::Command,
};

/// A Raft-managed state machine. Commands are applied sequentially from the
/// Raft log and must be deterministic across all nodes.
pub trait State: Send {
	/// Returns the last applied log index.
	fn get_applied_index(&self) -> Index;

	/// Applies a log entry to the state machine.
	fn apply(&mut self, entry: &Entry);

	/// Returns self as `Any` for downcasting (e.g. to `KVState` in tests).
	fn as_any(&self) -> &dyn Any;
}

/// Helper to construct a `Command::Write` with a single key=value delta.
/// Uses the entry's index as the commit version for simplicity.
pub fn test_write(key: &str, value: &str, version: u64) -> Command {
	Command::Write {
		deltas: vec![Delta::Set {
			key: EncodedKey::new(key.as_bytes().to_vec()),
			row: EncodedRow(CowVec::new(value.as_bytes().to_vec())),
		}],
		version: CommitVersion(version),
	}
}

/// A simple key/value store state machine for testing.
pub struct KVState {
	applied_index: Index,
	data: std::collections::BTreeMap<String, String>,
}

impl KVState {
	pub fn new() -> Self {
		Self {
			applied_index: 0,
			data: std::collections::BTreeMap::new(),
		}
	}

	pub fn get(&self, key: &str) -> Option<&String> {
		self.data.get(key)
	}

	pub fn data(&self) -> &std::collections::BTreeMap<String, String> {
		&self.data
	}
}

impl State for KVState {
	fn get_applied_index(&self) -> Index {
		self.applied_index
	}

	fn as_any(&self) -> &dyn Any {
		self
	}

	fn apply(&mut self, entry: &Entry) {
		match &entry.command {
			Command::Write {
				deltas,
				..
			} => {
				for delta in deltas {
					match delta {
						Delta::Set {
							key,
							row,
						} => {
							let k = String::from_utf8_lossy(key.as_ref()).to_string();
							let v = String::from_utf8_lossy(row.as_ref()).to_string();
							self.data.insert(k, v);
						}
						Delta::Remove {
							key,
						} => {
							let k = String::from_utf8_lossy(key.as_ref()).to_string();
							self.data.remove(&k);
						}
						_ => {}
					}
				}
			}
			Command::Noop => {}
		}
		self.applied_index = entry.index;
	}
}
