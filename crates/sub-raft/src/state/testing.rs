// Copyright (c) 2026 ReifyDB
// SPDX-License-Identifier: Apache-2.0

use std::{any::Any, collections::BTreeMap};

use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{common::CommitVersion, delta::Delta};
use reifydb_value::util::cowvec::CowVec;

use super::State;
use crate::{
	log::{Entry, Index},
	message::Command,
};

pub fn write(key: &str, value: &str, version: u64) -> Command {
	Command::WriteMulti {
		deltas: vec![Delta::Set {
			key: EncodedKey::new(key.as_bytes().to_vec()),
			row: EncodedRow(CowVec::new(value.as_bytes().to_vec())),
		}],
		version: CommitVersion(version),
		changes: vec![],
	}
}

pub struct KV {
	applied_index: Index,
	data: BTreeMap<String, String>,
}

impl Default for KV {
	fn default() -> Self {
		Self::new()
	}
}

impl KV {
	pub fn new() -> Self {
		Self {
			applied_index: 0,
			data: BTreeMap::new(),
		}
	}

	pub fn get(&self, key: &str) -> Option<&String> {
		self.data.get(key)
	}

	pub fn data(&self) -> &BTreeMap<String, String> {
		&self.data
	}
}

impl State for KV {
	fn get_applied_index(&self) -> Index {
		self.applied_index
	}

	fn as_any(&self) -> &dyn Any {
		self
	}

	fn apply(&mut self, entry: &Entry) {
		match &entry.command {
			Command::WriteMulti {
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
			Command::WriteSingle {
				..
			} => {}
			Command::Noop => {}
		}
		self.applied_index = entry.index;
	}
}
