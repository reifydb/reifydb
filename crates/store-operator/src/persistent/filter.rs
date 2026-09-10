// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Debug, Formatter};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{interface::catalog::flow::OperatorId, key::operator::state::KeyspaceId};
use reifydb_filter::source::{FilterSlice, KeyFilterSource};
use tracing::warn;

use crate::{
	bound::parts,
	persistent::{Enumerate, PersistentTier},
	resident::state_hash,
	types::OperatorStateCensus,
};

pub struct OperatorStateKeySource {
	persistent: PersistentTier,
	pending: Vec<(OperatorId, KeyspaceId)>,
	cursor: Option<EncodedKey>,
	started: bool,
}

impl OperatorStateKeySource {
	pub fn new(persistent: PersistentTier) -> Self {
		Self {
			persistent,
			pending: Vec::new(),
			cursor: None,
			started: false,
		}
	}

	fn occupied(&self) -> Vec<OperatorStateCensus> {
		match self.persistent.census() {
			Ok(entries) => entries.into_iter().filter(|entry| entry.keys > 0).collect(),
			Err(error) => {
				warn!(error = %error, "operator census failed; filtering no keyspaces");
				Vec::new()
			}
		}
	}
}

impl Debug for OperatorStateKeySource {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("OperatorStateKeySource").field("remaining", &self.pending.len()).finish()
	}
}

impl KeyFilterSource for OperatorStateKeySource {
	fn name(&self) -> &'static str {
		"operator-state"
	}

	fn estimated_len(&self) -> u64 {
		self.occupied().iter().map(|entry| entry.keys).sum()
	}

	fn restart(&mut self) {
		self.pending = self.occupied().into_iter().map(|entry| (entry.operator, entry.keyspace)).collect();
		self.cursor = None;
		self.started = true;
	}

	fn next_slice(&mut self, budget: usize) -> FilterSlice {
		if !self.started {
			self.restart();
		}
		let limit = budget.max(1) as u64;
		while let Some((operator, keyspace)) = self.pending.last().copied() {
			let keys = self.persistent.state_keys_after(operator, keyspace, self.cursor.as_ref(), limit);
			if keys.is_empty() {
				self.pending.pop();
				self.cursor = None;
				continue;
			}
			let exhausted = (keys.len() as u64) < limit;
			self.cursor = keys.last().cloned();
			let hashes = keys
				.iter()
				.map(|key| {
					let (group, keyspace, suffix) = parts(key);
					state_hash(operator, keyspace, group, suffix)
				})
				.collect();
			if exhausted {
				self.pending.pop();
				self.cursor = None;
			}
			return FilterSlice {
				hashes,
				exhausted: exhausted && self.pending.is_empty(),
			};
		}
		self.started = false;
		FilterSlice {
			hashes: Vec::new(),
			exhausted: true,
		}
	}
}
