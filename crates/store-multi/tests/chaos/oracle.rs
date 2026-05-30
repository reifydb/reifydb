// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! The reference model: ground truth for what every read must return.

use std::{collections::BTreeMap, ops::Bound};

use reifydb_core::{common::CommitVersion, key::row::RowKey};
use reifydb_store_multi::MultiVersionScope;

use crate::SHAPE;

#[derive(Clone, Copy, Debug)]
pub enum Scope {
	AsOf {
		read: u64,
	},
	Between {
		after: u64,
		read: u64,
	},
}

impl Scope {
	pub fn store(self) -> MultiVersionScope {
		match self {
			Scope::AsOf {
				read,
			} => MultiVersionScope::AsOf {
				read: CommitVersion(read),
			},
			Scope::Between {
				after,
				read,
			} => MultiVersionScope::Between {
				after: CommitVersion(after),
				read: CommitVersion(read),
			},
		}
	}
}

/// Every committed version of every row, including tombstones (None). Only consulted at versions >= the
/// watermark, where the store is guaranteed to still retain the visible version.
#[derive(Default)]
pub struct Oracle {
	history: BTreeMap<u64, BTreeMap<u64, Option<Vec<u8>>>>,
}

impl Oracle {
	pub fn apply(&mut self, version: u64, deltas: &[(u64, Option<Vec<u8>>)]) {
		for (row, value) in deltas {
			self.history.entry(*row).or_default().insert(version, value.clone());
		}
	}

	/// Physically remove a key (TTL eviction or explicit drop): the whole history is gone, so it reads
	/// absent at every version until re-committed. Used by the lifecycle scenario, not the base workload.
	pub fn remove_key(&mut self, row: u64) {
		self.history.remove(&row);
	}

	/// The (value, version) a store must return for `row` under `scope`, or None if the visible version is
	/// a tombstone or no version qualifies.
	pub fn resolve(&self, row: u64, scope: Scope) -> Option<(Vec<u8>, u64)> {
		let versions = self.history.get(&row)?;
		let (version, value) = match scope {
			Scope::AsOf {
				read,
			} => versions.range(..=read).next_back()?,
			Scope::Between {
				after,
				read,
			} => versions.range((Bound::Excluded(after), Bound::Included(read))).next_back()?,
		};
		value.clone().map(|bytes| (bytes, *version))
	}

	/// The full ordered scan result: every present row as (encoded-key bytes, value, version), sorted by
	/// encoded-key bytes ascending (forward) or descending (reverse) - matching the store's key order.
	pub fn scan(&self, scope: Scope, reverse: bool) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
		let mut rows: Vec<(Vec<u8>, Vec<u8>, u64)> = self
			.history
			.keys()
			.filter_map(|&row| {
				self.resolve(row, scope)
					.map(|(value, version)| (RowKey::encoded(SHAPE, row).to_vec(), value, version))
			})
			.collect();
		rows.sort_by(|a, b| a.0.cmp(&b.0));
		if reverse {
			rows.reverse();
		}
		rows
	}
}
