// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::{BTreeMap, HashMap};

use parking_lot::RwLockReadGuard;
use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange, Result,
	interface::{FlowNodeId, MultiVersionValues, SourceId},
};

use super::{StorageType, classify_range};
use crate::backend::{
	memory::{MemoryBackend, VersionChain},
	multi::BackendMultiVersionRangeRev,
	result::MultiVersionIterResult,
};

impl BackendMultiVersionRangeRev for MemoryBackend {
	type RangeIterRev<'a>
		= MultiVersionRangeRevIter<'a>
	where
		Self: 'a;

	fn range_rev_batched(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		_batch_size: u64,
	) -> Result<Self::RangeIterRev<'_>> {
		// Classify the range to determine which storage to use
		match classify_range(&range) {
			Some(StorageType::Source(source_id)) => {
				let sources_guard = self.sources.read();
				let iter_kind = if let Some(table) = sources_guard.get(&source_id) {
					let iter = unsafe { std::mem::transmute(table.range(range.clone())) };
					RangeIterRevKind::Source {
						iter,
					}
				} else {
					RangeIterRevKind::Empty
				};

				Ok(MultiVersionRangeRevIter {
					_sources_guard: Some(sources_guard),
					_operators_guard: None,
					_multi_guard: None,
					iter_kind,
					version,
				})
			}
			Some(StorageType::Operator(flow_node_id)) => {
				let operators_guard = self.operators.read();
				let iter_kind = if let Some(table) = operators_guard.get(&flow_node_id) {
					let iter = unsafe { std::mem::transmute(table.range(range.clone())) };
					RangeIterRevKind::Operator {
						iter,
					}
				} else {
					RangeIterRevKind::Empty
				};

				Ok(MultiVersionRangeRevIter {
					_sources_guard: None,
					_operators_guard: Some(operators_guard),
					_multi_guard: None,
					iter_kind,
					version,
				})
			}
			Some(StorageType::Multi) | None => {
				let multi_guard = self.multi.read();
				let iter = unsafe { std::mem::transmute((*multi_guard).range(range)) };
				Ok(MultiVersionRangeRevIter {
					_sources_guard: None,
					_operators_guard: None,
					_multi_guard: Some(multi_guard),
					iter_kind: RangeIterRevKind::Multi {
						iter,
					},
					version,
				})
			}
		}
	}
}

pub(crate) enum RangeIterRevKind<'a> {
	Source {
		iter: std::collections::btree_map::Range<'a, EncodedKey, VersionChain>,
	},
	Operator {
		iter: std::collections::btree_map::Range<'a, EncodedKey, VersionChain>,
	},
	Multi {
		iter: std::collections::btree_map::Range<'a, EncodedKey, VersionChain>,
	},
	Empty,
}

pub struct MultiVersionRangeRevIter<'a> {
	pub(crate) _sources_guard: Option<RwLockReadGuard<'a, HashMap<SourceId, BTreeMap<EncodedKey, VersionChain>>>>,
	pub(crate) _operators_guard:
		Option<RwLockReadGuard<'a, HashMap<FlowNodeId, BTreeMap<EncodedKey, VersionChain>>>>,
	pub(crate) _multi_guard: Option<RwLockReadGuard<'a, BTreeMap<EncodedKey, VersionChain>>>,
	pub(crate) iter_kind: RangeIterRevKind<'a>,
	pub(crate) version: CommitVersion,
}

// SAFETY: We need to manually implement Send for the iterator
// The guard ensures the map stays valid for the iterator's lifetime
unsafe impl<'a> Send for MultiVersionRangeRevIter<'a> {}

impl Iterator for MultiVersionRangeRevIter<'_> {
	type Item = MultiVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		match &mut self.iter_kind {
			RangeIterRevKind::Source {
				iter,
			}
			| RangeIterRevKind::Operator {
				iter,
			}
			| RangeIterRevKind::Multi {
				iter,
			} => {
				// Use next_back for reverse iteration
				while let Some((key, chain)) = iter.next_back() {
					if let Some(values) = chain.get_at(self.version) {
						return Some(if let Some(vals) = values {
							MultiVersionIterResult::Value(MultiVersionValues {
								key: key.clone(),
								values: vals,
								version: self.version,
							})
						} else {
							// Tombstone
							MultiVersionIterResult::Tombstone {
								key: key.clone(),
								version: self.version,
							}
						});
					}
				}
				None
			}
			RangeIterRevKind::Empty => None,
		}
	}
}
