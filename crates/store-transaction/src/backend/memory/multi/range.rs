// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::{BTreeMap, HashMap, btree_map::Range};

use parking_lot::RwLockReadGuard;
use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange, Result,
	interface::{FlowNodeId, MultiVersionValues, SourceId},
};

use super::{StorageType, classify_range};
use crate::backend::{
	memory::{MemoryBackend, VersionChain},
	multi::BackendMultiVersionRange,
	result::MultiVersionIterResult,
};

impl BackendMultiVersionRange for MemoryBackend {
	type RangeIter<'a>
		= MultiVersionRangeIter<'a>
	where
		Self: 'a;

	fn range_batched(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		_batch_size: u64,
	) -> Result<Self::RangeIter<'_>> {
		// Classify the range to determine which storage to use
		match classify_range(&range) {
			Some(StorageType::Source(source_id)) => {
				let sources_guard = self.sources.read();
				// SAFETY: We use transmute to extend the lifetime of the iterator
				// The guard ensures the map stays valid for the iterator's lifetime
				let iter_kind = if let Some(table) = sources_guard.get(&source_id) {
					let iter = unsafe { std::mem::transmute(table.range(range.clone())) };
					RangeIterKind::Source {
						iter,
					}
				} else {
					// Source doesn't exist yet, return empty iterator
					RangeIterKind::Empty
				};

				Ok(MultiVersionRangeIter {
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
					RangeIterKind::Operator {
						iter,
					}
				} else {
					// Operator doesn't exist yet, return empty iterator
					RangeIterKind::Empty
				};

				Ok(MultiVersionRangeIter {
					_sources_guard: None,
					_operators_guard: Some(operators_guard),
					_multi_guard: None,
					iter_kind,
					version,
				})
			}
			Some(StorageType::Multi) | None => {
				// Use multi storage for unclassified ranges or ranges that span multiple storages
				let multi_guard = self.multi.read();
				let iter = unsafe { std::mem::transmute((*multi_guard).range(range)) };
				Ok(MultiVersionRangeIter {
					_sources_guard: None,
					_operators_guard: None,
					_multi_guard: Some(multi_guard),
					iter_kind: RangeIterKind::Multi {
						iter,
					},
					version,
				})
			}
		}
	}
}

pub(crate) enum RangeIterKind<'a> {
	Source {
		iter: Range<'a, EncodedKey, VersionChain>,
	},
	Operator {
		iter: Range<'a, EncodedKey, VersionChain>,
	},
	Multi {
		iter: Range<'a, EncodedKey, VersionChain>,
	},
	Empty,
}

pub struct MultiVersionRangeIter<'a> {
	pub(crate) _sources_guard: Option<RwLockReadGuard<'a, HashMap<SourceId, BTreeMap<EncodedKey, VersionChain>>>>,
	pub(crate) _operators_guard:
		Option<RwLockReadGuard<'a, HashMap<FlowNodeId, BTreeMap<EncodedKey, VersionChain>>>>,
	pub(crate) _multi_guard: Option<RwLockReadGuard<'a, BTreeMap<EncodedKey, VersionChain>>>,
	pub(crate) iter_kind: RangeIterKind<'a>,
	pub(crate) version: CommitVersion,
}

// SAFETY: We need to manually implement Send for the iterator
// The guard ensures the map stays valid for the iterator's lifetime
unsafe impl<'a> Send for MultiVersionRangeIter<'a> {}

impl Iterator for MultiVersionRangeIter<'_> {
	type Item = MultiVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		match &mut self.iter_kind {
			RangeIterKind::Source {
				iter,
			}
			| RangeIterKind::Operator {
				iter,
			}
			| RangeIterKind::Multi {
				iter,
			} => {
				while let Some((key, chain)) = iter.next() {
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
			RangeIterKind::Empty => None,
		}
	}
}
