// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, ops::Bound};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::operator::EncodedOperatorRow,
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
};

use crate::{
	commit::batch::DropMarker,
	store::{OperatorStore, StandardOperatorStore},
	types::{OperatorBatch, OperatorWrite},
};

impl StandardOperatorStore {
	pub fn set(&self, operator: OperatorId, key: EncodedKey, row: EncodedOperatorRow) {
		self.commit.record_state_set(operator, key, row);
	}

	pub fn remove(&self, operator: OperatorId, key: &EncodedKey) {
		self.commit.record_state_remove(operator, key.clone());
	}

	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		self.commit.apply_batch(writes);
	}

	pub fn apply_batch_with_checkpoints(
		&self,
		writes: &[OperatorWrite],
		checkpoints: &[(FlowId, CommitVersion)],
		checkpoint_deletes: &[FlowId],
	) {
		self.commit.apply_batch_with_checkpoints(writes, checkpoints, checkpoint_deletes);
	}

	pub fn drop_operator_state(&self, operator: OperatorId) {
		self.commit.record_drop(DropMarker::OperatorState(operator));
	}

	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedOperatorRow> {
		if let Some(entry) = self.commit.lookup_state(operator, key) {
			return entry;
		}
		if self.commit.has_pending_state_drop(operator) {
			return None;
		}
		self.persistent.as_ref()?.get(operator, key)
	}

	pub fn contains(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		if let Some(entry) = self.commit.lookup_state(operator, key) {
			return entry.is_some();
		}
		if self.commit.has_pending_state_drop(operator) {
			return false;
		}
		self.persistent.as_ref().is_some_and(|persistent| persistent.contains(operator, key))
	}

	pub fn range_batch(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64) -> OperatorBatch {
		let limit = batch_size.max(1);
		let target = (limit as usize).saturating_add(1);
		let buffered = self.commit.state_range(operator, range.start.as_ref(), range.end.as_ref());
		let mut exhausted = self.commit.has_pending_state_drop(operator);
		let mut lower = range.start.clone();
		let mut page: Vec<(EncodedKey, EncodedOperatorRow)> = Vec::new();
		let mut page_index = 0usize;
		let mut buffer_index = 0usize;
		let mut items: Vec<(EncodedKey, EncodedOperatorRow)> = Vec::new();

		while items.len() < target {
			if page_index == page.len() && !exhausted {
				let Some(persistent) = self.persistent.as_ref() else {
					exhausted = true;
					continue;
				};
				let batch = persistent.range_batch(
					operator,
					EncodedKeyRange::new(lower.clone(), range.end.clone()),
					limit,
				);
				exhausted = !batch.has_more;
				page = batch.items;
				page_index = 0;
				match page.last() {
					Some((key, _)) => lower = Bound::Excluded(key.clone()),
					None => exhausted = true,
				}
				continue;
			}

			match (buffered.get(buffer_index), page.get(page_index)) {
				(None, None) => break,
				(Some((key, entry)), None) => {
					buffer_index += 1;
					if let Some(row) = entry {
						items.push((key.clone(), row.clone()));
					}
				}
				(None, Some((key, row))) => {
					page_index += 1;
					items.push((key.clone(), row.clone()));
				}
				(Some((buffer_key, entry)), Some((page_key, page_row))) => {
					match buffer_key.cmp(page_key) {
						Ordering::Less => {
							buffer_index += 1;
							if let Some(row) = entry {
								items.push((buffer_key.clone(), row.clone()));
							}
						}
						Ordering::Greater => {
							page_index += 1;
							items.push((page_key.clone(), page_row.clone()));
						}
						Ordering::Equal => {
							buffer_index += 1;
							page_index += 1;
							if let Some(row) = entry {
								items.push((buffer_key.clone(), row.clone()));
							}
						}
					}
				}
			}
		}

		let has_more = items.len() > limit as usize;
		items.truncate(limit as usize);
		OperatorBatch {
			items,
			has_more,
		}
	}
}

impl OperatorStore {
	pub fn set(&self, operator: OperatorId, key: EncodedKey, row: EncodedOperatorRow) {
		match self {
			Self::Standard(store) => store.set(operator, key, row),
		}
	}

	pub fn remove(&self, operator: OperatorId, key: &EncodedKey) {
		match self {
			Self::Standard(store) => store.remove(operator, key),
		}
	}

	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		match self {
			Self::Standard(store) => store.apply_batch(writes),
		}
	}

	pub fn apply_batch_with_checkpoints(
		&self,
		writes: &[OperatorWrite],
		checkpoints: &[(FlowId, CommitVersion)],
		checkpoint_deletes: &[FlowId],
	) {
		match self {
			Self::Standard(store) => {
				store.apply_batch_with_checkpoints(writes, checkpoints, checkpoint_deletes)
			}
		}
	}

	pub fn drop_operator_state(&self, operator: OperatorId) {
		match self {
			Self::Standard(store) => store.drop_operator_state(operator),
		}
	}

	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedOperatorRow> {
		match self {
			Self::Standard(store) => store.get(operator, key),
		}
	}

	pub fn contains(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		match self {
			Self::Standard(store) => store.contains(operator, key),
		}
	}

	pub fn range_batch(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64) -> OperatorBatch {
		match self {
			Self::Standard(store) => store.range_batch(operator, range, batch_size),
		}
	}
}
