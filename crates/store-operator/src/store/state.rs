// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, ops::Bound};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
};
use tracing::instrument;

use crate::{
	store::{OperatorStore, StandardOperatorStore},
	tier::{commit::batch::DropMarker, range::BucketId},
	types::{BufferedState, OperatorBatch, OperatorWrite},
};

impl StandardOperatorStore {
	#[instrument(name = "store::operator::set", level = "debug", skip(self, key, row), fields(operator = operator.0, key_len = key.len()))]
	pub fn set(&self, operator: OperatorId, key: EncodedKey, row: EncodedPodRow) {
		self.commit.record_state_set(operator, key.clone(), row);
		self.invalidate_read(operator, &key);
	}

	#[instrument(name = "store::operator::remove", level = "debug", skip(self, key), fields(operator = operator.0, key_len = key.len()))]
	pub fn remove(&self, operator: OperatorId, key: &EncodedKey) {
		self.commit.record_state_remove(operator, key.clone());
		self.invalidate_read(operator, key);
	}

	#[instrument(name = "store::operator::apply_batch", level = "debug", skip(self, writes), fields(write_count = writes.len()))]
	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		self.commit.apply_batch(writes);
		self.invalidate_read_batch(writes);
	}

	#[instrument(name = "store::operator::apply_batch_with_checkpoints", level = "debug", skip(self, writes, checkpoints, checkpoint_deletes), fields(write_count = writes.len(), checkpoint_count = checkpoints.len()))]
	pub fn apply_batch_with_checkpoints(
		&self,
		writes: &[OperatorWrite],
		checkpoints: &[(FlowId, CommitVersion)],
		checkpoint_deletes: &[FlowId],
	) {
		self.commit.apply_batch_with_checkpoints(writes, checkpoints, checkpoint_deletes);
		self.invalidate_read_batch(writes);
	}

	#[instrument(name = "store::operator::drop_operator_state", level = "debug", skip(self), fields(operator = operator.0))]
	pub fn drop_operator_state(&self, operator: OperatorId) {
		self.commit.record_drop(DropMarker::OperatorState(operator));
		if let Some(range) = self.range.as_ref() {
			range.invalidate_operator(operator);
		}
		if let Some(point) = self.point.as_ref() {
			point.invalidate_operator(operator);
		}
	}

	fn invalidate_read(&self, operator: OperatorId, key: &EncodedKey) {
		if let Some(range) = self.range.as_ref() {
			range.invalidate(operator, key);
		}
		if let Some(point) = self.point.as_ref() {
			point.invalidate(operator, key);
		}
	}

	fn repair_absence(&self, operator: OperatorId, key: &EncodedKey, row: &EncodedPodRow) {
		if let Some(point) = self.point.as_ref() {
			point.overwrite(operator, key.clone(), row.clone());
		}
	}

	fn invalidate_read_batch(&self, writes: &[OperatorWrite]) {
		if self.point.is_none() && self.range.is_none() {
			return;
		}
		for write in writes {
			match write {
				OperatorWrite::Set {
					operator,
					key,
					..
				}
				| OperatorWrite::Remove {
					operator,
					key,
				} => self.invalidate_read(*operator, key),
				OperatorWrite::AnchorSet {
					..
				}
				| OperatorWrite::AnchorRemove {
					..
				} => {}
			}
		}
	}

	#[instrument(name = "store::operator::get", level = "trace", skip(self, key), fields(operator = operator.0, key_len = key.len()))]
	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedPodRow> {
		match self.commit.lookup_state(operator, key) {
			BufferedState::Row(row) => Some(row),
			BufferedState::Tombstone | BufferedState::Dropped => None,
			BufferedState::Absent => self.persistent_get(operator, key),
		}
	}

	fn persistent_get(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedPodRow> {
		let persistent = self.persistent.as_ref()?;
		let cached = self.point.as_ref().and_then(|point| point.get(operator, key));
		if let Some(Some(row)) = cached {
			return Some(row);
		}
		if let Some(authoritative) = self.range.as_ref().and_then(|range| range.lookup(operator, key)) {
			if let (Some(None), Some(row)) = (&cached, authoritative.as_ref()) {
				self.repair_absence(operator, key, row);
			}
			return authoritative;
		}
		if cached.is_some() {
			return None;
		}
		if !persistent.filter().may_contain(operator, key) {
			return None;
		}
		match self.point.as_ref() {
			Some(point) if point.begin_fill(operator, key) => {
				let row = persistent.get(operator, key);
				point.finish_fill(operator, key.clone(), row.clone());
				row
			}
			_ => persistent.get(operator, key),
		}
	}

	#[instrument(name = "store::operator::contains", level = "trace", skip(self, key), fields(operator = operator.0, key_len = key.len()), ret)]
	pub fn contains(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		match self.commit.lookup_state(operator, key) {
			BufferedState::Row(_) => true,
			BufferedState::Tombstone | BufferedState::Dropped => false,
			BufferedState::Absent => self.persistent_contains(operator, key),
		}
	}

	fn persistent_contains(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		let Some(persistent) = self.persistent.as_ref() else {
			return false;
		};
		let cached = self.point.as_ref().and_then(|point| point.contains(operator, key));
		if cached == Some(true) {
			return true;
		}
		if let Some(authoritative) = self.range.as_ref().and_then(|range| range.lookup(operator, key)) {
			if let (Some(false), Some(row)) = (&cached, authoritative.as_ref()) {
				self.repair_absence(operator, key, row);
			}
			return authoritative.is_some();
		}
		if cached.is_some() {
			return false;
		}
		if !persistent.filter().may_contain(operator, key) {
			return false;
		}
		match self.point.as_ref() {
			Some(point) if point.begin_fill(operator, key) => {
				let row = persistent.get(operator, key);
				point.finish_fill(operator, key.clone(), row.clone());
				row.is_some()
			}
			_ => persistent.contains(operator, key),
		}
	}

	#[instrument(name = "store::operator::range_batch", level = "trace", skip(self, range), fields(operator = operator.0, batch_size = batch_size))]
	pub fn range_batch(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64) -> OperatorBatch {
		let limit = batch_size.max(1);
		let target = (limit as usize).saturating_add(1);
		let snapshot = self.commit.state_range(operator, range.start.as_ref(), range.end.as_ref());
		let buffered = snapshot.items;
		let mut exhausted = snapshot.dropped;
		let mut lower = range.start.clone();
		let mut page: Vec<(EncodedKey, EncodedPodRow)> = Vec::new();
		let mut page_index = 0usize;
		let mut buffer_index = 0usize;
		let mut items: Vec<(EncodedKey, EncodedPodRow)> = Vec::new();

		let range_tier = self.range.as_ref();
		let mut fill: Option<BucketId> = None;
		let mut scanned_all = false;
		if !exhausted {
			let served = range_tier
				.and_then(|tier| tier.range(operator, &range, target.saturating_add(buffered.len())));
			match served {
				Some(rows) => {
					page = rows;
					exhausted = true;
				}
				None => fill = range_tier.and_then(|tier| tier.begin_fill(operator, &range)),
			}
		}

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
				if let (Some(tier), Some(bucket)) = (range_tier, fill)
					&& !tier.extend_fill(bucket, &page)
				{
					fill = None;
				}
				scanned_all = exhausted;
				match page.last() {
					Some((key, _)) => lower = Bound::Excluded(key.clone()),
					None => {
						exhausted = true;
						scanned_all = true;
					}
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

		if let (Some(tier), Some(bucket)) = (range_tier, fill) {
			if scanned_all {
				tier.finish_fill(bucket);
			} else {
				tier.abort_fill(bucket);
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
	pub fn set(&self, operator: OperatorId, key: EncodedKey, row: EncodedPodRow) {
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

	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedPodRow> {
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
