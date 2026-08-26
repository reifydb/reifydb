// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(reifydb_assertions)]
use std::collections::BTreeMap;
use std::{cmp::Ordering, collections::HashMap, ops::Bound};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
#[cfg(reifydb_assertions)]
use reifydb_core::key::operator_state::GroupId;
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
};
use reifydb_store::coverage::{
	ExclusiveUpperEnd,
	cursor::{RangeCursor, ServedChunk},
	interval::Interval,
	plan::Segment,
	successor,
};
#[cfg(reifydb_assertions)]
use reifydb_value::value::row_number::RowNumber;
use reifydb_value::{byte_size::ByteSize, reifydb_assertions};
use tracing::instrument;

#[cfg(reifydb_assertions)]
use crate::types::DurablePre;
use crate::{
	store::{OperatorStore, StandardOperatorStore},
	tier::{
		commit::batch::DropMarker,
		range::{Materialize, proven_span, scan_range},
	},
	types::{BufferedState, OperatorBatch, OperatorWrite},
};

impl StandardOperatorStore {
	#[instrument(name = "store::operator::apply_batch", level = "debug", skip(self, writes), fields(write_count = writes.len()))]
	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		reifydb_assertions! {
			self.verify_classification(writes);
		}
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
		reifydb_assertions! {
			self.verify_classification(writes);
		}
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

	#[cfg(reifydb_assertions)]
	fn verify_classification(&self, writes: &[OperatorWrite]) {
		let mut overlay: BTreeMap<(OperatorId, EncodedKey), Option<ByteSize>> = BTreeMap::new();
		let mut anchors: BTreeMap<(OperatorId, GroupId, u8, RowNumber), bool> = BTreeMap::new();
		for write in writes {
			let (operator, key, claimed, post) = match write {
				OperatorWrite::Insert {
					operator,
					key,
					post,
				} => (*operator, key, Some(None), Some(post)),
				OperatorWrite::Replace {
					operator,
					key,
					pre_value_bytes,
					post,
				} => (*operator, key, Some(Some(*pre_value_bytes)), Some(post)),
				OperatorWrite::Remove {
					operator,
					key,
					pre,
				} => (
					*operator,
					key,
					match pre {
						DurablePre::Absent => Some(None),
						DurablePre::Present(bytes) => Some(Some(*bytes)),
					},
					None,
				),
				OperatorWrite::AnchorInsert {
					operator,
					group,
					side,
					row_num,
					..
				} => {
					self.verify_anchor_claim(
						&mut anchors,
						*operator,
						*group,
						*side,
						*row_num,
						false,
					);
					continue;
				}
				OperatorWrite::AnchorReplace {
					operator,
					group,
					side,
					row_num,
					..
				} => {
					self.verify_anchor_claim(
						&mut anchors,
						*operator,
						*group,
						*side,
						*row_num,
						true,
					);
					continue;
				}
				OperatorWrite::AnchorRemove {
					operator,
					group,
					side,
					row_num,
				} => {
					anchors.insert((*operator, *group, *side, *row_num), false);
					continue;
				}
			};
			let slot = (operator, key.clone());
			let observed = match overlay.get(&slot) {
				Some(pending) => *pending,
				None => self.durable_pre_image(operator, key).map(|row| value_bytes(&row)),
			};
			if let Some(claimed) = claimed {
				assert_eq!(
					claimed, observed,
					"operator {} classified a write against a pre-image the store does not hold; the \
					 census is delta arithmetic over that claim, so a wrong one drifts the bucket \
					 until the next restart",
					operator.0
				);
			}
			overlay.insert(slot, post.map(value_bytes));
		}
	}

	#[cfg(reifydb_assertions)]
	fn durable_pre_image(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedPodRow> {
		match self.commit.lookup_state(operator, key) {
			BufferedState::Row(row) => Some(row),
			BufferedState::Tombstone | BufferedState::Dropped => None,
			BufferedState::Absent => self.persistent.as_ref()?.get(operator, key),
		}
	}

	#[cfg(reifydb_assertions)]
	fn verify_anchor_claim(
		&self,
		overlay: &mut BTreeMap<(OperatorId, GroupId, u8, RowNumber), bool>,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_num: RowNumber,
		claimed: bool,
	) {
		let slot = (operator, group, side, row_num);
		let observed = match overlay.get(&slot) {
			Some(pending) => *pending,
			None => self.anchor_get(operator, group, side, row_num).is_some(),
		};
		assert_eq!(
			claimed, observed,
			"operator {} classified an anchor write against a slot the store does not hold; the census \
			 never bills anchors, but the unclassified anchor write is removed on the strength of these \
			 claims, so a wrong one leaves a caller no variant that describes what it did",
			operator.0
		);
		overlay.insert(slot, true);
	}

	fn overwrite_range_read(&self, operator: OperatorId, key: &EncodedKey, row: &EncodedPodRow) {
		if let Some(range) = self.range.as_ref() {
			range.overwrite(operator, key.clone(), row.clone());
		}
		if let Some(point) = self.point.as_ref() {
			point.invalidate(operator, key);
		}
	}

	fn insert_range_read(&self, operator: OperatorId, key: &EncodedKey, row: &EncodedPodRow) {
		if let Some(range) = self.range.as_ref() {
			range.insert(operator, key.clone(), row.clone());
		}
		if let Some(point) = self.point.as_ref() {
			point.invalidate(operator, key);
		}
	}

	fn remove_range_read(&self, operator: OperatorId, key: &EncodedKey) {
		if let Some(range) = self.range.as_ref() {
			range.mark_deleted(operator, key);
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
				OperatorWrite::Replace {
					operator,
					key,
					post,
					..
				} => self.overwrite_range_read(*operator, key, post),
				OperatorWrite::Insert {
					operator,
					key,
					post,
				} => self.insert_range_read(*operator, key, post),
				OperatorWrite::Remove {
					operator,
					key,
					..
				} => self.remove_range_read(*operator, key),
				OperatorWrite::AnchorInsert {
					..
				}
				| OperatorWrite::AnchorReplace {
					..
				}
				| OperatorWrite::AnchorRemove {
					..
				} => {}
			}
		}
	}

	#[instrument(name = "store::operator::state_sizes", level = "trace", skip(self, probes), fields(probe_count = probes.len()))]
	pub fn state_sizes(&self, probes: &[(OperatorId, EncodedKey)]) -> Vec<Option<ByteSize>> {
		let mut sizes: Vec<Option<ByteSize>> = Vec::with_capacity(probes.len());
		let mut residual: HashMap<OperatorId, Vec<(usize, EncodedKey)>> = HashMap::new();
		for (index, (operator, key)) in probes.iter().enumerate() {
			match self.resolve_size(*operator, key) {
				SizeProbe::Known(size) => sizes.push(size),
				SizeProbe::Persistent => {
					sizes.push(None);
					residual.entry(*operator).or_default().push((index, key.clone()));
				}
			}
		}
		let Some(persistent) = self.persistent.as_ref() else {
			return sizes;
		};
		for (operator, pending) in residual {
			let keys: Vec<EncodedKey> = pending.iter().map(|(_, key)| key.clone()).collect();
			let found = persistent.state_sizes(operator, &keys);
			for (index, key) in pending {
				sizes[index] = found.get(&key).copied();
			}
		}
		sizes
	}

	fn resolve_size(&self, operator: OperatorId, key: &EncodedKey) -> SizeProbe {
		match self.commit.lookup_state(operator, key) {
			BufferedState::Row(row) => return SizeProbe::Known(Some(row_size(&row))),
			BufferedState::Tombstone | BufferedState::Dropped => return SizeProbe::Known(None),
			BufferedState::Absent => {}
		}
		let Some(persistent) = self.persistent.as_ref() else {
			return SizeProbe::Known(None);
		};
		let cached = self.point.as_ref().and_then(|point| point.get(operator, key));
		if let Some(Some(row)) = &cached {
			return SizeProbe::Known(Some(row_size(row)));
		}
		if let Some(authoritative) = self.range.as_ref().and_then(|range| range.lookup(operator, key)) {
			return SizeProbe::Known(authoritative.as_ref().map(row_size));
		}
		if cached.is_some() {
			return SizeProbe::Known(None);
		}
		if !persistent.filter().may_contain((operator, key)) {
			return SizeProbe::Known(None);
		}
		SizeProbe::Persistent
	}

	#[instrument(name = "store::operator::get", level = "trace", skip(self, key), fields(operator = operator.0, key_len = key.len()))]
	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedPodRow> {
		match self.commit.lookup_state(operator, key) {
			BufferedState::Row(row) => Some(row),
			BufferedState::Tombstone | BufferedState::Dropped => None,
			BufferedState::Absent => self.persistent_get(operator, key),
		}
	}

	#[instrument(name = "store::operator::get_many", level = "trace", skip(self, keys), fields(operator = operator.0, key_count = keys.len()))]
	pub fn get_many(&self, operator: OperatorId, keys: &[EncodedKey]) -> Vec<Option<EncodedPodRow>> {
		let mut results: Vec<Option<EncodedPodRow>> = Vec::with_capacity(keys.len());
		let mut buffered: Vec<(usize, &EncodedKey)> = Vec::new();
		for (index, key) in keys.iter().enumerate() {
			match self.commit.lookup_state(operator, key) {
				BufferedState::Row(row) => results.push(Some(row)),
				BufferedState::Tombstone | BufferedState::Dropped => results.push(None),
				BufferedState::Absent => {
					results.push(None);
					buffered.push((index, key));
				}
			}
		}
		let Some(persistent) = self.persistent.as_ref() else {
			return results;
		};

		let mut fetch: Vec<(usize, &EncodedKey)> = Vec::new();
		for (index, key) in buffered {
			let cached = self.point.as_ref().and_then(|point| point.get(operator, key));
			if let Some(Some(row)) = cached {
				results[index] = Some(row);
				continue;
			}
			if let Some(authoritative) = self.range.as_ref().and_then(|range| range.lookup(operator, key)) {
				if let (Some(None), Some(row)) = (&cached, authoritative.as_ref()) {
					self.repair_absence(operator, key, row);
				}
				results[index] = authoritative;
				continue;
			}
			if cached.is_some() {
				continue;
			}
			if !persistent.filter().may_contain((operator, key)) {
				continue;
			}
			fetch.push((index, key));
		}
		if fetch.is_empty() {
			return results;
		}

		let filling: Vec<bool> = fetch
			.iter()
			.map(|(_, key)| self.point.as_ref().is_some_and(|point| point.begin_fill(operator, key)))
			.collect();
		let batch: Vec<EncodedKey> = fetch.iter().map(|(_, key)| (*key).clone()).collect();
		let found = persistent.get_many(operator, &batch);
		for ((index, key), filling) in fetch.into_iter().zip(filling) {
			let row = found.get(key).cloned();
			if filling && let Some(point) = self.point.as_ref() {
				point.finish_fill(operator, key.clone(), row.clone());
			}
			results[index] = row;
		}
		results
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
		if !persistent.filter().may_contain((operator, key)) {
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
		if !persistent.filter().may_contain((operator, key)) {
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
		let mut items: Vec<(EncodedKey, EncodedPodRow)> = Vec::new();
		let mut buffer_index = 0usize;
		let mut page: Vec<(EncodedKey, EncodedPodRow)> = Vec::new();
		let mut page_index = 0usize;
		let mut lower = range.start.clone();

		let tier = self.range.as_ref();
		let scan = if exhausted {
			None
		} else {
			tier.and_then(|tier| tier.plan_scan(operator, &range))
		};
		let mut segment_index = 0usize;
		let mut cursor = RangeCursor::new();
		let mut pending: Option<(Interval, bool, usize)> = None;
		let mut claim_start: Option<EncodedKey> = None;
		let mut materializing = true;

		while items.len() < target {
			if page_index == page.len() && !exhausted {
				page = Vec::new();
				page_index = 0;

				match (tier, scan.as_ref()) {
					(Some(tier), Some(scan)) => loop {
						if let Some((interval, materializable, consumed)) = pending.take() {
							let Some(persistent) = self.persistent.as_ref() else {
								exhausted = true;
								break;
							};
							let from = match cursor.last_key() {
								Some(key) => Bound::Excluded(key.clone()),
								None => Bound::Included(interval.start.clone()),
							};
							let read = scan_range(&interval);
							let batch = persistent.range_batch(
								operator,
								EncodedKeyRange::new(from, read.end),
								limit,
							);
							let complete = !batch.has_more || batch.items.is_empty();

							if materializable && materializing {
								let start = claim_start
									.clone()
									.unwrap_or_else(|| interval.start.clone());
								let span = Interval::new(start, interval.end.clone());
								let last = batch.items.last().map(|(key, _)| key);
								if let Some(proven) = proven_span(&span, last, complete)
								{
									match tier.materialize(
										scan,
										&proven,
										&batch.items,
									) {
										Materialize::Materialized
										| Materialize::NothingCacheable => {
											claim_start = batch
												.items
												.last()
												.map(|(key, _)| {
													successor(key)
												});
										}
										Materialize::Refused => {
											materializing = false
										}
									}
								}
							}

							if let Some((key, _)) = batch.items.last() {
								cursor.advance(key.clone());
							}
							if complete {
								segment_index += consumed;
								cursor.reset();
								claim_start = None;
							} else {
								pending = Some((interval, materializable, consumed));
							}

							if batch.items.is_empty() {
								continue;
							}
							page = batch.items;
							break;
						}

						let Some(segment) = scan.segments().get(segment_index) else {
							exhausted = true;
							break;
						};
						match segment {
							Segment::Resident(interval) => {
								match tier.serve(
									scan,
									interval,
									&mut cursor,
									limit as usize,
								) {
									ServedChunk::Served(rows) => {
										let done = cursor.is_exhausted();
										assert!(
											done || !rows.is_empty(),
											"a served chunk that reports more work must carry a row, or the cursor never advances"
										);
										if done {
											segment_index += 1;
											cursor.reset();
										}
										if rows.is_empty() {
											continue;
										}
										page = rows;
										break;
									}
									ServedChunk::Gap => {
										pending = Some((
											interval.clone(),
											false,
											1,
										));
									}
								}
							}
							Segment::Gap {
								interval,
								..
							} => {
								let mut span = interval.clone();
								let mut consumed = 1usize;
								while let Some(Segment::Gap {
									interval: next,
									..
								}) = scan.segments().get(segment_index + consumed)
								{
									if span.end
										!= ExclusiveUpperEnd::Key(
											next.start.clone(),
										) {
										break;
									}
									span.end = next.end.clone();
									consumed += 1;
								}
								pending = Some((span, true, consumed));
							}
						}
					},
					_ => {
						let Some(persistent) = self.persistent.as_ref() else {
							exhausted = true;
							continue;
						};
						let batch = persistent.range_batch(
							operator,
							EncodedKeyRange::new(lower.clone(), range.end.clone()),
							limit,
						);
						exhausted = !batch.has_more || batch.items.is_empty();
						if let Some((key, _)) = batch.items.last() {
							lower = Bound::Excluded(key.clone());
						}
						page = batch.items;
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

		let has_more = items.len() > limit as usize;
		items.truncate(limit as usize);
		OperatorBatch {
			items,
			has_more,
		}
	}
}

impl OperatorStore {
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

	pub fn state_sizes(&self, probes: &[(OperatorId, EncodedKey)]) -> Vec<Option<ByteSize>> {
		match self {
			Self::Standard(store) => store.state_sizes(probes),
		}
	}

	pub fn contains(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		match self {
			Self::Standard(store) => store.contains(operator, key),
		}
	}

	pub fn get_many(&self, operator: OperatorId, keys: &[EncodedKey]) -> Vec<Option<EncodedPodRow>> {
		match self {
			Self::Standard(store) => store.get_many(operator, keys),
		}
	}

	pub fn range_batch(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64) -> OperatorBatch {
		match self {
			Self::Standard(store) => store.range_batch(operator, range, batch_size),
		}
	}
}

#[cfg(reifydb_assertions)]
fn value_bytes(row: &EncodedPodRow) -> ByteSize {
	ByteSize::from_bytes(row.bytes().len() as u64)
}

enum SizeProbe {
	Known(Option<ByteSize>),
	Persistent,
}

fn row_size(row: &EncodedPodRow) -> ByteSize {
	ByteSize::from_bytes(row.bytes().len() as u64)
}
