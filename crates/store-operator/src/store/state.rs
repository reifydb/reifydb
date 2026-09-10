// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(reifydb_assertions)]
use std::collections::BTreeMap;
use std::{
	cmp::{Ordering, Reverse},
	collections::HashMap,
	ops::Bound,
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
#[cfg(reifydb_assertions)]
use reifydb_core::key::operator::{keyspace::group_scoped_id, state::OperatorStateKey};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::{
		keyspace::dispatch,
		state::{
			GroupId, GroupStateKey, KeyspaceId, group_inner_range, group_inner_range_split,
			keyspace_inner_range_split,
		},
	},
	metrics::scan::record_page,
};
use reifydb_filter::adaptive::FilterMetrics;
use reifydb_value::{byte_size::ByteSize, reifydb_assertions};
use tracing::{instrument, warn};

#[cfg(reifydb_assertions)]
use crate::types::LayeredPre;
use crate::{
	error::{OperatorError, Result},
	persistent::{Fetch, Measure, Page as PersistentPage, Persistent},
	range::RangeSink,
	store::{
		OperatorStore, StandardOperatorStore,
		pager::{
			ExhaustedPager, GroupKeyspacePager, GroupPager, GroupsPager, PageSource, PersistentPager,
			PlanScan, keyspaces_of,
		},
	},
	types::{BufferedRange, BufferedState, DropMarker, OperatorBatch, OperatorWrite, Scan},
};

const SCAN_BUDGET_FACTOR: usize = 16;

impl StandardOperatorStore {
	#[instrument(name = "store::operator::apply_batch", level = "debug", skip(self, writes), fields(write_count = writes.len()))]
	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		reifydb_assertions! {
			self.verify_classification(writes);
			verify_group_scope(writes);
		}
		let _flushing = self.resident.flush_guard();
		self.occupancy.record(writes);
		self.census.record(writes);
		self.resident.apply_batch(writes);
		self.invalidate_read_batch(writes);
	}

	#[instrument(name = "store::operator::apply_batch_with_checkpoints", level = "debug", skip(self, writes, checkpoints, checkpoint_deletes), fields(write_count = writes.len(), checkpoint_count = checkpoints.len()))]
	pub fn apply_batch_with_checkpoints(
		&self,
		writes: &[OperatorWrite],
		checkpoints: &[(FlowId, CommitVersion)],
		checkpoint_deletes: &[FlowId],
	) -> Result<()> {
		reifydb_assertions! {
			self.verify_classification(writes);
			verify_group_scope(writes);
		}
		let _flushing = self.resident.flush_guard();
		for (flow, version) in checkpoints {
			if let Some(current) = self.checkpoint_get(*flow)?
				&& *version < current
			{
				return Err(OperatorError::CheckpointOutOfRange {
					flow: *flow,
				});
			}
		}
		self.occupancy.record(writes);
		self.census.record(writes);
		self.resident.apply_batch_with_checkpoints(writes, checkpoints, checkpoint_deletes);
		self.invalidate_read_batch(writes);
		Ok(())
	}

	#[instrument(name = "store::operator::drop_operator", level = "debug", skip(self), fields(operator = operator.0))]
	pub fn drop_operator(&self, operator: OperatorId) -> Result<()> {
		self.resident.record_drop(DropMarker::OperatorState(operator));
		self.occupancy.forget(operator);
		self.census.forget(operator);
		self.range.invalidate_operator(operator);
		Ok(())
	}

	#[instrument(name = "store::operator::state_write", level = "trace", skip(self, write))]
	pub fn state_write(&self, write: OperatorWrite) -> Result<()> {
		self.apply_batch(&[write]);
		Ok(())
	}

	#[cfg(reifydb_assertions)]
	fn verify_classification(&self, writes: &[OperatorWrite]) {
		let mut overlay: BTreeMap<(OperatorId, EncodedKey), Option<ByteSize>> = BTreeMap::new();
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
						LayeredPre::Absent => Some(None),
						LayeredPre::Present(bytes) => Some(Some(*bytes)),
					},
					None,
				),
			};
			let slot = (operator, key.as_encoded().clone());
			let observed = match overlay.get(&slot) {
				Some(pending) => *pending,
				None => self.layered_pre_image(operator, key.as_encoded()).map(|row| value_bytes(&row)),
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
	fn layered_pre_image(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedPodRow> {
		match self.resident.lookup_state(operator, key) {
			BufferedState::Row(row) => Some(row),
			BufferedState::Tombstone | BufferedState::Dropped => None,
			BufferedState::Absent => self
				.durable_get(operator, key)
				.expect("operator durable read failed while verifying write classification"),
		}
	}

	fn overwrite_range_read(&self, operator: OperatorId, key: &EncodedKey, row: &EncodedPodRow) {
		self.range.overwrite(operator, key, row.clone());
	}

	fn insert_range_read(&self, operator: OperatorId, key: &EncodedKey, row: &EncodedPodRow) {
		self.range.insert(operator, key, row.clone());
	}

	fn remove_range_read(&self, operator: OperatorId, key: &EncodedKey) {
		self.range.mark_deleted(operator, key);
	}

	#[instrument(name = "store::operator::invalidate_read_batch", level = "debug", skip_all, fields(write_count = writes.len()))]
	fn invalidate_read_batch(&self, writes: &[OperatorWrite]) {
		if self.range.is_absent() {
			return;
		}
		for write in writes {
			match write {
				OperatorWrite::Replace {
					operator,
					key,
					post,
					..
				} => self.overwrite_range_read(*operator, key.as_encoded(), post),
				OperatorWrite::Insert {
					operator,
					key,
					post,
				} => self.insert_range_read(*operator, key.as_encoded(), post),
				OperatorWrite::Remove {
					operator,
					key,
					..
				} => self.remove_range_read(*operator, key.as_encoded()),
			}
		}
	}

	#[instrument(name = "store::operator::state_sizes", level = "trace", skip(self, probes), fields(probe_count = probes.len()))]
	pub fn state_sizes(&self, probes: &[(OperatorId, GroupStateKey)]) -> Result<Vec<Option<ByteSize>>> {
		let mut sizes: Vec<Option<ByteSize>> = Vec::with_capacity(probes.len());
		let mut residual: HashMap<OperatorId, Vec<(usize, EncodedKey)>> = HashMap::new();
		for (index, (operator, key)) in probes.iter().enumerate() {
			let key = key.as_encoded();
			match self.resolve_size(*operator, key) {
				SizeProbe::Known(size) => sizes.push(size),
				SizeProbe::Persistent => {
					sizes.push(None);
					residual.entry(*operator).or_default().push((index, key.clone()));
				}
			}
		}
		if self.persistent.is_absent() {
			return Ok(sizes);
		}
		for (operator, pending) in residual {
			let keys: Vec<GroupStateKey> =
				pending.iter().map(|(_, key)| GroupStateKey::bound_unchecked(key.clone())).collect();
			let found = self.persistent.state_sizes(operator, &keys)?;
			for (index, key) in pending {
				sizes[index] = found.get(&GroupStateKey::bound_unchecked(key)).copied();
			}
		}
		Ok(sizes)
	}

	fn resolve_size(&self, operator: OperatorId, key: &EncodedKey) -> SizeProbe {
		match self.resident.lookup_state(operator, key) {
			BufferedState::Row(row) => return SizeProbe::Known(Some(row_size(&row))),
			BufferedState::Tombstone | BufferedState::Dropped => return SizeProbe::Known(None),
			BufferedState::Absent => {}
		}
		if self.persistent.is_absent() {
			return SizeProbe::Known(None);
		}
		if let Some(authoritative) = self.range.lookup(operator, key) {
			return SizeProbe::Known(authoritative.as_ref().map(row_size));
		}
		if self.resident.never_persisted(operator, key) {
			return SizeProbe::Known(None);
		}
		SizeProbe::Persistent
	}

	#[instrument(name = "store::operator::state_get", level = "trace", skip(self, key), fields(operator = operator.0, key_len = key.as_slice().len()))]
	pub fn state_get(&self, operator: OperatorId, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		let key = key.as_encoded();
		match self.resident.lookup_state(operator, key) {
			BufferedState::Row(row) => Ok(Some(row)),
			BufferedState::Tombstone | BufferedState::Dropped => Ok(None),
			BufferedState::Absent => self.persistent_get(operator, key),
		}
	}

	#[instrument(name = "store::operator::state_get_many", level = "trace", skip(self, keys, visit), fields(operator = operator.0, key_count = keys.len()))]
	pub fn state_get_many(
		&self,
		operator: OperatorId,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		let mut results: Vec<Option<EncodedPodRow>> = Vec::with_capacity(keys.len());
		let mut buffered: Vec<(usize, &EncodedKey)> = Vec::new();
		for (index, key) in keys.iter().enumerate() {
			let key = key.as_encoded();
			match self.resident.lookup_state(operator, key) {
				BufferedState::Row(row) => results.push(Some(row)),
				BufferedState::Tombstone | BufferedState::Dropped => results.push(None),
				BufferedState::Absent => {
					results.push(None);
					buffered.push((index, key));
				}
			}
		}

		if !self.persistent.is_absent() {
			let mut fetch: Vec<(usize, &EncodedKey)> = Vec::new();
			for (index, key) in buffered {
				if let Some(authoritative) = self.range.lookup(operator, key) {
					results[index] = authoritative;
					continue;
				}
				if self.resident.never_persisted(operator, key) {
					continue;
				}
				fetch.push((index, key));
			}
			if !fetch.is_empty() {
				let batch: Vec<GroupStateKey> = fetch
					.iter()
					.map(|(_, key)| GroupStateKey::bound_unchecked((*key).clone()))
					.collect();
				let found = self.persistent.get_many(operator, &batch)?;
				for (index, key) in fetch {
					results[index] =
						found.get(&GroupStateKey::bound_unchecked(key.clone())).cloned();
				}
			}
		}

		for (key, row) in keys.iter().zip(results) {
			if let Some(row) = row {
				visit(key.clone(), row)?;
			}
		}
		Ok(())
	}

	fn persistent_get(&self, operator: OperatorId, key: &EncodedKey) -> Result<Option<EncodedPodRow>> {
		if self.persistent.is_absent() {
			return Ok(None);
		}
		if let Some(authoritative) = self.range.lookup(operator, key) {
			return Ok(authoritative);
		}
		if self.resident.never_persisted(operator, key) {
			return Ok(None);
		}
		self.durable_get(operator, key)
	}

	fn durable_get(&self, operator: OperatorId, key: &EncodedKey) -> Result<Option<EncodedPodRow>> {
		self.persistent.get(operator, &GroupStateKey::bound_unchecked(key.clone()))
	}

	#[instrument(name = "store::operator::contains", level = "trace", skip(self, key), fields(operator = operator.0, key_len = key.as_slice().len()), ret)]
	pub fn contains(&self, operator: OperatorId, key: &GroupStateKey) -> Result<bool> {
		let key = key.as_encoded();
		match self.resident.lookup_state(operator, key) {
			BufferedState::Row(_) => Ok(true),
			BufferedState::Tombstone | BufferedState::Dropped => Ok(false),
			BufferedState::Absent => self.persistent_contains(operator, key),
		}
	}

	fn persistent_contains(&self, operator: OperatorId, key: &EncodedKey) -> Result<bool> {
		if self.persistent.is_absent() {
			return Ok(false);
		}
		if let Some(authoritative) = self.range.lookup(operator, key) {
			return Ok(authoritative.is_some());
		}
		if self.resident.never_persisted(operator, key) {
			return Ok(false);
		}
		self.persistent.contains(operator, &GroupStateKey::bound_unchecked(key.clone()))
	}

	#[instrument(name = "store::operator::range_batch", level = "trace", skip(self, range), fields(operator = operator.0, batch_size = batch_size))]
	pub fn range_batch(
		&self,
		operator: OperatorId,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> Result<OperatorBatch> {
		let limit = batch_size.max(1);
		let target = (limit as usize).saturating_add(1);
		let mut buffer_lower = range.start.clone();
		let snapshot = self.resident.state_page(operator, buffer_lower.as_ref(), range.end.as_ref(), target);
		let mut buffered = snapshot.items;
		let mut buffer_exhausted = buffered.len() < target;
		if let Some((key, _)) = buffered.last() {
			buffer_lower = Bound::Excluded(key.as_encoded().clone());
		}
		let mut source = self.page_source(operator, &range, snapshot.dropped);
		let mut items: Vec<(GroupStateKey, EncodedPodRow)> = Vec::new();
		let mut buffer_index = 0usize;
		let mut page: Vec<(EncodedKey, EncodedPodRow)> = Vec::new();
		let mut page_shadow: Vec<bool> = Vec::new();
		let mut page_index = 0usize;
		let scan_budget = target.saturating_mul(SCAN_BUDGET_FACTOR);
		let mut consumed = 0usize;
		let mut skipped = 0u64;
		let mut spent = 0usize;
		let mut walked: Option<GroupStateKey> = None;
		let mut resume: Option<GroupStateKey> = None;

		while items.len() < target {
			let scanning = buffer_index < buffered.len()
				|| !buffer_exhausted || page_index < page.len()
				|| !source.is_exhausted();
			if consumed >= scan_budget
				&& scanning && !items.is_empty()
				&& let Some(key) = walked.take()
			{
				resume = Some(key);
				break;
			}
			if buffer_index == buffered.len() && !buffer_exhausted {
				let next = self.resident.state_page(
					operator,
					buffer_lower.as_ref(),
					range.end.as_ref(),
					target,
				);
				buffer_exhausted = next.items.len() < target;
				if let Some((key, _)) = next.items.last() {
					buffer_lower = Bound::Excluded(key.as_encoded().clone());
				}
				buffered = next.items;
				buffer_index = 0;
				continue;
			}
			if page_index == page.len() && !source.is_exhausted() {
				page = source.next_page(target.saturating_add(spent).min(scan_budget) as u64)?;
				page_shadow = self.resident.tombstoned(operator, page.iter().map(|(key, _)| key));
				page_index = 0;
				spent = 0;
				continue;
			}

			match (buffered.get(buffer_index), page.get(page_index)) {
				(None, None) => break,
				(Some((key, entry)), None) => {
					buffer_index += 1;
					consumed += 1;
					if consumed >= scan_budget {
						walked = Some(key.clone());
					}
					if let Some(row) = entry {
						items.push((key.clone(), row.clone()));
					} else {
						skipped += 1;
					}
				}
				(None, Some((key, row))) => {
					let dead = page_shadow.get(page_index).copied().unwrap_or(false);
					page_index += 1;
					consumed += 1;
					if consumed >= scan_budget {
						walked = Some(GroupStateKey::bound_unchecked(key.clone()));
					}
					if !dead {
						items.push((GroupStateKey::bound_unchecked(key.clone()), row.clone()));
					} else {
						spent += 1;
						skipped += 1;
					}
				}
				(Some((buffer_key, entry)), Some((page_key, page_row))) => {
					match buffer_key.as_encoded().cmp(page_key) {
						Ordering::Less => {
							buffer_index += 1;
							consumed += 1;
							if consumed >= scan_budget {
								walked = Some(buffer_key.clone());
							}
							if let Some(row) = entry {
								items.push((buffer_key.clone(), row.clone()));
							} else {
								skipped += 1;
							}
						}
						Ordering::Greater => {
							let dead =
								page_shadow.get(page_index).copied().unwrap_or(false);
							page_index += 1;
							consumed += 1;
							if consumed >= scan_budget {
								walked = Some(GroupStateKey::bound_unchecked(
									page_key.clone(),
								));
							}
							if !dead {
								items.push((
									GroupStateKey::bound_unchecked(
										page_key.clone(),
									),
									page_row.clone(),
								));
							} else {
								spent += 1;
								skipped += 1;
							}
						}
						Ordering::Equal => {
							buffer_index += 1;
							page_index += 1;
							spent += 1;
							consumed += 2;
							if consumed >= scan_budget {
								walked = Some(buffer_key.clone());
							}
							if let Some(row) = entry {
								items.push((buffer_key.clone(), row.clone()));
							} else {
								skipped += 1;
							}
						}
					}
				}
			}
		}

		record_page(0, skipped);
		if items.len() > limit as usize {
			resume = Some(items[limit as usize].0.clone());
		}
		let has_more = items.len() > limit as usize || resume.is_some();
		items.truncate(limit as usize);
		Ok(OperatorBatch {
			items,
			has_more,
			resume,
		})
	}

	#[instrument(name = "store::operator::group_page", level = "trace", skip(self, groups), fields(operator = operator.0, group_count = groups.len(), batch_size = batch_size))]
	pub fn group_page(&self, operator: OperatorId, groups: &[GroupId], batch_size: u64) -> Result<OperatorBatch> {
		let limit = batch_size.max(1);
		let target = (limit as usize).saturating_add(1);
		let mut ordered: Vec<GroupId> = groups.to_vec();
		ordered.sort_by_key(|group| Reverse(*group.as_bytes()));
		ordered.dedup();

		let mut buffer = GroupBuffer::new(self, operator, &ordered, target);
		buffer.peek();
		let mask = self.occupancy.mask(operator, || self.occupied_keyspaces(operator));
		let mut source: Box<dyn PageSource + '_> = match self.range.tiers() {
			Some(tiers) => Box::new(GroupsPager::new(
				tiers,
				operator,
				&self.persistent,
				&ordered,
				mask,
				buffer.dropped,
			)),
			None => Box::new(GroupPager::new(operator, &self.persistent, &ordered, mask, buffer.dropped)),
		};

		let mut items: Vec<(GroupStateKey, EncodedPodRow)> = Vec::new();
		let mut page: Vec<(EncodedKey, EncodedPodRow)> = Vec::new();
		let mut page_shadow: Vec<bool> = Vec::new();
		let mut page_index = 0usize;
		let mut skipped = 0u64;
		let mut resume: Option<GroupStateKey> = None;

		while items.len() < target {
			if page_index == page.len() && !source.is_exhausted() {
				page = source.next_page(target as u64)?;
				page_shadow = self.resident.tombstoned(operator, page.iter().map(|(key, _)| key));
				page_index = 0;
				continue;
			}
			let buffered = buffer.peek().cloned();
			match (buffered, page.get(page_index)) {
				(None, None) => break,
				(Some((key, entry)), None) => {
					if source.ceiling().is_some_and(|ceiling| key.as_slice() > ceiling.as_slice()) {
						resume = Some(key);
						break;
					}
					buffer.bump();
					if let Some(row) = entry {
						items.push((key, row));
					}
				}
				(None, Some((key, row))) => {
					let dead = page_shadow.get(page_index).copied().unwrap_or(false);
					page_index += 1;
					if !dead {
						items.push((GroupStateKey::bound_unchecked(key.clone()), row.clone()));
					} else {
						skipped += 1;
					}
				}
				(Some((buffer_key, entry)), Some((page_key, page_row))) => {
					match buffer_key.as_encoded().cmp(page_key) {
						Ordering::Less => {
							buffer.bump();
							if let Some(row) = entry {
								items.push((buffer_key, row));
							}
						}
						Ordering::Greater => {
							let dead =
								page_shadow.get(page_index).copied().unwrap_or(false);
							page_index += 1;
							if !dead {
								items.push((
									GroupStateKey::bound_unchecked(
										page_key.clone(),
									),
									page_row.clone(),
								));
							} else {
								skipped += 1;
							}
						}
						Ordering::Equal => {
							buffer.bump();
							page_index += 1;
							if let Some(row) = entry {
								items.push((buffer_key, row));
							}
						}
					}
				}
			}
		}

		record_page(0, skipped);
		if items.len() > limit as usize {
			resume = Some(items[limit as usize].0.clone());
		}
		let has_more = items.len() > limit as usize || resume.is_some();
		items.truncate(limit as usize);
		Ok(OperatorBatch {
			items,
			has_more,
			resume,
		})
	}

	fn occupied_keyspaces(&self, operator: OperatorId) -> Vec<KeyspaceId> {
		self.persistent.occupied_keyspaces(operator)
	}

	fn page_source<'a>(
		&'a self,
		operator: OperatorId,
		range: &EncodedKeyRange,
		dropped: bool,
	) -> Box<dyn PageSource + 'a> {
		if dropped {
			return Box::new(ExhaustedPager);
		}
		let persistent = &self.persistent;
		let Some(tiers) = self.range.tiers() else {
			return Box::new(PersistentPager::new(operator, persistent, range));
		};
		let Some((group, keyspace, start, end)) = keyspace_inner_range_split(range) else {
			let Some(group) = group_inner_range_split(range) else {
				return Box::new(PersistentPager::new(operator, persistent, range));
			};
			return Box::new(GroupKeyspacePager::new(
				tiers,
				operator,
				group,
				persistent,
				keyspaces_of(
					group,
					range,
					self.occupancy.mask(operator, || self.occupied_keyspaces(operator)),
				),
			));
		};
		dispatch(
			keyspace,
			PlanScan {
				tiers,
				operator,
				group,
				persistent,
				start,
				end,
			},
		)
		.flatten()
		.unwrap_or_else(|| Box::new(PersistentPager::new(operator, persistent, range)))
	}

	#[instrument(name = "store::operator::state_page", level = "trace", skip(self, range), fields(operator = operator.0))]
	pub fn state_page(
		&self,
		operator: OperatorId,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		Ok(self.range_batch(operator, range, limit.unwrap_or(usize::MAX) as u64)?.items)
	}

	#[instrument(name = "store::operator::state_range", level = "trace", skip(self), fields(operator = operator.0, limit = limit))]
	pub fn state_range(
		&self,
		operator: OperatorId,
		group: GroupId,
		scan: Scan,
		limit: usize,
	) -> Result<BufferedRange> {
		let range = group_inner_range(group);
		let (start, end) = (range.start.as_ref(), range.end.as_ref());
		Ok(match scan {
			Scan::Forward => self.resident.state_page(operator, start, end, limit),
			Scan::Backward => self.resident.state_last_page(operator, start, end, limit),
		})
	}

	#[instrument(name = "store::operator::state_last_iter", level = "trace", skip(self, range), fields(operator = operator.0))]
	pub fn state_last_iter(&self, operator: OperatorId, range: EncodedKeyRange) -> StateLastIter<'_> {
		let first = self.resident.state_last_page(
			operator,
			range.start.as_ref(),
			range.end.as_ref(),
			STATE_LAST_PAGE,
		);
		let stored_done = first.dropped || self.persistent.is_absent();
		let buffer_done = first.items.len() < STATE_LAST_PAGE;
		let mut buffer_end = range.end.clone();
		if let Some((key, _)) = first.items.last() {
			buffer_end = Bound::Excluded(key.as_encoded().clone());
		}

		StateLastIter {
			store: self,
			operator,
			start: range.start,
			buffer: first.items,
			buffer_index: 0,
			buffer_end,
			buffer_done,
			stored: Vec::new(),
			stored_shadow: Vec::new(),
			stored_index: 0,
			stored_end: range.end,
			stored_done,
			failed: false,
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
	) -> Result<()> {
		match self {
			Self::Standard(store) => {
				store.apply_batch_with_checkpoints(writes, checkpoints, checkpoint_deletes)
			}
		}
	}

	pub fn drop_operator(&self, operator: OperatorId) -> Result<()> {
		match self {
			Self::Standard(store) => store.drop_operator(operator),
		}
	}

	pub fn state_write(&self, write: OperatorWrite) -> Result<()> {
		match self {
			Self::Standard(store) => store.state_write(write),
		}
	}

	pub fn state_get(&self, operator: OperatorId, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		match self {
			Self::Standard(store) => store.state_get(operator, key),
		}
	}

	pub fn state_sizes(&self, probes: &[(OperatorId, GroupStateKey)]) -> Result<Vec<Option<ByteSize>>> {
		match self {
			Self::Standard(store) => store.state_sizes(probes),
		}
	}

	pub fn filter_metrics(&self) -> FilterMetrics {
		match self {
			Self::Standard(store) => store.filter_metrics(),
		}
	}

	pub fn contains(&self, operator: OperatorId, key: &GroupStateKey) -> Result<bool> {
		match self {
			Self::Standard(store) => store.contains(operator, key),
		}
	}

	pub fn state_get_many(
		&self,
		operator: OperatorId,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		match self {
			Self::Standard(store) => store.state_get_many(operator, keys, visit),
		}
	}

	pub fn range_batch(
		&self,
		operator: OperatorId,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> Result<OperatorBatch> {
		match self {
			Self::Standard(store) => store.range_batch(operator, range, batch_size),
		}
	}

	pub fn group_page(&self, operator: OperatorId, groups: &[GroupId], batch_size: u64) -> Result<OperatorBatch> {
		match self {
			Self::Standard(store) => store.group_page(operator, groups, batch_size),
		}
	}

	pub fn state_page(
		&self,
		operator: OperatorId,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		match self {
			Self::Standard(store) => store.state_page(operator, range, limit),
		}
	}

	pub fn state_range(
		&self,
		operator: OperatorId,
		group: GroupId,
		scan: Scan,
		limit: usize,
	) -> Result<BufferedRange> {
		match self {
			Self::Standard(store) => store.state_range(operator, group, scan, limit),
		}
	}

	pub fn state_last_iter(&self, operator: OperatorId, range: EncodedKeyRange) -> StateLastIter<'_> {
		match self {
			Self::Standard(store) => store.state_last_iter(operator, range),
		}
	}
}

#[cfg(reifydb_assertions)]
fn value_bytes(row: &EncodedPodRow) -> ByteSize {
	ByteSize::from_bytes(row.bytes().len() as u64)
}

#[cfg(reifydb_assertions)]
fn verify_group_scope(writes: &[OperatorWrite]) {
	for write in writes {
		let key = match write {
			OperatorWrite::Insert {
				key,
				..
			}
			| OperatorWrite::Replace {
				key,
				..
			}
			| OperatorWrite::Remove {
				key,
				..
			} => key,
		};
		let Some((group, keyspace, _)) = OperatorStateKey::decode_inner(key.as_slice()) else {
			continue;
		};
		if group == GroupId::ROOT {
			continue;
		}
		assert!(
			group_scoped_id(keyspace).unwrap_or(true),
			"{} is not group-scoped but was written at group {group}: the persistent tier rebuilds the \
			 key through the typed layout, so this row reads back stamped ROOT and collides with every \
			 other group holding the same suffix",
			keyspace.name()
		);
	}
}

pub const STATE_LAST_PAGE: usize = 64;

enum SizeProbe {
	Known(Option<ByteSize>),
	Persistent,
}

struct GroupBuffer<'a> {
	store: &'a StandardOperatorStore,
	operator: OperatorId,
	groups: &'a [GroupId],
	next: usize,
	lower: Bound<EncodedKey>,
	end: Bound<EncodedKey>,
	items: Vec<(GroupStateKey, Option<EncodedPodRow>)>,
	at: usize,
	drained: bool,
	dropped: bool,
	target: usize,
}

impl<'a> GroupBuffer<'a> {
	fn new(store: &'a StandardOperatorStore, operator: OperatorId, groups: &'a [GroupId], target: usize) -> Self {
		Self {
			store,
			operator,
			groups,
			next: 0,
			lower: Bound::Unbounded,
			end: Bound::Unbounded,
			items: Vec::new(),
			at: 0,
			drained: true,
			dropped: false,
			target,
		}
	}

	fn open(&mut self) -> bool {
		let Some(group) = self.groups.get(self.next) else {
			return false;
		};
		self.next += 1;
		let range = group_inner_range(*group);
		self.lower = range.start;
		self.end = range.end;
		self.items = Vec::new();
		self.at = 0;
		self.drained = false;
		true
	}

	fn fill(&mut self) {
		let page = self.store.resident.state_page(
			self.operator,
			self.lower.as_ref(),
			self.end.as_ref(),
			self.target,
		);
		self.dropped |= page.dropped;
		self.drained = page.items.len() < self.target;
		if let Some((key, _)) = page.items.last() {
			self.lower = Bound::Excluded(key.as_encoded().clone());
		}
		self.items = page.items;
		self.at = 0;
	}

	fn peek(&mut self) -> Option<&(GroupStateKey, Option<EncodedPodRow>)> {
		loop {
			if self.at < self.items.len() {
				return self.items.get(self.at);
			}
			if !self.drained {
				self.fill();
				continue;
			}
			if !self.open() {
				return None;
			}
		}
	}

	fn bump(&mut self) {
		self.at += 1;
	}
}

fn row_size(row: &EncodedPodRow) -> ByteSize {
	ByteSize::from_bytes(row.bytes().len() as u64)
}

pub struct StateLastIter<'a> {
	store: &'a StandardOperatorStore,
	operator: OperatorId,
	start: Bound<EncodedKey>,
	buffer: Vec<(GroupStateKey, Option<EncodedPodRow>)>,
	buffer_index: usize,
	buffer_end: Bound<EncodedKey>,
	buffer_done: bool,
	stored: Vec<(EncodedKey, EncodedPodRow)>,
	stored_shadow: Vec<bool>,
	stored_index: usize,
	stored_end: Bound<EncodedKey>,
	stored_done: bool,
	failed: bool,
}

impl Iterator for StateLastIter<'_> {
	type Item = Result<(EncodedKey, EncodedPodRow)>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.failed {
			return None;
		}
		loop {
			if self.buffer_index == self.buffer.len() && !self.buffer_done {
				let page = self.store.resident.state_last_page(
					self.operator,
					self.start.as_ref(),
					self.buffer_end.as_ref(),
					STATE_LAST_PAGE,
				);
				self.buffer = page.items;
				self.buffer_index = 0;
				self.buffer_done = self.buffer.len() < STATE_LAST_PAGE;
				if let Some((key, _)) = self.buffer.last() {
					self.buffer_end = Bound::Excluded(key.as_encoded().clone());
				}
			}
			if self.stored_index == self.stored.len() && !self.stored_done {
				let batch = match self.store.persistent.last_batch(
					self.operator,
					EncodedKeyRange::new(self.start.clone(), self.stored_end.clone()),
					STATE_LAST_PAGE as u64,
					u64::MAX,
				) {
					Ok(batch) => batch,
					Err(error) => {
						self.failed = true;
						return Some(Err(error));
					}
				};
				self.stored_done = !batch.has_more;
				self.stored =
					batch.items.into_iter().map(|(key, row)| (key.into_encoded(), row)).collect();
				self.stored_shadow = self
					.store
					.resident
					.tombstoned(self.operator, self.stored.iter().map(|(key, _)| key));
				self.stored_index = 0;
				if let Some((key, _)) = self.stored.last() {
					self.stored_end = Bound::Excluded(key.clone());
				}
			}

			let buffered = self.buffer.get(self.buffer_index).cloned();
			let stored = self.stored.get(self.stored_index).cloned();
			match (buffered, stored) {
				(None, None) => return None,
				(Some((key, entry)), None) => {
					self.buffer_index += 1;
					if let Some(row) = entry {
						return Some(Ok((key.into_encoded(), row)));
					}
				}
				(None, Some((key, row))) => {
					let dead = self.stored_shadow.get(self.stored_index).copied().unwrap_or(false);
					self.stored_index += 1;
					if !dead {
						return Some(Ok((key, row)));
					}
				}
				(Some((buffer_key, entry)), Some((stored_key, stored_row))) => {
					match buffer_key.as_encoded().cmp(&stored_key) {
						Ordering::Greater => {
							self.buffer_index += 1;
							if let Some(row) = entry {
								return Some(Ok((buffer_key.into_encoded(), row)));
							}
						}
						Ordering::Less => {
							let dead = self
								.stored_shadow
								.get(self.stored_index)
								.copied()
								.unwrap_or(false);
							self.stored_index += 1;
							if !dead {
								return Some(Ok((stored_key, stored_row)));
							}
						}
						Ordering::Equal => {
							self.buffer_index += 1;
							self.stored_index += 1;
							if let Some(row) = entry {
								return Some(Ok((buffer_key.into_encoded(), row)));
							}
						}
					}
				}
			}
		}
	}
}
