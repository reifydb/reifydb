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
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::{
		keyspace::dispatch,
		state::{GroupId, group_inner_range, keyspace_inner_range_split},
	},
};
use reifydb_value::{byte_size::ByteSize, reifydb_assertions};
use tracing::instrument;

#[cfg(reifydb_assertions)]
use crate::types::DurablePre;
use crate::{
	store::{
		OperatorStore, StandardOperatorStore,
		pager::{ExhaustedPager, GroupPager, PageSource, PersistentPager, PlanScan},
	},
	tier::resident::batch::DropMarker,
	types::{BufferedState, OperatorBatch, OperatorWrite},
};

const SCAN_BUDGET_FACTOR: usize = 16;

impl StandardOperatorStore {
	#[instrument(name = "store::operator::apply_batch", level = "debug", skip(self, writes), fields(write_count = writes.len()))]
	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		reifydb_assertions! {
			self.verify_classification(writes);
		}
		let _flushing = self.resident.flush_guard();
		self.resident.apply_batch(writes);
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
		let _flushing = self.resident.flush_guard();
		self.resident.apply_batch_with_checkpoints(writes, checkpoints, checkpoint_deletes);
		self.invalidate_read_batch(writes);
	}

	#[instrument(name = "store::operator::drop_operator_state", level = "debug", skip(self), fields(operator = operator.0))]
	pub fn drop_operator_state(&self, operator: OperatorId) {
		self.resident.record_drop(DropMarker::OperatorState(operator));
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
		match self.resident.lookup_state(operator, key) {
			BufferedState::Row(row) => Some(row),
			BufferedState::Tombstone | BufferedState::Dropped => None,
			BufferedState::Absent => self.persistent.as_ref()?.get(operator, key),
		}
	}

	fn overwrite_range_read(&self, operator: OperatorId, key: &EncodedKey, row: &EncodedPodRow) {
		if let Some(range) = self.range.as_ref() {
			range.overwrite(operator, key, row.clone());
		}
		if let Some(point) = self.point.as_ref() {
			point.invalidate(operator, key);
		}
	}

	fn insert_range_read(&self, operator: OperatorId, key: &EncodedKey, row: &EncodedPodRow) {
		if let Some(range) = self.range.as_ref() {
			range.insert(operator, key, row.clone());
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
			point.overwrite(operator, key, row.clone());
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
		match self.resident.lookup_state(operator, key) {
			BufferedState::Row(row) => return SizeProbe::Known(Some(row_size(&row))),
			BufferedState::Tombstone | BufferedState::Dropped => return SizeProbe::Known(None),
			BufferedState::Absent => {}
		}
		if self.persistent.is_none() {
			return SizeProbe::Known(None);
		}
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
		SizeProbe::Persistent
	}

	#[instrument(name = "store::operator::get", level = "trace", skip(self, key), fields(operator = operator.0, key_len = key.len()))]
	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedPodRow> {
		match self.resident.lookup_state(operator, key) {
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
			match self.resident.lookup_state(operator, key) {
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
				point.finish_fill(operator, key, row.clone());
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
		match self.point.as_ref() {
			Some(point) if point.begin_fill(operator, key) => {
				let row = persistent.get(operator, key);
				point.finish_fill(operator, key, row.clone());
				row
			}
			_ => persistent.get(operator, key),
		}
	}

	#[instrument(name = "store::operator::contains", level = "trace", skip(self, key), fields(operator = operator.0, key_len = key.len()), ret)]
	pub fn contains(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		match self.resident.lookup_state(operator, key) {
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
		match self.point.as_ref() {
			Some(point) if point.begin_fill(operator, key) => {
				let row = persistent.get(operator, key);
				point.finish_fill(operator, key, row.clone());
				row.is_some()
			}
			_ => persistent.contains(operator, key),
		}
	}

	#[instrument(name = "store::operator::range_batch", level = "trace", skip(self, range), fields(operator = operator.0, batch_size = batch_size))]
	pub fn range_batch(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64) -> OperatorBatch {
		let limit = batch_size.max(1);
		let target = (limit as usize).saturating_add(1);
		let mut buffer_lower = range.start.clone();
		let snapshot = self.resident.state_page(operator, buffer_lower.as_ref(), range.end.as_ref(), target);
		let mut buffered = snapshot.items;
		let mut buffer_exhausted = buffered.len() < target;
		if let Some((key, _)) = buffered.last() {
			buffer_lower = Bound::Excluded(key.clone());
		}
		let mut source = self.page_source(operator, &range, snapshot.dropped);
		let mut items: Vec<(EncodedKey, EncodedPodRow)> = Vec::new();
		let mut buffer_index = 0usize;
		let mut page: Vec<(EncodedKey, EncodedPodRow)> = Vec::new();
		let mut page_index = 0usize;
		let scan_budget = target.saturating_mul(SCAN_BUDGET_FACTOR);
		let mut consumed = 0usize;
		let mut walked: Option<EncodedKey> = None;
		let mut resume: Option<EncodedKey> = None;

		while items.len() < target {
			if consumed >= scan_budget
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
					buffer_lower = Bound::Excluded(key.clone());
				}
				buffered = next.items;
				buffer_index = 0;
				continue;
			}
			if page_index == page.len() && !source.is_exhausted() {
				page = source.next_page(limit);
				page_index = 0;
				continue;
			}

			match (buffered.get(buffer_index), page.get(page_index)) {
				(None, None) => break,
				(Some((key, entry)), None) => {
					buffer_index += 1;
					consumed += 1;
					walked = Some(key.clone());
					if let Some(row) = entry {
						items.push((key.clone(), row.clone()));
					}
				}
				(None, Some((key, row))) => {
					page_index += 1;
					consumed += 1;
					walked = Some(key.clone());
					items.push((key.clone(), row.clone()));
				}
				(Some((buffer_key, entry)), Some((page_key, page_row))) => {
					match buffer_key.cmp(page_key) {
						Ordering::Less => {
							buffer_index += 1;
							consumed += 1;
							walked = Some(buffer_key.clone());
							if let Some(row) = entry {
								items.push((buffer_key.clone(), row.clone()));
							}
						}
						Ordering::Greater => {
							page_index += 1;
							consumed += 1;
							walked = Some(page_key.clone());
							items.push((page_key.clone(), page_row.clone()));
						}
						Ordering::Equal => {
							buffer_index += 1;
							page_index += 1;
							consumed += 2;
							walked = Some(buffer_key.clone());
							if let Some(row) = entry {
								items.push((buffer_key.clone(), row.clone()));
							}
						}
					}
				}
			}
		}

		let has_more = items.len() > limit as usize || resume.is_some();
		items.truncate(limit as usize);
		OperatorBatch {
			items,
			has_more,
			resume,
		}
	}

	#[instrument(name = "store::operator::group_page", level = "trace", skip(self, groups), fields(operator = operator.0, group_count = groups.len(), batch_size = batch_size))]
	pub fn group_page(&self, operator: OperatorId, groups: &[GroupId], batch_size: u64) -> OperatorBatch {
		let limit = batch_size.max(1);
		let target = (limit as usize).saturating_add(1);
		let mut ordered: Vec<GroupId> = groups.to_vec();
		ordered.sort_by_key(|group| Reverse(*group.as_bytes()));
		ordered.dedup();

		let mut buffer = GroupBuffer::new(self, operator, &ordered, target);
		buffer.peek();
		let persistent = self.persistent.as_ref().filter(|_| !buffer.dropped);
		let mut source = GroupPager::new(operator, persistent, &ordered);

		let mut items: Vec<(EncodedKey, EncodedPodRow)> = Vec::new();
		let mut page: Vec<(EncodedKey, EncodedPodRow)> = Vec::new();
		let mut page_index = 0usize;

		while items.len() < target {
			if page_index == page.len() && !source.is_exhausted() {
				page = source.next_page(target as u64);
				page_index = 0;
				continue;
			}
			let buffered = buffer.peek().cloned();
			match (buffered, page.get(page_index)) {
				(None, None) => break,
				(Some((key, entry)), None) => {
					if source.ceiling().is_some_and(|ceiling| key.as_slice() > ceiling.as_slice()) {
						break;
					}
					buffer.bump();
					if let Some(row) = entry {
						items.push((key, row));
					}
				}
				(None, Some((key, row))) => {
					page_index += 1;
					items.push((key.clone(), row.clone()));
				}
				(Some((buffer_key, entry)), Some((page_key, page_row))) => {
					match buffer_key.cmp(page_key) {
						Ordering::Less => {
							buffer.bump();
							if let Some(row) = entry {
								items.push((buffer_key, row));
							}
						}
						Ordering::Greater => {
							page_index += 1;
							items.push((page_key.clone(), page_row.clone()));
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

		let has_more = items.len() > limit as usize || source.ceiling().is_some();
		items.truncate(limit as usize);
		OperatorBatch {
			items,
			has_more,
			resume: None,
		}
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
		let persistent = self.persistent.as_ref();
		let Some((group, keyspace, start, end)) = keyspace_inner_range_split(range) else {
			return Box::new(PersistentPager::new(operator, persistent, range));
		};
		let Some(tiers) = self.range.as_ref() else {
			return Box::new(PersistentPager::new(operator, persistent, range));
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

	#[instrument(name = "store::operator::state_last_iter", level = "trace", skip(self, range), fields(operator = operator.0))]
	pub fn state_last_iter(&self, operator: OperatorId, range: EncodedKeyRange) -> StateLastIter<'_> {
		let first = self.resident.state_last_page(
			operator,
			range.start.as_ref(),
			range.end.as_ref(),
			STATE_LAST_PAGE,
		);
		let stored_done = first.dropped || self.persistent.is_none();
		let buffer_done = first.items.len() < STATE_LAST_PAGE;
		let mut buffer_end = range.end.clone();
		if let Some((key, _)) = first.items.last() {
			buffer_end = Bound::Excluded(key.clone());
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
			stored_index: 0,
			stored_end: range.end,
			stored_done,
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

	pub fn group_page(&self, operator: OperatorId, groups: &[GroupId], batch_size: u64) -> OperatorBatch {
		match self {
			Self::Standard(store) => store.group_page(operator, groups, batch_size),
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

const STATE_LAST_PAGE: usize = 64;

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
	items: Vec<(EncodedKey, Option<EncodedPodRow>)>,
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
			self.lower = Bound::Excluded(key.clone());
		}
		self.items = page.items;
		self.at = 0;
	}

	fn peek(&mut self) -> Option<&(EncodedKey, Option<EncodedPodRow>)> {
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
	buffer: Vec<(EncodedKey, Option<EncodedPodRow>)>,
	buffer_index: usize,
	buffer_end: Bound<EncodedKey>,
	buffer_done: bool,
	stored: Vec<(EncodedKey, EncodedPodRow)>,
	stored_index: usize,
	stored_end: Bound<EncodedKey>,
	stored_done: bool,
}

impl Iterator for StateLastIter<'_> {
	type Item = (EncodedKey, EncodedPodRow);

	fn next(&mut self) -> Option<Self::Item> {
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
					self.buffer_end = Bound::Excluded(key.clone());
				}
			}
			if self.stored_index == self.stored.len() && !self.stored_done {
				let persistent =
					self.store.persistent.as_ref().expect(
						"a store without a persistent tier never reaches a stored page",
					);
				let batch = persistent.last_batch(
					self.operator,
					EncodedKeyRange::new(self.start.clone(), self.stored_end.clone()),
					STATE_LAST_PAGE as u64,
				);
				self.stored_done = !batch.has_more;
				self.stored = batch.items;
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
						return Some((key, row));
					}
				}
				(None, Some((key, row))) => {
					self.stored_index += 1;
					return Some((key, row));
				}
				(Some((buffer_key, entry)), Some((stored_key, stored_row))) => {
					match buffer_key.cmp(&stored_key) {
						Ordering::Greater => {
							self.buffer_index += 1;
							if let Some(row) = entry {
								return Some((buffer_key, row));
							}
						}
						Ordering::Less => {
							self.stored_index += 1;
							return Some((stored_key, stored_row));
						}
						Ordering::Equal => {
							self.buffer_index += 1;
							self.stored_index += 1;
							if let Some(row) = entry {
								return Some((buffer_key, row));
							}
						}
					}
				}
			}
		}
	}
}
