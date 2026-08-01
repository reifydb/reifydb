// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound, slice::from_ref, sync::Arc};

use dashmap::DashMap;
use reifydb_codec::{
	encoded::row::EncodedRow,
	key::{
		decode_u64_asc, encode_u64_asc,
		encoded::{EncodedKey, EncodedKeyRange},
	},
	state::{OperatorState, StateBytes, decode_state},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator_state::OperatorStateKey,
		operator_group_state::{GroupId, Keyspace, OperatorGroupStateKey, GroupStateKey, keyspace_inner_range},
	},
	metrics::heap::{StateCompleteness, StateMemory},
	state::{
		group::{ActivityBuckets, GroupRecord},
		horizon::{Cutoff, Position, activity_buckets},
		membership::{MEMBERSHIP_BYTE_CAP, MembershipTracker},
	},
};
use reifydb_runtime::cache::slab::SlabLru;
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	count::Count,
	reifydb_assertions,
	util::hash::xxh3_64,
	value::{datetime::DateTime, duration::Duration},
};

use super::FlowTransaction;

const DEFAULT_BYTE_BUDGET: u64 = 1024 * 1024;
const HYDRATE_CHUNK: usize = 8_192;
const DEFAULT_ACTIVITY_BUCKET_WIDTH: u64 = 1 << 20;

fn entry_bytes(key: &EncodedKey) -> u64 {
	SlabLru::<EncodedKey, Interned>::entry_struct_bytes() as u64 + key.heap_bytes() as u64
}

fn membership_hash(key: &EncodedKey) -> u64 {
	xxh3_64(key.as_ref()).0
}

fn dictionary_key(group: &EncodedKey) -> GroupStateKey {
	OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::GROUP_DICTIONARY, group)
}

fn record_key(id: GroupId) -> GroupStateKey {
	OperatorGroupStateKey::inner_encoded(id, Keyspace::GROUP_RECORD, vec![])
}

fn index_key(keyspace: Keyspace, bucket: u64, id: GroupId) -> GroupStateKey {
	let mut suffix = Vec::with_capacity(16);
	suffix.extend_from_slice(&encode_u64_asc(bucket));
	suffix.extend_from_slice(&encode_u64_asc(id.0));
	OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, keyspace, suffix)
}

fn index_bound(keyspace: Keyspace, bucket: u64) -> GroupStateKey {
	OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, keyspace, encode_u64_asc(bucket))
}

fn side_index_key(side: Keyspace, bucket: u64, id: GroupId) -> GroupStateKey {
	let mut suffix = Vec::with_capacity(17);
	suffix.push(side.0);
	suffix.extend_from_slice(&encode_u64_asc(bucket));
	suffix.extend_from_slice(&encode_u64_asc(id.0));
	OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::SIDE_ACTIVITY_INDEX, suffix)
}

fn side_index_bound(side: Keyspace, bucket: u64) -> GroupStateKey {
	let mut suffix = Vec::with_capacity(9);
	suffix.push(side.0);
	suffix.extend_from_slice(&encode_u64_asc(bucket));
	OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::SIDE_ACTIVITY_INDEX, suffix)
}

fn side_record_key(id: GroupId, side: Keyspace) -> GroupStateKey {
	OperatorGroupStateKey::inner_encoded(id, Keyspace::SIDE_ACTIVITY_RECORD, vec![side.0])
}

fn decode_side_suffix(suffix: &[u8]) -> Option<(Keyspace, u64, GroupId)> {
	if suffix.len() != 17 {
		return None;
	}
	let side = Keyspace(suffix[0]);
	let bucket = decode_u64_asc(suffix[1..9].try_into().ok()?);
	let id = decode_u64_asc(suffix[9..].try_into().ok()?);
	Some((side, bucket, GroupId(id)))
}

fn watermark_key() -> GroupStateKey {
	OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::NODE_WATERMARK, vec![])
}

fn decode_activity_suffix(suffix: &[u8]) -> Option<(u64, GroupId)> {
	if suffix.len() != 16 {
		return None;
	}
	let bucket = decode_u64_asc(suffix[..8].try_into().ok()?);
	let id = decode_u64_asc(suffix[8..].try_into().ok()?);
	Some((bucket, GroupId(id)))
}

fn counter_key() -> GroupStateKey {
	OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::NODE_COUNTER, vec![])
}

pub(super) fn encode_payload<T: OperatorState>(value: &T, now: DateTime) -> Result<EncodedRow> {
	Ok(value.encode_state(now)?.into_row())
}

pub(super) fn decode_payload<T: OperatorState>(row: &EncodedRow) -> Result<T> {
	Ok(decode_state(&StateBytes::from_row(row.clone())?)?)
}

#[derive(Clone, Copy)]
struct Interned {
	id: GroupId,
	bucket: u64,
}

struct NodeState {
	cache: SlabLru<EncodedKey, Interned>,
	cache_size: ByteSize,
	membership: MembershipTracker,
	hydrated: bool,
	complete: bool,
	next: Option<u64>,
	revocations: u64,
	position: Option<Position>,
	buckets: Option<ActivityBuckets>,
}

impl Default for NodeState {
	fn default() -> Self {
		Self {
			cache: SlabLru::unbounded(),
			cache_size: ByteSize::ZERO,
			membership: MembershipTracker::new(MEMBERSHIP_BYTE_CAP),
			hydrated: false,
			complete: false,
			next: None,
			revocations: 0,
			position: None,
			buckets: None,
		}
	}
}

impl NodeState {
	fn remember(&mut self, group: &EncodedKey, id: GroupId, bucket: u64) {
		if self.cache
			.put(
				group.clone(),
				Interned {
					id,
					bucket,
				},
			)
			.is_none()
		{
			self.cache_size = self.cache_size.saturating_add(ByteSize::from_bytes(entry_bytes(group)));
		}
	}

	fn forget(&mut self, group: &EncodedKey) -> bool {
		if self.cache.remove(group).is_some() {
			self.cache_size = self.cache_size.saturating_sub(ByteSize::from_bytes(entry_bytes(group)));
			true
		} else {
			false
		}
	}

	fn revoke_complete(&mut self) {
		if self.complete {
			self.complete = false;
			self.revocations += 1;
		}
	}

	fn evict_to_budget(&mut self, budget: ByteSize) {
		while self.cache_size > budget {
			let Some((group, _)) = self.cache.pop_tail() else {
				break;
			};
			self.cache_size = self.cache_size.saturating_sub(ByteSize::from_bytes(entry_bytes(&group)));
			self.revoke_complete();
		}
	}

	fn completeness(&self) -> StateCompleteness {
		if !self.hydrated {
			return StateCompleteness::MERGE_IDENTITY;
		}
		StateCompleteness {
			values_complete: self.complete,
			membership_complete: self.membership.is_tracked(),
			absences_served: Count::new(self.membership.absences_served()),
			false_positives: Count::new(self.membership.false_positives()),
			revocations: Count::new(self.revocations),
		}
	}

	fn memory(&self) -> StateMemory {
		let key_heap: u64 = self.cache.keys().map(|key| key.heap_bytes() as u64).sum();
		let bytes = ByteSize::from_bytes(self.cache.struct_bytes() as u64 + key_heap);
		StateMemory::new(Count::new(self.cache.len() as u64), bytes)
	}
}

pub struct GroupInternerSample {
	pub cache: StateMemory,
	pub membership: StateMemory,
	pub completeness: StateCompleteness,
}

#[derive(Clone)]
pub struct GroupInterner {
	inner: Arc<GroupInternerInner>,
}

struct GroupInternerInner {
	nodes: DashMap<OperatorId, NodeState>,
	budget: ByteSize,
	buckets: ActivityBuckets,
}

impl Default for GroupInterner {
	fn default() -> Self {
		Self::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), DEFAULT_ACTIVITY_BUCKET_WIDTH)
	}
}

impl GroupInterner {
	pub fn new(budget: ByteSize, activity_bucket_width: u64) -> Self {
		Self {
			inner: Arc::new(GroupInternerInner {
				nodes: DashMap::new(),
				budget,
				buckets: ActivityBuckets::undeclared(activity_bucket_width),
			}),
		}
	}

	pub fn set_activity_grid(&self, node: OperatorId, scale: Option<Duration>) {
		let mut state = self.inner.nodes.entry(node).or_default();
		state.buckets = Some(activity_buckets(scale));
	}

	pub fn buckets(&self, node: OperatorId) -> ActivityBuckets {
		self.buckets_of(node)
	}

	fn buckets_of(&self, node: OperatorId) -> ActivityBuckets {
		self.inner.nodes.get(&node).and_then(|state| state.buckets).unwrap_or(self.inner.buckets)
	}

	pub fn intern(
		&self,
		node: OperatorId,
		txn: &mut FlowTransaction,
		group: &EncodedKey,
	) -> Result<(GroupId, bool)> {
		Ok(self.intern_many(node, txn, from_ref(group))?.into_iter().next().unwrap())
	}

	pub fn intern_many(
		&self,
		node: OperatorId,
		txn: &mut FlowTransaction,
		groups: &[EncodedKey],
	) -> Result<Vec<(GroupId, bool)>> {
		let now = txn.clock().now();
		let budget = self.inner.budget;
		let mut guard = self.inner.nodes.entry(node).or_default();
		Self::hydrate_once(&mut guard, node, txn, budget)?;
		let state = &mut *guard;
		let position = Self::stamp_position(txn);

		let buckets = state.buckets.unwrap_or(self.inner.buckets);
		Self::advance_position(state, node, txn, position, buckets, now)?;

		let bucket = buckets.of(position);
		let mut results: Vec<Option<(GroupId, bool)>> = (0..groups.len()).map(|_| None).collect();
		let mut to_resolve: Vec<usize> = Vec::new();
		let mut to_stamp: Vec<(usize, GroupId)> = Vec::new();
		for (i, group) in groups.iter().enumerate() {
			match state.cache.get(group) {
				Some(interned) => {
					if interned.bucket == GroupRecord::RECLAIMED_BUCKET || bucket > interned.bucket
					{
						to_stamp.push((i, interned.id));
					}
					results[i] = Some((interned.id, false));
				}
				None => to_resolve.push(i),
			}
		}
		for (i, id) in to_stamp {
			let effective = Self::stamp(txn, node, id, &groups[i], bucket, now)?;
			state.remember(&groups[i], id, effective);
		}
		if to_resolve.is_empty() {
			state.evict_to_budget(budget);
			return Ok(results.into_iter().map(|r| r.expect("every position filled")).collect());
		}

		let dictionary_keys: Vec<GroupStateKey> = to_resolve.iter().map(|i| dictionary_key(&groups[*i])).collect();

		let mut consulted_store: Vec<bool> = Vec::new();
		let found: HashMap<Vec<u8>, EncodedRow> = if state.complete {
			HashMap::new()
		} else {
			let mut lookup: Vec<GroupStateKey> = Vec::new();
			for (slot, i) in to_resolve.iter().enumerate() {
				let maybe = state.membership.contains(membership_hash(&groups[*i])).unwrap_or(true);
				consulted_store.push(maybe);
				if maybe {
					lookup.push(dictionary_keys[slot].clone());
				} else {
					state.membership.count_absence();
				}
			}
			if lookup.is_empty() {
				HashMap::new()
			} else {
				let batch = txn.state_get_many(node, &lookup)?;
				let mut found = HashMap::with_capacity(batch.items.len());
				for item in batch.items {
					let decoded = OperatorStateKey::decode(&item.key)
						.expect("state_get_many must return OperatorState keys");
					found.insert(decoded.key, item.row);
				}
				found
			}
		};

		let mut resolved_from_store: Vec<(usize, GroupId)> = Vec::new();
		let mut new_slots: Vec<bool> = vec![false; dictionary_keys.len()];
		let mut distinct_new: Vec<usize> = Vec::new();
		let mut first_new_slot: HashMap<Vec<u8>, usize> = HashMap::new();
		for (slot, dictionary) in dictionary_keys.iter().enumerate() {
			let i = to_resolve[slot];
			match found.get(dictionary.as_slice()) {
				Some(existing) => {
					let id = GroupId(decode_payload::<u64>(existing)?);
					resolved_from_store.push((i, id));
					results[i] = Some((id, false));
				}
				None => {
					if consulted_store.get(slot) == Some(&true) {
						state.membership.record_store_miss();
					}
					new_slots[slot] = true;
					if !first_new_slot.contains_key(dictionary.as_slice()) {
						first_new_slot.insert(dictionary.as_slice().to_vec(), slot);
						distinct_new.push(slot);
					}
				}
			}
		}

		if !distinct_new.is_empty() {
			let start = Self::mint(state, node, txn, distinct_new.len() as u64)?;
			let mut assigned: HashMap<Vec<u8>, GroupId> = HashMap::with_capacity(distinct_new.len());
			for (offset, &slot) in distinct_new.iter().enumerate() {
				let i = to_resolve[slot];
				let dictionary = &dictionary_keys[slot];
				let id = GroupId(start + offset as u64);
				txn.state_set(node, dictionary, encode_payload(&id.0, now)?)?;
				Self::stamp(txn, node, id, &groups[i], bucket, now)?;
				state.remember(&groups[i], id, bucket);
				state.membership.insert(membership_hash(&groups[i]));
				assigned.insert(dictionary.as_slice().to_vec(), id);
			}
			for (slot, dictionary) in dictionary_keys.iter().enumerate() {
				if new_slots[slot] {
					let i = to_resolve[slot];
					let id = assigned[dictionary.as_slice()];
					let is_new = first_new_slot.get(dictionary.as_slice()) == Some(&slot);
					results[i] = Some((id, is_new));
				}
			}
		}

		for (i, id) in resolved_from_store {
			let effective = Self::stamp(txn, node, id, &groups[i], bucket, now)?;
			state.remember(&groups[i], id, effective);
		}

		state.evict_to_budget(budget);

		Ok(results.into_iter().map(|r| r.expect("every position filled")).collect())
	}

	fn stamp_position(txn: &FlowTransaction) -> Position {
		let coordinate = txn.change_coordinate();
		reifydb_assertions! {
			assert!(
				coordinate.is_some(),
				"an intern ran before the substrate set the change coordinate; operators can \
				 no longer supply a position, so a missing coordinate means the executor \
				 skipped set_change_coordinate for this dispatch"
			);
		}
		Position(coordinate.expect("an intern requires the substrate change coordinate").at)
	}

	fn stamp(
		txn: &mut FlowTransaction,
		node: OperatorId,
		id: GroupId,
		group: &EncodedKey,
		bucket: u64,
		now: DateTime,
	) -> Result<u64> {
		reifydb_assertions! {
			assert!(
				bucket != GroupRecord::RECLAIMED_BUCKET,
				"a live stamp landed on the bucket phase 1 reserves to mark reclaimed data; the group \
				 would read as data-reclaimed while it is being written, and phase 2 would drop the \
				 row-number mapping a live sink row still names (group={id:?})"
			);
		}
		let effective = match Self::load_record(node, txn, id)? {
			Some(record) if record.activity_bucket != GroupRecord::RECLAIMED_BUCKET => {
				record.activity_bucket.max(bucket)
			}
			_ => bucket,
		};
		if effective != bucket {
			return Ok(effective);
		}
		txn.state_set(
			node,
			&record_key(id),
			encode_payload(&GroupRecord::new(group.as_ref().to_vec(), bucket), now)?,
		)?;
		txn.state_set(node, &index_key(Keyspace::ACTIVITY_INDEX, bucket, id), encode_payload(&1u64, now)?)?;
		Ok(bucket)
	}

	fn advance_position(
		state: &mut NodeState,
		node: OperatorId,
		txn: &mut FlowTransaction,
		position: Position,
		buckets: ActivityBuckets,
		now: DateTime,
	) -> Result<()> {
		let persist = match state.position {
			Some(previous) => {
				if position.raw() <= previous.raw() {
					return Ok(());
				}
				buckets.of(position) > buckets.of(previous)
			}
			None => true,
		};
		state.position = Some(position);
		if persist {
			txn.state_set(node, &watermark_key(), encode_payload(&position.raw(), now)?)?;
		}
		Ok(())
	}

	pub fn position(&self, node: OperatorId, txn: &mut FlowTransaction) -> Result<Position> {
		let budget = self.inner.budget;
		let mut guard = self.inner.nodes.entry(node).or_default();
		Self::hydrate_once(&mut guard, node, txn, budget)?;
		Ok(guard.position.unwrap_or(Position::from_raw(0)))
	}

	pub fn defer(&self, node: OperatorId, txn: &mut FlowTransaction, id: GroupId) -> Result<bool> {
		let budget = self.inner.budget;
		let now = txn.clock().now();
		let mut guard = self.inner.nodes.entry(node).or_default();
		Self::hydrate_once(&mut guard, node, txn, budget)?;
		let state = &mut *guard;

		let Some(record) = Self::load_record(node, txn, id)? else {
			return Ok(false);
		};
		if record.is_data_reclaimed() {
			return Ok(true);
		}

		let bucket = record.activity_bucket;
		let group = EncodedKey::new(record.group.clone());
		txn.state_remove(node, &index_key(Keyspace::ACTIVITY_INDEX, bucket, id))?;
		txn.state_set(node, &index_key(Keyspace::IDENTITY_INDEX, bucket, id), encode_payload(&1u64, now)?)?;
		txn.state_set(node, &record_key(id), encode_payload(&GroupRecord::reclaimed(record.group), now)?)?;
		state.remember(&group, id, GroupRecord::RECLAIMED_BUCKET);
		Ok(true)
	}

	pub fn lookup(
		&self,
		node: OperatorId,
		txn: &mut FlowTransaction,
		group: &EncodedKey,
	) -> Result<Option<GroupId>> {
		let budget = self.inner.budget;
		let mut guard = self.inner.nodes.entry(node).or_default();
		Self::hydrate_once(&mut guard, node, txn, budget)?;
		let state = &mut *guard;

		if let Some(interned) = state.cache.get(group) {
			return Ok(Some(interned.id));
		}
		if state.complete {
			return Ok(None);
		}
		if state.membership.contains(membership_hash(group)) == Some(false) {
			state.membership.count_absence();
			return Ok(None);
		}
		let Some(row) = txn.state_get(node, &dictionary_key(group))? else {
			state.membership.record_store_miss();
			return Ok(None);
		};
		let id = GroupId(decode_payload::<u64>(&row)?);
		let bucket = match txn.state_get(node, &record_key(id))? {
			Some(record) => decode_payload::<GroupRecord>(&record)?.activity_bucket,
			None => 0,
		};
		state.remember(group, id, bucket);
		state.evict_to_budget(budget);
		Ok(Some(id))
	}

	pub fn forget(&self, node: OperatorId, txn: &mut FlowTransaction, group: &EncodedKey) -> Result<bool> {
		let budget = self.inner.budget;
		let mut guard = self.inner.nodes.entry(node).or_default();
		Self::hydrate_once(&mut guard, node, txn, budget)?;
		let state = &mut *guard;

		let cached = state.cache.get(group);
		state.forget(group);
		state.membership.remove(membership_hash(group));
		let existed = cached.is_some() || !state.complete;
		if let Some(interned) = cached
			&& interned.bucket != GroupRecord::RECLAIMED_BUCKET
		{
			txn.state_remove(node, &index_key(Keyspace::ACTIVITY_INDEX, interned.bucket, interned.id))?;
		}
		txn.state_remove(node, &dictionary_key(group))?;
		Ok(existed)
	}

	pub fn due_groups(
		&self,
		node: OperatorId,
		txn: &mut FlowTransaction,
		cutoff: Cutoff,
		limit: usize,
	) -> Result<Vec<GroupId>> {
		self.due_in(node, txn, Keyspace::ACTIVITY_INDEX, cutoff, limit, |record, bucket| {
			record.activity_bucket == bucket
		})
	}

	pub fn stamp_side(
		&self,
		node: OperatorId,
		txn: &mut FlowTransaction,
		id: GroupId,
		side: Keyspace,
	) -> Result<()> {
		reifydb_assertions! {
			assert!(
				side.is_data(),
				"only a data keyspace ages on its own clock; stamping a control keyspace would \
				 enrol the group's identity in a sweep that reclaims rows (side={side:?})"
			);
		}
		let now = txn.clock().now();
		let position = Self::stamp_position(txn);
		let bucket = self.buckets(node).of(position);
		reifydb_assertions! {
			assert!(
				bucket != GroupRecord::RECLAIMED_BUCKET,
				"a live side stamp landed on the bucket reserved to mark reclaimed data; the side \
				 would read as already reclaimed while it is being written (group={id:?}, \
				 side={side:?})"
			);
		}
		let key = side_record_key(id, side);
		if let Some(row) = txn.state_get(node, &key)?
			&& decode_payload::<u64>(&row)? >= bucket
		{
			return Ok(());
		}
		txn.state_set(node, &key, encode_payload(&bucket, now)?)?;
		txn.state_set(node, &side_index_key(side, bucket, id), encode_payload(&1u64, now)?)?;
		Ok(())
	}

	pub fn due_side_groups(
		&self,
		node: OperatorId,
		txn: &mut FlowTransaction,
		side: Keyspace,
		cutoff: Cutoff,
		limit: usize,
	) -> Result<Vec<GroupId>> {
		if limit == 0 {
			return Ok(Vec::new());
		}
		let first_live = self.buckets_of(node).first_live(cutoff);
		let range = EncodedKeyRange::new(
			Bound::Included(side_index_bound(side, 0).into_encoded()),
			Bound::Excluded(side_index_bound(side, first_live).into_encoded()),
		);
		let batch = txn.state_range(node, range, Some(limit))?;

		let mut due = Vec::new();
		let mut stale: Vec<GroupStateKey> = Vec::new();
		for item in &batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_range must return OperatorState keys");
			let inner = OperatorGroupStateKey::decode_inner(&decoded.key)
				.expect("the index range must yield structured operator state keys");
			let Some((_found, bucket, id)) = decode_side_suffix(&inner.2) else {
				continue;
			};
			reifydb_assertions! {
				assert!(
					_found == side,
					"the side index range must only yield entries of the side it scanned; \
					 another side here means the bucket bounds are wrong and reclamation \
					 would drop rows the other side's ttl still covers (wanted={side:?}, \
					 found={_found:?})"
				);
			}
			let current = match txn.state_get(node, &side_record_key(id, side))? {
				Some(row) => Some(decode_payload::<u64>(&row)?),
				None => None,
			};
			match current {
				Some(current) if current == bucket => due.push(id),
				_ => stale.push(GroupStateKey::from_framed(EncodedKey::new(decoded.key.clone()))
					.expect("the index range yields framed inner keys")),
			}
		}
		for key in &stale {
			txn.state_remove(node, key)?;
		}
		Ok(due)
	}

	pub fn forget_side(
		&self,
		node: OperatorId,
		txn: &mut FlowTransaction,
		id: GroupId,
		side: Keyspace,
	) -> Result<()> {
		txn.state_remove(node, &side_record_key(id, side))
	}

	pub fn due_identity_groups(
		&self,
		node: OperatorId,
		txn: &mut FlowTransaction,
		cutoff: Cutoff,
		limit: usize,
	) -> Result<Vec<GroupId>> {
		self.due_in(node, txn, Keyspace::IDENTITY_INDEX, cutoff, limit, |record, _| record.is_data_reclaimed())
	}

	fn due_in(
		&self,
		node: OperatorId,
		txn: &mut FlowTransaction,
		keyspace: Keyspace,
		cutoff: Cutoff,
		limit: usize,
		live: impl Fn(&GroupRecord, u64) -> bool,
	) -> Result<Vec<GroupId>> {
		if limit == 0 {
			return Ok(Vec::new());
		}
		let first_live = self.buckets_of(node).first_live(cutoff);
		let range = EncodedKeyRange::new(
			Bound::Included(index_bound(keyspace, 0).into_encoded()),
			Bound::Excluded(index_bound(keyspace, first_live).into_encoded()),
		);
		let batch = txn.state_range(node, range, Some(limit))?;

		let mut due = Vec::new();
		let mut stale: Vec<GroupStateKey> = Vec::new();
		for item in &batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_range must return OperatorState keys");
			let inner = OperatorGroupStateKey::decode_inner(&decoded.key)
				.expect("the index range must yield structured operator state keys");
			reifydb_assertions! {
				let found = inner.1;
				assert!(
					found == keyspace,
					"the index range scan must only yield keys of the index it scanned; another \
					 keyspace here means the bucket bounds are wrong and reclamation would act on \
					 unrelated rows (wanted={keyspace:?}, found={found:?})"
				);
			}
			let Some((bucket, id)) = decode_activity_suffix(&inner.2) else {
				continue;
			};
			match Self::load_record(node, txn, id)? {
				Some(record) if live(&record, bucket) => due.push(id),
				_ => stale.push(GroupStateKey::from_framed(EncodedKey::new(decoded.key.clone()))
					.expect("the index range yields framed inner keys")),
			}
		}
		for key in &stale {
			txn.state_remove(node, key)?;
		}
		Ok(due)
	}

	fn load_record(node: OperatorId, txn: &mut FlowTransaction, id: GroupId) -> Result<Option<GroupRecord>> {
		let Some(row) = txn.state_get(node, &record_key(id))? else {
			return Ok(None);
		};
		Ok(Some(decode_payload::<GroupRecord>(&row)?))
	}

	pub fn group_bytes(
		&self,
		node: OperatorId,
		txn: &mut FlowTransaction,
		id: GroupId,
	) -> Result<Option<EncodedKey>> {
		let Some(row) = txn.state_get(node, &record_key(id))? else {
			return Ok(None);
		};
		Ok(Some(EncodedKey::new(decode_payload::<GroupRecord>(&row)?.group)))
	}

	pub fn samples(&self) -> Vec<(OperatorId, GroupInternerSample)> {
		let mut out: Vec<(OperatorId, GroupInternerSample)> = self
			.inner
			.nodes
			.iter()
			.map(|entry| {
				let state = entry.value();
				(
					*entry.key(),
					GroupInternerSample {
						cache: state.memory(),
						membership: state.membership.memory(),
						completeness: state.completeness(),
					},
				)
			})
			.collect();
		out.sort_by_key(|(node, _)| *node);
		out
	}

	fn hydrate_once(
		state: &mut NodeState,
		node: OperatorId,
		txn: &mut FlowTransaction,
		budget: ByteSize,
	) -> Result<()> {
		if state.hydrated {
			return Ok(());
		}
		state.hydrated = true;
		state.complete = true;
		if let Some(row) = txn.state_get(node, &watermark_key())? {
			state.position = Some(Position::from_raw(decode_payload::<u64>(&row)?));
		}
		let base = keyspace_inner_range(GroupId::NODE_SCOPE, Keyspace::GROUP_DICTIONARY);
		let mut hashes: Vec<u64> = Vec::new();
		let mut start = base.start.clone();
		loop {
			let range = EncodedKeyRange::new(start, base.end.clone());
			let batch = txn.state_range(node, range, Some(HYDRATE_CHUNK))?;
			let mut last_inner: Option<EncodedKey> = None;
			for item in &batch.items {
				let decoded = OperatorStateKey::decode(&item.key)
					.expect("state_range must return OperatorState keys");
				let inner = OperatorGroupStateKey::decode_inner(&decoded.key)
					.expect("the dictionary range must yield structured operator state keys");
				reifydb_assertions! {
					let (group_id, keyspace) = (inner.0, inner.1);
					assert!(
						group_id == GroupId::NODE_SCOPE
							&& keyspace == Keyspace::GROUP_DICTIONARY,
						"the dictionary range scan must only yield node-scope dictionary keys; \
						 anything else means the range bounds are wrong and hydration would \
						 poison the interning cache with another keyspace's payloads \
						 (group={group_id:?}, keyspace={keyspace:?})"
					);
				}
				let group = EncodedKey::new(inner.2);
				hashes.push(membership_hash(&group));
				let id = GroupId(decode_payload::<u64>(&item.row)?);
				let bucket = match txn.state_get(node, &record_key(id))? {
					Some(row) => decode_payload::<GroupRecord>(&row)?.activity_bucket,
					None => 0,
				};
				state.remember(&group, id, bucket);
				last_inner = Some(EncodedKey::new(decoded.key.clone()));
			}
			state.evict_to_budget(budget);
			if !batch.has_more {
				break;
			}
			let Some(last) = last_inner else {
				break;
			};
			start = Bound::Excluded(last);
		}
		state.membership.install(&hashes);
		Ok(())
	}

	fn mint(state: &mut NodeState, node: OperatorId, txn: &mut FlowTransaction, count: u64) -> Result<u64> {
		let seed = match state.next {
			Some(next) => next,
			None => match txn.state_get(node, &counter_key())? {
				Some(row) => decode_payload::<u64>(&row)?,
				None => GroupId::FIRST.0,
			},
		};
		reifydb_assertions! {
			assert!(
				seed >= GroupId::FIRST.0,
				"group id 0 is reserved for node scope, where the interning dictionary and the \
				 counter live; minting it would put a real group's state on top of the table that \
				 resolves every group (seed={seed})"
			);
		}
		let high_water = seed + count;
		state.next = Some(high_water);
		let now = txn.clock().now();
		txn.state_set(node, &counter_key(), encode_payload(&high_water, now)?)?;
		Ok(seed)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{actors::pending::PendingWrite, common::CommitVersion};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::value::{duration::Duration, identity::IdentityId};

	use super::*;
	use crate::transaction::ChangeCoordinate;

	const NODE: OperatorId = OperatorId(1);

	fn group(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes())
	}

	fn deferred(engine: &TestEngine) -> FlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(0)),
		)
	}

	fn commit_pending(engine: &TestEngine, txn: &mut FlowTransaction) {
		let pending = txn.take_pending();
		let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
		cmd.disable_conflict_tracking().unwrap();
		for (k, pw) in pending.iter_sorted() {
			match pw {
				PendingWrite::Set(v) => cmd.set(k, v.clone()).unwrap(),
				PendingWrite::Remove {
					announce: true,
				} => cmd.remove(k).unwrap(),
				PendingWrite::Remove {
					announce: false,
				} => cmd.remove_silent(k).unwrap(),
			};
		}
		cmd.commit_unchecked().unwrap();
	}

	fn set_position(txn: &mut FlowTransaction, position: Position) {
		txn.set_change_coordinate(ChangeCoordinate {
			at: position.instant(),
			version: CommitVersion(0),
		});
	}

	fn intern_at(
		interner: &GroupInterner,
		node: OperatorId,
		txn: &mut FlowTransaction,
		group: &EncodedKey,
		position: Position,
	) -> Result<(GroupId, bool)> {
		set_position(txn, position);
		interner.intern(node, txn, group)
	}

	fn intern_many_at(
		interner: &GroupInterner,
		node: OperatorId,
		txn: &mut FlowTransaction,
		groups: &[EncodedKey],
		position: Position,
	) -> Result<Vec<(GroupId, bool)>> {
		set_position(txn, position);
		interner.intern_many(node, txn, groups)
	}

	#[test]
	fn reported_memory_counts_retained_containers_not_entry_bookkeeping() {
		let mut state = NodeState::default();
		for i in 0..64u64 {
			state.remember(&group(&format!("g{i}")), GroupId(i + 1), 0);
		}

		assert!(
			state.cache.keys().all(|k| k.heap_bytes() == 0),
			"short group keys must stay inline or this test proves nothing"
		);
		assert_eq!(state.memory().entries.as_u64(), 64);
		assert_eq!(state.memory().bytes.as_bytes(), state.cache.struct_bytes() as u64);
	}

	#[test]
	fn reported_memory_counts_a_shared_out_of_line_key_once() {
		let long = EncodedKey::new(vec![7u8; 200]);
		assert!(long.heap_bytes() > 0, "key must spill out of line or this test proves nothing");

		let mut state = NodeState::default();
		state.remember(&long, GroupId(1), 0);

		assert_eq!(
			state.memory().bytes.as_bytes(),
			state.cache.struct_bytes() as u64 + long.heap_bytes() as u64
		);
	}

	#[test]
	fn reported_memory_survives_eviction_of_every_entry() {
		let mut state = NodeState::default();
		for i in 0..64u64 {
			state.remember(&group(&format!("g{i}")), GroupId(i + 1), 0);
		}
		let full = state.memory().bytes.as_bytes();

		state.evict_to_budget(ByteSize::ZERO);

		assert_eq!(state.memory().entries.as_u64(), 0, "budget of zero must drain every entry");
		assert_eq!(
			state.memory().bytes.as_bytes(),
			state.cache.struct_bytes() as u64,
			"a drained cache holds no key payload, so it reports exactly its containers"
		);

		assert!(
			state.memory().bytes.as_bytes() >= full,
			"retained capacity must not shrink on eviction: {} < {}",
			state.memory().bytes.as_bytes(),
			full
		);
	}

	#[test]
	fn eviction_charge_covers_what_an_entry_actually_retains() {
		let mut state = NodeState::default();
		for i in 0..256u64 {
			state.remember(&group(&format!("g{i}")), GroupId(i + 1), 0);
		}

		let retained = state.cache.len() as u64 * SlabLru::<EncodedKey, Interned>::entry_struct_bytes() as u64;
		assert!(
			state.cache_size.as_bytes() >= retained,
			"charged {} for {} entries that retain {}",
			state.cache_size.as_bytes(),
			state.cache.len(),
			retained
		);
	}

	#[test]
	fn a_budget_bounds_the_memory_its_surviving_entries_retain() {
		let budget = ByteSize::from_bytes(64 * 1024);
		let mut state = NodeState::default();
		for i in 0..4096u64 {
			state.remember(&group(&format!("g{i}")), GroupId(i + 1), 0);
		}

		state.evict_to_budget(budget);

		let retained = state.cache.len() as u64 * SlabLru::<EncodedKey, Interned>::entry_struct_bytes() as u64;
		assert!(
			retained <= budget.as_bytes(),
			"{} entries survived a {} byte budget and retain {}",
			state.cache.len(),
			budget.as_bytes(),
			retained
		);
	}

	#[test]
	fn the_first_group_interns_to_the_first_usable_id() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let (id, is_new) =
			intern_at(&interner, NODE, &mut txn, &group("first"), Position(DateTime::from_nanos(0)))
				.unwrap();

		assert_eq!(id, GroupId::FIRST, "the first group must not take the node-scope id");
		assert!(!id.is_node_scope());
		assert!(is_new, "a never-seen group must report as newly interned");
	}

	#[test]
	fn a_repeated_group_resolves_to_the_same_id() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let (first, new_first) =
			intern_at(&interner, NODE, &mut txn, &group("mint"), Position(DateTime::from_nanos(0)))
				.unwrap();
		let (second, new_second) =
			intern_at(&interner, NODE, &mut txn, &group("mint"), Position(DateTime::from_nanos(0)))
				.unwrap();

		assert_eq!(first, second, "the same group bytes must always resolve to the same id");
		assert!(new_first);
		assert!(!new_second, "only the first sighting is newly interned");
	}

	#[test]
	fn distinct_groups_get_distinct_ids() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let ids: Vec<GroupId> = (0..5)
			.map(|i| {
				intern_at(
					&interner,
					NODE,
					&mut txn,
					&group(&format!("g{i}")),
					Position(DateTime::from_nanos(0)),
				)
				.unwrap()
				.0
			})
			.collect();

		let mut unique = ids.clone();
		unique.sort_unstable();
		unique.dedup();
		assert_eq!(unique.len(), ids.len(), "two groups sharing an id would share a state range");
	}

	#[test]
	fn a_batch_dedupes_repeated_groups_and_reports_one_mint() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let batch = vec![group("a"), group("b"), group("a"), group("b"), group("a")];
		let resolved =
			intern_many_at(&interner, NODE, &mut txn, &batch, Position(DateTime::from_nanos(0))).unwrap();

		assert_eq!(resolved[0].0, resolved[2].0);
		assert_eq!(resolved[0].0, resolved[4].0);
		assert_eq!(resolved[1].0, resolved[3].0);
		assert_ne!(resolved[0].0, resolved[1].0);
		assert_eq!(
			resolved.iter().filter(|(_, is_new)| *is_new).count(),
			2,
			"a batch of two distinct groups must report exactly two mints"
		);
	}

	#[test]
	fn ids_survive_a_restart() {
		let engine = TestEngine::new();
		let before = {
			let interner = GroupInterner::default();
			let mut txn = deferred(&engine);
			let id = intern_at(
				&interner,
				NODE,
				&mut txn,
				&group("survivor"),
				Position(DateTime::from_nanos(0)),
			)
			.unwrap()
			.0;
			intern_at(&interner, NODE, &mut txn, &group("other"), Position(DateTime::from_nanos(0)))
				.unwrap();
			commit_pending(&engine, &mut txn);
			id
		};

		let cold = GroupInterner::default();
		let mut txn = deferred(&engine);
		let (after, is_new) =
			intern_at(&cold, NODE, &mut txn, &group("survivor"), Position(DateTime::from_nanos(0)))
				.unwrap();

		assert_eq!(after, before, "a restarted interner must resolve an existing group to its stored id");
		assert!(!is_new, "an existing group must not be reported as newly interned after a restart");
	}

	#[test]
	fn a_reborn_group_never_reuses_the_id_of_the_generation_before_it() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let original =
			intern_at(&interner, NODE, &mut txn, &group("reborn"), Position(DateTime::from_nanos(0)))
				.unwrap()
				.0;
		interner.forget(NODE, &mut txn, &group("reborn")).unwrap();
		commit_pending(&engine, &mut txn);

		let mut txn = deferred(&engine);
		let (reborn, is_new) =
			intern_at(&interner, NODE, &mut txn, &group("reborn"), Position(DateTime::from_nanos(0)))
				.unwrap();

		assert!(is_new, "a forgotten group is unknown again and must mint afresh");
		assert_ne!(reborn, original, "a reclaimed id must never be handed back out");
	}

	#[test]
	fn a_forgotten_group_stops_resolving() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);
		intern_at(&interner, NODE, &mut txn, &group("gone"), Position(DateTime::from_nanos(0))).unwrap();
		commit_pending(&engine, &mut txn);

		let mut txn = deferred(&engine);
		interner.forget(NODE, &mut txn, &group("gone")).unwrap();
		commit_pending(&engine, &mut txn);

		let cold = GroupInterner::default();
		let mut txn = deferred(&engine);
		assert_eq!(
			cold.lookup(NODE, &mut txn, &group("gone")).unwrap(),
			None,
			"a forgotten group must not resurrect through hydration"
		);
	}

	#[test]
	fn an_id_resolves_back_to_the_bytes_it_was_interned_from() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);
		let bytes = group("two-address-key");

		let (id, _) = intern_at(&interner, NODE, &mut txn, &bytes, Position(DateTime::from_nanos(0))).unwrap();

		assert_eq!(
			interner.group_bytes(NODE, &mut txn, id).unwrap(),
			Some(bytes),
			"an interned group must be resolvable from its id alone"
		);
	}

	#[test]
	fn the_reverse_record_survives_the_data_phase() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);
		let bytes = group("outlives-its-data");
		let (id, _) = intern_at(&interner, NODE, &mut txn, &bytes, Position(DateTime::from_nanos(0))).unwrap();

		txn.reclaim_group_data(NODE, id, 100).unwrap();

		assert_eq!(
			interner.group_bytes(NODE, &mut txn, id).unwrap(),
			Some(bytes),
			"phase 1 must not take the record that phase 2 depends on"
		);
	}

	#[test]
	fn a_group_becomes_due_only_once_the_cutoff_clears_the_bucket_it_was_active_in() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let (id, _) =
			intern_at(&interner, NODE, &mut txn, &group("quiet"), Position(DateTime::from_nanos(150)))
				.unwrap();

		assert!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(150)), 10).unwrap().is_empty(),
			"a group is not idle at the very position it was last active"
		);
		assert!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(199)), 10).unwrap().is_empty(),
			"a cutoff inside the group's own bucket must not retire it"
		);
		assert_eq!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(200)), 10).unwrap(),
			vec![id],
			"once the cutoff clears the whole bucket the group is due"
		);
	}

	#[test]
	fn touching_a_group_at_an_older_position_never_drags_its_activity_backwards() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let busy = group("busy");

		let (id, _) = intern_at(&interner, NODE, &mut txn, &busy, Position(DateTime::from_nanos(950))).unwrap();
		intern_at(&interner, NODE, &mut txn, &busy, Position(DateTime::from_nanos(50))).unwrap();

		assert!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(900)), 10).unwrap().is_empty(),
			"a cutoff far past the old touch must not retire a group whose latest activity is 950"
		);
		assert_eq!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(1_000)), 10).unwrap(),
			vec![id],
			"the group is still reclaimable once the cutoff clears the bucket it was really last active in"
		);
	}

	#[test]
	fn a_reclaimed_group_that_wakes_restarts_its_activity_rather_than_staying_pinned() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let waking = group("waking");

		let (id, _) =
			intern_at(&interner, NODE, &mut txn, &waking, Position(DateTime::from_nanos(150))).unwrap();
		assert_eq!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(200)), 10).unwrap(),
			vec![id]
		);
		interner.defer(NODE, &mut txn, id).unwrap();

		intern_at(&interner, NODE, &mut txn, &waking, Position(DateTime::from_nanos(550))).unwrap();

		assert!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(500)), 10).unwrap().is_empty(),
			"a woken group must be live again, not still parked at the reclaimed sentinel"
		);
		assert_eq!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(600)), 10).unwrap(),
			vec![id],
			"and it must age out again on its new activity rather than never"
		);
	}

	fn stamp_side_at(
		interner: &GroupInterner,
		txn: &mut FlowTransaction,
		id: GroupId,
		side: Keyspace,
		position: Position,
	) {
		set_position(txn, position);
		interner.stamp_side(NODE, txn, id, side).unwrap();
	}

	#[test]
	fn each_side_of_a_join_group_retires_on_its_own_clock() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let (id, _) =
			intern_at(&interner, NODE, &mut txn, &group("k"), Position(DateTime::from_nanos(150))).unwrap();

		stamp_side_at(&interner, &mut txn, id, Keyspace::JOIN_LEFT, Position(DateTime::from_nanos(150)));
		stamp_side_at(&interner, &mut txn, id, Keyspace::JOIN_RIGHT, Position(DateTime::from_nanos(350)));

		assert!(
			interner.due_side_groups(
				NODE,
				&mut txn,
				Keyspace::JOIN_LEFT,
				Cutoff(DateTime::from_nanos(199)),
				10
			)
			.unwrap()
			.is_empty(),
			"a cutoff inside the left side's own bucket must not retire it"
		);
		assert_eq!(
			interner.due_side_groups(
				NODE,
				&mut txn,
				Keyspace::JOIN_LEFT,
				Cutoff(DateTime::from_nanos(200)),
				10
			)
			.unwrap(),
			vec![id],
			"once the cutoff clears the left side's bucket that side is due"
		);
		assert!(
			interner.due_side_groups(
				NODE,
				&mut txn,
				Keyspace::JOIN_RIGHT,
				Cutoff(DateTime::from_nanos(200)),
				10
			)
			.unwrap()
			.is_empty(),
			"the same cutoff must leave the right side alone: it was active two buckets later, and \
			 retiring it here is exactly the over-eager reclamation a shared bucket causes"
		);
		assert_eq!(
			interner.due_side_groups(
				NODE,
				&mut txn,
				Keyspace::JOIN_RIGHT,
				Cutoff(DateTime::from_nanos(400)),
				10
			)
			.unwrap(),
			vec![id],
			"the right side retires on its own, later, boundary"
		);
	}

	#[test]
	fn a_side_that_goes_active_again_is_not_reported_by_its_earlier_bucket() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let (id, _) =
			intern_at(&interner, NODE, &mut txn, &group("k"), Position(DateTime::from_nanos(150))).unwrap();

		stamp_side_at(&interner, &mut txn, id, Keyspace::JOIN_LEFT, Position(DateTime::from_nanos(150)));
		stamp_side_at(&interner, &mut txn, id, Keyspace::JOIN_LEFT, Position(DateTime::from_nanos(350)));

		assert!(
			interner.due_side_groups(
				NODE,
				&mut txn,
				Keyspace::JOIN_LEFT,
				Cutoff(DateTime::from_nanos(200)),
				10
			)
			.unwrap()
			.is_empty(),
			"the side moved to a later bucket, so the entry the cutoff cleared is stale and must \
			 not retire it"
		);
		assert!(
			interner.due_side_groups(
				NODE,
				&mut txn,
				Keyspace::JOIN_LEFT,
				Cutoff(DateTime::from_nanos(200)),
				10
			)
			.unwrap()
			.is_empty(),
			"the stale entry is dropped on sight, so a second scan finds nothing to re-examine"
		);
		assert_eq!(
			interner.due_side_groups(
				NODE,
				&mut txn,
				Keyspace::JOIN_LEFT,
				Cutoff(DateTime::from_nanos(400)),
				10
			)
			.unwrap(),
			vec![id],
			"the surviving entry is the one describing the side's current bucket"
		);
	}

	#[test]
	fn stamping_a_side_leaves_the_group_activity_bucket_untouched() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let (id, _) =
			intern_at(&interner, NODE, &mut txn, &group("k"), Position(DateTime::from_nanos(150))).unwrap();

		stamp_side_at(&interner, &mut txn, id, Keyspace::JOIN_LEFT, Position(DateTime::from_nanos(950)));

		assert_eq!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(200)), 10).unwrap(),
			vec![id],
			"the group is still due at its own last activity, not at the side's"
		);
	}

	#[test]
	fn forgetting_a_side_retires_the_entries_that_addressed_it() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let (id, _) =
			intern_at(&interner, NODE, &mut txn, &group("k"), Position(DateTime::from_nanos(150))).unwrap();
		stamp_side_at(&interner, &mut txn, id, Keyspace::JOIN_LEFT, Position(DateTime::from_nanos(150)));
		assert_eq!(
			interner.due_side_groups(
				NODE,
				&mut txn,
				Keyspace::JOIN_LEFT,
				Cutoff(DateTime::from_nanos(400)),
				10
			)
			.unwrap(),
			vec![id]
		);

		interner.forget_side(NODE, &mut txn, id, Keyspace::JOIN_LEFT).unwrap();

		assert!(
			interner.due_side_groups(
				NODE,
				&mut txn,
				Keyspace::JOIN_LEFT,
				Cutoff(DateTime::from_nanos(400)),
				10
			)
			.unwrap()
			.is_empty(),
			"a forgotten side must not linger in the index"
		);
	}

	#[test]
	fn a_side_stamp_survives_a_restart() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let (id, _) =
			intern_at(&interner, NODE, &mut txn, &group("k"), Position(DateTime::from_nanos(150))).unwrap();
		stamp_side_at(&interner, &mut txn, id, Keyspace::JOIN_LEFT, Position(DateTime::from_nanos(150)));
		commit_pending(&engine, &mut txn);

		let cold = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);

		assert!(
			cold.due_side_groups(
				NODE,
				&mut txn,
				Keyspace::JOIN_LEFT,
				Cutoff(DateTime::from_nanos(199)),
				10
			)
			.unwrap()
			.is_empty(),
			"a restart must not retire a side that is still inside its bucket"
		);
		assert_eq!(
			cold.due_side_groups(
				NODE,
				&mut txn,
				Keyspace::JOIN_LEFT,
				Cutoff(DateTime::from_nanos(200)),
				10
			)
			.unwrap(),
			vec![id],
			"the side's bucket is durable and still governs when it retires"
		);
	}

	#[test]
	fn an_active_group_leaves_no_stale_entry_behind_in_the_index() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);

		let (id, _) = intern_at(&interner, NODE, &mut txn, &group("busy"), Position(DateTime::from_nanos(50)))
			.unwrap();
		intern_at(&interner, NODE, &mut txn, &group("busy"), Position(DateTime::from_nanos(350))).unwrap();

		assert!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(300)), 10).unwrap().is_empty(),
			"the group moved on to a later bucket, so its old entry must not make it due"
		);
		assert!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(300)), 10).unwrap().is_empty(),
			"the stale entry must have been cleaned up rather than found again every scan"
		);
		assert_eq!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(400)), 10).unwrap(),
			vec![id],
			"the group is still reclaimable once the cutoff clears its current bucket"
		);
	}

	#[test]
	fn staying_inside_one_bucket_writes_nothing() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		intern_at(&interner, NODE, &mut txn, &group("chatty"), Position(DateTime::from_nanos(10))).unwrap();
		let baseline = txn.take_pending().iter_sorted().count();

		let mut txn = deferred(&engine);
		intern_at(&interner, NODE, &mut txn, &group("chatty"), Position(DateTime::from_nanos(20))).unwrap();
		intern_at(&interner, NODE, &mut txn, &group("chatty"), Position(DateTime::from_nanos(30))).unwrap();
		intern_at(&interner, NODE, &mut txn, &group("chatty"), Position(DateTime::from_nanos(99))).unwrap();

		assert_eq!(
			txn.take_pending().iter_sorted().count(),
			0,
			"repeat activity inside one bucket must not write at all (first batch wrote {baseline})"
		);
	}

	#[test]
	fn the_scan_is_bounded_by_its_limit() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		for i in 0..10 {
			intern_at(
				&interner,
				NODE,
				&mut txn,
				&group(&format!("g{i}")),
				Position(DateTime::from_nanos(50)),
			)
			.unwrap();
		}

		assert_eq!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(1000)), 3).unwrap().len(),
			3
		);
		assert_eq!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(1000)), 100).unwrap().len(),
			10
		);
	}

	#[test]
	fn a_forgotten_group_leaves_nothing_in_the_activity_index() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		intern_at(&interner, NODE, &mut txn, &group("temporary"), Position(DateTime::from_nanos(50))).unwrap();

		interner.forget(NODE, &mut txn, &group("temporary")).unwrap();

		assert!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(1000)), 10).unwrap().is_empty(),
			"a forgotten group must not linger in the activity index"
		);
	}

	#[test]
	fn activity_survives_a_restart() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let (id, _) =
			intern_at(&interner, NODE, &mut txn, &group("persisted"), Position(DateTime::from_nanos(150)))
				.unwrap();
		commit_pending(&engine, &mut txn);

		let cold = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);

		assert!(
			cold.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(199)), 10).unwrap().is_empty(),
			"a restarted process must not treat a recently active group as idle"
		);
		assert_eq!(
			cold.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(200)), 10).unwrap(),
			vec![id],
			"and must still retire it once the cutoff clears its bucket"
		);
	}

	#[test]
	fn each_node_buckets_activity_at_its_own_width() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 1_000);

		interner.set_activity_grid(OperatorId(2), Some(Duration::from_milliseconds(1_600).unwrap()));
		let mut txn = deferred(&engine);

		let (wide, _) = intern_at(
			&interner,
			OperatorId(1),
			&mut txn,
			&group("wide"),
			Position(DateTime::from_nanos(150)),
		)
		.unwrap();
		let (narrow, _) = intern_at(
			&interner,
			OperatorId(2),
			&mut txn,
			&group("narrow"),
			Position(DateTime::from_millis(150)),
		)
		.unwrap();

		let ActivityBuckets::Undeclared(wide_grid) = interner.buckets(OperatorId(1)) else {
			panic!("an unconfigured node declares no domain to bucket in");
		};
		assert_eq!(wide_grid.width(), 1_000, "an unconfigured node keeps the default");
		assert_eq!(
			interner.buckets(OperatorId(2))
				.event_grid()
				.expect("a seal horizon buckets in event time")
				.width(),
			Duration::from_milliseconds(100).unwrap()
		);
		assert!(
			interner.due_groups(OperatorId(1), &mut txn, Cutoff(DateTime::from_nanos(999)), 10)
				.unwrap()
				.is_empty(),
			"the wide node's group is still inside its first bucket"
		);
		assert_eq!(
			interner.due_groups(OperatorId(2), &mut txn, Cutoff(DateTime::from_millis(999)), 10).unwrap(),
			vec![narrow],
			"the narrow node's group has cleared several of its own buckets by the same cutoff"
		);
		assert_eq!(
			interner.due_groups(OperatorId(1), &mut txn, Cutoff(DateTime::from_nanos(1_000)), 10).unwrap(),
			vec![wide]
		);
	}

	#[test]
	fn the_node_position_is_the_high_water_of_everything_ever_stamped() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);

		intern_at(&interner, NODE, &mut txn, &group("a"), Position(DateTime::from_nanos(150))).unwrap();
		assert_eq!(interner.position(NODE, &mut txn).unwrap(), Position(DateTime::from_nanos(150)));

		intern_at(&interner, NODE, &mut txn, &group("b"), Position(DateTime::from_nanos(50))).unwrap();
		assert_eq!(
			interner.position(NODE, &mut txn).unwrap(),
			Position(DateTime::from_nanos(150)),
			"an out-of-order event must not lower it"
		);
	}

	#[test]
	fn the_node_position_survives_a_restart() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		intern_at(&interner, NODE, &mut txn, &group("persisted"), Position(DateTime::from_nanos(4_500)))
			.unwrap();
		commit_pending(&engine, &mut txn);

		let cold = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);

		assert_eq!(cold.position(NODE, &mut txn).unwrap(), Position(DateTime::from_nanos(4_500)));
	}

	#[test]
	fn deferring_a_group_moves_it_from_the_data_scan_to_the_identity_scan() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let (id, _) = intern_at(&interner, NODE, &mut txn, &group("idle"), Position(DateTime::from_nanos(50)))
			.unwrap();

		assert_eq!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(1000)), 10).unwrap(),
			vec![id]
		);
		assert!(interner
			.due_identity_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(1000)), 10)
			.unwrap()
			.is_empty());

		assert!(interner.defer(NODE, &mut txn, id).unwrap());

		assert!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(1000)), 10).unwrap().is_empty(),
			"a group whose data is gone must stop being handed back to the data phase"
		);
		assert_eq!(
			interner.due_identity_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(1000)), 10).unwrap(),
			vec![id],
			"and must be findable by the identity phase instead"
		);
	}

	#[test]
	fn a_deferred_group_that_wakes_in_its_old_bucket_stops_being_identity_due() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let (id, _) = intern_at(&interner, NODE, &mut txn, &group("wakes"), Position(DateTime::from_nanos(50)))
			.unwrap();
		interner.defer(NODE, &mut txn, id).unwrap();

		let (again, _) =
			intern_at(&interner, NODE, &mut txn, &group("wakes"), Position(DateTime::from_nanos(60)))
				.unwrap();

		assert_eq!(again, id, "a woken group keeps its id; its state address must not move");
		assert!(
			interner.due_identity_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(1000)), 10)
				.unwrap()
				.is_empty(),
			"a live group must never be identity-due"
		);
		assert_eq!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(1000)), 10).unwrap(),
			vec![id],
			"and it rejoins the data phase like any other group"
		);
	}

	#[test]
	fn the_reclaimed_marker_outlives_the_process_that_wrote_it() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let (id, _) =
			intern_at(&interner, NODE, &mut txn, &group("cold-wake"), Position(DateTime::from_nanos(50)))
				.unwrap();
		interner.defer(NODE, &mut txn, id).unwrap();
		commit_pending(&engine, &mut txn);

		let cold = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		assert_eq!(
			cold.due_identity_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(1000)), 10).unwrap(),
			vec![id],
			"a restarted process must still see the group as awaiting its identity phase"
		);

		intern_at(&cold, NODE, &mut txn, &group("cold-wake"), Position(DateTime::from_nanos(60))).unwrap();

		assert!(
			cold.due_identity_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(1000)), 10)
				.unwrap()
				.is_empty(),
			"the wake must clear the marker even when it arrives through a cold cache"
		);
	}

	#[test]
	fn deferring_is_idempotent_and_refuses_a_group_it_cannot_resolve() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let (id, _) = intern_at(&interner, NODE, &mut txn, &group("twice"), Position(DateTime::from_nanos(50)))
			.unwrap();

		assert!(interner.defer(NODE, &mut txn, id).unwrap());
		assert!(interner.defer(NODE, &mut txn, id).unwrap());

		assert_eq!(
			interner.due_identity_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(1000)), 10).unwrap(),
			vec![id]
		);
		assert!(
			!interner.defer(NODE, &mut txn, GroupId(9_999)).unwrap(),
			"a group with no record cannot be deferred; there is nothing to mark"
		);
	}

	#[test]
	fn lookup_does_not_intern() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		assert_eq!(interner.lookup(NODE, &mut txn, &group("absent")).unwrap(), None);

		let (id, is_new) =
			intern_at(&interner, NODE, &mut txn, &group("absent"), Position(DateTime::from_nanos(0)))
				.unwrap();
		assert!(is_new, "the earlier lookup must not have interned the group");
		assert_eq!(id, GroupId::FIRST, "a lookup must not consume an id from the counter");
	}

	#[test]
	fn nodes_intern_independently() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let first = intern_at(
			&interner,
			OperatorId(1),
			&mut txn,
			&group("shared"),
			Position(DateTime::from_nanos(0)),
		)
		.unwrap()
		.0;
		let second = intern_at(
			&interner,
			OperatorId(2),
			&mut txn,
			&group("shared"),
			Position(DateTime::from_nanos(0)),
		)
		.unwrap()
		.0;

		assert_eq!(first, second, "each node numbers its own groups from the same starting point");

		let other = intern_at(
			&interner,
			OperatorId(2),
			&mut txn,
			&group("only-on-two"),
			Position(DateTime::from_nanos(0)),
		)
		.unwrap()
		.0;
		let mut txn = deferred(&engine);
		assert_eq!(
			interner.lookup(OperatorId(1), &mut txn, &group("only-on-two")).unwrap(),
			None,
			"a group interned on one node must not resolve on another"
		);
		assert_ne!(other, first);
	}
}

impl FlowTransaction {
	pub fn intern_group(&mut self, node: OperatorId, group: &EncodedKey) -> Result<(GroupId, bool)> {
		let interner = self.group_interner();
		let (id, is_new) = interner.intern(node, self, group)?;
		if is_new {
			self.row_numbers().mark_fresh(node, id);
		}
		Ok((id, is_new))
	}

	pub fn intern_groups(&mut self, node: OperatorId, groups: &[EncodedKey]) -> Result<Vec<(GroupId, bool)>> {
		let interner = self.group_interner();
		let results = interner.intern_many(node, self, groups)?;
		let provider = self.row_numbers();
		for (id, is_new) in &results {
			if *is_new {
				provider.mark_fresh(node, *id);
			}
		}
		Ok(results)
	}

	pub fn due_groups(&mut self, node: OperatorId, cutoff: Cutoff, limit: usize) -> Result<Vec<GroupId>> {
		let interner = self.group_interner();
		interner.due_groups(node, self, cutoff, limit)
	}

	pub fn due_identity_groups(&mut self, node: OperatorId, cutoff: Cutoff, limit: usize) -> Result<Vec<GroupId>> {
		let interner = self.group_interner();
		interner.due_identity_groups(node, self, cutoff, limit)
	}

	pub fn stamp_side(&mut self, node: OperatorId, id: GroupId, side: Keyspace) -> Result<()> {
		let interner = self.group_interner();
		interner.stamp_side(node, self, id, side)
	}

	pub fn due_side_groups(
		&mut self,
		node: OperatorId,
		side: Keyspace,
		cutoff: Cutoff,
		limit: usize,
	) -> Result<Vec<GroupId>> {
		let interner = self.group_interner();
		interner.due_side_groups(node, self, side, cutoff, limit)
	}

	pub fn forget_side(&mut self, node: OperatorId, id: GroupId, side: Keyspace) -> Result<()> {
		let interner = self.group_interner();
		interner.forget_side(node, self, id, side)
	}

	pub fn node_position(&mut self, node: OperatorId) -> Result<Position> {
		let interner = self.group_interner();
		interner.position(node, self)
	}

	pub fn defer_group(&mut self, node: OperatorId, id: GroupId) -> Result<bool> {
		let interner = self.group_interner();
		interner.defer(node, self, id)
	}

	pub fn lookup_group(&mut self, node: OperatorId, group: &EncodedKey) -> Result<Option<GroupId>> {
		let interner = self.group_interner();
		interner.lookup(node, self, group)
	}

	pub fn forget_group(&mut self, node: OperatorId, group: &EncodedKey) -> Result<bool> {
		let interner = self.group_interner();
		interner.forget(node, self, group)
	}

	pub fn group_bytes(&mut self, node: OperatorId, id: GroupId) -> Result<Option<EncodedKey>> {
		let interner = self.group_interner();
		interner.group_bytes(node, self, id)
	}
}
