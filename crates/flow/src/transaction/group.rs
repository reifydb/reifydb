// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeSet, HashMap},
	ops::Bound,
	slice::from_ref,
	sync::Arc,
};

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
	interface::{catalog::flow::OperatorId, store::MultiVersionBatch},
	key::{
		EncodableKey,
		operator_group_state::{GroupId, GroupStateKey, Keyspace, OperatorGroupStateKey, keyspace_inner_range},
		operator_state::OperatorStateKey,
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
use tracing::{Span, instrument};

use super::FlowTransaction;

const DEFAULT_BYTE_BUDGET: u64 = 1024 * 1024;
const HYDRATE_CHUNK: usize = 8_192;
const DUE_PREFETCH: usize = 4_096;
const SIDE_BUCKET_CACHE_CAP: usize = 65_536;
const APPROXIMATE_BTREE_ENTRY_BYTES: u64 = 32;
const APPROXIMATE_MAP_SLOT_BYTES: u64 = 17;
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

struct DueBatch {
	due: Vec<(u64, GroupId)>,
	last_bucket: Option<u64>,
}

#[derive(Default)]
struct DuePrefix {
	entries: BTreeSet<(u64, u64)>,
	bucket_of: HashMap<u64, u64>,
	complete_through: u64,
}

impl DuePrefix {
	fn covers(&self, first_live: u64) -> bool {
		first_live <= self.complete_through
	}

	fn serve(&self, first_live: u64, limit: usize) -> Vec<GroupId> {
		self.entries.range((0, 0)..(first_live, 0)).take(limit).map(|(_, id)| GroupId(*id)).collect()
	}

	fn insert(&mut self, bucket: u64, id: GroupId) {
		self.remove_group(id);
		if bucket >= self.complete_through {
			return;
		}
		self.entries.insert((bucket, id.0));
		self.bucket_of.insert(id.0, bucket);
	}

	fn remove_group(&mut self, id: GroupId) {
		if let Some(bucket) = self.bucket_of.remove(&id.0) {
			self.entries.remove(&(bucket, id.0));
		}
	}

	fn approximate_memory(&self) -> StateMemory {
		let entries = self.entries.len() as u64;
		let tree = entries * APPROXIMATE_BTREE_ENTRY_BYTES;
		let map = self.bucket_of.capacity() as u64 * APPROXIMATE_MAP_SLOT_BYTES;
		StateMemory::new(Count::new(entries), ByteSize::from_bytes(tree + map))
	}

	fn adopt(&mut self, due: &[(u64, GroupId)], complete_through: u64) {
		reifydb_assertions! {
			assert!(
				complete_through >= self.complete_through,
				"a refill must never shrink the region the prefix claims to know; walking the \
				 boundary backwards would leave groups below it unreachable to every later sweep \
				 and they would never be reclaimed (was={}, now={complete_through})",
				self.complete_through
			);
		}
		self.complete_through = complete_through;
		for (bucket, id) in due {
			self.insert(*bucket, *id);
		}
	}
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
	activity: DuePrefix,
	identity: DuePrefix,
	sides: HashMap<u8, DuePrefix>,
	side_now: HashMap<(u64, u8), u64>,
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
			activity: DuePrefix::default(),
			identity: DuePrefix::default(),
			sides: HashMap::new(),
			side_now: HashMap::new(),
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

	fn remember_side(&mut self, id: GroupId, side: Keyspace, bucket: u64) {
		if self.side_now.len() >= SIDE_BUCKET_CACHE_CAP {
			self.side_now.clear();
		}
		self.side_now.insert((id.0, side.0), bucket);
	}

	fn forget_side_of(&mut self, id: GroupId, side: Keyspace) {
		self.side_now.remove(&(id.0, side.0));
	}

	fn forget_all_sides_of(&mut self, id: GroupId) {
		self.side_now.retain(|(group, _), _| *group != id.0);
	}

	fn due_memory(&self) -> StateMemory {
		self.sides
			.values()
			.fold(self.activity.approximate_memory() + self.identity.approximate_memory(), |total, side| {
				total + side.approximate_memory()
			})
	}
}

pub struct GroupInternerSample {
	pub cache: StateMemory,
	pub membership: StateMemory,
	pub due: StateMemory,
	pub completeness: StateCompleteness,
}

#[derive(Clone)]
pub struct GroupInterner {
	inner: Arc<GroupInternerInner>,
}

struct GroupInternerInner {
	operators: DashMap<OperatorId, NodeState>,
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
				operators: DashMap::new(),
				budget,
				buckets: ActivityBuckets::undeclared(activity_bucket_width),
			}),
		}
	}

	pub fn set_activity_grid(&self, operator: OperatorId, scale: Option<Duration>) {
		let mut state = self.inner.operators.entry(operator).or_default();
		state.buckets = Some(activity_buckets(scale));
	}

	pub fn buckets(&self, operator: OperatorId) -> ActivityBuckets {
		self.buckets_of(operator)
	}

	fn buckets_of(&self, operator: OperatorId) -> ActivityBuckets {
		self.inner.operators.get(&operator).and_then(|state| state.buckets).unwrap_or(self.inner.buckets)
	}

	pub fn intern(
		&self,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		group: &EncodedKey,
	) -> Result<(GroupId, bool)> {
		Ok(self.intern_many(operator, txn, from_ref(group))?.into_iter().next().unwrap())
	}

	pub fn intern_many(
		&self,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		groups: &[EncodedKey],
	) -> Result<Vec<(GroupId, bool)>> {
		let now = txn.clock().now();
		let budget = self.inner.budget;
		let mut guard = self.inner.operators.entry(operator).or_default();
		Self::hydrate_once(&mut guard, operator, txn, budget)?;
		let state = &mut *guard;
		let position = Self::stamp_position(txn);

		let buckets = state.buckets.unwrap_or(self.inner.buckets);
		Self::advance_position(state, operator, txn, position, buckets, now)?;

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
			let effective = Self::stamp(txn, operator, id, &groups[i], bucket, now)?;
			state.remember(&groups[i], id, effective);
			Self::track_stamp(state, id, effective);
		}
		if to_resolve.is_empty() {
			state.evict_to_budget(budget);
			return Ok(results.into_iter().map(|r| r.expect("every position filled")).collect());
		}

		let dictionary_keys: Vec<GroupStateKey> =
			to_resolve.iter().map(|i| dictionary_key(&groups[*i])).collect();

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
				let batch = txn.state_get_many(operator, &lookup)?;
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
			let start = Self::mint(state, operator, txn, distinct_new.len() as u64)?;
			let mut assigned: HashMap<Vec<u8>, GroupId> = HashMap::with_capacity(distinct_new.len());
			for (offset, &slot) in distinct_new.iter().enumerate() {
				let i = to_resolve[slot];
				let dictionary = &dictionary_keys[slot];
				let id = GroupId(start + offset as u64);
				txn.state_set(operator, dictionary, encode_payload(&id.0, now)?)?;
				Self::stamp(txn, operator, id, &groups[i], bucket, now)?;
				state.remember(&groups[i], id, bucket);
				Self::track_stamp(state, id, bucket);
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
			let effective = Self::stamp(txn, operator, id, &groups[i], bucket, now)?;
			state.remember(&groups[i], id, effective);
			Self::track_stamp(state, id, effective);
		}

		state.evict_to_budget(budget);

		Ok(results.into_iter().map(|r| r.expect("every position filled")).collect())
	}

	fn track_stamp(state: &mut NodeState, id: GroupId, bucket: u64) {
		state.activity.insert(bucket, id);
		state.identity.remove_group(id);
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
		operator: OperatorId,
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
		let previous = Self::load_record(operator, txn, id)?
			.map(|record| record.activity_bucket)
			.filter(|bucket| *bucket != GroupRecord::RECLAIMED_BUCKET);
		let effective = previous.map_or(bucket, |previous| previous.max(bucket));
		if effective != bucket {
			return Ok(effective);
		}
		if let Some(previous) = previous
			&& previous != bucket
		{
			reifydb_assertions! {
				assert!(
					previous < bucket,
					"the superseded activity entry must sit strictly below the bucket replacing \
					 it; removing one at or above it would drop the entry due_in needs to find \
					 this group (group={id:?}, previous={previous}, bucket={bucket})"
				);
			}
			txn.state_remove(operator, &index_key(Keyspace::ACTIVITY_INDEX, previous, id))?;
		}
		txn.state_set(
			operator,
			&record_key(id),
			encode_payload(&GroupRecord::new(group.as_ref().to_vec(), bucket), now)?,
		)?;
		txn.state_set(operator, &index_key(Keyspace::ACTIVITY_INDEX, bucket, id), encode_payload(&1u64, now)?)?;
		Ok(bucket)
	}

	fn advance_position(
		state: &mut NodeState,
		operator: OperatorId,
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
			txn.state_set(operator, &watermark_key(), encode_payload(&position.raw(), now)?)?;
		}
		Ok(())
	}

	pub fn position(&self, operator: OperatorId, txn: &mut FlowTransaction) -> Result<Position> {
		let budget = self.inner.budget;
		let mut guard = self.inner.operators.entry(operator).or_default();
		Self::hydrate_once(&mut guard, operator, txn, budget)?;
		Ok(guard.position.unwrap_or(Position::from_raw(0)))
	}

	pub fn defer(&self, operator: OperatorId, txn: &mut FlowTransaction, id: GroupId) -> Result<bool> {
		let budget = self.inner.budget;
		let now = txn.clock().now();
		let mut guard = self.inner.operators.entry(operator).or_default();
		Self::hydrate_once(&mut guard, operator, txn, budget)?;
		let state = &mut *guard;

		let Some(record) = Self::load_record(operator, txn, id)? else {
			return Ok(false);
		};
		if record.is_data_reclaimed() {
			return Ok(true);
		}

		let bucket = record.activity_bucket;
		let group = EncodedKey::new(record.group.clone());
		txn.state_remove(operator, &index_key(Keyspace::ACTIVITY_INDEX, bucket, id))?;
		txn.state_set(operator, &index_key(Keyspace::IDENTITY_INDEX, bucket, id), encode_payload(&1u64, now)?)?;
		txn.state_set(operator, &record_key(id), encode_payload(&GroupRecord::reclaimed(record.group), now)?)?;
		state.remember(&group, id, GroupRecord::RECLAIMED_BUCKET);
		state.activity.remove_group(id);
		state.identity.insert(bucket, id);
		Ok(true)
	}

	pub fn lookup(
		&self,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		group: &EncodedKey,
	) -> Result<Option<GroupId>> {
		let budget = self.inner.budget;
		let mut guard = self.inner.operators.entry(operator).or_default();
		Self::hydrate_once(&mut guard, operator, txn, budget)?;
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
		let Some(row) = txn.state_get(operator, &dictionary_key(group))? else {
			state.membership.record_store_miss();
			return Ok(None);
		};
		let id = GroupId(decode_payload::<u64>(&row)?);
		let bucket = match txn.state_get(operator, &record_key(id))? {
			Some(record) => decode_payload::<GroupRecord>(&record)?.activity_bucket,
			None => 0,
		};
		state.remember(group, id, bucket);
		state.evict_to_budget(budget);
		Ok(Some(id))
	}

	pub fn forget(&self, operator: OperatorId, txn: &mut FlowTransaction, group: &EncodedKey) -> Result<bool> {
		let budget = self.inner.budget;
		let mut guard = self.inner.operators.entry(operator).or_default();
		Self::hydrate_once(&mut guard, operator, txn, budget)?;
		let state = &mut *guard;

		let cached = state.cache.get(group);
		state.forget(group);
		state.membership.remove(membership_hash(group));
		let existed = cached.is_some() || !state.complete;
		if let Some(interned) = cached
			&& interned.bucket != GroupRecord::RECLAIMED_BUCKET
		{
			txn.state_remove(operator, &index_key(Keyspace::ACTIVITY_INDEX, interned.bucket, interned.id))?;
		}
		let id = match cached {
			Some(interned) => Some(interned.id),
			None => match txn.state_get(operator, &dictionary_key(group))? {
				Some(row) => Some(GroupId(decode_payload::<u64>(&row)?)),
				None => None,
			},
		};
		if let Some(id) = id {
			state.activity.remove_group(id);
			state.identity.remove_group(id);
			for prefix in state.sides.values_mut() {
				prefix.remove_group(id);
			}
			state.forget_all_sides_of(id);
		}
		txn.state_remove(operator, &dictionary_key(group))?;
		Ok(existed)
	}

	pub fn due_groups(
		&self,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		cutoff: Cutoff,
		limit: usize,
	) -> Result<Vec<GroupId>> {
		self.due_in(operator, txn, Keyspace::ACTIVITY_INDEX, cutoff, limit, |record, bucket| {
			record.activity_bucket == bucket
		})
	}

	pub fn stamp_side(
		&self,
		operator: OperatorId,
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
		let bucket = self.buckets(operator).of(Self::stamp_position(txn));

		if side.ages_per_row() {
			if self.inner.operators.entry(operator).or_default().side_now.contains_key(&(id.0, side.0)) {
				return Ok(());
			}
			if txn.state_get(operator, &side_record_key(id, side))?.is_some() {
				return Ok(());
			}
			return self.write_side_stamp(operator, txn, id, side, bucket, None);
		}
		if self.inner
			.operators
			.entry(operator)
			.or_default()
			.side_now
			.get(&(id.0, side.0))
			.is_some_and(|previous| *previous >= bucket)
		{
			return Ok(());
		}
		let previous = match txn.state_get(operator, &side_record_key(id, side))? {
			Some(row) => {
				let previous = decode_payload::<u64>(&row)?;
				if previous >= bucket {
					return Ok(());
				}
				Some(previous)
			}
			None => None,
		};
		self.write_side_stamp(operator, txn, id, side, bucket, previous)
	}

	pub fn restamp_side(
		&self,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		id: GroupId,
		side: Keyspace,
		oldest: DateTime,
	) -> Result<()> {
		let bucket = self.buckets(operator).of(Position(oldest));
		let previous = match txn.state_get(operator, &side_record_key(id, side))? {
			Some(row) => Some(decode_payload::<u64>(&row)?),
			None => None,
		};
		if previous == Some(bucket) {
			return Ok(());
		}
		self.write_side_stamp(operator, txn, id, side, bucket, previous)
	}

	fn write_side_stamp(
		&self,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		id: GroupId,
		side: Keyspace,
		bucket: u64,
		previous: Option<u64>,
	) -> Result<()> {
		reifydb_assertions! {
			assert!(
				bucket != GroupRecord::RECLAIMED_BUCKET,
				"a live side stamp landed on the bucket reserved to mark reclaimed data; the side \
				 would read as already reclaimed while it is being written (group={id:?}, \
				 side={side:?})"
			);
		}
		let now = txn.clock().now();
		if let Some(previous) = previous {
			txn.state_remove(operator, &side_index_key(side, previous, id))?;
		}
		txn.state_set(operator, &side_record_key(id, side), encode_payload(&bucket, now)?)?;
		txn.state_set(operator, &side_index_key(side, bucket, id), encode_payload(&1u64, now)?)?;
		let mut guard = self.inner.operators.entry(operator).or_default();
		guard.sides.entry(side.0).or_default().insert(bucket, id);
		guard.remember_side(id, side, bucket);
		Ok(())
	}

	#[instrument(name = "flow::reclaim::due_side_groups", level = "trace", skip_all)]
	pub fn due_side_groups(
		&self,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		side: Keyspace,
		cutoff: Cutoff,
		limit: usize,
	) -> Result<Vec<GroupId>> {
		if limit == 0 {
			return Ok(Vec::new());
		}
		let first_live = self.buckets_of(operator).first_live(cutoff);
		let mut guard = self.inner.operators.entry(operator).or_default();
		let prefix = guard.sides.entry(side.0).or_default();
		if prefix.covers(first_live) {
			return Ok(prefix.serve(first_live, limit));
		}
		let from = prefix.complete_through;
		let batch = Self::due_side_scan(operator, txn, side, from, first_live, limit.max(DUE_PREFETCH))?;
		let scanned = Self::due_side_verify(operator, txn, side, &batch)?;
		match Self::settled_through(&scanned, &batch, from, first_live) {
			Some(complete_through) => {
				prefix.adopt(&scanned.due, complete_through);
				Ok(prefix.serve(first_live, limit))
			}
			None => Ok(scanned.due.iter().map(|(_, id)| *id).take(limit).collect()),
		}
	}

	fn due_side_scan(
		operator: OperatorId,
		txn: &mut FlowTransaction,
		side: Keyspace,
		from: u64,
		first_live: u64,
		limit: usize,
	) -> Result<MultiVersionBatch> {
		let range = EncodedKeyRange::new(
			Bound::Included(side_index_bound(side, from).into_encoded()),
			Bound::Excluded(side_index_bound(side, first_live).into_encoded()),
		);
		txn.state_range(operator, range, Some(limit))
	}

	fn due_side_verify(
		operator: OperatorId,
		txn: &mut FlowTransaction,
		side: Keyspace,
		batch: &MultiVersionBatch,
	) -> Result<DueBatch> {
		let mut due = Vec::new();
		let mut last_bucket = None;
		let mut stale: Vec<GroupStateKey> = Vec::new();
		for item in &batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_range must return OperatorState keys");
			let inner = OperatorGroupStateKey::decode_inner(&decoded.key)
				.expect("the index range must yield structured operator state keys");
			let Some((_found, bucket, id)) = decode_side_suffix(&inner.2) else {
				continue;
			};
			last_bucket = Some(bucket);
			reifydb_assertions! {
				assert!(
					_found == side,
					"the side index range must only yield entries of the side it scanned; \
					 another side here means the bucket bounds are wrong and reclamation \
					 would drop rows the other side's ttl still covers (wanted={side:?}, \
					 found={_found:?})"
				);
			}
			let current = match txn.state_get(operator, &side_record_key(id, side))? {
				Some(row) => Some(decode_payload::<u64>(&row)?),
				None => None,
			};
			match current {
				Some(current) if current == bucket => due.push((bucket, id)),
				_ => stale.push(GroupStateKey::from_framed(EncodedKey::new(decoded.key.clone()))
					.expect("the index range yields framed inner keys")),
			}
		}
		for key in &stale {
			txn.state_remove(operator, key)?;
		}
		let span = Span::current();
		span.record("candidates", batch.items.len() as u64);
		span.record("due", due.len() as u64);
		span.record("stale", stale.len() as u64);
		Ok(DueBatch {
			due,
			last_bucket,
		})
	}

	pub fn forget_side(
		&self,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		id: GroupId,
		side: Keyspace,
	) -> Result<()> {
		let mut guard = self.inner.operators.entry(operator).or_default();
		guard.sides.entry(side.0).or_default().remove_group(id);
		guard.forget_side_of(id, side);
		drop(guard);
		txn.state_remove(operator, &side_record_key(id, side))
	}

	pub fn due_identity_groups(
		&self,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		cutoff: Cutoff,
		limit: usize,
	) -> Result<Vec<GroupId>> {
		self.due_in(operator, txn, Keyspace::IDENTITY_INDEX, cutoff, limit, |record, _| {
			record.is_data_reclaimed()
		})
	}

	#[instrument(name = "flow::reclaim::due_in", level = "trace", skip_all, fields(candidates = tracing::field::Empty, due = tracing::field::Empty, stale = tracing::field::Empty))]
	fn due_in(
		&self,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		keyspace: Keyspace,
		cutoff: Cutoff,
		limit: usize,
		live: impl Fn(&GroupRecord, u64) -> bool,
	) -> Result<Vec<GroupId>> {
		if limit == 0 {
			return Ok(Vec::new());
		}
		let first_live = self.buckets_of(operator).first_live(cutoff);
		let mut guard = self.inner.operators.entry(operator).or_default();
		let prefix = if keyspace == Keyspace::IDENTITY_INDEX {
			&mut guard.identity
		} else {
			&mut guard.activity
		};
		if prefix.covers(first_live) {
			return Ok(prefix.serve(first_live, limit));
		}
		let from = prefix.complete_through;
		let batch = Self::due_scan(operator, txn, keyspace, from, first_live, limit.max(DUE_PREFETCH))?;
		let scanned = Self::due_verify(operator, txn, keyspace, &batch, live)?;
		match Self::settled_through(&scanned, &batch, from, first_live) {
			Some(complete_through) => {
				prefix.adopt(&scanned.due, complete_through);
				Ok(prefix.serve(first_live, limit))
			}
			None => Ok(scanned.due.iter().map(|(_, id)| *id).take(limit).collect()),
		}
	}

	fn settled_through(scanned: &DueBatch, batch: &MultiVersionBatch, from: u64, first_live: u64) -> Option<u64> {
		if !batch.has_more {
			return Some(first_live);
		}
		match scanned.last_bucket {
			Some(last) if last > from => Some(last),
			_ => None,
		}
	}

	fn due_scan(
		operator: OperatorId,
		txn: &mut FlowTransaction,
		keyspace: Keyspace,
		from: u64,
		first_live: u64,
		limit: usize,
	) -> Result<MultiVersionBatch> {
		let range = EncodedKeyRange::new(
			Bound::Included(index_bound(keyspace, from).into_encoded()),
			Bound::Excluded(index_bound(keyspace, first_live).into_encoded()),
		);
		txn.state_range(operator, range, Some(limit))
	}

	#[cfg_attr(not(reifydb_assertions), allow(unused_variables))]
	fn due_verify(
		operator: OperatorId,
		txn: &mut FlowTransaction,
		keyspace: Keyspace,
		batch: &MultiVersionBatch,
		live: impl Fn(&GroupRecord, u64) -> bool,
	) -> Result<DueBatch> {
		let mut due = Vec::new();
		let mut last_bucket = None;
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
			last_bucket = Some(bucket);
			match Self::load_record(operator, txn, id)? {
				Some(record) if live(&record, bucket) => due.push((bucket, id)),
				_ => stale.push(GroupStateKey::from_framed(EncodedKey::new(decoded.key.clone()))
					.expect("the index range yields framed inner keys")),
			}
		}
		for key in &stale {
			txn.state_remove(operator, key)?;
		}
		Ok(DueBatch {
			due,
			last_bucket,
		})
	}

	fn load_record(operator: OperatorId, txn: &mut FlowTransaction, id: GroupId) -> Result<Option<GroupRecord>> {
		let Some(row) = txn.state_get(operator, &record_key(id))? else {
			return Ok(None);
		};
		Ok(Some(decode_payload::<GroupRecord>(&row)?))
	}

	pub fn group_bytes(
		&self,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		id: GroupId,
	) -> Result<Option<EncodedKey>> {
		let Some(row) = txn.state_get(operator, &record_key(id))? else {
			return Ok(None);
		};
		Ok(Some(EncodedKey::new(decode_payload::<GroupRecord>(&row)?.group)))
	}

	pub fn samples(&self) -> Vec<(OperatorId, GroupInternerSample)> {
		let mut out: Vec<(OperatorId, GroupInternerSample)> = self
			.inner
			.operators
			.iter()
			.map(|entry| {
				let state = entry.value();
				(
					*entry.key(),
					GroupInternerSample {
						cache: state.memory(),
						membership: state.membership.memory(),
						due: state.due_memory(),
						completeness: state.completeness(),
					},
				)
			})
			.collect();
		out.sort_by_key(|(operator, _)| *operator);
		out
	}

	fn hydrate_once(
		state: &mut NodeState,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		budget: ByteSize,
	) -> Result<()> {
		if state.hydrated {
			return Ok(());
		}
		state.hydrated = true;
		state.complete = true;
		if let Some(row) = txn.state_get(operator, &watermark_key())? {
			state.position = Some(Position::from_raw(decode_payload::<u64>(&row)?));
		}
		let base = keyspace_inner_range(GroupId::NODE_SCOPE, Keyspace::GROUP_DICTIONARY);
		let mut hashes: Vec<u64> = Vec::new();
		let mut start = base.start.clone();
		loop {
			let range = EncodedKeyRange::new(start, base.end.clone());
			let batch = txn.state_range(operator, range, Some(HYDRATE_CHUNK))?;
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
						"the dictionary range scan must only yield operator-scope dictionary keys; \
						 anything else means the range bounds are wrong and hydration would \
						 poison the interning cache with another keyspace's payloads \
						 (group={group_id:?}, keyspace={keyspace:?})"
					);
				}
				let group = EncodedKey::new(inner.2);
				hashes.push(membership_hash(&group));
				let id = GroupId(decode_payload::<u64>(&item.row)?);
				let bucket = match txn.state_get(operator, &record_key(id))? {
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

	fn mint(state: &mut NodeState, operator: OperatorId, txn: &mut FlowTransaction, count: u64) -> Result<u64> {
		let seed = match state.next {
			Some(next) => next,
			None => match txn.state_get(operator, &counter_key())? {
				Some(row) => decode_payload::<u64>(&row)?,
				None => GroupId::FIRST.0,
			},
		};
		reifydb_assertions! {
			assert!(
				seed >= GroupId::FIRST.0,
				"group id 0 is reserved for operator scope, where the interning dictionary and the \
				 counter live; minting it would put a real group's state on top of the table that \
				 resolves every group (seed={seed})"
			);
		}
		let high_water = seed + count;
		state.next = Some(high_water);
		let now = txn.clock().now();
		txn.state_set(operator, &counter_key(), encode_payload(&high_water, now)?)?;
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

	#[test]
	fn a_group_that_moves_forward_leaves_nothing_at_the_bucket_it_left() {
		// The prefix answers without consulting storage, so a superseded entry it keeps is one no
		// verification read will ever reject: the group reads as due at the bucket it has already
		// left, and reclamation drops rows its real activity still covers.
		let mut prefix = DuePrefix::default();
		prefix.adopt(&[], 100);

		prefix.insert(10, GroupId(7));
		prefix.insert(20, GroupId(7));

		assert!(
			prefix.serve(15, 10).is_empty(),
			"the group moved to bucket 20, so a cutoff that only clears bucket 10 must not retire it"
		);
		assert_eq!(
			prefix.serve(25, 10),
			vec![GroupId(7)],
			"and it must still retire once the cutoff clears the bucket it actually holds"
		);
	}

	#[test]
	fn a_prefix_holding_groups_reports_more_memory_than_an_empty_one() {
		// The prefix is what C' trades disk scanning for, so an estimate that stays flat as it
		// fills would hide exactly the growth the design promises to bound.
		let empty = DuePrefix::default();
		assert_eq!(empty.approximate_memory().entries, Count::new(0));

		let mut prefix = DuePrefix::default();
		prefix.adopt(&[(1, GroupId(1)), (2, GroupId(2)), (3, GroupId(3))], 100);

		let memory = prefix.approximate_memory();
		assert_eq!(memory.entries, Count::new(3), "entries is the exact live count, not an estimate");
		assert!(
			memory.bytes > empty.approximate_memory().bytes,
			"three tracked groups must read as more resident memory than none"
		);
	}

	#[test]
	fn due_memory_counts_every_side_not_just_the_group_clocks() {
		// A join holds one prefix per side, so sides are the largest consumer in the workload this
		// design was built for. Summing only activity and identity would under-report precisely the
		// operator whose memory matters most.
		let mut state = NodeState::default();
		state.activity.adopt(&[(1, GroupId(1))], 100);
		let clocks_only = state.due_memory();

		state.sides.entry(7).or_default().adopt(&[(1, GroupId(1)), (2, GroupId(2))], 100);

		assert!(
			state.due_memory().bytes > clocks_only.bytes,
			"a populated side prefix must add to the operator's reported memory"
		);
		assert_eq!(
			state.due_memory().entries,
			Count::new(3),
			"entries totals every prefix the operator holds, across both group clocks and its sides"
		);
	}

	#[test]
	fn a_group_that_moves_beyond_the_known_region_still_loses_its_old_entry() {
		// Dropping the old entry only when the new one is storable would strand the group at its
		// old bucket forever: every stamp past complete_through is the common case, so the stale
		// entry outlives all of them and retires a group that never stopped being active.
		let mut prefix = DuePrefix::default();
		prefix.adopt(&[], 100);

		prefix.insert(10, GroupId(7));
		prefix.insert(500, GroupId(7));

		assert!(
			prefix.serve(50, 10).is_empty(),
			"the group is active at bucket 500, past what the prefix tracks, so it is not due here"
		);
		assert!(!prefix.covers(500), "and the prefix must not claim to know a region it never scanned");
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
		operator: OperatorId,
		txn: &mut FlowTransaction,
		group: &EncodedKey,
		position: Position,
	) -> Result<(GroupId, bool)> {
		set_position(txn, position);
		interner.intern(operator, txn, group)
	}

	fn intern_many_at(
		interner: &GroupInterner,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		groups: &[EncodedKey],
		position: Position,
	) -> Result<Vec<(GroupId, bool)>> {
		set_position(txn, position);
		interner.intern_many(operator, txn, groups)
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

		assert_eq!(id, GroupId::FIRST, "the first group must not take the operator-scope id");
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

	fn activity_buckets_of(txn: &mut FlowTransaction, operator: OperatorId, id: GroupId) -> Vec<u64> {
		// Every entry lives under NODE_SCOPE with the group in its suffix, so the only way to ask
		// "what does this group hold" is to scan the index and filter.
		let range = keyspace_inner_range(GroupId::NODE_SCOPE, Keyspace::ACTIVITY_INDEX);
		let mut buckets: Vec<u64> = txn
			.state_range(operator, range, None)
			.unwrap()
			.items
			.iter()
			.filter_map(|item| {
				let decoded = OperatorGroupStateKey::decode(&item.key)?;
				let (bucket, found) = decode_activity_suffix(&decoded.suffix)?;
				(found == id).then_some(bucket)
			})
			.collect();
		buckets.sort_unstable();
		buckets
	}

	fn side_buckets_of(txn: &mut FlowTransaction, operator: OperatorId, id: GroupId, side: Keyspace) -> Vec<u64> {
		let range = keyspace_inner_range(GroupId::NODE_SCOPE, Keyspace::SIDE_ACTIVITY_INDEX);
		let mut buckets: Vec<u64> = txn
			.state_range(operator, range, None)
			.unwrap()
			.items
			.iter()
			.filter_map(|item| {
				let decoded = OperatorGroupStateKey::decode(&item.key)?;
				let (found_side, bucket, found) = decode_side_suffix(&decoded.suffix)?;
				(found == id && found_side == side).then_some(bucket)
			})
			.collect();
		buckets.sort_unstable();
		buckets
	}

	#[test]
	fn a_group_active_across_many_buckets_keeps_only_its_newest_activity_entry() {
		// The index is a per-group cell, not an append-only heartbeat log. Leaving the superseded
		// entry behind makes the index grow with UPTIME rather than with group count: measured on
		// raptor, 16 quote mints held 4,005 rows because each wrote one row per 625ms bucket and
		// nothing removed the previous one. Only the newest entry carries information, since
		// due_in decides an entry is live by comparing it against GroupRecord::activity_bucket.
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let busy = group("busy");

		let (id, _) = intern_at(&interner, NODE, &mut txn, &busy, Position(DateTime::from_nanos(50))).unwrap();
		for position in [150, 250, 350, 450, 550] {
			intern_at(&interner, NODE, &mut txn, &busy, Position(DateTime::from_nanos(position))).unwrap();
		}

		assert_eq!(
			activity_buckets_of(&mut txn, NODE, id),
			vec![5],
			"six buckets of activity must leave exactly one entry, at the newest bucket"
		);
	}

	#[test]
	fn a_side_stamped_across_many_buckets_keeps_only_its_newest_entry() {
		// stamp_side has the same shape as stamp and the same defect; on raptor it accounted for a
		// further 27,755 rows. It must be fixed with its sibling or the leak simply moves.
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);

		let (id, _) = intern_at(&interner, NODE, &mut txn, &group("keyed"), Position(DateTime::from_nanos(50)))
			.unwrap();
		for position in [150, 250, 350] {
			stamp_side_at(
				&interner,
				&mut txn,
				id,
				Keyspace::JOIN_LEFT,
				Position(DateTime::from_nanos(position)),
			);
		}

		assert_eq!(
			side_buckets_of(&mut txn, NODE, id, Keyspace::JOIN_LEFT),
			vec![3],
			"only the newest side bucket may survive"
		);
	}

	#[test]
	fn two_sides_of_one_group_do_not_evict_each_others_activity_entries() {
		// The side index is keyed by (side, bucket, group), so a removal that ignored the side
		// would let a left stamp erase the right side's entry and strand right rows past their
		// own horizon.
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);

		let (id, _) = intern_at(&interner, NODE, &mut txn, &group("keyed"), Position(DateTime::from_nanos(50)))
			.unwrap();
		// Both sides must cross a bucket, and cross to different ones: if only one side ever
		// transitions, a removal that ignored `side` and always addressed the other would be a
		// no-op and the test would pass while the bug is present.
		stamp_side_at(&interner, &mut txn, id, Keyspace::JOIN_LEFT, Position(DateTime::from_nanos(150)));
		stamp_side_at(&interner, &mut txn, id, Keyspace::JOIN_RIGHT, Position(DateTime::from_nanos(150)));
		stamp_side_at(&interner, &mut txn, id, Keyspace::JOIN_LEFT, Position(DateTime::from_nanos(250)));
		stamp_side_at(&interner, &mut txn, id, Keyspace::JOIN_RIGHT, Position(DateTime::from_nanos(350)));

		assert_eq!(
			side_buckets_of(&mut txn, NODE, id, Keyspace::JOIN_LEFT),
			vec![2],
			"the left side must retire its own bucket 1 entry and keep only bucket 2"
		);
		assert_eq!(
			side_buckets_of(&mut txn, NODE, id, Keyspace::JOIN_RIGHT),
			vec![3],
			"and the right side must retire its own, not have the left side retire it for them"
		);
	}

	#[test]
	fn stamping_an_older_bucket_writes_nothing_and_keeps_the_newest_entry() {
		// The removal must sit AFTER stamp's `effective != bucket` guard. Ahead of it, an older
		// bucket would delete the live entry and the group would vanish from due_in entirely -
		// reclaimable state that no sweep can ever find again.
		// stamp is called directly because nothing reaches this guard through intern_many: its
		// cache check (`bucket > interned.bucket`) short-circuits first, and hydrate_once warms
		// that cache on restart, so the guard is defence in depth rather than a live path.
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let busy = group("busy");
		let (id, _) = intern_at(&interner, NODE, &mut txn, &busy, Position(DateTime::from_nanos(950))).unwrap();

		let now = txn.clock().now();
		let effective = GroupInterner::stamp(&mut txn, NODE, id, &busy, 0, now).unwrap();

		assert_eq!(effective, 9, "the stored bucket outranks the older one and is reported back");
		assert_eq!(
			activity_buckets_of(&mut txn, NODE, id),
			vec![9],
			"the stale stamp must neither add an entry nor remove the live one"
		);
		assert_eq!(
			interner.due_groups(NODE, &mut txn, Cutoff(DateTime::from_nanos(1_000)), 10).unwrap(),
			vec![id],
			"and the surviving entry must still be the one due_in finds"
		);
	}

	#[test]
	fn a_group_on_an_undeclared_grid_never_transitions_so_pays_no_removal() {
		// The whole cost of this change is one removal per bucket crossing. An undeclared scale
		// resolves every position to a single bucket, so operators with no declared retention must
		// cross nothing and keep their single entry - this is what makes the fix self-gating and
		// removes any need to branch on the ttl declaration shape.
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		interner.set_activity_grid(NODE, None);
		let mut txn = deferred(&engine);
		let busy = group("busy");

		let (id, _) = intern_at(&interner, NODE, &mut txn, &busy, Position(DateTime::from_nanos(50))).unwrap();
		for position in [10_000, 20_000, 30_000] {
			intern_at(&interner, NODE, &mut txn, &busy, Position(DateTime::from_nanos(position))).unwrap();
		}

		assert_eq!(
			activity_buckets_of(&mut txn, NODE, id),
			vec![0],
			"an undeclared grid has one bucket, so every touch lands on it"
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
	fn a_forgotten_side_that_is_stamped_again_writes_its_record_back() {
		// stamp_side answers from a RAM cache of the bucket it last wrote, so a side whose record
		// was deleted has to leave that cache too. Otherwise the re-stamp reads as already-done,
		// the record never returns, and no later sweep can ever retire that side's rows.
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let (id, _) =
			intern_at(&interner, NODE, &mut txn, &group("k"), Position(DateTime::from_nanos(150))).unwrap();

		stamp_side_at(&interner, &mut txn, id, Keyspace::JOIN_LEFT, Position(DateTime::from_nanos(150)));
		assert!(txn.state_get(NODE, &side_record_key(id, Keyspace::JOIN_LEFT)).unwrap().is_some());

		interner.forget_side(NODE, &mut txn, id, Keyspace::JOIN_LEFT).unwrap();
		assert!(txn.state_get(NODE, &side_record_key(id, Keyspace::JOIN_LEFT)).unwrap().is_none());

		stamp_side_at(&interner, &mut txn, id, Keyspace::JOIN_LEFT, Position(DateTime::from_nanos(150)));

		assert!(
			txn.state_get(NODE, &side_record_key(id, Keyspace::JOIN_LEFT)).unwrap().is_some(),
			"restamping at the same bucket must rewrite the record the forget removed"
		);
	}

	#[test]
	fn forgetting_a_group_drops_the_side_buckets_it_had_cached() {
		// Ids are never reused, so a leftover entry cannot mis-answer a later stamp - but it is
		// never read again either, and the cache would grow with every group the sweep retires.
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		let key = group("k");
		let (id, _) = intern_at(&interner, NODE, &mut txn, &key, Position(DateTime::from_nanos(150))).unwrap();

		stamp_side_at(&interner, &mut txn, id, Keyspace::JOIN_LEFT, Position(DateTime::from_nanos(150)));
		stamp_side_at(&interner, &mut txn, id, Keyspace::JOIN_RIGHT, Position(DateTime::from_nanos(150)));
		assert_eq!(interner.inner.operators.get(&NODE).unwrap().side_now.len(), 2);

		interner.forget(NODE, &mut txn, &key).unwrap();

		assert!(
			interner.inner.operators.get(&NODE).unwrap().side_now.is_empty(),
			"a forgotten group must leave no side buckets behind for either of its sides"
		);
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
			panic!("an unconfigured operator declares no domain to bucket in");
		};
		assert_eq!(wide_grid.width(), 1_000, "an unconfigured operator keeps the default");
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
			"the wide operator's group is still inside its first bucket"
		);
		assert_eq!(
			interner.due_groups(OperatorId(2), &mut txn, Cutoff(DateTime::from_millis(999)), 10).unwrap(),
			vec![narrow],
			"the narrow operator's group has cleared several of its own buckets by the same cutoff"
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

		assert_eq!(first, second, "each operator numbers its own groups from the same starting point");

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
			"a group interned on one operator must not resolve on another"
		);
		assert_ne!(other, first);
	}
}

impl FlowTransaction {
	pub fn intern_group(&mut self, operator: OperatorId, group: &EncodedKey) -> Result<(GroupId, bool)> {
		let interner = self.group_interner();
		let (id, is_new) = interner.intern(operator, self, group)?;
		if is_new {
			self.row_numbers().mark_fresh(operator, id);
		}
		Ok((id, is_new))
	}

	pub fn intern_groups(&mut self, operator: OperatorId, groups: &[EncodedKey]) -> Result<Vec<(GroupId, bool)>> {
		let interner = self.group_interner();
		let results = interner.intern_many(operator, self, groups)?;
		let provider = self.row_numbers();
		for (id, is_new) in &results {
			if *is_new {
				provider.mark_fresh(operator, *id);
			}
		}
		Ok(results)
	}

	pub fn due_groups(&mut self, operator: OperatorId, cutoff: Cutoff, limit: usize) -> Result<Vec<GroupId>> {
		let interner = self.group_interner();
		interner.due_groups(operator, self, cutoff, limit)
	}

	pub fn due_identity_groups(
		&mut self,
		operator: OperatorId,
		cutoff: Cutoff,
		limit: usize,
	) -> Result<Vec<GroupId>> {
		let interner = self.group_interner();
		interner.due_identity_groups(operator, self, cutoff, limit)
	}

	pub fn stamp_side(&mut self, operator: OperatorId, id: GroupId, side: Keyspace) -> Result<()> {
		let interner = self.group_interner();
		interner.stamp_side(operator, self, id, side)
	}

	pub fn due_side_groups(
		&mut self,
		operator: OperatorId,
		side: Keyspace,
		cutoff: Cutoff,
		limit: usize,
	) -> Result<Vec<GroupId>> {
		let interner = self.group_interner();
		interner.due_side_groups(operator, self, side, cutoff, limit)
	}

	pub fn forget_side(&mut self, operator: OperatorId, id: GroupId, side: Keyspace) -> Result<()> {
		let interner = self.group_interner();
		interner.forget_side(operator, self, id, side)
	}

	pub fn restamp_side(
		&mut self,
		operator: OperatorId,
		id: GroupId,
		side: Keyspace,
		oldest: DateTime,
	) -> Result<()> {
		let interner = self.group_interner();
		interner.restamp_side(operator, self, id, side, oldest)
	}

	pub fn node_position(&mut self, operator: OperatorId) -> Result<Position> {
		let interner = self.group_interner();
		interner.position(operator, self)
	}

	pub fn defer_group(&mut self, operator: OperatorId, id: GroupId) -> Result<bool> {
		let interner = self.group_interner();
		interner.defer(operator, self, id)
	}

	pub fn lookup_group(&mut self, operator: OperatorId, group: &EncodedKey) -> Result<Option<GroupId>> {
		let interner = self.group_interner();
		interner.lookup(operator, self, group)
	}

	pub fn forget_group(&mut self, operator: OperatorId, group: &EncodedKey) -> Result<bool> {
		let interner = self.group_interner();
		interner.forget(operator, self, group)
	}

	pub fn group_bytes(&mut self, operator: OperatorId, id: GroupId) -> Result<Option<EncodedKey>> {
		let interner = self.group_interner();
		interner.group_bytes(operator, self, id)
	}
}
