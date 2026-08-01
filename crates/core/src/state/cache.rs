// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	hash::Hash,
	mem,
	sync::Arc,
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	state::{OperatorState, SealMutableState, StateBytes, decode_state},
};
use reifydb_runtime::cache::slab::SlabLru;
use reifydb_value::{
	Result, byte_size::ByteSize, count::Count, reifydb_assertions, util::hash::xxh3_64_hashable,
	value::datetime::DateTime,
};
use rkyv::seal::Seal;

use crate::{
	key::operator_group_state::{GroupSet, GroupStateKey, IntoGroupStateKey, group_data_of_inner},
	metrics::heap::{HeapSize, StateCompleteness, StateMemory},
	state::{
		budget::OperatorStateBudgetHandle,
		membership::{MEMBERSHIP_BYTE_CAP, MembershipAnswer, MembershipTracker},
		store::StateStore,
	},
};

const ENTRY_OVERHEAD: u64 = (mem::size_of::<usize>() * 2) as u64;

fn membership_hash<K: Hash>(key: &K) -> u64 {
	xxh3_64_hashable(key).0
}

#[derive(Clone, Copy)]
enum Presence {
	Live,
	New,
	Unknown,
}

enum CleanEntry<V> {
	Archived(StateBytes),
	Native(Arc<V>),
}

impl<V> Clone for CleanEntry<V> {
	fn clone(&self) -> Self {
		match self {
			CleanEntry::Archived(bytes) => CleanEntry::Archived(bytes.clone()),
			CleanEntry::Native(arc) => CleanEntry::Native(arc.clone()),
		}
	}
}

enum DirtyEntry<V> {
	Live(Arc<V>),
	LiveArchived(StateBytes),
	Removed,
}

pub enum StateView<'a, V: OperatorState> {
	Archived(&'a V::Archived),
	Native(&'a V),
}

#[derive(Clone, Copy, Debug, Default)]
struct CacheLedger {
	clean: u64,
	dirty: u64,
}

impl CacheLedger {
	fn charge_clean(&mut self, bytes: u64) {
		self.clean = self.clean.saturating_add(bytes);
	}

	fn release_clean(&mut self, bytes: u64) {
		reifydb_assertions! {
			assert!(
				self.clean >= bytes,
				"state cache clean ledger released below zero: held={} released={}",
				self.clean,
				bytes
			);
		}
		self.clean = self.clean.saturating_sub(bytes);
	}

	fn charge_dirty(&mut self, bytes: u64) {
		self.dirty = self.dirty.saturating_add(bytes);
	}

	fn release_dirty(&mut self, bytes: u64) {
		reifydb_assertions! {
			assert!(
				self.dirty >= bytes,
				"state cache dirty ledger released below zero: held={} released={}",
				self.dirty,
				bytes
			);
		}
		self.dirty = self.dirty.saturating_sub(bytes);
	}
}

pub struct StateCache<K, V> {
	clean: SlabLru<K, CleanEntry<V>>,
	dirty: HashMap<K, DirtyEntry<V>>,
	dirty_order: Vec<K>,
	dirty_bytes: HashMap<K, u64>,
	ledger: CacheLedger,
	pool: OperatorStateBudgetHandle,
	seal_copies: u64,
	values_complete: bool,
	membership: MembershipTracker,
	completeness_revocations: u64,
}

fn native_charge<V: HeapSize>(value: &V) -> u64 {
	(mem::size_of::<V>() + value.heap_size()) as u64 + ENTRY_OVERHEAD
}

fn archived_charge(bytes: &StateBytes) -> u64 {
	bytes.byte_size().as_bytes() + ENTRY_OVERHEAD
}

fn key_charge<K: HeapSize>(key: &K) -> u64 {
	(mem::size_of::<K>() + key.heap_size()) as u64
}

impl<K, V> StateCache<K, V>
where
	K: Hash + Eq + Clone + HeapSize,
	for<'a> &'a K: IntoGroupStateKey,
	V: Clone + OperatorState + HeapSize,
{
	pub fn new(pool: OperatorStateBudgetHandle) -> Self {
		Self {
			clean: SlabLru::unbounded(),
			dirty: HashMap::new(),
			dirty_order: Vec::new(),
			dirty_bytes: HashMap::new(),
			ledger: CacheLedger::default(),
			pool,
			seal_copies: 0,
			values_complete: false,
			membership: MembershipTracker::new(MEMBERSHIP_BYTE_CAP),
			completeness_revocations: 0,
		}
	}

	fn revoke_values_complete(&mut self) {
		if self.values_complete {
			self.values_complete = false;
			self.completeness_revocations += 1;
		}
	}

	fn try_restore_values_complete(&mut self) {
		if self.values_complete || !self.dirty.is_empty() {
			return;
		}
		if self.membership.population() == Some(self.clean.len() as u64) {
			self.values_complete = true;
		}
	}

	fn miss_proves_absence(&mut self, key: &K) -> bool {
		if self.values_complete {
			return true;
		}
		matches!(self.membership.probe(membership_hash(key)), MembershipAnswer::DefinitelyAbsent)
	}

	fn record_store_miss(&mut self) {
		self.membership.record_store_miss();
	}

	fn membership_insert(&mut self, key: &K) {
		self.membership.insert(membership_hash(key));
	}

	fn membership_remove(&mut self, key: &K) {
		self.membership.remove(membership_hash(key));
	}

	fn live_before_write(&mut self, store: &mut impl StateStore, key: &K) -> Result<bool> {
		if let Some(entry) = self.dirty.get(key) {
			return Ok(!matches!(entry, DirtyEntry::Removed));
		}
		if self.clean.contains_key(key) {
			return Ok(true);
		}
		if self.values_complete {
			return Ok(false);
		}
		if self.membership.contains(membership_hash(key)) == Some(false) {
			return Ok(false);
		}
		let encoded_key = key.into_group_state_key();
		let loaded = store.state_get(&encoded_key)?;
		Ok(loaded.is_some())
	}

	fn note_write(
		&mut self,
		store: &mut impl StateStore,
		key: &K,
		is_drop: bool,
		presence: Presence,
	) -> Result<()> {
		if !self.membership.is_tracked() {
			return Ok(());
		}
		let live = match presence {
			Presence::Live => true,
			Presence::New => false,
			Presence::Unknown => self.live_before_write(store, key)?,
		};
		match (is_drop, live) {
			(false, false) => self.membership_insert(key),
			(true, true) => self.membership_remove(key),
			_ => {}
		}
		Ok(())
	}

	fn get_arc(&mut self, store: &mut impl StateStore, key: &K) -> Result<Option<Arc<V>>> {
		if let Some(slot) = self.dirty.get(key) {
			return match slot {
				DirtyEntry::Live(arc) => Ok(Some(arc.clone())),
				DirtyEntry::LiveArchived(bytes) => {
					// SAFETY: LiveArchived bytes were validated at insertion.
					let archived = unsafe { V::archived_trusted(bytes) };
					Ok(Some(Arc::new(V::materialize(archived)?)))
				}
				DirtyEntry::Removed => Ok(None),
			};
		}

		if self.clean.contains_key(key) {
			return Ok(Some(self.promote(key)?));
		}

		if self.miss_proves_absence(key) {
			return Ok(None);
		}

		let encoded_key = key.into_group_state_key();
		let loaded = store.state_get(&encoded_key)?;
		match loaded {
			Some(bytes) => {
				let value = decode_state::<V>(&bytes)?;
				let arc = Arc::new(value);
				self.insert_clean_native(key.clone(), arc.clone());
				self.evict_to_budget();
				self.try_restore_values_complete();
				Ok(Some(arc))
			}
			None => {
				self.record_store_miss();
				Ok(None)
			}
		}
	}

	fn promote(&mut self, key: &K) -> Result<Arc<V>> {
		let entry = self.clean.get(key).expect("promote called for a resident clean key");
		match entry {
			CleanEntry::Native(arc) => Ok(arc),
			CleanEntry::Archived(bytes) => {
				// SAFETY: every clean Archived entry was validated by V::archived at insertion.
				let archived = unsafe { V::archived_trusted(&bytes) };
				let value = V::materialize(archived)?;
				let arc = Arc::new(value);
				self.insert_clean_native(key.clone(), arc.clone());
				self.evict_to_budget();
				Ok(arc)
			}
		}
	}

	fn insert_clean_native(&mut self, key: K, arc: Arc<V>) {
		let key_bytes = key_charge(&key);
		let charge = native_charge(arc.as_ref()) + key_bytes;
		if let Some(old) = self.clean.put(key, CleanEntry::Native(arc)) {
			self.release_clean_entry(key_bytes, &old);
		}
		self.ledger.charge_clean(charge);
		self.pool.charge_clean(ByteSize::from_bytes(charge));
	}

	fn insert_clean_archived(&mut self, key: K, bytes: StateBytes) {
		let key_bytes = key_charge(&key);
		let charge = archived_charge(&bytes) + key_bytes;
		if let Some(old) = self.clean.put(key, CleanEntry::Archived(bytes)) {
			self.release_clean_entry(key_bytes, &old);
		}
		self.ledger.charge_clean(charge);
		self.pool.charge_clean(ByteSize::from_bytes(charge));
	}

	fn release_clean_entry(&mut self, key_bytes: u64, entry: &CleanEntry<V>) {
		let bytes = key_bytes
			+ match entry {
				CleanEntry::Archived(b) => archived_charge(b),
				CleanEntry::Native(arc) => native_charge(arc.as_ref()),
			};
		self.ledger.release_clean(bytes);
		self.pool.release_clean(ByteSize::from_bytes(bytes));
	}

	fn insert_dirty(
		&mut self,
		store: &mut impl StateStore,
		key: K,
		entry: DirtyEntry<V>,
		presence: Presence,
	) -> Result<()> {
		self.note_write(store, &key, matches!(entry, DirtyEntry::Removed), presence)?;
		let key_bytes = key_charge(&key);
		if let Some(old) = self.clean.remove(&key) {
			self.release_clean_entry(key_bytes, &old);
		}
		let charge = key_bytes
			+ match &entry {
				DirtyEntry::Live(arc) => native_charge(arc.as_ref()),
				DirtyEntry::LiveArchived(bytes) => archived_charge(bytes),
				DirtyEntry::Removed => ENTRY_OVERHEAD,
			};
		if let Some(previous) = self.dirty_bytes.insert(key.clone(), charge) {
			self.ledger.release_dirty(previous);
			self.pool.release_dirty(ByteSize::from_bytes(previous));
		}
		if self.dirty.insert(key.clone(), entry).is_none() {
			self.dirty_order.push(key);
		}
		self.ledger.charge_dirty(charge);
		self.pool.charge_dirty(ByteSize::from_bytes(charge));
		self.evict_to_budget();
		Ok(())
	}

	fn evict_to_budget(&mut self) {
		while self.pool.over_budget() {
			let Some((key, entry)) = self.clean.pop_tail() else {
				break;
			};
			self.revoke_values_complete();
			let bytes = key_charge(&key)
				+ match &entry {
					CleanEntry::Archived(b) => archived_charge(b),
					CleanEntry::Native(arc) => native_charge(arc.as_ref()),
				};
			self.ledger.release_clean(bytes);
			self.pool.release_clean(ByteSize::from_bytes(bytes));
			self.pool.record_eviction(ByteSize::from_bytes(bytes));
		}
	}

	pub fn get(&mut self, store: &mut impl StateStore, key: &K) -> Result<Option<V>> {
		Ok(self.get_arc(store, key)?.map(|arc| (*arc).clone()))
	}

	pub fn read<R>(
		&mut self,
		store: &mut impl StateStore,
		key: &K,
		f: impl FnOnce(StateView<'_, V>) -> R,
	) -> Result<Option<R>> {
		if let Some(slot) = self.dirty.get(key) {
			return Ok(match slot {
				DirtyEntry::Live(arc) => Some(f(StateView::Native(arc.as_ref()))),
				DirtyEntry::LiveArchived(bytes) => {
					// SAFETY: LiveArchived bytes were validated at insertion.
					let archived = unsafe { V::archived_trusted(bytes) };
					Some(f(StateView::Archived(archived)))
				}
				DirtyEntry::Removed => None,
			});
		}

		if let Some(entry) = self.clean.get(key) {
			return Ok(Some(match &entry {
				CleanEntry::Native(arc) => f(StateView::Native(arc.as_ref())),
				CleanEntry::Archived(bytes) => {
					// SAFETY: every clean Archived entry was validated by V::archived at insertion.
					let archived = unsafe { V::archived_trusted(bytes) };
					f(StateView::Archived(archived))
				}
			}));
		}

		if self.miss_proves_absence(key) {
			return Ok(None);
		}

		let encoded_key = key.into_group_state_key();
		let loaded = store.state_get(&encoded_key)?;
		let Some(bytes) = loaded else {
			self.record_store_miss();
			return Ok(None);
		};
		let result = f(StateView::Archived(V::archived(&bytes)?));
		self.insert_clean_archived(key.clone(), bytes);
		self.evict_to_budget();
		self.try_restore_values_complete();
		Ok(Some(result))
	}

	pub fn take(&mut self, store: &mut impl StateStore, key: &K) -> Result<Option<V>> {
		self.take_owned(store, key)
	}

	fn take_owned(&mut self, store: &mut impl StateStore, key: &K) -> Result<Option<V>> {
		if let Some(slot) = self.dirty.get(key) {
			return match slot {
				DirtyEntry::Live(arc) => Ok(Some((**arc).clone())),
				DirtyEntry::LiveArchived(bytes) => {
					// SAFETY: LiveArchived bytes were validated at insertion.
					let archived = unsafe { V::archived_trusted(bytes) };
					Ok(Some(V::materialize(archived)?))
				}
				DirtyEntry::Removed => Ok(None),
			};
		}

		if let Some(entry) = self.clean.remove(key) {
			self.release_clean_entry(key_charge(key), &entry);
			self.revoke_values_complete();
			return Ok(Some(match entry {
				CleanEntry::Native(arc) => Arc::try_unwrap(arc).unwrap_or_else(|arc| (*arc).clone()),
				CleanEntry::Archived(bytes) => {
					// SAFETY: every clean Archived entry was validated by V::archived at insertion.
					let archived = unsafe { V::archived_trusted(&bytes) };
					V::materialize(archived)?
				}
			}));
		}

		if self.miss_proves_absence(key) {
			return Ok(None);
		}

		let encoded_key = key.into_group_state_key();
		let loaded = store.state_get(&encoded_key)?;
		match loaded {
			Some(bytes) => Ok(Some(decode_state::<V>(&bytes)?)),
			None => {
				self.record_store_miss();
				Ok(None)
			}
		}
	}

	pub fn warm(&mut self, store: &mut impl StateStore, keys: &[K]) -> Result<()> {
		let mut to_load: Vec<K> = Vec::new();
		for key in keys {
			if self.clean.contains_key(key) || self.dirty.contains_key(key) {
				continue;
			}
			to_load.push(key.clone());
		}
		if to_load.is_empty() || self.values_complete {
			return Ok(());
		}
		if self.membership.is_tracked() {
			let requested = to_load.len();
			to_load.retain(|key| self.membership.contains(membership_hash(key)) == Some(true));
			self.membership.count_absences((requested - to_load.len()) as u64);
			if to_load.is_empty() {
				return Ok(());
			}
		}

		let mut by_encoded: HashMap<Vec<u8>, K> = HashMap::with_capacity(to_load.len());
		let mut encoded_keys: Vec<GroupStateKey> = Vec::with_capacity(to_load.len());
		for key in &to_load {
			let encoded = key.into_group_state_key();
			by_encoded.insert(encoded.as_slice().to_vec(), key.clone());
			encoded_keys.push(encoded);
		}

		let mut loaded: Vec<(K, StateBytes)> = Vec::new();
		let mut visit = |encoded: GroupStateKey, bytes: StateBytes| -> Result<()> {
			if let Some(key) = by_encoded.get(encoded.as_slice()) {
				V::archived(&bytes)?;
				loaded.push((key.clone(), bytes));
			}
			Ok(())
		};
		store.state_get_many_visit(&encoded_keys, &mut visit)?;
		for (key, bytes) in loaded {
			self.insert_clean_archived(key, bytes);
		}
		self.evict_to_budget();
		self.try_restore_values_complete();
		Ok(())
	}

	pub fn hydrate(
		&mut self,
		store: &mut impl StateStore,
		range: EncodedKeyRange,
		decode_key: impl Fn(&EncodedKey) -> Option<K>,
	) -> Result<()> {
		if self.values_complete {
			return Ok(());
		}
		let mut loaded: Vec<(K, StateBytes)> = Vec::new();
		store.state_range_visit(range, None, &mut |encoded, bytes| {
			if let Some(key) = decode_key(encoded.as_encoded()) {
				V::archived(&bytes)?;
				loaded.push((key, bytes));
			}
			Ok(())
		})?;
		self.membership.reset_with_capacity(loaded.len() + self.dirty.len());
		for (key, _) in &loaded {
			self.membership.insert(membership_hash(key));
		}
		if !self.dirty.is_empty() {
			let scanned: HashSet<&K> = loaded.iter().map(|(key, _)| key).collect();
			for (key, entry) in &self.dirty {
				match entry {
					DirtyEntry::Removed => {
						if scanned.contains(key) {
							self.membership.remove(membership_hash(key));
						}
					}
					_ => {
						if !scanned.contains(key) {
							self.membership.insert(membership_hash(key));
						}
					}
				}
			}
		}
		for (key, bytes) in loaded {
			if self.dirty.contains_key(&key) || self.clean.contains_key(&key) {
				continue;
			}
			self.insert_clean_archived(key, bytes);
		}
		self.values_complete = true;
		self.evict_to_budget();
		Ok(())
	}

	pub fn set(&mut self, store: &mut impl StateStore, key: &K, value: &V) -> Result<()> {
		self.insert_dirty(store, key.clone(), DirtyEntry::Live(Arc::new(value.clone())), Presence::Unknown)
	}

	pub fn put(&mut self, store: &mut impl StateStore, key: &K, value: V) -> Result<()> {
		self.insert_dirty(store, key.clone(), DirtyEntry::Live(Arc::new(value)), Presence::Unknown)
	}

	fn insert_native_modified<R>(
		&mut self,
		store: &mut impl StateStore,
		key: &K,
		mut value: V,
		native_f: impl FnOnce(&mut V) -> R,
		presence: Presence,
	) -> Result<R> {
		let result = native_f(&mut value);
		self.insert_dirty(store, key.clone(), DirtyEntry::Live(Arc::new(value)), presence)?;
		Ok(result)
	}

	pub fn modify_in_place<R, A>(
		&mut self,
		store: &mut impl StateStore,
		key: &K,
		seal_f: impl FnOnce(Seal<'_, A>) -> Option<R>,
		native_f: impl FnOnce(&mut V) -> R,
	) -> Result<R>
	where
		V: SealMutableState + Default + OperatorState<Archived = A>,
	{
		if let Some(slot) = self.dirty.get_mut(key) {
			match slot {
				DirtyEntry::LiveArchived(bytes) => {
					if bytes.row().0.is_shared() {
						self.seal_copies += 1;
					}
					// SAFETY: LiveArchived bytes were validated at insertion.
					let seal = unsafe { V::archived_seal_trusted(bytes) };
					if let Some(result) = seal_f(seal) {
						return Ok(result);
					}
					// SAFETY: LiveArchived bytes were validated at insertion.
					let archived = unsafe { V::archived_trusted(bytes) };
					let value = V::materialize(archived)?;
					return self.insert_native_modified(
						store,
						key,
						value,
						native_f,
						Presence::Live,
					);
				}
				DirtyEntry::Live(arc) => {
					let value = (**arc).clone();
					return self.insert_native_modified(
						store,
						key,
						value,
						native_f,
						Presence::Live,
					);
				}
				DirtyEntry::Removed => {
					return self.insert_native_modified(
						store,
						key,
						V::default(),
						native_f,
						Presence::New,
					);
				}
			}
		}

		if let Some(entry) = self.clean.remove(key) {
			self.release_clean_entry(key_charge(key), &entry);
			match entry {
				CleanEntry::Archived(mut bytes) => {
					if bytes.row().0.is_shared() {
						self.seal_copies += 1;
					}
					// SAFETY: every Archived entry was validated by V::archived at insertion.
					let seal = unsafe { V::archived_seal_trusted(&mut bytes) };
					if let Some(result) = seal_f(seal) {
						self.insert_dirty(
							store,
							key.clone(),
							DirtyEntry::LiveArchived(bytes),
							Presence::Live,
						)?;
						return Ok(result);
					}
					// SAFETY: every Archived entry was validated by V::archived at insertion.
					let archived = unsafe { V::archived_trusted(&bytes) };
					let value = V::materialize(archived)?;
					return self.insert_native_modified(
						store,
						key,
						value,
						native_f,
						Presence::Live,
					);
				}
				CleanEntry::Native(arc) => {
					let value = Arc::try_unwrap(arc).unwrap_or_else(|arc| (*arc).clone());
					return self.insert_native_modified(
						store,
						key,
						value,
						native_f,
						Presence::Live,
					);
				}
			}
		}

		if self.miss_proves_absence(key) {
			return self.insert_native_modified(store, key, V::default(), native_f, Presence::New);
		}

		let encoded_key = key.into_group_state_key();
		let loaded = store.state_get(&encoded_key)?;
		match loaded {
			Some(mut bytes) => {
				V::archived(&bytes)?;
				if bytes.row().0.is_shared() {
					self.seal_copies += 1;
				}
				// SAFETY: bytes passed V::archived validation above.
				let seal = unsafe { V::archived_seal_trusted(&mut bytes) };
				if let Some(result) = seal_f(seal) {
					self.insert_dirty(
						store,
						key.clone(),
						DirtyEntry::LiveArchived(bytes),
						Presence::Live,
					)?;
					return Ok(result);
				}
				// SAFETY: bytes passed V::archived validation above.
				let archived = unsafe { V::archived_trusted(&bytes) };
				let value = V::materialize(archived)?;
				self.insert_native_modified(store, key, value, native_f, Presence::Live)
			}
			None => {
				self.record_store_miss();
				self.insert_native_modified(store, key, V::default(), native_f, Presence::New)
			}
		}
	}

	pub fn seal_copies(&self) -> Count {
		Count::new(self.seal_copies)
	}

	pub fn remove(&mut self, store: &mut impl StateStore, key: &K) -> Result<()> {
		self.insert_dirty(store, key.clone(), DirtyEntry::Removed, Presence::Unknown)
	}

	pub fn flush(&mut self, store: &mut impl StateStore) -> Result<()> {
		let order = mem::take(&mut self.dirty_order);
		let now = store.clock_now();
		for (index, key) in order.iter().enumerate() {
			let Some(mut slot) = self.dirty.remove(key) else {
				continue;
			};
			let encoded_key = key.into_group_state_key();
			match Self::write_dirty_slot(store, &encoded_key, &mut slot, now) {
				Ok(()) => {
					self.release_flushed(key);
					match slot {
						DirtyEntry::Live(value) => {
							self.insert_clean_native(key.clone(), value);
						}
						DirtyEntry::LiveArchived(bytes) => {
							self.insert_clean_archived(key.clone(), bytes);
						}
						DirtyEntry::Removed => {}
					}
				}
				Err(error) => {
					self.dirty.insert(key.clone(), slot);
					self.dirty_order = order[index..].to_vec();
					return Err(error);
				}
			}
		}
		self.evict_to_budget();
		self.try_restore_values_complete();
		reifydb_assertions! {
			assert!(
				self.dirty.is_empty() && self.ledger.dirty == 0,
				"flush must drain every dirty entry and its ledger bytes (left={} bytes={})",
				self.dirty.len(),
				self.ledger.dirty
			);
		}
		Ok(())
	}

	fn write_dirty_slot(
		store: &mut impl StateStore,
		encoded_key: &GroupStateKey,
		slot: &mut DirtyEntry<V>,
		now: DateTime,
	) -> Result<()> {
		match slot {
			DirtyEntry::Live(value) => {
				let payload = value.encode_state(now)?;
				store.state_set(encoded_key, payload)
			}
			DirtyEntry::LiveArchived(bytes) => {
				bytes.refresh_updated_at(now);
				store.state_set(encoded_key, bytes.clone())
			}
			DirtyEntry::Removed => store.state_remove(encoded_key),
		}
	}

	fn release_flushed(&mut self, key: &K) {
		if let Some(bytes) = self.dirty_bytes.remove(key) {
			self.ledger.release_dirty(bytes);
			self.pool.release_dirty(ByteSize::from_bytes(bytes));
		}
	}

	pub fn clear_cache(&mut self) {
		self.revoke_values_complete();
		let released = self.ledger.clean;
		self.clean.clear();
		self.ledger.release_clean(released);
		self.pool.release_clean(ByteSize::from_bytes(released));
	}

	pub fn invalidate(&mut self, key: &K) {
		if let Some(entry) = self.clean.remove(key) {
			self.release_clean_entry(key_charge(key), &entry);
			self.revoke_values_complete();
		}
	}

	pub fn invalidate_group_data(&mut self, groups: &GroupSet) -> usize {
		if groups.is_empty() {
			return 0;
		}

		let selects = |key: &K| {
			group_data_of_inner(key.into_group_state_key().as_slice())
				.is_some_and(|group| groups.contains(group))
		};
		let clean: Vec<K> = self.clean.keys().filter(|key| selects(key)).cloned().collect();
		let dirty: HashSet<K> =
			self.dirty.iter().filter(|(key, _)| selects(key)).map(|(key, _)| key.clone()).collect();

		reifydb_assertions! {
			let written: Vec<&K> = dirty
				.iter()
				.filter(|key| !matches!(self.dirty.get(*key), Some(DirtyEntry::Removed)))
				.collect();
			assert!(
				written.is_empty(),
				"reclaiming {} group(s) found {} unflushed write(s): a group past its horizon must \
				 not have been written this batch, so either the horizon admitted a live group or \
				 the driver ran after the operator wrote (a pending tombstone is fine, a pending \
				 value is not)",
				groups.len(),
				written.len()
			);
		}

		let tracked = self.membership.is_tracked();
		for key in &clean {
			if let Some(entry) = self.clean.remove(key) {
				self.release_clean_entry(key_charge(key), &entry);
			}
			if tracked {
				self.membership_remove(key);
			}
		}
		for key in &dirty {
			let was_live = !matches!(self.dirty.remove(key), Some(DirtyEntry::Removed));
			self.release_flushed(key);
			if tracked && was_live {
				self.membership_remove(key);
			}
		}
		if !dirty.is_empty() {
			self.dirty_order.retain(|key| !dirty.contains(key));
		}

		clean.len() + dirty.len()
	}

	pub fn is_cached(&self, key: &K) -> bool {
		self.clean.contains_key(key)
			|| matches!(self.dirty.get(key), Some(DirtyEntry::Live(_) | DirtyEntry::LiveArchived(_)))
	}

	pub fn len(&self) -> usize {
		self.clean.len()
			+ self.dirty
				.values()
				.filter(|e| matches!(e, DirtyEntry::Live(_) | DirtyEntry::LiveArchived(_)))
				.count()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn capacity(&self) -> ByteSize {
		self.pool.snapshot().budget
	}

	pub fn approximate_memory(&self) -> StateMemory {
		StateMemory::new(
			Count::new((self.clean.len() + self.dirty.len()) as u64),
			ByteSize::from_bytes(
				self.ledger
					.clean
					.saturating_add(self.ledger.dirty)
					.saturating_add(self.clean.struct_bytes() as u64),
			),
		)
	}

	pub fn dirty_memory(&self) -> StateMemory {
		StateMemory::new(Count::new(self.dirty.len() as u64), ByteSize::from_bytes(self.ledger.dirty))
	}

	pub fn membership_memory(&self) -> StateMemory {
		self.membership.memory()
	}

	pub fn completeness(&self) -> StateCompleteness {
		StateCompleteness {
			values_complete: self.values_complete,
			membership_complete: self.membership.is_tracked(),
			absences_served: Count::new(self.membership.absences_served()),
			false_positives: Count::new(self.membership.false_positives()),
			revocations: Count::new(self.completeness_revocations),
		}
	}
}

impl<K, V> StateCache<K, V>
where
	K: Hash + Eq + Clone + HeapSize,
	for<'a> &'a K: IntoGroupStateKey,
	V: Clone + Default + OperatorState + HeapSize,
{
	pub fn get_or_default(&mut self, store: &mut impl StateStore, key: &K) -> Result<V> {
		match self.get(store, key)? {
			Some(value) => Ok(value),
			None => Ok(V::default()),
		}
	}

	pub fn update<U>(&mut self, store: &mut impl StateStore, key: &K, updater: U) -> Result<V>
	where
		U: FnOnce(&mut V) -> Result<()>,
	{
		let mut value = self.get_or_default(store, key)?;
		updater(&mut value)?;
		self.set(store, key, &value)?;
		Ok(value)
	}
}

impl<K, V> Drop for StateCache<K, V> {
	fn drop(&mut self) {
		self.pool.release_clean(ByteSize::from_bytes(self.ledger.clean));
		self.pool.release_dirty(ByteSize::from_bytes(self.ledger.dirty));
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::HashMap,
		hash::{Hash, Hasher},
		ops::Bound,
		sync::atomic::{AtomicUsize, Ordering},
	};

	use reifydb_abi::operator::timer::TimerKind;
	use reifydb_codec::key::encoded::EncodedKeyRange;
	use reifydb_macro::operator_state;
	use reifydb_value::{error::Error as ValueError, value::row_number::RowNumber};
	use rkyv::{munge::munge, primitive::ArchivedU64};
	use serde::{Deserialize, Serialize};

	use super::*;
	use crate::{
		error::diagnostic::flow::flow_error,
		key::operator_group_state::{
			GroupId, GroupStateKey, IntoGroupStateKey, Keyspace, OperatorGroupStateKey,
		},
	};

	/// A bare `String` would read as some other group's prefix; this frames the tests' string keys
	/// the way an operator does.
	#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
	struct Key(String);

	impl Key {
		fn new(key: impl Into<String>) -> Self {
			Self(key.into())
		}
	}

	impl HeapSize for Key {
		fn heap_size(&self) -> usize {
			self.0.capacity()
		}
	}

	impl IntoGroupStateKey for &Key {
		fn into_group_state_key(self) -> GroupStateKey {
			GroupStateKey::node_scoped(Keyspace::FIRST_CUSTOM, self.0.as_bytes())
		}
	}

	#[operator_state]
	#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
	struct Cell {
		value: i32,
	}

	impl HeapSize for Cell {
		fn heap_size(&self) -> usize {
			0
		}
	}

	fn cell(value: i32) -> Cell {
		Cell {
			value,
		}
	}

	fn pool_of(bytes: u64) -> OperatorStateBudgetHandle {
		OperatorStateBudgetHandle::new(ByteSize::from_bytes(bytes))
	}

	fn big_pool() -> OperatorStateBudgetHandle {
		pool_of(64 * 1024 * 1024)
	}

	fn skey(key: &Key) -> u64 {
		key_charge(key)
	}

	#[derive(Default)]
	struct MockStore {
		data: HashMap<Vec<u8>, StateBytes>,
		groups: HashMap<Vec<u8>, GroupId>,
		removes: usize,
		// The Nth state_set attempt (1-based) errors instead of writing; set_attempts
		// records every attempted key so retry ordering can be asserted.
		sets: usize,
		fail_state_set_at: Option<usize>,
		set_attempts: Vec<Vec<u8>>,
		gets: usize,
		// Settable flush clock (default epoch) so timestamp refresh is observable.
		now: DateTime,
	}

	impl StateStore for MockStore {
		fn arm_timer(&mut self, _at: DateTime, _kind: TimerKind, _key: &EncodedKey) -> Result<()> {
			unreachable!("the window engine never arms timers; only the shell above it does")
		}

		fn disarm_timer(&mut self, _at: DateTime, _kind: TimerKind, _key: &EncodedKey) -> Result<()> {
			unreachable!("the window engine never disarms timers; only the shell above it does")
		}

		fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
			Ok(None)
		}

		fn intern_group(&mut self, group: &EncodedKey) -> Result<GroupId> {
			let next = GroupId(self.groups.len() as u64 + GroupId::FIRST.0);
			Ok(*self.groups.entry(group.as_bytes().to_vec()).or_insert(next))
		}

		fn lookup_group(&mut self, group: &EncodedKey) -> Result<Option<GroupId>> {
			Ok(self.groups.get(group.as_bytes()).copied())
		}

		fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<StateBytes>> {
			self.gets += 1;
			Ok(self.data.get(key.as_slice()).cloned())
		}
		fn state_get_many_visit(
			&mut self,
			keys: &[GroupStateKey],
			visit: &mut dyn FnMut(GroupStateKey, StateBytes) -> Result<()>,
		) -> Result<()> {
			for key in keys {
				if let Some(b) = self.data.get(key.as_slice()) {
					visit(key.clone(), b.clone())?;
				}
			}
			Ok(())
		}
		fn state_set(&mut self, key: &GroupStateKey, payload: StateBytes) -> Result<()> {
			self.sets += 1;
			self.set_attempts.push(key.as_slice().to_vec());
			if self.fail_state_set_at == Some(self.sets) {
				return Err(ValueError(Box::new(flow_error("injected state_set failure".to_string()))));
			}
			self.data.insert(key.as_slice().to_vec(), payload);
			Ok(())
		}
		fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
			self.removes += 1;
			self.data.remove(key.as_slice());
			Ok(())
		}
		fn state_range_visit(
			&mut self,
			range: EncodedKeyRange,
			limit: Option<usize>,
			visit: &mut dyn FnMut(GroupStateKey, StateBytes) -> Result<()>,
		) -> Result<()> {
			let after_start = |k: &[u8]| match &range.start {
				Bound::Included(s) => k >= s.as_bytes(),
				Bound::Excluded(s) => k > s.as_bytes(),
				Bound::Unbounded => true,
			};
			let before_end = |k: &[u8]| match &range.end {
				Bound::Included(e) => k <= e.as_bytes(),
				Bound::Excluded(e) => k < e.as_bytes(),
				Bound::Unbounded => true,
			};
			let mut matched: Vec<(Vec<u8>, StateBytes)> = self
				.data
				.iter()
				.filter(|(k, _)| after_start(k) && before_end(k))
				.map(|(k, v)| (k.clone(), v.clone()))
				.collect();
			matched.sort_by(|a, b| a.0.cmp(&b.0));
			if let Some(limit) = limit {
				matched.truncate(limit);
			}
			for (k, b) in matched {
				let Some(k) = GroupStateKey::from_framed(EncodedKey::new(k)) else {
					continue;
				};
				visit(k, b)?;
			}
			Ok(())
		}
		fn get_or_create_row_number(
			&mut self,
			_group: GroupId,
			_key: &EncodedKey,
		) -> Result<(RowNumber, bool)> {
			Ok((RowNumber(1), true))
		}
		fn get_or_create_row_numbers(
			&mut self,
			_group: GroupId,
			keys: &[EncodedKey],
		) -> Result<Vec<(RowNumber, bool)>> {
			Ok(keys.iter().enumerate().map(|(i, _)| (RowNumber(i as u64 + 1), true)).collect())
		}
		fn remove_row_number(&mut self, _group: GroupId, _key: &EncodedKey) -> Result<()> {
			Ok(())
		}
		fn clock_now(&self) -> DateTime {
			self.now
		}
	}

	#[test]
	fn set_then_flush_persists_to_store_and_survives_cache_clear() {
		let mut store = MockStore::default();
		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());

		cache.set(&mut store, &Key::new("a"), &cell(7)).unwrap();
		assert_eq!(cache.get(&mut store, &Key::new("a")).unwrap(), Some(cell(7)));
		assert!(store.data.is_empty());

		cache.flush(&mut store).unwrap();
		assert!(!store.data.is_empty(), "flush must write dirty entries to the store");

		cache.clear_cache();
		assert_eq!(cache.get(&mut store, &Key::new("a")).unwrap(), Some(cell(7)));
	}

	#[test]
	fn warm_bulk_loads_present_keys_and_skips_absent() {
		let mut store = MockStore::default();
		{
			let mut seed: StateCache<Key, Cell> = StateCache::new(big_pool());
			seed.set(&mut store, &Key::new("a"), &cell(1)).unwrap();
			seed.set(&mut store, &Key::new("b"), &cell(2)).unwrap();
			seed.flush(&mut store).unwrap();
		}

		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());
		let keys = vec![Key::new("a"), Key::new("b"), Key::new("missing")];
		cache.warm(&mut store, &keys).unwrap();

		assert!(cache.is_cached(&Key::new("a")));
		assert!(cache.is_cached(&Key::new("b")));
		assert!(!cache.is_cached(&Key::new("missing")));
	}

	#[test]
	fn dirty_write_shadows_committed_value_during_warm() {
		let mut store = MockStore::default();
		{
			let mut seed: StateCache<Key, Cell> = StateCache::new(big_pool());
			seed.set(&mut store, &Key::new("a"), &cell(1)).unwrap();
			seed.flush(&mut store).unwrap();
		}

		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());
		cache.set(&mut store, &Key::new("a"), &cell(99)).unwrap();
		cache.warm(&mut store, &[Key::new("a")]).unwrap();
		assert_eq!(
			cache.get(&mut store, &Key::new("a")).unwrap(),
			Some(cell(99)),
			"pending write must shadow store"
		);
	}

	#[test]
	fn take_returns_value_and_evicts_it_from_cache() {
		// take() is the load half of load-mutate-persist: a clean copy left behind
		// would shadow the caller's write.
		let mut store = MockStore::default();
		{
			let mut seed: StateCache<Key, Cell> = StateCache::new(big_pool());
			seed.set(&mut store, &Key::new("a"), &cell(42)).unwrap();
			seed.flush(&mut store).unwrap();
		}

		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());
		cache.warm(&mut store, &[Key::new("a")]).unwrap();
		assert!(cache.is_cached(&Key::new("a")), "warm must populate the cache");

		let taken = cache.take(&mut store, &Key::new("a")).unwrap();
		assert_eq!(taken, Some(cell(42)), "take must return the stored value");
		assert!(!cache.is_cached(&Key::new("a")), "take must evict the entry from the cache");

		assert_eq!(cache.get(&mut store, &Key::new("a")).unwrap(), Some(cell(42)));
	}

	#[test]
	fn take_of_absent_key_is_none() {
		let mut store = MockStore::default();
		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());
		assert_eq!(cache.take(&mut store, &Key::new("missing")).unwrap(), None);
	}

	#[test]
	fn take_then_persist_round_trips_a_mutation() {
		// take + put must be a faithful read-modify-write: the mutation, not the
		// pre-image, is what lands in the store.
		let mut store = MockStore::default();
		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());
		cache.set(&mut store, &Key::new("a"), &cell(1)).unwrap();
		cache.flush(&mut store).unwrap();

		let mut value = cache.take(&mut store, &Key::new("a")).unwrap().unwrap_or_default();
		value.value += 40;
		cache.put(&mut store, &Key::new("a"), value).unwrap();
		cache.flush(&mut store).unwrap();

		cache.clear_cache();
		assert_eq!(cache.get(&mut store, &Key::new("a")).unwrap(), Some(cell(41)));
	}

	#[test]
	fn warm_inserts_archived_entries_without_decode_and_promotes_on_access() {
		// warm must insert archived entries at their exact byte charge; the first
		// typed access promotes exactly once, switching the charge to approximate.
		let mut store = MockStore::default();
		{
			let mut seed: StateCache<Key, Cell> = StateCache::new(big_pool());
			seed.set(&mut store, &Key::new("a"), &cell(7)).unwrap();
			seed.flush(&mut store).unwrap();
		}

		let pool = big_pool();
		let mut cache: StateCache<Key, Cell> = StateCache::new(pool.clone());
		cache.warm(&mut store, &[Key::new("a")]).unwrap();

		let stored = store.data.values().next().unwrap();
		let archived_bytes = archived_charge(stored) + skey(&Key::new("a"));
		assert_eq!(pool.snapshot().resident.as_bytes(), archived_bytes, "warm charges the exact archived size");

		let value = cache.get(&mut store, &Key::new("a")).unwrap();
		assert_eq!(value, Some(cell(7)));
		let native_bytes = native_charge(&cell(7)) + skey(&Key::new("a"));
		assert_eq!(
			pool.snapshot().resident.as_bytes(),
			native_bytes,
			"promotion must transfer the charge from exact archived to approximate native"
		);
	}

	#[test]
	fn ledger_clean_plus_dirty_matches_pool_after_interleaved_ops() {
		// The ledger is what the metrics pipeline reads; if it drifts from the pool
		// the memory bound is fiction.
		let mut store = MockStore::default();
		let pool = big_pool();
		let mut cache: StateCache<Key, Cell> = StateCache::new(pool.clone());

		let check = |cache: &StateCache<Key, Cell>, pool: &OperatorStateBudgetHandle| {
			let snapshot = pool.snapshot();
			assert_eq!(cache.ledger.clean, snapshot.resident.as_bytes());
			assert_eq!(cache.ledger.dirty, snapshot.dirty.as_bytes());
		};

		cache.set(&mut store, &Key::new("a"), &cell(1)).unwrap();
		check(&cache, &pool);
		cache.set(&mut store, &Key::new("a"), &cell(2)).unwrap();
		check(&cache, &pool);
		cache.put(&mut store, &Key::new("b"), cell(3)).unwrap();
		check(&cache, &pool);
		cache.flush(&mut store).unwrap();
		check(&cache, &pool);
		assert_eq!(cache.ledger.dirty, 0);
		cache.remove(&mut store, &Key::new("a")).unwrap();
		check(&cache, &pool);
		cache.flush(&mut store).unwrap();
		check(&cache, &pool);
		cache.take(&mut store, &Key::new("b")).unwrap();
		check(&cache, &pool);
		cache.clear_cache();
		check(&cache, &pool);
		assert_eq!(pool.snapshot().total(), ByteSize::ZERO);
	}

	#[test]
	fn writing_same_key_twice_transfers_bytes_instead_of_adding() {
		let mut store = MockStore::default();
		let pool = big_pool();
		let mut cache: StateCache<Key, Cell> = StateCache::new(pool.clone());

		cache.set(&mut store, &Key::new("a"), &cell(1)).unwrap();
		let once = pool.snapshot().dirty;
		cache.set(&mut store, &Key::new("a"), &cell(2)).unwrap();
		assert_eq!(pool.snapshot().dirty, once, "rewriting a dirty key must not double-charge");
	}

	#[test]
	fn dirty_entries_are_never_evicted_and_cap_violation_is_visible() {
		// An all-dirty cache over budget must not spin, error, or lose a write; the
		// overage is reported rather than hidden.
		let mut store = MockStore::default();
		let pool = pool_of(1);
		let mut cache: StateCache<Key, Cell> = StateCache::new(pool.clone());

		for i in 0..8 {
			cache.put(&mut store, &Key::new(format!("k{}", i)), cell(i)).unwrap();
		}
		assert!(pool.over_budget());
		assert!(pool.snapshot().overage().as_bytes() > 0);
		assert_eq!(cache.dirty_memory().entries, Count::new(8));

		cache.flush(&mut store).unwrap();
		assert_eq!(store.data.len(), 8, "every dirty write must reach the store despite the overage");
		assert_eq!(cache.ledger.dirty, 0);
	}

	#[test]
	fn approximate_memory_already_includes_the_dirty_tier() {
		// dirty_memory is a subset of approximate_memory, not a second bucket beside
		// it: adding the two charges the dirty bytes twice.
		let mut store = MockStore::default();
		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());

		for i in 0..3 {
			cache.set(&mut store, &Key::new(format!("clean{}", i)), &cell(i)).unwrap();
		}
		cache.flush(&mut store).unwrap();
		let clean_only = cache.approximate_memory();
		assert_eq!(cache.dirty_memory(), StateMemory::ZERO, "a flushed cache holds nothing dirty");
		assert_eq!(clean_only.entries, Count::new(3));

		cache.set(&mut store, &Key::new("dirty0"), &cell(7)).unwrap();
		cache.set(&mut store, &Key::new("dirty1"), &cell(8)).unwrap();

		let total = cache.approximate_memory();
		let dirty = cache.dirty_memory();
		assert_eq!(dirty.entries, Count::new(2));
		assert!(dirty.bytes.as_bytes() > 0, "two pending writes must carry a non-zero charge");

		assert_eq!(
			total.entries,
			clean_only.entries + dirty.entries,
			"approximate_memory counts the clean and the dirty entries together"
		);
		assert_eq!(
			total.bytes,
			clean_only.bytes + dirty.bytes,
			"approximate_memory already contains the dirty bytes, so adding dirty_memory to it \
			 would report clean + 2 * dirty and over-charge the operator lease"
		);
	}

	#[test]
	fn flush_makes_entries_clean_and_evictable_restoring_the_bound() {
		// Flushed entries are clean, so the eviction pass can restore the bound -
		// their bytes are in the store now.
		let mut store = MockStore::default();
		let pool = pool_of(1);
		let mut cache: StateCache<Key, Cell> = StateCache::new(pool.clone());

		for i in 0..8 {
			cache.put(&mut store, &Key::new(format!("k{}", i)), cell(i)).unwrap();
		}
		cache.flush(&mut store).unwrap();
		assert!(!pool.over_budget(), "flush + eviction must restore the bound once nothing is pinned");
		assert!(pool.snapshot().resident.as_bytes() <= 1);

		for i in 0..8 {
			assert_eq!(cache.get(&mut store, &Key::new(format!("k{}", i))).unwrap(), Some(cell(i)));
		}
	}

	#[test]
	fn eviction_issues_no_storage_operation() {
		// Eviction is memory-only: nothing may reach the store, and the evicted key
		// must reload to its original value.
		let mut store = MockStore::default();
		let pool = big_pool();
		let mut cache: StateCache<Key, Cell> = StateCache::new(pool.clone());
		cache.set(&mut store, &Key::new("a"), &cell(9)).unwrap();
		cache.flush(&mut store).unwrap();

		pool.set_budget(ByteSize::from_bytes(1));
		cache.put(&mut store, &Key::new("b"), cell(1)).unwrap();
		assert!(!cache.is_cached(&Key::new("a")), "the clean entry must be evicted under pressure");
		assert_eq!(store.removes, 0, "eviction must not remove stored state");
		assert_eq!(cache.get(&mut store, &Key::new("a")).unwrap(), Some(cell(9)));
	}

	#[test]
	fn removed_key_shadows_the_store_and_flushes_as_a_state_remove() {
		let mut store = MockStore::default();
		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());
		cache.set(&mut store, &Key::new("a"), &cell(1)).unwrap();
		cache.flush(&mut store).unwrap();

		cache.remove(&mut store, &Key::new("a")).unwrap();
		assert_eq!(
			cache.get(&mut store, &Key::new("a")).unwrap(),
			None,
			"a pending remove must shadow the stored value"
		);
		cache.flush(&mut store).unwrap();
		assert_eq!(
			store.removes, 1,
			"a removed entry must reach the store as exactly one state_remove; there is no longer a \
			 separate drop route, so this is the only path by which cached removals become durable"
		);
		assert_eq!(cache.get(&mut store, &Key::new("a")).unwrap(), None);
	}

	#[test]
	fn two_caches_share_one_pool_and_the_toucher_evicts_its_own_tail() {
		let mut store = MockStore::default();
		let pool = big_pool();
		let mut a: StateCache<Key, Cell> = StateCache::new(pool.clone());
		let mut b: StateCache<Key, Cell> = StateCache::new(pool.clone());

		a.set(&mut store, &Key::new("a"), &cell(1)).unwrap();
		a.flush(&mut store).unwrap();
		b.set(&mut store, &Key::new("b"), &cell(2)).unwrap();
		b.flush(&mut store).unwrap();

		pool.set_budget(ByteSize::from_bytes(1));
		b.put(&mut store, &Key::new("c"), cell(3)).unwrap();
		assert!(!b.is_cached(&Key::new("b")), "the touching cache must evict its own clean tail");
		assert!(a.is_cached(&Key::new("a")), "an idle cache keeps its entries under another cache's pressure");
	}

	#[operator_state]
	#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
	struct CountingCell {
		value: i32,
	}

	static COUNTING_CELL_CLONES: AtomicUsize = AtomicUsize::new(0);

	impl Clone for CountingCell {
		fn clone(&self) -> Self {
			COUNTING_CELL_CLONES.fetch_add(1, Ordering::Relaxed);
			Self {
				value: self.value,
			}
		}
	}

	impl HeapSize for CountingCell {
		fn heap_size(&self) -> usize {
			0
		}
	}

	fn view_value(view: StateView<'_, Cell>) -> i32 {
		match view {
			StateView::Archived(archived) => archived.value.to_native(),
			StateView::Native(value) => value.value,
		}
	}

	#[test]
	fn read_on_archived_entry_does_not_promote() {
		// A pure read must serve the archived form at its exact byte charge; get()
		// promotes by design, read() must not.
		let mut store = MockStore::default();
		{
			let mut seed: StateCache<Key, Cell> = StateCache::new(big_pool());
			seed.set(&mut store, &Key::new("a"), &cell(7)).unwrap();
			seed.flush(&mut store).unwrap();
		}

		let pool = big_pool();
		let mut cache: StateCache<Key, Cell> = StateCache::new(pool.clone());
		cache.warm(&mut store, &[Key::new("a")]).unwrap();
		let archived_bytes = archived_charge(store.data.values().next().unwrap()) + skey(&Key::new("a"));
		assert_eq!(pool.snapshot().resident.as_bytes(), archived_bytes);

		let seen = cache.read(&mut store, &Key::new("a"), view_value).unwrap();
		assert_eq!(seen, Some(7));
		assert_eq!(
			pool.snapshot().resident.as_bytes(),
			archived_bytes,
			"read() must keep the entry archived at its exact byte charge, not promote it"
		);

		assert_eq!(cache.get(&mut store, &Key::new("a")).unwrap(), Some(cell(7)));
		assert_eq!(
			pool.snapshot().resident.as_bytes(),
			native_charge(&cell(7)) + skey(&Key::new("a")),
			"promotion after read() must still transfer the charge to native"
		);
	}

	#[test]
	fn read_miss_inserts_archived() {
		// A read() miss must leave archived residency, so the second read costs no
		// store roundtrip.
		let mut store = MockStore::default();
		{
			let mut seed: StateCache<Key, Cell> = StateCache::new(big_pool());
			seed.set(&mut store, &Key::new("a"), &cell(9)).unwrap();
			seed.flush(&mut store).unwrap();
		}

		let pool = big_pool();
		let mut cache: StateCache<Key, Cell> = StateCache::new(pool.clone());
		let gets_before = store.gets;

		let seen = cache.read(&mut store, &Key::new("a"), view_value).unwrap();
		assert_eq!(seen, Some(9));
		assert_eq!(store.gets, gets_before + 1);
		let archived_bytes = archived_charge(store.data.values().next().unwrap()) + skey(&Key::new("a"));
		assert_eq!(
			pool.snapshot().resident.as_bytes(),
			archived_bytes,
			"a read() miss must cache the archived bytes at their exact charge"
		);

		let again = cache.read(&mut store, &Key::new("a"), view_value).unwrap();
		assert_eq!(again, Some(9));
		assert_eq!(store.gets, gets_before + 1, "the archived entry must serve the second read");

		let absent = cache.read(&mut store, &Key::new("missing"), view_value).unwrap();
		assert_eq!(absent, None);
	}

	#[test]
	fn read_sees_dirty_and_dropped() {
		// read() honours the same overlay order as get(): pending writes and removes win.
		let mut store = MockStore::default();
		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());

		cache.put(&mut store, &Key::new("a"), cell(3)).unwrap();
		let seen = cache.read(&mut store, &Key::new("a"), view_value).unwrap();
		assert_eq!(seen, Some(3), "a dirty entry must be served as a native view");

		cache.remove(&mut store, &Key::new("a")).unwrap();
		let removed = cache.read(&mut store, &Key::new("a"), view_value).unwrap();
		assert_eq!(removed, None, "a pending remove must shadow everything below it");
	}

	#[test]
	fn flush_error_retains_unflushed_dirty_entries() {
		// A mid-flush store error must not lose pending writes: unwritten entries stay
		// dirty with their charges and order intact, and a later flush drains them.
		let mut store = MockStore::default();
		let pool = big_pool();
		let mut cache: StateCache<Key, Cell> = StateCache::new(pool.clone());

		cache.put(&mut store, &Key::new("a"), cell(1)).unwrap();
		cache.put(&mut store, &Key::new("b"), cell(2)).unwrap();
		cache.put(&mut store, &Key::new("c"), cell(3)).unwrap();

		store.fail_state_set_at = Some(2);
		assert!(cache.flush(&mut store).is_err(), "the injected store failure must surface");

		assert_eq!(store.data.len(), 1, "only the write before the failure reached the store");
		assert_eq!(cache.dirty_memory().entries, Count::new(2), "unflushed entries must stay dirty");
		assert_eq!(
			cache.ledger.dirty,
			pool.snapshot().dirty.as_bytes(),
			"ledger and pool must agree after a failed flush"
		);
		assert!(pool.snapshot().dirty.as_bytes() > 0, "retained dirty entries keep their charges");
		assert_eq!(cache.get(&mut store, &Key::new("b")).unwrap(), Some(cell(2)));
		assert_eq!(cache.get(&mut store, &Key::new("c")).unwrap(), Some(cell(3)));

		store.fail_state_set_at = None;
		cache.flush(&mut store).unwrap();
		assert_eq!(store.data.len(), 3, "the healed flush must drain the retained entries");
		assert_eq!(cache.dirty_memory(), StateMemory::ZERO);

		// Attempt log: a, b (failed), b (retried first), c.
		assert_eq!(store.set_attempts.len(), 4);
		assert_eq!(store.set_attempts[1], store.set_attempts[2], "the failed key must be retried first");
		assert_ne!(store.set_attempts[0], store.set_attempts[1]);
		assert_ne!(store.set_attempts[2], store.set_attempts[3]);
	}

	#[test]
	fn flush_error_then_success_releases_exactly_once() {
		// Charge conservation across a failed and retried flush: no leaked dirty
		// bytes, no double release.
		let mut store = MockStore::default();
		let pool = big_pool();
		let mut cache: StateCache<Key, Cell> = StateCache::new(pool.clone());

		cache.put(&mut store, &Key::new("a"), cell(1)).unwrap();
		cache.put(&mut store, &Key::new("b"), cell(2)).unwrap();
		cache.put(&mut store, &Key::new("c"), cell(3)).unwrap();

		store.fail_state_set_at = Some(2);
		assert!(cache.flush(&mut store).is_err());
		store.fail_state_set_at = None;
		cache.flush(&mut store).unwrap();

		let snapshot = pool.snapshot();
		assert_eq!(snapshot.dirty.as_bytes(), 0, "every dirty charge must be released exactly once");
		assert_eq!(
			snapshot.resident.as_bytes(),
			3 * native_charge(&cell(0))
				+ skey(&Key::new("a")) + skey(&Key::new("b"))
				+ skey(&Key::new("c")),
			"the pool must hold exactly the three clean native charges"
		);
		assert_eq!(cache.ledger.clean, snapshot.resident.as_bytes());
	}

	#[test]
	fn take_and_put_do_not_clone_resident_value() {
		// take_owned drops the clean entry first so the Arc is uniquely held: the
		// clean-resident path must clone zero times. The dirty-hit clone is the
		// accepted cost and is pinned at exactly one.
		let mut store = MockStore::default();
		let mut cache: StateCache<Key, CountingCell> = StateCache::new(big_pool());

		cache.put(
			&mut store,
			&Key::new("a"),
			CountingCell {
				value: 1,
			},
		)
		.unwrap();
		cache.flush(&mut store).unwrap();

		COUNTING_CELL_CLONES.store(0, Ordering::Relaxed);
		let mut value = cache.take(&mut store, &Key::new("a")).unwrap().unwrap();
		value.value += 1;
		cache.put(&mut store, &Key::new("a"), value).unwrap();
		assert_eq!(
			COUNTING_CELL_CLONES.load(Ordering::Relaxed),
			0,
			"taking and re-putting a clean-resident value must not clone it"
		);

		COUNTING_CELL_CLONES.store(0, Ordering::Relaxed);
		let mut value = cache.take(&mut store, &Key::new("a")).unwrap().unwrap();
		value.value += 1;
		cache.put(&mut store, &Key::new("a"), value).unwrap();
		assert_eq!(
			COUNTING_CELL_CLONES.load(Ordering::Relaxed),
			1,
			"the dirty-hit path pays exactly the one accepted clone"
		);

		cache.flush(&mut store).unwrap();
		cache.clear_cache();
		assert_eq!(
			cache.get(&mut store, &Key::new("a")).unwrap(),
			Some(CountingCell {
				value: 3,
			}),
			"both mutations must have landed"
		);
	}

	#[test]
	fn take_and_put_of_clean_key_transfers_charge_exactly_once() {
		// take -> put on a clean key must release the clean charge and hold exactly
		// one dirty charge, with ledger and pool in agreement.
		let mut store = MockStore::default();
		let pool = big_pool();
		let mut cache: StateCache<Key, Cell> = StateCache::new(pool.clone());

		cache.put(&mut store, &Key::new("a"), cell(1)).unwrap();
		cache.flush(&mut store).unwrap();
		assert_eq!(pool.snapshot().resident.as_bytes(), native_charge(&cell(1)) + skey(&Key::new("a")));
		assert_eq!(pool.snapshot().dirty.as_bytes(), 0);

		let mut value = cache.take(&mut store, &Key::new("a")).unwrap().unwrap();
		value.value += 1;
		cache.put(&mut store, &Key::new("a"), value).unwrap();

		let snapshot = pool.snapshot();
		assert_eq!(snapshot.resident.as_bytes(), 0, "the clean charge must be released by the take");
		assert_eq!(
			snapshot.dirty.as_bytes(),
			native_charge(&cell(2)) + skey(&Key::new("a")),
			"exactly one dirty charge must be held after modify"
		);
		assert_eq!(cache.ledger.clean, snapshot.resident.as_bytes());
		assert_eq!(cache.ledger.dirty, snapshot.dirty.as_bytes());
	}

	#[test]
	fn drop_releases_all_pool_charges() {
		// The pool outlives the caches, so a drop that keeps its charges ratchets the
		// pool toward artificial exhaustion on every engine rebuild.
		let mut store = MockStore::default();
		{
			let mut seed: StateCache<Key, Cell> = StateCache::new(big_pool());
			seed.set(&mut store, &Key::new("a"), &cell(1)).unwrap();
			seed.set(&mut store, &Key::new("b"), &cell(2)).unwrap();
			seed.flush(&mut store).unwrap();
		}

		let pool = big_pool();
		{
			let mut cache: StateCache<Key, Cell> = StateCache::new(pool.clone());
			cache.warm(&mut store, &[Key::new("a")]).unwrap();
			cache.get(&mut store, &Key::new("b")).unwrap();
			cache.put(&mut store, &Key::new("c"), cell(3)).unwrap();
			let held = pool.snapshot();
			assert!(held.resident.as_bytes() > 0, "archived + native entries must be charged");
			assert!(held.dirty.as_bytes() > 0, "the pending write must be charged");
		}

		assert_eq!(
			pool.snapshot().total(),
			ByteSize::ZERO,
			"dropping the cache must release every charge it held"
		);
	}

	#[test]
	fn drop_after_failed_flush_releases_dirty_charges() {
		// A failed flush retains dirty entries and the engine rebuild then drops the
		// cache; the retained charges must come back with it.
		let mut store = MockStore::default();
		let pool = big_pool();
		{
			let mut cache: StateCache<Key, Cell> = StateCache::new(pool.clone());
			cache.put(&mut store, &Key::new("a"), cell(1)).unwrap();
			cache.put(&mut store, &Key::new("b"), cell(2)).unwrap();
			cache.put(&mut store, &Key::new("c"), cell(3)).unwrap();

			store.fail_state_set_at = Some(2);
			assert!(cache.flush(&mut store).is_err());
			let held = pool.snapshot();
			assert!(held.resident.as_bytes() > 0, "the flushed entry is clean-resident");
			assert!(held.dirty.as_bytes() > 0, "the retained entries are still dirty");
		}

		assert_eq!(
			pool.snapshot().total(),
			ByteSize::ZERO,
			"dropping a cache after a failed flush must release the retained dirty charges"
		);
	}

	#[test]
	fn key_bytes_are_charged_per_tier() {
		// Key bytes are charged once per tier: a longer key holds a proportionally
		// larger charge in both tiers and returns to baseline on removal.
		let mut store = MockStore::default();
		let pool = big_pool();
		let mut cache: StateCache<Key, Cell> = StateCache::new(pool.clone());

		let short = Key::new("k");
		let long = Key::new("k".repeat(65));
		cache.put(&mut store, &short, cell(1)).unwrap();
		let short_dirty = pool.snapshot().dirty.as_bytes();
		cache.put(&mut store, &long, cell(1)).unwrap();
		assert_eq!(
			pool.snapshot().dirty.as_bytes() - short_dirty,
			native_charge(&cell(1)) + skey(&long),
			"the dirty charge covers the key bytes"
		);

		cache.flush(&mut store).unwrap();
		assert_eq!(
			pool.snapshot().resident.as_bytes(),
			2 * native_charge(&cell(1)) + skey(&short) + skey(&long),
			"the clean charge covers the key bytes"
		);
		assert_eq!(cache.ledger.clean, pool.snapshot().resident.as_bytes());

		cache.remove(&mut store, &short).unwrap();
		cache.remove(&mut store, &long).unwrap();
		cache.flush(&mut store).unwrap();
		assert_eq!(pool.snapshot().total(), ByteSize::ZERO, "removal returns the key charges to baseline");
	}

	#[operator_state(seal)]
	#[derive(Debug, Clone, Default, PartialEq)]
	struct SealCell {
		value: u64,
	}

	impl HeapSize for SealCell {
		fn heap_size(&self) -> usize {
			0
		}
	}

	fn seal_cell_write(seal: Seal<'_, ArchivedSealCell>, value: u64) {
		munge!(let ArchivedSealCell { value: mut slot } = seal);
		*slot = ArchivedU64::from_native(value);
	}

	fn warmed_seal_cache(
		store: &mut MockStore,
		pool: OperatorStateBudgetHandle,
		value: u64,
	) -> StateCache<Key, SealCell> {
		// Leaves the cache holding one clean Archived entry: validated bytes, exact charge.
		let mut cache: StateCache<Key, SealCell> = StateCache::new(pool);
		cache.put(
			store,
			&Key::new("a"),
			SealCell {
				value,
			},
		)
		.unwrap();
		cache.flush(store).unwrap();
		cache.clear_cache();
		cache.warm(store, &[Key::new("a")]).unwrap();
		cache
	}

	#[test]
	fn modify_in_place_seals_archived_entry_without_materializing() {
		// An archived-resident entry must be mutated through its bytes: no native
		// residency may appear, and every read path serves the sealed value pre-flush.
		let mut store = MockStore::default();
		let pool = big_pool();
		let mut cache = warmed_seal_cache(&mut store, pool.clone(), 1);
		let archived = pool.snapshot().resident.as_bytes();
		assert!(archived > 0, "the warmed entry is archived-resident");

		cache.modify_in_place(
			&mut store,
			&Key::new("a"),
			|seal| {
				seal_cell_write(seal, 7);
				Some(())
			},
			|_| panic!("an archived-resident entry must be served by the seal closure"),
		)
		.unwrap();

		assert!(
			matches!(cache.dirty.get(&Key::new("a")), Some(DirtyEntry::LiveArchived(_))),
			"the sealed entry must become a dirty LiveArchived slot, not materialize"
		);
		let snapshot = pool.snapshot();
		assert_eq!(snapshot.resident.as_bytes(), 0, "the clean charge moved to the dirty tier");
		assert_eq!(snapshot.dirty.as_bytes(), archived, "the dirty charge is the exact archived size");
		assert_eq!(cache.ledger.dirty, snapshot.dirty.as_bytes());

		let via_read = cache
			.read(&mut store, &Key::new("a"), |view| match view {
				StateView::Archived(a) => a.value.to_native(),
				StateView::Native(_) => panic!("the dirty slot must stay archived"),
			})
			.unwrap();
		assert_eq!(via_read, Some(7));
		assert_eq!(
			cache.get(&mut store, &Key::new("a")).unwrap(),
			Some(SealCell {
				value: 7,
			})
		);
	}

	#[test]
	fn seal_flush_writes_bytes_verbatim_with_refreshed_updated_at() {
		// A LiveArchived flush skips the encoder: the stored body is byte-identical to
		// an encode of the sealed value, updated_at carries the flush clock, and
		// created_at survives (clobbering it would break TTL).
		let mut store = MockStore::default();
		let pool = big_pool();
		let mut cache = warmed_seal_cache(&mut store, pool.clone(), 1);
		cache.modify_in_place(
			&mut store,
			&Key::new("a"),
			|seal| {
				seal_cell_write(seal, 7);
				Some(())
			},
			|_| panic!("seal path expected"),
		)
		.unwrap();

		store.now = DateTime::from_nanos(99);
		cache.flush(&mut store).unwrap();

		let stored = store.data.values().next().unwrap();
		assert_eq!(
			stored.row().updated_at(),
			DateTime::from_nanos(99),
			"the verbatim write refreshes updated_at"
		);
		assert_eq!(stored.row().created_at(), DateTime::EPOCH, "created_at survives the verbatim rewrite");
		let expected = SealCell {
			value: 7,
		}
		.encode_state(DateTime::EPOCH)
		.unwrap();
		assert_eq!(stored.body(), expected.body(), "the stored body is the sealed bytes, not a re-encode");

		assert!(
			matches!(cache.clean.get(&Key::new("a")), Some(CleanEntry::Archived(_))),
			"the flushed entry re-enters the clean tier archived"
		);
		assert_eq!(pool.snapshot().dirty.as_bytes(), 0);
		cache.clear_cache();
		assert_eq!(
			cache.get(&mut store, &Key::new("a")).unwrap(),
			Some(SealCell {
				value: 7,
			}),
			"the persisted row round-trips"
		);
	}

	#[test]
	fn seal_cow_is_paid_once_per_shared_buffer() {
		// The store holds an Arc of the same row, so the first seal must copy-on-write;
		// the buffer is unique afterwards and further seals mutate in place.
		let mut store = MockStore::default();
		let mut cache = warmed_seal_cache(&mut store, big_pool(), 1);
		assert_eq!(cache.seal_copies, 0);

		cache.modify_in_place(
			&mut store,
			&Key::new("a"),
			|seal| {
				seal_cell_write(seal, 2);
				Some(())
			},
			|_| panic!("seal path expected"),
		)
		.unwrap();
		assert_eq!(cache.seal_copies, 1, "the first seal pays the CoW for the store-shared row");

		cache.modify_in_place(
			&mut store,
			&Key::new("a"),
			|seal| {
				seal_cell_write(seal, 3);
				Some(())
			},
			|_| panic!("seal path expected"),
		)
		.unwrap();
		assert_eq!(cache.seal_copies, 1, "the second seal mutates the now-unique buffer in place");
		assert_eq!(
			cache.get(&mut store, &Key::new("a")).unwrap(),
			Some(SealCell {
				value: 3,
			})
		);
	}

	#[test]
	fn modify_in_place_falls_back_to_native_when_seal_declines() {
		// A declined seal (a mutation the archive cannot express) must fall back to the
		// native path; seal_f is required to be side-effect-free on None.
		let mut store = MockStore::default();
		let mut cache = warmed_seal_cache(&mut store, big_pool(), 1);

		let result = cache
			.modify_in_place(
				&mut store,
				&Key::new("a"),
				|_seal| None::<u64>,
				|value| {
					value.value += 10;
					value.value
				},
			)
			.unwrap();
		assert_eq!(result, 11);
		assert!(
			matches!(cache.dirty.get(&Key::new("a")), Some(DirtyEntry::Live(_))),
			"the declined seal falls back to a native dirty slot"
		);
	}

	#[test]
	fn modify_in_place_miss_paths() {
		// An absent key has no archive to seal, so the native closure runs on the
		// default; a store-resident key is sealed without materializing.
		let mut store = MockStore::default();
		let mut cache: StateCache<Key, SealCell> = StateCache::new(big_pool());

		let result = cache
			.modify_in_place(
				&mut store,
				&Key::new("absent"),
				|_seal| panic!("no archive exists to seal"),
				|value| {
					value.value += 1;
					value.value
				},
			)
			.unwrap();
		assert_eq!(result, 1, "the absent key runs the native closure on the default");

		cache.put(
			&mut store,
			&Key::new("m"),
			SealCell {
				value: 5,
			},
		)
		.unwrap();
		cache.flush(&mut store).unwrap();
		cache.clear_cache();
		cache.modify_in_place(
			&mut store,
			&Key::new("m"),
			|seal| {
				seal_cell_write(seal, 6);
				Some(())
			},
			|_| panic!("a store-resident archive must be sealed, not materialized"),
		)
		.unwrap();
		assert!(
			matches!(cache.dirty.get(&Key::new("m")), Some(DirtyEntry::LiveArchived(_))),
			"the loaded bytes land as LiveArchived"
		);
		assert_eq!(
			cache.get(&mut store, &Key::new("m")).unwrap(),
			Some(SealCell {
				value: 6,
			})
		);
	}

	#[test]
	fn take_on_live_archived_leaves_the_pending_write() {
		// take() on a dirty slot is a read-out, not a removal: marking it Dropped would
		// turn a read into a store delete.
		let mut store = MockStore::default();
		let mut cache = warmed_seal_cache(&mut store, big_pool(), 1);
		cache.modify_in_place(
			&mut store,
			&Key::new("a"),
			|seal| {
				seal_cell_write(seal, 7);
				Some(())
			},
			|_| panic!("seal path expected"),
		)
		.unwrap();

		let taken = cache.take(&mut store, &Key::new("a")).unwrap();
		assert_eq!(
			taken,
			Some(SealCell {
				value: 7,
			})
		);
		assert!(
			matches!(cache.dirty.get(&Key::new("a")), Some(DirtyEntry::LiveArchived(_))),
			"take must leave the pending sealed write in place"
		);

		cache.flush(&mut store).unwrap();
		cache.clear_cache();
		assert_eq!(
			cache.get(&mut store, &Key::new("a")).unwrap(),
			Some(SealCell {
				value: 7,
			}),
			"the sealed write persisted despite the intermediate take"
		);
	}

	#[test]
	fn seal_flush_error_retains_live_archived() {
		// A failed verbatim write keeps the sealed slot, its charge, and its order; the
		// healed reflush drains it without re-encoding.
		let mut store = MockStore::default();
		let pool = big_pool();
		let mut cache = warmed_seal_cache(&mut store, pool.clone(), 1);
		cache.modify_in_place(
			&mut store,
			&Key::new("a"),
			|seal| {
				seal_cell_write(seal, 7);
				Some(())
			},
			|_| panic!("seal path expected"),
		)
		.unwrap();
		let dirty_charge = pool.snapshot().dirty.as_bytes();

		store.fail_state_set_at = Some(store.sets + 1);
		assert!(cache.flush(&mut store).is_err());
		assert!(
			matches!(cache.dirty.get(&Key::new("a")), Some(DirtyEntry::LiveArchived(_))),
			"the failed write must retain the sealed slot"
		);
		assert_eq!(pool.snapshot().dirty.as_bytes(), dirty_charge, "the dirty charge is retained");

		store.fail_state_set_at = None;
		cache.flush(&mut store).unwrap();
		assert_eq!(pool.snapshot().dirty.as_bytes(), 0);
		cache.clear_cache();
		assert_eq!(
			cache.get(&mut store, &Key::new("a")).unwrap(),
			Some(SealCell {
				value: 7,
			})
		);
	}

	fn full_range() -> EncodedKeyRange {
		EncodedKeyRange::new(Bound::Unbounded, Bound::Unbounded)
	}

	fn string_key_decoder(candidates: &'static [&'static str]) -> impl Fn(&EncodedKey) -> Option<Key> {
		// Matches candidate encodings rather than reversing the string codec.
		move |encoded| {
			candidates
				.iter()
				.find(|c| {
					Key::new(c.to_string()).into_group_state_key().as_slice() == encoded.as_slice()
				})
				.map(|c| Key::new(c.to_string()))
		}
	}

	fn seeded_internal_store(cells: &[(&str, i32)]) -> MockStore {
		let mut store = MockStore::default();
		let mut seed: StateCache<Key, Cell> = StateCache::new(big_pool());
		for (key, value) in cells {
			seed.set(&mut store, &Key::new(*key), &cell(*value)).unwrap();
		}
		seed.flush(&mut store).unwrap();
		store
	}

	#[test]
	fn hydrated_cache_proves_absence_without_store_reads() {
		// After hydration a miss IS an absence proof; reading through again would make
		// every fresh group key pay a persistent-tier roundtrip.
		let mut store = seeded_internal_store(&[("a", 1)]);
		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());
		cache.hydrate(&mut store, full_range(), string_key_decoder(&["a", "missing"])).unwrap();

		store.gets = 0;
		assert_eq!(cache.get(&mut store, &Key::new("a")).unwrap(), Some(cell(1)));
		assert_eq!(cache.get(&mut store, &Key::new("missing")).unwrap(), None);
		cache.warm(&mut store, &[Key::new("missing")]).unwrap();
		assert_eq!(store.gets, 0, "a hydrated cache must serve hits and absences without touching the store");
	}

	#[test]
	fn eviction_revokes_absence_proofs_instead_of_losing_state() {
		// Keeping the complete flag through an eviction would answer None for a key
		// the store still holds - silent state loss.
		let mut store = seeded_internal_store(&[("a", 1)]);
		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());
		cache.hydrate(&mut store, full_range(), string_key_decoder(&["a"])).unwrap();

		cache.pool.set_budget(ByteSize::from_bytes(1));
		cache.evict_to_budget();
		assert!(!cache.is_cached(&Key::new("a")), "the 1-byte budget must evict the only entry");

		assert_eq!(
			cache.get(&mut store, &Key::new("a")).unwrap(),
			Some(cell(1)),
			"after eviction a miss no longer proves absence; the value must be re-read from the store"
		);
	}

	#[test]
	fn hydrate_does_not_resurrect_a_pending_drop() {
		// A drop buffered before hydration is not yet in the store; if the scan
		// shadowed it, the deleted state would come back for one flush interval.
		let mut store = seeded_internal_store(&[("a", 1)]);
		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());
		cache.remove(&mut store, &Key::new("a")).unwrap();
		cache.hydrate(&mut store, full_range(), string_key_decoder(&["a"])).unwrap();

		assert_eq!(
			cache.get(&mut store, &Key::new("a")).unwrap(),
			None,
			"the pending drop must win over the hydrated store copy"
		);
	}

	#[test]
	fn take_revokes_absence_proofs_for_the_taken_key() {
		// take() drops the clean entry while the store still holds the value; keeping
		// the complete flag would make the next get claim absence.
		let mut store = seeded_internal_store(&[("a", 1)]);
		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());
		cache.hydrate(&mut store, full_range(), string_key_decoder(&["a"])).unwrap();

		assert_eq!(cache.take(&mut store, &Key::new("a")).unwrap(), Some(cell(1)));
		assert_eq!(
			cache.get(&mut store, &Key::new("a")).unwrap(),
			Some(cell(1)),
			"after take the store copy must remain reachable through read-through"
		);
	}

	#[test]
	fn eviction_keeps_absence_proofs_through_membership() {
		// Membership survives eviction, so absence stays a RAM answer and only the
		// evicted value itself costs a point read.
		let mut store = seeded_internal_store(&[("a", 1), ("b", 2)]);
		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());
		cache.hydrate(&mut store, full_range(), string_key_decoder(&["a", "b", "missing"])).unwrap();

		cache.pool.set_budget(ByteSize::from_bytes(1));
		cache.evict_to_budget();
		assert!(!cache.is_cached(&Key::new("a")) && !cache.is_cached(&Key::new("b")));

		store.gets = 0;
		assert_eq!(cache.get(&mut store, &Key::new("missing")).unwrap(), None);
		assert_eq!(store.gets, 0, "absence must be served from membership, not the store");

		cache.pool.set_budget(ByteSize::from_bytes(64 * 1024 * 1024));
		assert_eq!(cache.get(&mut store, &Key::new("a")).unwrap(), Some(cell(1)));
		assert_eq!(store.gets, 1, "the evicted value costs exactly one point read");

		let completeness = cache.completeness();
		assert!(!completeness.values_complete, "eviction must revoke values-completeness");
		assert!(completeness.membership_complete, "eviction must NOT revoke membership");
		assert_eq!(completeness.revocations.as_u64(), 1);
		assert_eq!(completeness.absences_served.as_u64(), 1);
	}

	#[test]
	fn drop_of_a_nonresident_key_updates_membership_exactly() {
		// Dropping a key whose value was evicted must still remove its membership
		// evidence, or every later probe pays a pointless store read.
		let mut store = seeded_internal_store(&[("a", 1), ("b", 2)]);
		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());
		cache.hydrate(&mut store, full_range(), string_key_decoder(&["a", "b"])).unwrap();
		cache.pool.set_budget(ByteSize::from_bytes(1));
		cache.evict_to_budget();
		cache.pool.set_budget(ByteSize::from_bytes(64 * 1024 * 1024));

		cache.remove(&mut store, &Key::new("a")).unwrap();
		cache.flush(&mut store).unwrap();

		store.gets = 0;
		assert_eq!(cache.get(&mut store, &Key::new("a")).unwrap(), None, "the dropped key must read absent");
		assert_eq!(store.gets, 0, "the dropped key's absence must be a membership answer");
		assert_eq!(cache.get(&mut store, &Key::new("b")).unwrap(), Some(cell(2)));
	}

	#[test]
	fn values_completeness_is_restored_once_every_live_key_is_resident_again() {
		// The cache only ever holds live keys, so resident-count == membership-count
		// proves the resident set IS the live set. Promoting before that would answer
		// None for a still non-resident live key - silent state loss.
		let mut store = seeded_internal_store(&[("a", 1), ("b", 2)]);
		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());
		cache.hydrate(&mut store, full_range(), string_key_decoder(&["a", "b", "missing"])).unwrap();

		cache.pool.set_budget(ByteSize::from_bytes(1));
		cache.evict_to_budget();
		cache.pool.set_budget(ByteSize::from_bytes(64 * 1024 * 1024));

		assert_eq!(cache.get(&mut store, &Key::new("a")).unwrap(), Some(cell(1)));
		assert!(
			!cache.completeness().values_complete,
			"with 'b' still non-resident the resident set is not the live set"
		);
		assert_eq!(
			cache.get(&mut store, &Key::new("b")).unwrap(),
			Some(cell(2)),
			"a live but non-resident key must be read through, never answered absent"
		);
		assert!(
			cache.completeness().values_complete,
			"once every live key is resident again the fast path must come back"
		);

		store.gets = 0;
		assert_eq!(cache.get(&mut store, &Key::new("missing")).unwrap(), None);
		assert_eq!(store.gets, 0, "after promotion an absence is a RAM answer again");
		assert_eq!(cache.completeness().revocations.as_u64(), 1);
	}

	#[test]
	fn dropping_the_last_nonresident_key_restores_completeness_at_flush() {
		// The gap can also close from the live side, but a pending tombstone makes the
		// resident count ambiguous, so promotion must wait for the flush.
		let mut store = seeded_internal_store(&[("a", 1), ("b", 2)]);
		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());
		cache.hydrate(&mut store, full_range(), string_key_decoder(&["a", "b"])).unwrap();

		cache.pool.set_budget(ByteSize::from_bytes(1));
		cache.evict_to_budget();
		cache.pool.set_budget(ByteSize::from_bytes(64 * 1024 * 1024));
		assert_eq!(cache.get(&mut store, &Key::new("a")).unwrap(), Some(cell(1)));

		cache.remove(&mut store, &Key::new("b")).unwrap();
		assert!(!cache.completeness().values_complete, "a pending tombstone must defer promotion");
		cache.flush(&mut store).unwrap();
		assert!(cache.completeness().values_complete, "flushing the drop closes the live-set gap");

		store.gets = 0;
		assert_eq!(cache.get(&mut store, &Key::new("b")).unwrap(), None);
		assert_eq!(store.gets, 0, "the dropped key's absence must be a RAM answer");
	}

	#[test]
	fn a_new_key_written_after_hydration_is_tracked_by_membership() {
		// Keys born after hydration must join membership at write time, or an eviction
		// would make freshly written state read as absent.
		let mut store = seeded_internal_store(&[]);
		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());
		cache.hydrate(&mut store, full_range(), string_key_decoder(&["fresh", "missing"])).unwrap();

		cache.put(&mut store, &Key::new("fresh"), cell(9)).unwrap();
		cache.flush(&mut store).unwrap();
		cache.pool.set_budget(ByteSize::from_bytes(1));
		cache.evict_to_budget();
		cache.pool.set_budget(ByteSize::from_bytes(64 * 1024 * 1024));

		store.gets = 0;
		assert_eq!(cache.get(&mut store, &Key::new("missing")).unwrap(), None);
		assert_eq!(store.gets, 0, "absence still served from membership after the new write");
		assert_eq!(
			cache.get(&mut store, &Key::new("fresh")).unwrap(),
			Some(cell(9)),
			"the post-hydration key must be reachable through membership + store"
		);
		assert_eq!(store.gets, 1);
	}

	#[test]
	fn clear_cache_and_take_keep_membership_alive() {
		// clear_cache and take drop values, not existence, so absence proofs must
		// survive both.
		let mut store = seeded_internal_store(&[("a", 1)]);
		let mut cache: StateCache<Key, Cell> = StateCache::new(big_pool());
		cache.hydrate(&mut store, full_range(), string_key_decoder(&["a", "missing"])).unwrap();

		assert_eq!(cache.take(&mut store, &Key::new("a")).unwrap(), Some(cell(1)));
		cache.clear_cache();

		store.gets = 0;
		assert_eq!(cache.get(&mut store, &Key::new("missing")).unwrap(), None);
		assert_eq!(store.gets, 0, "absence must survive take + clear_cache");
		assert_eq!(cache.get(&mut store, &Key::new("a")).unwrap(), Some(cell(1)));
	}

	#[derive(Clone, PartialEq, Eq)]
	struct CollidingKey {
		id: &'static str,
	}

	impl Hash for CollidingKey {
		fn hash<H: Hasher>(&self, state: &mut H) {
			state.write_u64(0xDEAD_BEEF);
		}
	}

	impl HeapSize for CollidingKey {
		fn heap_size(&self) -> usize {
			0
		}
	}

	impl IntoGroupStateKey for &CollidingKey {
		fn into_group_state_key(self) -> GroupStateKey {
			GroupStateKey::node_scoped(Keyspace::FIRST_CUSTOM, self.id.as_bytes())
		}
	}

	#[test]
	fn a_full_hash_collision_costs_a_false_positive_but_never_a_false_negative() {
		// key1 live, key2 absent, one shared membership hash: the collision must cost a
		// counted false positive and never make the live key read absent.
		let mut store = MockStore::default();
		let key1 = CollidingKey {
			id: "one",
		};
		let key2 = CollidingKey {
			id: "two",
		};
		{
			let mut seed: StateCache<CollidingKey, Cell> = StateCache::new(big_pool());
			seed.set(&mut store, &key1, &cell(1)).unwrap();
			seed.flush(&mut store).unwrap();
		}

		let mut cache: StateCache<CollidingKey, Cell> = StateCache::new(big_pool());
		let candidates = [key1.clone(), key2.clone()];
		cache.hydrate(&mut store, full_range(), move |encoded| {
			candidates
				.iter()
				.find(|c| (*c).into_group_state_key().as_slice() == encoded.as_bytes())
				.cloned()
		})
		.unwrap();
		cache.pool.set_budget(ByteSize::from_bytes(1));
		cache.evict_to_budget();
		cache.pool.set_budget(ByteSize::from_bytes(64 * 1024 * 1024));

		assert_eq!(cache.get(&mut store, &key2).unwrap(), None, "the colliding absent key reads None");
		assert_eq!(
			cache.completeness().false_positives.as_u64(),
			1,
			"the collision must be visible as a counted false positive"
		);
		assert_eq!(
			cache.get(&mut store, &key1).unwrap(),
			Some(cell(1)),
			"the live key must never be shadowed by its collision partner"
		);

		cache.remove(&mut store, &key1).unwrap();
		cache.flush(&mut store).unwrap();
		store.gets = 0;
		assert_eq!(
			cache.get(&mut store, &key2).unwrap(),
			None,
			"after the live key's drop the shared hash is fully untracked"
		);
		assert_eq!(store.gets, 0, "exactly one instance was removed, so absence is now in RAM");
	}

	#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
	struct GroupKey(GroupId, Keyspace, u8);

	impl HeapSize for GroupKey {
		fn heap_size(&self) -> usize {
			0
		}
	}

	impl IntoGroupStateKey for &GroupKey {
		fn into_group_state_key(self) -> GroupStateKey {
			OperatorGroupStateKey::inner_encoded(self.0, self.1, vec![self.2])
		}
	}

	const G1: GroupId = GroupId(1);
	const G2: GroupId = GroupId(2);

	fn acc(group: GroupId, suffix: u8) -> GroupKey {
		GroupKey(group, Keyspace::ACCUMULATOR, suffix)
	}

	fn seed_groups(store: &mut MockStore, cache: &mut StateCache<GroupKey, Cell>) {
		for (group, suffix, value) in [(G1, 1u8, 10), (G1, 2, 11), (G2, 1, 20), (G2, 2, 21)] {
			cache.set(store, &acc(group, suffix), &cell(value)).unwrap();
		}
		cache.set(store, &GroupKey(G1, Keyspace::ROW_NUMBER_MAPPING, 1), &cell(99)).unwrap();
		cache.flush(store).unwrap();
	}

	#[test]
	fn invalidating_a_group_drops_its_rows_and_leaves_every_other_group_whole() {
		// A neighbouring group losing entries here is silent state loss - the operator
		// would restart its aggregation from scratch.
		let mut store = MockStore::default();
		let mut cache: StateCache<GroupKey, Cell> = StateCache::new(big_pool());
		seed_groups(&mut store, &mut cache);

		let dropped = cache.invalidate_group_data(&GroupSet::new([G1]));

		assert_eq!(dropped, 2, "both of the reclaimed group's data rows must go");
		assert!(!cache.is_cached(&acc(G1, 1)));
		assert!(!cache.is_cached(&acc(G1, 2)));
		assert!(cache.is_cached(&acc(G2, 1)), "the neighbouring group must be untouched");
		assert!(cache.is_cached(&acc(G2, 2)), "the neighbouring group must be untouched");
	}

	#[test]
	fn invalidation_never_drops_an_identity_row() {
		// The row-number mapping outlives the data keyspaces on disk; dropping the cached
		// mapping would answer DefinitelyAbsent for a row that is still stored.
		let mut store = MockStore::default();
		let mut cache: StateCache<GroupKey, Cell> = StateCache::new(big_pool());
		seed_groups(&mut store, &mut cache);

		cache.invalidate_group_data(&GroupSet::new([G1]));

		assert!(
			cache.is_cached(&GroupKey(G1, Keyspace::ROW_NUMBER_MAPPING, 1)),
			"the identity row must survive the data phase"
		);
	}

	#[test]
	fn invalidation_keeps_completeness_and_answers_the_dropped_key_from_ram() {
		// The disk rows go in the same transaction, so the cache still holds every live
		// key; revoking here would knock every operator back into read-through.
		let mut store = MockStore::default();
		let mut cache: StateCache<GroupKey, Cell> = StateCache::new(big_pool());
		seed_groups(&mut store, &mut cache);
		cache.hydrate(&mut store, full_range(), |encoded| {
			[acc(G1, 1), acc(G1, 2), acc(G2, 1), acc(G2, 2), GroupKey(G1, Keyspace::ROW_NUMBER_MAPPING, 1)]
				.into_iter()
				.find(|candidate| (&candidate).into_group_state_key().as_slice() == encoded.as_bytes())
		})
		.unwrap();
		assert!(cache.completeness().values_complete, "precondition: hydration establishes completeness");

		cache.invalidate_group_data(&GroupSet::new([G1]));

		assert!(cache.completeness().values_complete, "reclaiming a group must not revoke completeness");
		store.gets = 0;
		assert_eq!(cache.get(&mut store, &acc(G1, 1)).unwrap(), None, "the dropped key must read absent");
		assert_eq!(store.gets, 0, "and it must do so without a store read");
	}

	#[test]
	fn invalidation_releases_the_bytes_it_dropped() {
		// Dropping entries without releasing their charge makes the budget evict live
		// entries to free space that is already free.
		let pool = big_pool();
		let mut store = MockStore::default();
		let mut cache: StateCache<GroupKey, Cell> = StateCache::new(pool.clone());
		seed_groups(&mut store, &mut cache);
		let before = pool.snapshot().total();

		cache.invalidate_group_data(&GroupSet::new([G1, G2]));

		let after = pool.snapshot().total();
		assert!(after < before, "reclaiming four rows must release their charge: {before:?} -> {after:?}");
		assert_eq!(cache.ledger.clean, pool.snapshot().resident.as_bytes(), "the ledger must stay exact");
	}

	#[test]
	fn a_pending_tombstone_is_not_removed_from_membership_twice() {
		// The tombstone already removed the key's membership evidence; removing it twice
		// would delete a fingerprint belonging to a different, still-stored key.
		let mut store = MockStore::default();
		let mut cache: StateCache<GroupKey, Cell> = StateCache::new(big_pool());
		seed_groups(&mut store, &mut cache);
		let population = cache.membership.population();

		cache.remove(&mut store, &acc(G1, 1)).unwrap();
		let after_drop = cache.membership.population();
		assert_eq!(after_drop, population.map(|p| p - 1), "precondition: the drop removed one instance");

		cache.invalidate_group_data(&GroupSet::new([G1]));

		assert_eq!(
			cache.membership.population(),
			after_drop.map(|p| p - 1),
			"only the surviving live row may be removed; the tombstoned key must not be removed twice"
		);
	}

	#[test]
	fn an_empty_group_set_touches_nothing() {
		let mut store = MockStore::default();
		let mut cache: StateCache<GroupKey, Cell> = StateCache::new(big_pool());
		seed_groups(&mut store, &mut cache);

		assert_eq!(cache.invalidate_group_data(&GroupSet::new([])), 0);
		assert!(cache.is_cached(&acc(G1, 1)));
	}
}
