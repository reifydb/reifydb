// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod error;
pub mod store;

use std::{
	collections::HashMap,
	slice,
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
};

use dashmap::{DashMap, DashSet};
use postcard::to_stdvec;
use reifydb_codec::encoded::row::EncodedRow;
use reifydb_core::{
	interface::catalog::dictionary::Dictionary,
	key::dictionary::{DictionaryEntryIndexKey, DictionaryEntryKey},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{
	Result,
	util::{cowvec::CowVec, hash::xxh3_128},
	value::{
		Value,
		dictionary::{DictionaryEntryId, DictionaryId},
		value_type::ValueType,
	},
};

use crate::dictionary::{
	error::DictionaryError,
	store::{DictEntryWrite, DictionaryStore, UnconfiguredDictionaryStore},
};

const CACHE_CAPACITY: usize = 65_536;

#[derive(Debug)]
pub struct InternOutcome {
	pub id: DictionaryEntryId,
	pub created: bool,
}

#[derive(Clone)]
pub struct DictionaryAllocatorRegistry {
	inner: Arc<Inner>,
}

impl Default for DictionaryAllocatorRegistry {
	fn default() -> Self {
		Self::new(Arc::new(UnconfiguredDictionaryStore))
	}
}

struct Inner {
	slots: DashMap<DictionaryId, Arc<DictSlot>>,
	dropped: DashSet<DictionaryId>,
	store: Arc<dyn DictionaryStore>,
}

enum Counter {
	Narrow(AtomicU64),
	Wide(Mutex<u128>),
}

impl Counter {
	fn new(id_type: &ValueType) -> Self {
		match id_type {
			ValueType::Uint16 => Counter::Wide(Mutex::new(0u128)),
			_ => Counter::Narrow(AtomicU64::new(0)),
		}
	}

	fn next(&self) -> Option<u128> {
		match self {
			Counter::Narrow(counter) => {
				let prev = counter.fetch_add(1, Ordering::SeqCst);
				prev.checked_add(1).map(|next| next as u128)
			}
			Counter::Wide(counter) => {
				let mut guard = counter.lock();
				let next = (*guard).checked_add(1)?;
				*guard = next;
				Some(next)
			}
		}
	}

	fn raise_to(&self, seed: u128) {
		match self {
			Counter::Narrow(counter) => {
				counter.fetch_max(seed.min(u64::MAX as u128) as u64, Ordering::SeqCst);
			}
			Counter::Wide(counter) => {
				let mut guard = counter.lock();
				if *guard < seed {
					*guard = seed;
				}
			}
		}
	}
}

struct DictSlot {
	counter: Counter,
	seeded: AtomicBool,
	alloc: Mutex<()>,
	cache: DashMap<[u8; 16], CacheEntry>,
	values: DashMap<u128, Arc<[u8]>>,
}

impl DictSlot {
	fn new(id_type: &ValueType) -> Self {
		Self {
			counter: Counter::new(id_type),
			seeded: AtomicBool::new(false),
			alloc: Mutex::new(()),
			cache: DashMap::new(),
			values: DashMap::new(),
		}
	}
}

struct CacheEntry {
	id: u128,
	value: Arc<[u8]>,
}

fn cache_into_slot(slot: &DictSlot, hash: [u8; 16], id: u128, value: Arc<[u8]>) {
	if slot.cache.len() >= CACHE_CAPACITY {
		slot.cache.clear();
	}
	if slot.values.len() >= CACHE_CAPACITY {
		slot.values.clear();
	}
	slot.cache.insert(
		hash,
		CacheEntry {
			id,
			value: value.clone(),
		},
	);
	slot.values.insert(id, value);
}

impl DictionaryAllocatorRegistry {
	pub fn new(store: Arc<dyn DictionaryStore>) -> Self {
		Self {
			inner: Arc::new(Inner {
				slots: DashMap::new(),
				dropped: DashSet::new(),
				store,
			}),
		}
	}

	pub fn intern(&self, dictionary: &Dictionary, value: &Value) -> Result<InternOutcome> {
		let mut outcomes = self.intern_batch(dictionary, slice::from_ref(value))?;
		Ok(outcomes.pop().expect("a batch of one value must yield exactly one outcome"))
	}

	pub fn intern_batch(&self, dictionary: &Dictionary, values: &[Value]) -> Result<Vec<InternOutcome>> {
		let serialized: Vec<Vec<u8>> = values
			.iter()
			.map(|value| to_stdvec(value).expect("failed to serialize dictionary value"))
			.collect();
		let hashes: Vec<[u8; 16]> = serialized.iter().map(|bytes| xxh3_128(bytes).0.to_be_bytes()).collect();

		let mut resolved: Vec<Option<u128>> = vec![None; values.len()];

		if let Some(slot) = self.inner.slots.get(&dictionary.id) {
			for (index, hash) in hashes.iter().enumerate() {
				if let Some(entry) = slot.cache.get(hash) {
					if entry.value.as_ref() != serialized[index].as_slice() {
						return Err(DictionaryError::HashCollision {
							dictionary: dictionary.id,
							hash: *hash,
						}
						.into());
					}
					resolved[index] = Some(entry.id);
				}
			}
		}

		if resolved.iter().all(Option::is_some) {
			return outcomes(dictionary, resolved, &[]);
		}

		let slot = self.slot(dictionary);
		let _guard = slot.alloc.lock();

		for index in 0..values.len() {
			if resolved[index].is_some() {
				continue;
			}
			let entry_key = DictionaryEntryKey::encoded(dictionary.id, hashes[index]);
			if let Some(existing) = self.inner.store.read_committed(&entry_key)? {
				let id = decode_entry_id(dictionary, &serialized[index], hashes[index], &existing)?;
				cache_into_slot(&slot, hashes[index], id, Arc::from(serialized[index].as_slice()));
				resolved[index] = Some(id);
			}
		}

		let missing: Vec<usize> = (0..values.len()).filter(|index| resolved[*index].is_none()).collect();
		if missing.is_empty() {
			return outcomes(dictionary, resolved, &[]);
		}

		self.seed_if_needed(dictionary, &slot)?;

		if self.inner.dropped.contains(&dictionary.id) {
			return Err(DictionaryError::Dropped {
				dictionary: dictionary.id,
			}
			.into());
		}

		let mut allocated: HashMap<[u8; 16], u128> = HashMap::new();
		let mut writes: Vec<DictEntryWrite> = Vec::new();
		let mut created_ids: Vec<u128> = Vec::new();

		for &index in &missing {
			let hash = hashes[index];
			if let Some(&id) = allocated.get(&hash) {
				resolved[index] = Some(id);
				continue;
			}
			let id = slot.counter.next().ok_or(DictionaryError::Exhausted {
				dictionary: dictionary.id,
			})?;
			allocated.insert(hash, id);
			writes.push(entry_write(dictionary, &serialized[index], hash, id));
			created_ids.push(id);
			resolved[index] = Some(id);
		}

		self.inner.store.commit_entries(dictionary.id, &writes)?;

		for &index in &missing {
			let hash = hashes[index];
			let id = allocated[&hash];
			cache_into_slot(&slot, hash, id, Arc::from(serialized[index].as_slice()));
		}

		outcomes(dictionary, resolved, &created_ids)
	}

	pub fn find(&self, dictionary: &Dictionary, value: &Value) -> Result<Option<DictionaryEntryId>> {
		let value_bytes = to_stdvec(value).expect("failed to serialize dictionary value");
		let hash = xxh3_128(&value_bytes).0.to_be_bytes();

		if let Some(slot) = self.inner.slots.get(&dictionary.id)
			&& let Some(entry) = slot.cache.get(&hash)
		{
			if entry.value.as_ref() != value_bytes.as_slice() {
				return Err(DictionaryError::HashCollision {
					dictionary: dictionary.id,
					hash,
				}
				.into());
			}
			return Ok(Some(DictionaryEntryId::from_u128(entry.id, dictionary.id_type.clone())?));
		}

		let entry_key = DictionaryEntryKey::encoded(dictionary.id, hash);
		match self.inner.store.read_committed(&entry_key)? {
			Some(existing) => {
				let id = decode_entry_id(dictionary, &value_bytes, hash, &existing)?;
				let slot = self.slot(dictionary);
				cache_into_slot(&slot, hash, id, Arc::from(value_bytes.as_slice()));
				Ok(Some(DictionaryEntryId::from_u128(id, dictionary.id_type.clone())?))
			}
			None => Ok(None),
		}
	}

	pub fn get(&self, dictionary: &Dictionary, id: u128) -> Result<Option<Arc<[u8]>>> {
		if let Some(value) = self.resolve_value(dictionary.id, id) {
			return Ok(Some(value));
		}

		let index_key = DictionaryEntryIndexKey::encoded(dictionary.id, id);
		match self.inner.store.read_committed(&index_key)? {
			Some(existing) => {
				let value: Arc<[u8]> = Arc::from(&existing[..]);
				self.cache_value(dictionary, id, value.clone());
				Ok(Some(value))
			}
			None => Ok(None),
		}
	}

	fn slot(&self, dictionary: &Dictionary) -> Arc<DictSlot> {
		self.inner
			.slots
			.entry(dictionary.id)
			.or_insert_with(|| Arc::new(DictSlot::new(&dictionary.id_type)))
			.clone()
	}

	fn seed_if_needed(&self, dictionary: &Dictionary, slot: &DictSlot) -> Result<()> {
		if slot.seeded.load(Ordering::Acquire) {
			return Ok(());
		}
		let seed = self.inner.store.max_index_id(dictionary.id)?.unwrap_or(0);
		slot.counter.raise_to(seed);
		slot.seeded.store(true, Ordering::Release);
		Ok(())
	}

	pub fn resolve_value(&self, dictionary: DictionaryId, id: u128) -> Option<Arc<[u8]>> {
		let slot = self.inner.slots.get(&dictionary)?;
		let value = slot.values.get(&id)?;
		Some(value.clone())
	}

	pub fn cache_value(&self, dictionary: &Dictionary, id: u128, value: Arc<[u8]>) {
		let slot = self.slot(dictionary);
		if slot.values.len() >= CACHE_CAPACITY {
			slot.values.clear();
		}
		slot.values.insert(id, value);
	}

	pub fn begin_drop(&self, dictionary: DictionaryId) {
		self.inner.dropped.insert(dictionary);
	}

	pub fn evict(&self, dictionary: DictionaryId) {
		self.inner.slots.remove(&dictionary);
		self.inner.dropped.remove(&dictionary);
	}

	pub fn cached_entries(&self) -> (usize, u64) {
		let mut count = 0usize;
		let mut bytes = 0u64;
		for slot in self.inner.slots.iter() {
			for entry in slot.cache.iter() {
				count += 1;
				bytes += entry.value.len() as u64 + 16;
			}
		}
		(count, bytes)
	}
}

fn outcomes(dictionary: &Dictionary, resolved: Vec<Option<u128>>, created_ids: &[u128]) -> Result<Vec<InternOutcome>> {
	resolved.into_iter()
		.map(|id| {
			let id = id.expect("every interned value must resolve to an id");
			Ok(InternOutcome {
				id: DictionaryEntryId::from_u128(id, dictionary.id_type.clone())?,
				created: created_ids.contains(&id),
			})
		})
		.collect()
}

fn decode_entry_id(dictionary: &Dictionary, value_bytes: &[u8], hash: [u8; 16], existing: &EncodedRow) -> Result<u128> {
	if existing.len() < 16 {
		return Err(DictionaryError::TruncatedEntry {
			dictionary: dictionary.id,
			hash,
			len: existing.len(),
		}
		.into());
	}
	if &existing[16..] != value_bytes {
		return Err(DictionaryError::HashCollision {
			dictionary: dictionary.id,
			hash,
		}
		.into());
	}
	Ok(u128::from_be_bytes(existing[..16].try_into().unwrap()))
}

fn entry_write(dictionary: &Dictionary, value_bytes: &[u8], hash: [u8; 16], id: u128) -> DictEntryWrite {
	let mut entry_value = Vec::with_capacity(16 + value_bytes.len());
	entry_value.extend_from_slice(&id.to_be_bytes());
	entry_value.extend_from_slice(value_bytes);

	DictEntryWrite {
		entry_key: DictionaryEntryKey::encoded(dictionary.id, hash),
		entry_value: EncodedRow(CowVec::new(entry_value)),
		index_key: DictionaryEntryIndexKey::encoded(dictionary.id, id),
		index_value: EncodedRow(CowVec::new(value_bytes.to_vec())),
	}
}

#[cfg(test)]
mod tests {
	use std::{collections::BTreeMap, thread};

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{interface::catalog::id::NamespaceId, key::EncodableKey};

	use super::*;

	#[derive(Default)]
	struct MockStoreInner {
		rows: BTreeMap<EncodedKey, EncodedRow>,
		commits: usize,
	}

	#[derive(Clone, Default)]
	struct MockStore {
		inner: Arc<Mutex<MockStoreInner>>,
	}

	impl MockStore {
		fn commit_count(&self) -> usize {
			self.inner.lock().commits
		}

		fn contains(&self, key: &EncodedKey) -> bool {
			self.inner.lock().rows.contains_key(key)
		}
	}

	impl DictionaryStore for MockStore {
		fn read_committed(&self, key: &EncodedKey) -> Result<Option<EncodedRow>> {
			Ok(self.inner.lock().rows.get(key).cloned())
		}

		fn max_index_id(&self, dictionary: DictionaryId) -> Result<Option<u128>> {
			let inner = self.inner.lock();
			let mut max: Option<u128> = None;
			for key in inner.rows.keys() {
				if let Some(decoded) = DictionaryEntryIndexKey::decode(key)
					&& decoded.dictionary == dictionary
				{
					max = Some(max.map_or(decoded.id, |m: u128| m.max(decoded.id)));
				}
			}
			Ok(max)
		}

		fn commit_entries(&self, _dictionary: DictionaryId, writes: &[DictEntryWrite]) -> Result<()> {
			let mut inner = self.inner.lock();
			for write in writes {
				inner.rows.insert(write.entry_key.clone(), write.entry_value.clone());
				inner.rows.insert(write.index_key.clone(), write.index_value.clone());
			}
			inner.commits += 1;
			Ok(())
		}
	}

	fn dict(id_type: ValueType) -> Dictionary {
		Dictionary {
			id: DictionaryId(1),
			namespace: NamespaceId::SYSTEM,
			name: "d".to_string(),
			value_type: ValueType::Utf8,
			id_type,
		}
	}

	fn utf8(v: &str) -> Value {
		Value::Utf8(v.to_string())
	}

	fn registry_on(store: &MockStore) -> DictionaryAllocatorRegistry {
		DictionaryAllocatorRegistry::new(Arc::new(store.clone()))
	}

	fn entry_key_of(d: &Dictionary, value: &Value) -> EncodedKey {
		let bytes = to_stdvec(value).unwrap();
		DictionaryEntryKey::encoded(d.id, xxh3_128(&bytes).0.to_be_bytes())
	}

	// The invariant the whole design rests on: the id an intern hands back already has a committed
	// entry in the store. Nothing may observe an id whose entry is not yet in the store, otherwise a
	// cold registry could remint the same value under a different id.
	#[test]
	fn intern_commits_the_entry_before_returning_the_id() {
		let store = MockStore::default();
		let registry = registry_on(&store);
		let d = dict(ValueType::Uint8);
		let value = utf8("wsol");

		assert!(!store.contains(&entry_key_of(&d, &value)), "precondition: nothing is durable yet");

		let outcome = registry.intern(&d, &value).unwrap();

		assert!(outcome.created, "a first-seen value must be reported as created");
		assert!(
			store.contains(&entry_key_of(&d, &value)),
			"the entry must already be durable at the moment intern returns its id"
		);
		assert_eq!(store.commit_count(), 1, "the entry must be persisted by exactly one commit");
	}

	// Distinct values get distinct ids; the same value resolves to one id whether it comes from the
	// cache or from a cold read. A value that forks into two ids splits every operator state keyed on
	// it, so this is the property the allocation lock exists to hold.
	#[test]
	fn same_value_shares_one_id_distinct_values_differ() {
		let store = MockStore::default();
		let registry = registry_on(&store);
		let d = dict(ValueType::Uint8);

		let a = registry.intern(&d, &utf8("wsol")).unwrap();
		let b = registry.intern(&d, &utf8("wsol")).unwrap();
		let c = registry.intern(&d, &utf8("usdc")).unwrap();

		assert_eq!(a.id, b.id, "the same value must resolve to one id");
		assert!(a.created, "the first sight of a value creates it");
		assert!(!b.created, "the second sight must not create it again");
		assert_ne!(a.id, c.id, "distinct values must get distinct ids");
		assert_eq!(a.id.to_u128(), 1);
		assert_eq!(c.id.to_u128(), 2);
		assert_eq!(store.commit_count(), 2, "only the two first sights commit");
	}

	// A second process (or a rebuilt flow engine) shares the store but not the cache. It must resolve
	// an already-durable value to the existing id through the committed read rather than mint a
	// second one.
	#[test]
	fn a_cold_registry_resolves_a_durable_value_without_reminting() {
		let store = MockStore::default();
		let d = dict(ValueType::Uint8);

		let first = registry_on(&store).intern(&d, &utf8("wsol")).unwrap();

		let cold = registry_on(&store);
		let second = cold.intern(&d, &utf8("wsol")).unwrap();

		assert_eq!(second.id, first.id, "a durable value keeps its id across registries");
		assert!(!second.created, "an already-durable value is not created again");
		assert_eq!(store.commit_count(), 1, "resolving a durable value must not commit anything");
	}

	// Restart: the counter lives only in memory, so it is reseeded from the maximum durable index id.
	// Because no id is ever handed out without a committed entry, that maximum is at or above every id
	// ever issued, and an id can never be reissued to a different value.
	#[test]
	fn restart_reseeds_the_counter_above_every_issued_id() {
		let store = MockStore::default();
		let d = dict(ValueType::Uint8);

		let ids: Vec<u128> = ["a", "b", "c"]
			.iter()
			.map(|v| registry_on(&store).intern(&d, &utf8(v)).unwrap().id.to_u128())
			.collect();
		assert_eq!(ids, vec![1, 2, 3]);

		let restarted = registry_on(&store);
		let next = restarted.intern(&d, &utf8("d")).unwrap();

		assert_eq!(
			next.id.to_u128(),
			4,
			"a restarted registry must continue past the durable maximum, never reissue below it"
		);
	}

	// One commit per batch, not per value: a warmup burst of first-seen values would otherwise pay a
	// store transaction (and its coarse lock round trip) per value instead of one for the batch.
	#[test]
	fn a_batch_of_new_values_produces_exactly_one_commit() {
		let store = MockStore::default();
		let registry = registry_on(&store);
		let d = dict(ValueType::Uint8);

		let values: Vec<Value> = (0..8).map(|i| utf8(&format!("mint-{i}"))).collect();
		let outcomes = registry.intern_batch(&d, &values).unwrap();

		assert_eq!(outcomes.len(), 8);
		assert!(outcomes.iter().all(|o| o.created));
		assert_eq!(store.commit_count(), 1, "eight first-seen values must cost exactly one commit");
	}

	// A batch carrying the same value twice - two trades on one mint in one block - must allocate a
	// single id and write a single entry, not two.
	#[test]
	fn a_batch_dedupes_a_repeated_value_to_one_id() {
		let store = MockStore::default();
		let registry = registry_on(&store);
		let d = dict(ValueType::Uint8);

		let outcomes = registry.intern_batch(&d, &[utf8("wsol"), utf8("usdc"), utf8("wsol")]).unwrap();

		assert_eq!(outcomes[0].id, outcomes[2].id, "a repeated value in one batch shares its id");
		assert_ne!(outcomes[0].id, outcomes[1].id);
		assert_eq!(store.commit_count(), 1);
	}

	// Concurrent first sight of the same value on two threads: the allocation lock plus the re-read
	// under it must collapse them onto one id. Two ids for one value would silently split every
	// group-by and operator state keyed on that id, and nothing downstream would report an error.
	#[test]
	fn two_threads_interning_the_same_new_value_agree_on_one_id() {
		let store = MockStore::default();
		let registry = registry_on(&store);
		let d = dict(ValueType::Uint8);

		let ids: Vec<u128> = thread::scope(|scope| {
			let handles: Vec<_> = (0..8)
				.map(|_| {
					let registry = registry.clone();
					let d = d.clone();
					scope.spawn(move || registry.intern(&d, &utf8("wsol")).unwrap().id.to_u128())
				})
				.collect();
			handles.into_iter().map(|h| h.join().unwrap()).collect()
		});

		assert!(ids.iter().all(|id| *id == ids[0]), "all threads must agree on one id, got {ids:?}");
		assert_eq!(store.commit_count(), 1, "exactly one thread may commit the entry");
	}

	// Two different values whose hashes collide must be refused rather than silently aliased onto one
	// id, which would make one value decode as the other.
	#[test]
	fn a_hash_collision_is_refused_not_aliased() {
		let store = MockStore::default();
		let registry = registry_on(&store);
		let d = dict(ValueType::Uint8);

		let value = utf8("wsol");
		let bytes = to_stdvec(&value).unwrap();
		let hash = xxh3_128(&bytes).0.to_be_bytes();

		let mut poisoned = Vec::with_capacity(16 + 4);
		poisoned.extend_from_slice(&7u128.to_be_bytes());
		poisoned.extend_from_slice(b"other");
		store.inner
			.lock()
			.rows
			.insert(DictionaryEntryKey::encoded(d.id, hash), EncodedRow(CowVec::new(poisoned)));

		let err = registry.intern(&d, &value).unwrap_err();
		assert!(
			err.to_string().to_lowercase().contains("collision"),
			"a hash slot holding different bytes must raise a collision, got: {err}"
		);
	}
}
