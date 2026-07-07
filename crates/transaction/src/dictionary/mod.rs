// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod error;

use std::sync::{
	Arc,
	atomic::{AtomicBool, AtomicU64, Ordering},
};

use dashmap::{DashMap, DashSet, mapref::entry::Entry};
use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	interface::catalog::dictionary::Dictionary,
	key::{
		EncodableKey,
		dictionary::{DictionaryEntryIndexKey, DictionaryEntryKey},
	},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{
	Result,
	util::{cowvec::CowVec, hash::xxh3_128},
	value::{
		dictionary::{DictionaryEntryId, DictionaryId},
		value_type::ValueType,
	},
};

use crate::{
	dictionary::error::DictionaryError,
	multi::{RangeScope, transaction::read::MultiReadTransaction},
};

pub trait DictionaryReader {
	fn read(&mut self, key: &EncodedKey) -> Result<Option<EncodedRow>>;

	fn max_index_id(&mut self, dictionary: DictionaryId) -> Result<Option<u128>>;
}

pub struct DictWrites {
	pub entry_key: EncodedKey,
	pub entry_value: EncodedRow,
	pub index_key: EncodedKey,
	pub index_value: EncodedRow,
}

pub struct InternOutcome {
	pub id: DictionaryEntryId,
	pub hash: [u8; 16],
	pub writes: Option<DictWrites>,
}

#[derive(Clone, Default)]
pub struct DictionaryAllocatorRegistry {
	inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
	slots: DashMap<DictionaryId, DictSlot>,
	dropped: DashSet<DictionaryId>,
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
	reservations: DashMap<[u8; 16], Reservation>,
}

impl DictSlot {
	fn new(id_type: &ValueType) -> Self {
		Self {
			counter: Counter::new(id_type),
			seeded: AtomicBool::new(false),
			reservations: DashMap::new(),
		}
	}
}

struct Reservation {
	id: u128,
	value: Arc<[u8]>,
}

impl DictionaryAllocatorRegistry {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn intern(
		&self,
		dictionary: &Dictionary,
		value_bytes: &[u8],
		reader: &mut impl DictionaryReader,
	) -> Result<InternOutcome> {
		let hash = xxh3_128(value_bytes).0.to_be_bytes();
		let entry_key = DictionaryEntryKey::encoded(dictionary.id, hash);

		if let Some(existing) = reader.read(&entry_key)? {
			let id = decode_entry_id(dictionary, value_bytes, hash, &existing)?;
			return Ok(InternOutcome {
				id,
				hash,
				writes: None,
			});
		}

		self.seed_if_needed(dictionary, reader)?;

		let slot = match self.inner.slots.get(&dictionary.id) {
			Some(slot) => slot,
			None => {
				return Err(DictionaryError::Dropped {
					dictionary: dictionary.id,
				}
				.into());
			}
		};

		match slot.reservations.entry(hash) {
			Entry::Occupied(occupied) => {
				let reservation = occupied.get();
				if reservation.value.as_ref() != value_bytes {
					return Err(DictionaryError::HashCollision {
						dictionary: dictionary.id,
						hash,
					}
					.into());
				}
				let id = DictionaryEntryId::from_u128(reservation.id, dictionary.id_type.clone())?;
				Ok(write_outcome(dictionary, value_bytes, hash, reservation.id, id))
			}
			Entry::Vacant(vacant) => {
				if let Some(existing) = reader.read(&entry_key)? {
					let id = decode_entry_id(dictionary, value_bytes, hash, &existing)?;
					return Ok(InternOutcome {
						id,
						hash,
						writes: None,
					});
				}
				if self.inner.dropped.contains(&dictionary.id) {
					return Err(DictionaryError::Dropped {
						dictionary: dictionary.id,
					}
					.into());
				}
				let next = slot.counter.next().ok_or(DictionaryError::Exhausted {
					dictionary: dictionary.id,
				})?;
				let id = DictionaryEntryId::from_u128(next, dictionary.id_type.clone())?;
				vacant.insert(Reservation {
					id: next,
					value: Arc::from(value_bytes),
				});
				Ok(write_outcome(dictionary, value_bytes, hash, next, id))
			}
		}
	}

	pub fn mark_durable(&self, dictionary: DictionaryId, hashes: &[[u8; 16]]) {
		if let Some(slot) = self.inner.slots.get(&dictionary) {
			for hash in hashes {
				slot.reservations.remove(hash);
			}
		}
	}

	pub fn begin_drop(&self, dictionary: DictionaryId) {
		self.inner.dropped.insert(dictionary);
	}

	pub fn evict(&self, dictionary: DictionaryId) {
		self.inner.slots.remove(&dictionary);
		self.inner.dropped.remove(&dictionary);
	}

	pub fn reservation_len(&self, dictionary: DictionaryId) -> usize {
		self.inner.slots.get(&dictionary).map(|slot| slot.reservations.len()).unwrap_or(0)
	}

	pub fn reserved_id(&self, dictionary: DictionaryId, hash: &[u8; 16], value_bytes: &[u8]) -> Option<u128> {
		let slot = self.inner.slots.get(&dictionary)?;
		let reservation = slot.reservations.get(hash)?;
		(reservation.value.as_ref() == value_bytes).then_some(reservation.id)
	}

	pub fn total_reservations(&self) -> (usize, u64) {
		let mut count = 0usize;
		let mut bytes = 0u64;
		for slot in self.inner.slots.iter() {
			for reservation in slot.reservations.iter() {
				count += 1;
				bytes += reservation.value.len() as u64 + 16;
			}
		}
		(count, bytes)
	}

	fn seed_if_needed(&self, dictionary: &Dictionary, reader: &mut impl DictionaryReader) -> Result<()> {
		if let Some(slot) = self.inner.slots.get(&dictionary.id)
			&& slot.seeded.load(Ordering::Acquire)
		{
			return Ok(());
		}
		let seed = reader.max_index_id(dictionary.id)?.unwrap_or(0);
		let slot = self.inner.slots.entry(dictionary.id).or_insert_with(|| DictSlot::new(&dictionary.id_type));
		slot.counter.raise_to(seed);
		slot.seeded.store(true, Ordering::Release);
		Ok(())
	}
}

fn decode_entry_id(
	dictionary: &Dictionary,
	value_bytes: &[u8],
	hash: [u8; 16],
	existing: &EncodedRow,
) -> Result<DictionaryEntryId> {
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
	let id = u128::from_be_bytes(existing[..16].try_into().unwrap());
	DictionaryEntryId::from_u128(id, dictionary.id_type.clone())
}

fn write_outcome(
	dictionary: &Dictionary,
	value_bytes: &[u8],
	hash: [u8; 16],
	id: u128,
	entry_id: DictionaryEntryId,
) -> InternOutcome {
	let mut entry_value = Vec::with_capacity(16 + value_bytes.len());
	entry_value.extend_from_slice(&id.to_be_bytes());
	entry_value.extend_from_slice(value_bytes);

	let entry_key = DictionaryEntryKey::encoded(dictionary.id, hash);
	let index_key = DictionaryEntryIndexKey::encoded(dictionary.id, id);

	InternOutcome {
		id: entry_id,
		hash,
		writes: Some(DictWrites {
			entry_key,
			entry_value: EncodedRow(CowVec::new(entry_value)),
			index_key,
			index_value: EncodedRow(CowVec::new(value_bytes.to_vec())),
		}),
	}
}

impl DictionaryReader for MultiReadTransaction {
	fn read(&mut self, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		Ok(self.get(key)?.map(|value| value.row().clone()))
	}

	fn max_index_id(&mut self, dictionary: DictionaryId) -> Result<Option<u128>> {
		let range = DictionaryEntryIndexKey::full_scan(dictionary);
		let mut iter = self.range(range, RangeScope::All, 1);
		match iter.next() {
			Some(result) => Ok(DictionaryEntryIndexKey::decode(&result?.key).map(|key| key.id)),
			None => Ok(None),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_core::interface::catalog::id::NamespaceId;

	use super::*;

	struct MockReader {
		store: BTreeMap<EncodedKey, EncodedRow>,
	}

	impl MockReader {
		fn new() -> Self {
			Self {
				store: BTreeMap::new(),
			}
		}

		fn commit(&mut self, writes: &DictWrites) {
			self.store.insert(writes.entry_key.clone(), writes.entry_value.clone());
			self.store.insert(writes.index_key.clone(), writes.index_value.clone());
		}
	}

	impl DictionaryReader for MockReader {
		fn read(&mut self, key: &EncodedKey) -> Result<Option<EncodedRow>> {
			Ok(self.store.get(key).cloned())
		}

		fn max_index_id(&mut self, dictionary: DictionaryId) -> Result<Option<u128>> {
			let mut max: Option<u128> = None;
			for key in self.store.keys() {
				if let Some(decoded) = DictionaryEntryIndexKey::decode(key) {
					if decoded.dictionary == dictionary {
						max = Some(max.map_or(decoded.id, |m| m.max(decoded.id)));
					}
				}
			}
			Ok(max)
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

	// Two interns of the SAME brand-new value before it is durable must agree on one id
	// via the reservation, and each must co-write the identical record; a different value
	// gets a different id. This is the concurrent-flow-worker case that overflowed the
	// shared sequence key and aborted pump; distinct new values on parallel workers must
	// never collide, and the same new value must never fork into two ids.
	#[test]
	fn same_value_shares_one_id_distinct_values_differ() {
		let registry = DictionaryAllocatorRegistry::new();
		let d = dict(ValueType::Uint8);
		let mut reader = MockReader::new();

		let a = registry.intern(&d, b"wsol", &mut reader).unwrap();
		assert!(a.writes.is_some(), "first sight of a value must produce a durable record");

		let b = registry.intern(&d, b"wsol", &mut reader).unwrap();
		assert_eq!(a.id, b.id, "the same value must resolve to the same id via the reservation");
		assert!(b.writes.is_some(), "a concurrent sharer co-writes the byte-identical record");

		let c = registry.intern(&d, b"usdc", &mut reader).unwrap();
		assert_ne!(a.id, c.id, "distinct values must get distinct ids");
		assert_eq!(a.id.to_u128(), 1);
		assert_eq!(c.id.to_u128(), 2);
	}

	// Once the record is durable and its reservation evicted, interning the same value
	// resolves through the latest-committed Tier-1 read and produces no new write. Without
	// this the reservation map would leak one entry per interned value forever.
	#[test]
	fn durable_value_needs_no_write() {
		let registry = DictionaryAllocatorRegistry::new();
		let d = dict(ValueType::Uint8);
		let mut reader = MockReader::new();

		let a = registry.intern(&d, b"wsol", &mut reader).unwrap();
		reader.commit(a.writes.as_ref().unwrap());
		registry.mark_durable(d.id, &[a.hash]);

		let b = registry.intern(&d, b"wsol", &mut reader).unwrap();
		assert_eq!(a.id, b.id);
		assert!(b.writes.is_none(), "an already-durable value must not be re-written");
	}

	// The command-transaction commit hook evicts reservations via mark_durable; this pins that
	// primitive directly. A not-yet-durable interned value holds exactly one reservation, and
	// mark_durable frees it - without this eviction the command path leaks one reservation per
	// interned value until process exit.
	#[test]
	fn mark_durable_frees_the_reservation() {
		let registry = DictionaryAllocatorRegistry::new();
		let d = dict(ValueType::Uint8);
		let mut reader = MockReader::new();

		let a = registry.intern(&d, b"wsol", &mut reader).unwrap();
		assert_eq!(registry.reservation_len(d.id), 1, "an un-committed interned value holds one reservation");

		reader.commit(a.writes.as_ref().unwrap());
		registry.mark_durable(d.id, &[a.hash]);
		assert_eq!(registry.reservation_len(d.id), 0, "mark_durable must free the reservation");
	}

	// A value interned but not yet durable is invisible to the committed-version read snapshot a
	// downstream deferred flow reads through, yet that flow may key operator state on the value's
	// id in the same uncommitted cycle. reserved_id exposes the live reservation for that
	// read-through: a not-yet-interned value has no reservation, a different value sharing the hash
	// slot must not be mis-resolved, and once mark_durable evicts the reservation the reader falls
	// back to the committed tier. Without this a first-seen mint reaching a downstream operator
	// before its intern commits resolves to nothing and the operator aborts the process.
	#[test]
	fn reserved_id_exposes_live_reservation_until_durable() {
		let registry = DictionaryAllocatorRegistry::new();
		let d = dict(ValueType::Uint8);
		let mut reader = MockReader::new();

		let a = registry.intern(&d, b"wsol", &mut reader).unwrap();
		let hash = a.hash;

		assert_eq!(
			registry.reserved_id(d.id, &hash, b"wsol"),
			Some(a.id.to_u128()),
			"a live reservation must be resolvable by a concurrent reader before commit"
		);
		assert_eq!(
			registry.reserved_id(d.id, &hash, b"usdc"),
			None,
			"a value that does not match the reserved bytes under this hash must not be mis-resolved"
		);
		assert_eq!(
			registry.reserved_id(DictionaryId(999), &hash, b"wsol"),
			None,
			"a dictionary with no slot has no reservations"
		);

		registry.mark_durable(d.id, &[hash]);
		assert_eq!(
			registry.reserved_id(d.id, &hash, b"wsol"),
			None,
			"once durable the reservation is evicted and the reader must resolve via the committed tier"
		);
	}

	// total_reservations sums live (not-yet-durable) reservations across all dictionaries for the
	// memory gauge that surfaces leak-on-rollback growth: a healthy commit path drains to ~0 via
	// mark_durable, while a count that only climbs signals reservations leaked by rolled-back cycles.
	// Bytes must account the value payload plus the 16-byte id so the estimate tracks real footprint.
	#[test]
	fn total_reservations_counts_live_reservations_and_shrinks_on_durable() {
		let registry = DictionaryAllocatorRegistry::new();
		let d = dict(ValueType::Uint8);
		let mut reader = MockReader::new();

		assert_eq!(registry.total_reservations(), (0, 0), "a fresh registry holds no reservations");

		let a = registry.intern(&d, b"wsol", &mut reader).unwrap();
		let _b = registry.intern(&d, b"usdc", &mut reader).unwrap();
		assert_eq!(
			registry.total_reservations(),
			(2, (b"wsol".len() + b"usdc".len()) as u64 + 32),
			"two distinct not-yet-durable values hold two reservations; bytes = value lengths + 16 per id"
		);

		reader.commit(a.writes.as_ref().unwrap());
		registry.mark_durable(d.id, &[a.hash]);
		assert_eq!(
			registry.total_reservations(),
			(1, b"usdc".len() as u64 + 16),
			"mark_durable frees the committed value's reservation, leaving only the still-pending one"
		);
	}

	// A fresh registry (process restart) seeds its counter from the maximum committed index
	// id, so it continues the sequence and never reissues an id that is already durable.
	#[test]
	fn seeds_counter_from_max_committed_index() {
		let registry = DictionaryAllocatorRegistry::new();
		let d = dict(ValueType::Uint8);
		let mut reader = MockReader::new();
		reader.store.insert(
			DictionaryEntryIndexKey::encoded(d.id, 5),
			EncodedRow(CowVec::new(b"existing".to_vec())),
		);

		let a = registry.intern(&d, b"fresh", &mut reader).unwrap();
		assert_eq!(a.id.to_u128(), 6, "the next id must continue past the durable maximum");
	}

	// A Uint16 dictionary allocates genuine u128 ids through the mutex-backed counter and
	// the u128 index key; seeding from a beyond-u64 maximum must be exact.
	#[test]
	fn wide_dictionary_allocates_u128_ids() {
		let registry = DictionaryAllocatorRegistry::new();
		let d = dict(ValueType::Uint16);
		let mut reader = MockReader::new();
		let seed = (u64::MAX as u128) + 1;
		reader.store.insert(
			DictionaryEntryIndexKey::encoded(d.id, seed),
			EncodedRow(CowVec::new(b"existing".to_vec())),
		);

		let a = registry.intern(&d, b"wide", &mut reader).unwrap();
		assert_eq!(a.id.to_u128(), seed + 1, "wide ids must exceed u64 without truncation");
	}
}
