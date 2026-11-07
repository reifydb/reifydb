// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{bytes::EncodedBytes, pod::EncodedPodRow},
};
use reifydb_core::{
	delta::SetPre,
	interface::catalog::{
		ringbuffer::{
			RingBufferMetadata, decode_ringbuffer_free_page, decode_ringbuffer_order,
			encode_ringbuffer_free_page, encode_ringbuffer_order,
		},
		storage::StorageId,
	},
	key::{
		any::TaggedKey,
		ringbuffer::{RingBufferFreeKey, ring_order_key, ring_order_range},
	},
	ring::{MetadataWrite, Placement, Ring, RingRead, RingStore},
};
use reifydb_value::{
	Result,
	value::{partition::Partition, row_number::RowNumber},
};

use crate::{
	txn::{FlowStorageTransaction, Range, StorageTransaction},
	writer::{Written, table::TableRowAddress},
};

pub fn ring_row_encoded(storage: StorageId, partition: Option<Partition>, row: RowNumber) -> EncodedKey {
	TableRowAddress {
		storage,
		partition,
		row,
	}
	.encoded(None)
}

pub fn ring_row_tagged(storage: StorageId, partition: Option<Partition>, row: RowNumber) -> TaggedKey {
	TableRowAddress {
		storage,
		partition,
		row,
	}
	.tagged()
}

pub trait RingCounters<T> {
	fn metadata(&self, txn: &mut T, partitions: &[Option<Partition>]) -> Result<Vec<Option<RingBufferMetadata>>>;

	fn reserve(&self, txn: &mut T, capacity: u64) -> Result<u64>;

	fn save_metadata(&self, txn: &mut T, entries: &[(Option<Partition>, RingBufferMetadata)]) -> Result<()>;

	fn remove_metadata(&self, txn: &mut T, partitions: &[Option<Partition>]) -> Result<()>;
}

pub struct RingAdapter<'a, T, C> {
	pub txn: &'a mut T,
	pub counters: &'a C,
	pub storage: StorageId,
}

impl<T: StorageTransaction + Range, C: RingCounters<T>> RingRead for RingAdapter<'_, T, C> {
	fn metadata(&mut self, partitions: &[Option<Partition>]) -> Result<Vec<Option<RingBufferMetadata>>> {
		self.counters.metadata(self.txn, partitions)
	}

	fn slots(&mut self, rows: &[(Option<Partition>, RowNumber)]) -> Result<Vec<Option<EncodedBytes>>> {
		if rows.is_empty() {
			return Ok(Vec::new());
		}
		let keys: Vec<TaggedKey> =
			rows.iter().map(|&(partition, row)| ring_row_tagged(self.storage, partition, row)).collect();
		self.txn.get_tagged(&keys)
	}

	fn order_at(&mut self, entries: &[(Option<Partition>, u64)]) -> Result<Vec<Option<RowNumber>>> {
		if entries.is_empty() {
			return Ok(Vec::new());
		}
		let keys: Vec<TaggedKey> = entries
			.iter()
			.map(|&(partition, sequence)| ring_order_key(self.storage, partition, sequence))
			.collect();
		let rows = self.txn.get_tagged(&keys)?;
		keys.iter()
			.zip(rows)
			.map(|(key, row)| {
				row.map(|row| {
					decode_ringbuffer_order(key, EncodedPodRow::view(&row)).map(|(_, row)| row)
				})
				.transpose()
			})
			.collect()
	}

	fn order(
		&mut self,
		partition: Option<Partition>,
		after: Option<u64>,
		limit: usize,
	) -> Result<Vec<(u64, RowNumber)>> {
		let resume = after.map(|sequence| ring_order_key(self.storage, partition, sequence));
		let range = ring_order_range(self.storage, partition).resume_after(resume.as_ref());
		let mut entries = Vec::with_capacity(limit);
		for row in self.txn.range(range, limit)?.take(limit) {
			let row = row?;
			entries.push(decode_ringbuffer_order(&row.key, EncodedPodRow::view(&row.bytes))?);
		}
		Ok(entries)
	}
}

impl<T: StorageTransaction + Range, C: RingCounters<T>> RingStore for RingAdapter<'_, T, C> {
	fn reserve(&mut self, capacity: u64) -> Result<u64> {
		self.counters.reserve(self.txn, capacity)
	}

	fn free_pages(&mut self, partition: Option<Partition>, pages: &[u64]) -> Result<Vec<Vec<RowNumber>>> {
		let keys: Vec<TaggedKey> =
			pages.iter().map(|&page| RingBufferFreeKey::of(self.storage, partition, page).into()).collect();
		self.txn.get_tagged(&keys)?
			.into_iter()
			.map(|page| match page {
				None => Ok(Vec::new()),
				Some(bytes) => decode_ringbuffer_free_page(EncodedPodRow::view(&bytes)),
			})
			.collect()
	}
}

pub struct RingMetadata<'a, T: StorageTransaction + Range> {
	txn: &'a mut T,
	storage: StorageId,
}

impl<'a, T: StorageTransaction + Range> RingMetadata<'a, T> {
	pub fn new(txn: &'a mut T, storage: StorageId) -> Self {
		Self {
			txn,
			storage,
		}
	}

	fn adapter<'b, C: RingCounters<T>>(&'b mut self, counters: &'b C) -> RingAdapter<'b, T, C> {
		RingAdapter {
			txn: &mut *self.txn,
			counters,
			storage: self.storage,
		}
	}

	pub fn place<C: RingCounters<T>>(
		&mut self,
		counters: &C,
		ring: &mut Ring,
		requests: &[(Option<Partition>, usize)],
	) -> Result<Vec<Vec<Placement>>> {
		ring.place(&mut self.adapter(counters), requests)
	}

	pub fn release<C: RingCounters<T>>(
		&mut self,
		counters: &C,
		ring: &mut Ring,
		requests: &[(Option<Partition>, Vec<RowNumber>)],
	) -> Result<Vec<Vec<Option<EncodedBytes>>>> {
		ring.remove(&mut self.adapter(counters), requests)
	}

	pub fn save<C: RingCounters<T>>(&mut self, counters: &C, ring: &Ring) -> Result<()> {
		let mut saved = Vec::new();
		let mut dropped = Vec::new();
		let mut sets = Vec::new();
		let mut removes = Vec::new();
		for write in ring.writes() {
			match write {
				MetadataWrite::Save(partition, metadata) => saved.push((partition, metadata)),
				MetadataWrite::Remove(partition) => dropped.push(partition),
				MetadataWrite::Order(partition, sequence, Some(row)) => sets.push((
					ring_order_key(self.storage, partition, sequence),
					encode_ringbuffer_order(row).into_bytes(),
					SetPre::Unknown,
				)),
				MetadataWrite::Order(partition, sequence, None) => {
					removes.push(ring_order_key(self.storage, partition, sequence))
				}
				MetadataWrite::Free(partition, page, None) => {
					removes.push(RingBufferFreeKey::of(self.storage, partition, page).into())
				}
				MetadataWrite::Free(partition, page, Some(slots)) => sets.push((
					RingBufferFreeKey::of(self.storage, partition, page).into(),
					encode_ringbuffer_free_page(&slots).into_bytes(),
					SetPre::Unknown,
				)),
			}
		}
		if !saved.is_empty() {
			counters.save_metadata(self.txn, &saved)?;
		}
		if !dropped.is_empty() {
			counters.remove_metadata(self.txn, &dropped)?;
		}
		if !removes.is_empty() {
			self.txn.remove_tagged(removes)?;
		}
		if !sets.is_empty() {
			self.txn.set_with_pre(sets)?;
		}
		Ok(())
	}
}

pub struct RingWriter<'a, T: StorageTransaction> {
	txn: &'a mut T,
}

impl<'a, T: StorageTransaction> RingWriter<'a, T> {
	pub fn new(txn: &'a mut T) -> Self {
		Self {
			txn,
		}
	}

	pub fn insert(&mut self, rows: Vec<(TaggedKey, EncodedBytes)>) -> Result<Written> {
		self.set_tagged(rows)
	}

	pub fn update(&mut self, rows: Vec<(TaggedKey, EncodedBytes)>) -> Result<Written> {
		self.set_tagged(rows)
	}

	pub fn delete(&mut self, rows: Vec<(TaggedKey, EncodedBytes)>) -> Result<Written> {
		let replaced = rows.iter().map(|(_, pre)| pre.clone()).collect();
		self.txn.remove_with_pre(rows)?;
		Ok(Written {
			written: Vec::new(),
			replaced,
		})
	}

	fn set_tagged(&mut self, rows: Vec<(TaggedKey, EncodedBytes)>) -> Result<Written> {
		let written = rows.iter().map(|(_, row)| row.clone()).collect();
		self.txn.set_with_pre(rows.into_iter().map(|(key, row)| (key, row, SetPre::Unknown)).collect())?;
		Ok(Written {
			written,
			replaced: Vec::new(),
		})
	}
}

pub struct FlowRingWriter<'a, T: FlowStorageTransaction> {
	txn: &'a mut T,
}

impl<'a, T: FlowStorageTransaction> FlowRingWriter<'a, T> {
	pub fn new(txn: &'a mut T) -> Self {
		Self {
			txn,
		}
	}

	pub fn insert(&mut self, keys: Vec<EncodedKey>, rows: Vec<EncodedBytes>) -> Result<()> {
		self.txn.set(keys, rows)
	}

	pub fn update(&mut self, keys: Vec<EncodedKey>, rows: Vec<EncodedBytes>) -> Result<()> {
		self.txn.set(keys, rows)
	}

	pub fn delete(&mut self, keys: Vec<EncodedKey>) -> Result<()> {
		self.txn.remove(keys)
	}
}

#[cfg(test)]
mod tests {
	use std::{
		cell::{Cell, RefCell},
		collections::{BTreeMap, HashMap},
	};

	use reifydb_codec::row::{
		bytes::RowBuilder,
		ringbuffer::EncodedRingBufferRow,
		shape::{RowFamily, RowShape},
	};
	use reifydb_core::{
		common::CommitVersion,
		interface::{catalog::id::RingBufferId, store::MultiVersionRow},
		key::bound::TaggedKeyBoundRange,
		ring::{FREE_PAGE_SLOTS, oldest_first},
	};
	use reifydb_value::value::value_type::ValueType;

	use super::*;
	use crate::txn::{Batch, FlowBatch, FlowGet, Get};

	#[derive(Debug, PartialEq, Eq)]
	enum Seam {
		Set(usize),
		Remove(usize),
		SetWithPre(Vec<(TaggedKey, EncodedBytes, SetPre)>),
		RemoveWithPre(Vec<(TaggedKey, EncodedBytes)>),
		RemoveTagged(Vec<TaggedKey>),
	}

	#[derive(Default)]
	struct Memory {
		rows: BTreeMap<EncodedKey, EncodedBytes>,
		seam: Vec<Seam>,
		reads: usize,
		ranges: usize,
	}

	impl Get for Memory {
		fn get_tagged(&mut self, keys: &[TaggedKey]) -> Result<Vec<Option<EncodedBytes>>> {
			self.reads += 1;
			Ok(keys.iter().map(|key| self.rows.get(&key.encode()).cloned()).collect())
		}
	}

	impl Batch for Memory {
		fn set_with_pre(&mut self, entries: Vec<(TaggedKey, EncodedBytes, SetPre)>) -> Result<()> {
			for (key, row, _) in &entries {
				self.rows.insert(key.encode(), row.clone());
			}
			self.seam.push(Seam::SetWithPre(entries));
			Ok(())
		}

		fn remove_with_pre(&mut self, entries: Vec<(TaggedKey, EncodedBytes)>) -> Result<()> {
			for (key, _) in &entries {
				self.rows.remove(&key.encode());
			}
			self.seam.push(Seam::RemoveWithPre(entries));
			Ok(())
		}

		fn remove_tagged(&mut self, keys: Vec<TaggedKey>) -> Result<()> {
			for key in &keys {
				self.rows.remove(&key.encode());
			}
			self.seam.push(Seam::RemoveTagged(keys));
			Ok(())
		}
	}

	impl Range for Memory {
		fn range(
			&mut self,
			range: TaggedKeyBoundRange,
			_batch_size: usize,
		) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_>> {
			self.ranges += 1;
			let encoded = range.encode();
			let rows: Vec<Result<MultiVersionRow<TaggedKey>>> = self
				.rows
				.range::<EncodedKey, _>((encoded.start.as_ref(), encoded.end.as_ref()))
				.map(|(key, bytes)| {
					Ok(MultiVersionRow {
						key: TaggedKey::decode(key).unwrap(),
						bytes: bytes.clone(),
						version: CommitVersion(0),
					})
				})
				.collect();
			Ok(Box::new(rows.into_iter()))
		}
	}

	#[derive(Default)]
	struct FlowMemory {
		seam: Vec<Seam>,
		reads: usize,
	}

	impl FlowGet for FlowMemory {
		fn get(&mut self, keys: &[EncodedKey]) -> Result<Vec<Option<EncodedBytes>>> {
			self.reads += 1;
			Ok(vec![None; keys.len()])
		}
	}

	impl FlowBatch for FlowMemory {
		fn set(&mut self, keys: Vec<EncodedKey>, _rows: Vec<EncodedBytes>) -> Result<()> {
			self.seam.push(Seam::Set(keys.len()));
			Ok(())
		}

		fn remove(&mut self, keys: Vec<EncodedKey>) -> Result<()> {
			self.seam.push(Seam::Remove(keys.len()));
			Ok(())
		}
	}

	#[derive(Default)]
	struct Counters {
		metadata: RefCell<HashMap<Option<Partition>, RingBufferMetadata>>,
		next_base: Cell<u64>,
		removed: RefCell<Vec<Option<Partition>>>,
		saves: Cell<usize>,
		removes: Cell<usize>,
	}

	impl RingCounters<Memory> for Counters {
		fn metadata(
			&self,
			_txn: &mut Memory,
			partitions: &[Option<Partition>],
		) -> Result<Vec<Option<RingBufferMetadata>>> {
			let metadata = self.metadata.borrow();
			Ok(partitions.iter().map(|partition| metadata.get(partition).copied()).collect())
		}

		fn reserve(&self, _txn: &mut Memory, capacity: u64) -> Result<u64> {
			let base = self.next_base.get().max(100);
			self.next_base.set(base + capacity);
			Ok(base)
		}

		fn save_metadata(
			&self,
			_txn: &mut Memory,
			entries: &[(Option<Partition>, RingBufferMetadata)],
		) -> Result<()> {
			self.saves.set(self.saves.get() + 1);
			for (partition, metadata) in entries {
				self.metadata.borrow_mut().insert(*partition, *metadata);
			}
			Ok(())
		}

		fn remove_metadata(&self, _txn: &mut Memory, partitions: &[Option<Partition>]) -> Result<()> {
			self.removes.set(self.removes.get() + 1);
			for partition in partitions {
				self.metadata.borrow_mut().remove(partition);
				self.removed.borrow_mut().push(*partition);
			}
			Ok(())
		}
	}

	fn storage() -> StorageId {
		StorageId::from(RingBufferId(9))
	}

	fn row(sequence: u64) -> EncodedBytes {
		let shape = RowShape::testing(RowFamily::RingBuffer, &[ValueType::Int4]);
		let mut builder = shape.allocate_ringbuffer();
		builder.set_sequence(sequence);
		builder.freeze_bytes()
	}

	struct Fixture {
		txn: Memory,
		counters: Counters,
		capacity: u64,
	}

	impl Fixture {
		fn new(capacity: u64) -> Self {
			Self {
				txn: Memory::default(),
				counters: Counters::default(),
				capacity,
			}
		}

		fn insert(&mut self, partition: Option<Partition>, count: usize) -> Vec<Placement> {
			let mut ring = Ring::new(self.capacity);
			let placements = RingMetadata::new(&mut self.txn, storage())
				.place(&self.counters, &mut ring, &[(partition, count)])
				.unwrap()
				.remove(0);
			let rows = placements
				.iter()
				.map(|p| (ring_row_tagged(storage(), partition, p.row), row(p.sequence)))
				.collect();
			RingWriter::new(&mut self.txn).insert(rows).unwrap();
			RingMetadata::new(&mut self.txn, storage()).save(&self.counters, &ring).unwrap();
			placements
		}

		fn remove(&mut self, partition: Option<Partition>, rows: &[RowNumber]) -> Vec<Option<EncodedBytes>> {
			let mut ring = Ring::new(self.capacity);
			let removed = RingMetadata::new(&mut self.txn, storage())
				.release(&self.counters, &mut ring, &[(partition, rows.to_vec())])
				.unwrap()
				.remove(0);
			let held = rows
				.iter()
				.zip(&removed)
				.filter_map(|(&row, bytes)| {
					Some((ring_row_tagged(storage(), partition, row), bytes.clone()?))
				})
				.collect();
			RingWriter::new(&mut self.txn).delete(held).unwrap();
			RingMetadata::new(&mut self.txn, storage()).save(&self.counters, &ring).unwrap();
			removed
		}

		fn scan(&mut self, partition: Option<Partition>) -> Vec<RowNumber> {
			let mut store = RingAdapter {
				txn: &mut self.txn,
				counters: &self.counters,
				storage: storage(),
			};
			oldest_first(&mut store, partition).unwrap()
		}

		fn metadata(&self, partition: Option<Partition>) -> RingBufferMetadata {
			self.counters.metadata.borrow()[&partition]
		}

		fn free_len(&mut self, partition: Option<Partition>) -> usize {
			let mut store = RingAdapter {
				txn: &mut self.txn,
				counters: &self.counters,
				storage: storage(),
			};
			let metadata = store.counters.metadata.borrow().get(&partition).copied().unwrap();
			let pages: Vec<u64> =
				(0..(metadata.filled - metadata.count).div_ceil(FREE_PAGE_SLOTS)).collect();
			store.free_pages(partition, &pages).unwrap().iter().map(Vec::len).sum()
		}

		fn assert_free_invariant(&mut self, partition: Option<Partition>) {
			let metadata = self.metadata(partition);
			assert_eq!(self.free_len(partition) as u64, metadata.filled - metadata.count);
		}
	}

	#[test]
	fn a_full_ring_hands_back_the_oldest_rows_it_evicted_with_their_bytes() {
		// Lost evicted rows would drop the delete hooks and the commit log pre-image of every push out.
		let mut fixture = Fixture::new(3);
		let first = fixture.insert(None, 3);
		assert!(first.iter().all(|placement| placement.evicted.is_none()));
		let second = fixture.insert(None, 2);
		assert_eq!(
			second.iter().map(|p| (p.row, p.evicted.clone())).collect::<Vec<_>>(),
			vec![
				(first[0].row, Some(row(first[0].sequence))),
				(first[1].row, Some(row(first[1].sequence)))
			]
		);
		assert_eq!(second.iter().map(|p| p.sequence).collect::<Vec<_>>(), vec![3, 4]);
		assert_eq!(fixture.metadata(None).count, 3);
		assert_eq!(fixture.metadata(None).head, 2);
	}

	#[test]
	fn scans_stay_in_sequence_order_after_evictions_and_slot_reuse() {
		// Slot order and sequence order differ once a slot is reused, and a scan must follow the sequence.
		let mut fixture = Fixture::new(3);
		let first = fixture.insert(None, 3);
		let reused = first[0].row;
		fixture.remove(None, &[reused]);
		let second = fixture.insert(None, 1);
		assert_eq!(second[0].row, reused, "a free slot is filled before any eviction");
		assert_eq!(second[0].evicted, None);
		let third = fixture.insert(None, 1);
		assert_eq!(third[0].row, first[1].row, "the oldest live row is evicted, not the lowest slot");
		assert_eq!(fixture.scan(None), vec![first[2].row, second[0].row, third[0].row]);
	}

	#[test]
	fn an_order_page_resumes_strictly_after_its_cursor_and_honours_its_limit() {
		// Without the resume every page repeats the first one, so a walk past one page would never end.
		let mut fixture = Fixture::new(4);
		let placed = fixture.insert(None, 4);
		let mut store = RingAdapter {
			txn: &mut fixture.txn,
			counters: &fixture.counters,
			storage: storage(),
		};
		let entry = |index: usize| (placed[index].sequence, placed[index].row);
		assert_eq!(store.order(None, None, 2).unwrap(), vec![entry(0), entry(1)]);
		assert_eq!(store.order(None, Some(placed[1].sequence), 8).unwrap(), vec![entry(2), entry(3)]);
	}

	#[test]
	fn the_free_list_always_holds_the_filled_slots_no_row_occupies() {
		// A drifted free list either overwrites a live row or leaks a slot forever.
		let mut fixture = Fixture::new(4);
		let first = fixture.insert(None, 4);
		fixture.assert_free_invariant(None);
		fixture.remove(None, &[first[1].row, first[2].row]);
		assert_eq!(fixture.free_len(None), 2);
		fixture.assert_free_invariant(None);
		fixture.insert(None, 1);
		fixture.assert_free_invariant(None);
		fixture.insert(None, 2);
		assert_eq!(fixture.free_len(None), 0);
		fixture.assert_free_invariant(None);
	}

	#[test]
	fn next_sequence_only_grows_even_through_an_emptied_unpartitioned_ring() {
		// A reused sequence would make an order key point at the wrong row.
		let mut fixture = Fixture::new(2);
		let first = fixture.insert(None, 2);
		fixture.remove(None, &[first[0].row, first[1].row]);
		assert!(fixture.counters.removed.borrow().is_empty(), "an unpartitioned ring keeps its metadata");
		let second = fixture.insert(None, 1);
		assert_eq!(second[0].sequence, 2);
		assert_eq!(fixture.metadata(None).next_sequence, 3);
	}

	#[test]
	fn an_emptied_partition_removes_its_metadata_order_and_free_entries() {
		// Anything left behind makes a dropped partition look alive and grows storage forever.
		let partition = Some(Partition(5));
		let mut fixture = Fixture::new(2);
		let first = fixture.insert(partition, 2);
		fixture.remove(partition, &[first[0].row]);
		assert_eq!(fixture.free_len(partition), 1);
		fixture.remove(partition, &[first[1].row]);
		assert_eq!(*fixture.counters.removed.borrow(), vec![partition]);
		assert!(fixture.txn.rows.is_empty(), "no row, order or free entry may survive");
	}

	#[test]
	fn each_partition_places_inside_its_own_capacity_wide_block() {
		// Overlapping blocks would let one partition overwrite the slot of another.
		let (one, two) = (Some(Partition(1)), Some(Partition(2)));
		let mut fixture = Fixture::new(3);
		let in_one = fixture.insert(one, 3);
		let in_two = fixture.insert(two, 3);
		let (base_one, base_two) = (fixture.metadata(one).base_rn, fixture.metadata(two).base_rn);
		assert_eq!(base_two, base_one + 3);
		assert!(in_one.iter().all(|placement| placement.row.0 >= base_one && placement.row.0 < base_one + 3));
		assert!(in_two.iter().all(|placement| placement.row.0 >= base_two && placement.row.0 < base_two + 3));
	}

	#[test]
	fn release_returns_the_bytes_of_held_rows_and_none_for_a_slot_outside_the_block() {
		// A released foreign slot would free a row number the partition never owned.
		let mut fixture = Fixture::new(2);
		let placed = fixture.insert(None, 1);
		let removed = fixture.remove(None, &[placed[0].row, RowNumber(placed[0].row.0 + 1_000)]);
		assert_eq!(removed, vec![Some(row(placed[0].sequence)), None]);
	}

	#[test]
	fn save_writes_order_and_free_through_the_tagged_seam_with_an_unknown_pre() {
		// DML has always told the store no pre on these rows; any other pre would change what commit log sees.
		let mut fixture = Fixture::new(2);
		let first = fixture.insert(None, 2);
		fixture.txn.seam.clear();
		fixture.remove(None, &[first[0].row]);
		let order = ring_order_key(storage(), None, first[0].sequence);
		let free: TaggedKey = RingBufferFreeKey::of(storage(), None, 0).into();
		let free_row = encode_ringbuffer_free_page(&[first[0].row]).into_bytes();
		let held = (ring_row_tagged(storage(), None, first[0].row), row(first[0].sequence));
		assert_eq!(
			fixture.txn.seam,
			vec![
				Seam::RemoveWithPre(vec![held]),
				Seam::RemoveTagged(vec![order]),
				Seam::SetWithPre(vec![(free, free_row, SetPre::Unknown)]),
			]
		);
	}

	#[test]
	fn save_writes_every_order_entry_and_the_metadata_in_one_call_each() {
		// One call per entry would scale store round trips with every row a batch places.
		let mut fixture = Fixture::new(4);
		let placed = fixture.insert(None, 3);
		let orders: Vec<(TaggedKey, EncodedBytes, SetPre)> = placed
			.iter()
			.map(|p| {
				(
					ring_order_key(storage(), None, p.sequence),
					encode_ringbuffer_order(p.row).into_bytes(),
					SetPre::Unknown,
				)
			})
			.collect();
		let rows = placed
			.iter()
			.map(|p| (ring_row_tagged(storage(), None, p.row), row(p.sequence), SetPre::Unknown))
			.collect();
		assert_eq!(fixture.txn.seam, vec![Seam::SetWithPre(rows), Seam::SetWithPre(orders)]);
		assert_eq!(fixture.counters.saves.get(), 1);
	}

	#[test]
	fn an_emptied_partition_drops_all_its_order_entries_and_metadata_in_one_call_each() {
		// A remove per entry would leave a half-dropped partition visible if a later call failed.
		let partition = Some(Partition(5));
		let mut fixture = Fixture::new(3);
		let placed = fixture.insert(partition, 3);
		fixture.txn.seam.clear();
		let rows: Vec<RowNumber> = placed.iter().map(|p| p.row).collect();
		fixture.remove(partition, &rows);
		let held = placed
			.iter()
			.map(|p| (ring_row_tagged(storage(), partition, p.row), row(p.sequence)))
			.collect();
		let orders = placed.iter().map(|p| ring_order_key(storage(), partition, p.sequence)).collect();
		assert_eq!(fixture.txn.seam, vec![Seam::RemoveWithPre(held), Seam::RemoveTagged(orders)]);
		assert_eq!(fixture.counters.removes.get(), 1);
		assert_eq!(*fixture.counters.removed.borrow(), vec![partition]);
		assert!(fixture.txn.rows.is_empty(), "no row, order or free entry may survive");
	}

	#[test]
	fn tagged_rows_carry_an_unknown_pre_and_a_delete_carries_the_callers_pre() {
		// A dropped delete pre makes the transaction read its own view, which can differ from committed.
		let mut txn = Memory::default();
		let mut writer = RingWriter::new(&mut txn);
		let key = |row: u64| ring_row_tagged(storage(), None, RowNumber(row));
		let inserted = writer.insert(vec![(key(1), row(1))]).unwrap();
		let updated = writer.update(vec![(key(1), row(2))]).unwrap();
		let deleted = writer.delete(vec![(key(1), row(2))]).unwrap();
		assert_eq!((inserted.written, inserted.replaced), (vec![row(1)], vec![]));
		assert_eq!((updated.written, updated.replaced), (vec![row(2)], vec![]));
		assert_eq!((deleted.written, deleted.replaced), (vec![], vec![row(2)]));
		assert_eq!(
			txn.seam,
			vec![
				Seam::SetWithPre(vec![(key(1), row(1), SetPre::Unknown)]),
				Seam::SetWithPre(vec![(key(1), row(2), SetPre::Unknown)]),
				Seam::RemoveWithPre(vec![(key(1), row(2))]),
			]
		);
		assert_eq!((txn.reads, txn.ranges), (0, 0), "the caller owns the committed read");
	}

	#[test]
	fn the_flow_row_paths_never_read() {
		// A read here adds store load to every view write that has none today.
		let mut txn = FlowMemory::default();
		let mut writer = FlowRingWriter::new(&mut txn);
		let key = ring_row_encoded(storage(), None, RowNumber(1));
		writer.insert(vec![key.clone()], vec![row(1)]).unwrap();
		writer.update(vec![key.clone()], vec![row(2)]).unwrap();
		writer.delete(vec![key]).unwrap();
		assert_eq!(txn.seam, vec![Seam::Set(1), Seam::Set(1), Seam::Remove(1)]);
		assert_eq!(txn.reads, 0);
	}

	#[test]
	fn a_placed_row_keeps_the_sequence_the_ring_gave_it_when_read_back() {
		// Release finds the order key through this sequence, so a mismatch leaves a dead order entry behind.
		let mut fixture = Fixture::new(2);
		let placed = fixture.insert(None, 1);
		let stored = fixture.txn.rows[&ring_row_encoded(storage(), None, placed[0].row)].clone();
		assert_eq!(EncodedRingBufferRow::view(&stored).sequence(), placed[0].sequence);
	}

	#[test]
	fn the_row_key_is_the_same_tagged_and_encoded_for_each_partition_form() {
		// DML reads through the tagged key and views through the encoded one; a mismatch splits the row.
		for partition in [None, Some(Partition(5))] {
			assert_eq!(
				ring_row_tagged(storage(), partition, RowNumber(3)).encode().as_slice(),
				ring_row_encoded(storage(), partition, RowNumber(3)).as_slice()
			);
		}
	}
}
