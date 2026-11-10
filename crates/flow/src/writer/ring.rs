// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	delta::SetPre,
	interface::{
		catalog::{ringbuffer::RingBufferMetadata, storage::StorageId},
		store::MultiVersionRow,
	},
	key::{
		any::TaggedKey,
		bound::{TaggedKeyBound, TaggedKeyBoundRange},
		row::{PartitionedRowKey, RowKey},
	},
	return_internal_error,
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

pub fn ring_row_range(storage: StorageId, partition: Option<Partition>) -> TaggedKeyBoundRange {
	match partition {
		Some(partition) => PartitionedRowKey::partition_range(storage, partition),
		None => RowKey::full_scan(storage),
	}
}

pub fn ring_row_number(key: &TaggedKey) -> Result<RowNumber> {
	match key {
		TaggedKey::Row(key) => Ok(key.row),
		TaggedKey::PartitionedRow(key) => Ok(key.row),
		other => return_internal_error!("a ring buffer row range yielded a non-row key {:?}", other),
	}
}

pub fn collect_ring_rows(
	range: impl Iterator<Item = Result<MultiVersionRow<TaggedKey>>>,
	limit: usize,
) -> Result<Vec<(RowNumber, EncodedBytes)>> {
	let mut rows = Vec::new();
	for row in range.take(limit) {
		let row = row?;
		rows.push((ring_row_number(&row.key)?, row.bytes));
	}
	Ok(rows)
}

pub trait RingCounters<T> {
	fn metadata(&self, txn: &mut T, partitions: &[Option<Partition>]) -> Result<Vec<Option<RingBufferMetadata>>>;

	fn reserve(&self, txn: &mut T, batch_count: u64) -> Result<u64>;

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

	fn rows(&mut self, rows: &[(Option<Partition>, RowNumber)]) -> Result<Vec<Option<EncodedBytes>>> {
		if rows.is_empty() {
			return Ok(Vec::new());
		}
		let keys: Vec<TaggedKey> =
			rows.iter().map(|&(partition, row)| ring_row_tagged(self.storage, partition, row)).collect();
		self.txn.get_tagged(&keys)
	}

	fn oldest(
		&mut self,
		partition: Option<Partition>,
		after: Option<RowNumber>,
		limit: usize,
	) -> Result<Vec<(RowNumber, EncodedBytes)>> {
		let resume = after.map(|row| ring_row_tagged(self.storage, partition, row));
		let range = ring_row_range(self.storage, partition).resume_before(resume.as_ref());
		let rows = collect_ring_rows(self.txn.range_rev_untracked(range.clone(), limit)?, limit)?;
		let marked = match rows.last() {
			Some(&(last, _)) if rows.len() == limit => TaggedKeyBoundRange {
				start: Bound::Included(TaggedKeyBound::Key(ring_row_tagged(self.storage, partition, last))),
				end: range.end,
			},
			_ => range,
		};
		self.txn.mark_range(marked)?;
		Ok(rows)
	}
}

impl<T: StorageTransaction + Range, C: RingCounters<T>> RingStore for RingAdapter<'_, T, C> {
	fn reserve(&mut self, batch_count: u64) -> Result<u64> {
		self.counters.reserve(self.txn, batch_count)
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
		requests: &[(Option<Partition>, Vec<RowNumber>)],
	) -> Result<Vec<Vec<Placement>>> {
		ring.place(&mut self.adapter(counters), requests)
	}

	pub fn reserve<C: RingCounters<T>>(&mut self, counters: &C, count: usize) -> Result<u64> {
		self.adapter(counters).reserve(count as u64)
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
		for write in ring.writes() {
			match write {
				MetadataWrite::Save(partition, metadata) => saved.push((partition, metadata)),
				MetadataWrite::Remove(partition) => dropped.push(partition),
			}
		}
		if !saved.is_empty() {
			counters.save_metadata(self.txn, &saved)?;
		}
		if !dropped.is_empty() {
			counters.remove_metadata(self.txn, &dropped)?;
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

	pub fn insert(&mut self, rows: Vec<(TaggedKey, EncodedBytes, SetPre)>) -> Result<Written> {
		self.set_tagged(rows)
	}

	pub fn update(&mut self, rows: Vec<(TaggedKey, EncodedBytes)>) -> Result<Written> {
		self.set_tagged(rows.into_iter().map(|(key, row)| (key, row, SetPre::Unknown)).collect())
	}

	pub fn delete(&mut self, rows: Vec<(TaggedKey, EncodedBytes)>) -> Result<Written> {
		let replaced = rows.iter().map(|(_, pre)| pre.clone()).collect();
		self.txn.remove_with_pre(rows)?;
		Ok(Written {
			written: Vec::new(),
			replaced,
		})
	}

	fn set_tagged(&mut self, rows: Vec<(TaggedKey, EncodedBytes, SetPre)>) -> Result<Written> {
		let written = rows.iter().map(|(_, row, _)| row.clone()).collect();
		self.txn.set_with_pre(rows)?;
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
		ring::{Evicted, oldest_first},
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
		marked: Vec<TaggedKeyBoundRange>,
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

		fn range_rev(
			&mut self,
			range: TaggedKeyBoundRange,
			_batch_size: usize,
		) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_>> {
			self.ranges += 1;
			let encoded = range.encode();
			let rows: Vec<Result<MultiVersionRow<TaggedKey>>> = self
				.rows
				.range::<EncodedKey, _>((encoded.start.as_ref(), encoded.end.as_ref()))
				.rev()
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

		fn range_rev_untracked(
			&mut self,
			range: TaggedKeyBoundRange,
			batch_size: usize,
		) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_>> {
			Range::range_rev(self, range, batch_size)
		}

		fn mark_range(&mut self, range: TaggedKeyBoundRange) -> Result<()> {
			self.marked.push(range);
			Ok(())
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

		fn reserve(&self, _txn: &mut Memory, batch_count: u64) -> Result<u64> {
			let base = self.next_base.get().max(100);
			self.next_base.set(base + batch_count);
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
			let first = RingMetadata::new(&mut self.txn, storage()).reserve(&self.counters, count).unwrap();
			let rows: Vec<RowNumber> = (first..first + count as u64).map(RowNumber).collect();
			let placements = RingMetadata::new(&mut self.txn, storage())
				.place(&self.counters, &mut ring, &[(partition, rows)])
				.unwrap()
				.remove(0);
			let victims: Vec<(TaggedKey, EncodedBytes)> = placements
				.iter()
				.filter_map(|p| p.evicted.as_ref())
				.map(|evicted| {
					(ring_row_tagged(storage(), partition, evicted.row), evicted.bytes.clone())
				})
				.collect();
			if !victims.is_empty() {
				RingWriter::new(&mut self.txn).delete(victims).unwrap();
			}
			let rows = placements
				.iter()
				.map(|p| (ring_row_tagged(storage(), partition, p.row), row(p.row.0), SetPre::Absent))
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
	}

	#[test]
	fn a_full_ring_hands_back_the_oldest_rows_it_evicted_with_their_bytes() {
		// Lost evicted rows would drop the delete hooks and the commit log pre-image of every push out.
		let mut fixture = Fixture::new(3);
		let first = fixture.insert(None, 3);
		assert!(first.iter().all(|placement| placement.evicted.is_none()));
		let second = fixture.insert(None, 2);
		assert_eq!(
			second.iter().map(|p| p.evicted.clone()).collect::<Vec<_>>(),
			vec![
				Some(Evicted {
					row: first[0].row,
					bytes: row(first[0].row.0),
				}),
				Some(Evicted {
					row: first[1].row,
					bytes: row(first[1].row.0),
				}),
			]
		);
		assert_eq!(
			second.iter().map(|p| p.row.0).collect::<Vec<_>>(),
			vec![first[2].row.0 + 1, first[2].row.0 + 2]
		);
		assert_eq!(fixture.metadata(None).count, 3);
		assert_eq!(fixture.metadata(None).head, first[2].row.0);
	}

	#[test]
	fn scans_stay_in_row_order_after_evictions_and_removes() {
		// A scan that kept a removed or evicted row would return rows that are gone.
		let mut fixture = Fixture::new(3);
		let first = fixture.insert(None, 3);
		let reused = first[0].row;
		fixture.remove(None, &[reused]);
		let second = fixture.insert(None, 1);
		assert_eq!(second[0].evicted, None, "a removed row leaves room, so nothing is evicted");
		let third = fixture.insert(None, 1);
		assert_eq!(
			third[0].evicted.as_ref().map(|evicted| evicted.row),
			Some(first[1].row),
			"the oldest live row is evicted"
		);
		assert_eq!(fixture.scan(None), vec![first[2].row, second[0].row, third[0].row]);
	}

	#[test]
	fn an_oldest_page_resumes_strictly_after_its_cursor_and_honours_its_limit() {
		// Without the resume every page repeats the first one, so a walk past one page would never end.
		let mut fixture = Fixture::new(4);
		let placed = fixture.insert(None, 4);
		let mut store = RingAdapter {
			txn: &mut fixture.txn,
			counters: &fixture.counters,
			storage: storage(),
		};
		let entry = |index: usize| (placed[index].row, row(placed[index].row.0));
		assert_eq!(store.oldest(None, None, 2).unwrap(), vec![entry(0), entry(1)]);
		assert_eq!(store.oldest(None, Some(placed[1].row), 8).unwrap(), vec![entry(2), entry(3)]);
	}

	#[test]
	fn a_full_oldest_page_marks_only_the_span_it_returned() {
		// A mark over the whole ring makes every insert into a full ring abort against writers of rows it never read.
		let mut fixture = Fixture::new(4);
		let placed = fixture.insert(None, 3);
		let mut store = RingAdapter {
			txn: &mut fixture.txn,
			counters: &fixture.counters,
			storage: storage(),
		};
		store.oldest(None, None, 2).unwrap();
		assert_eq!(
			fixture.txn.marked,
			vec![TaggedKeyBoundRange {
				start: Bound::Included(TaggedKeyBound::Key(ring_row_tagged(storage(), None, placed[1].row))),
				end: ring_row_range(storage(), None).end,
			}]
		);
	}

	#[test]
	fn a_short_oldest_page_marks_the_whole_range_it_saw() {
		// A short page observed the whole range, so a narrower mark would miss a concurrent write anywhere in it.
		let mut fixture = Fixture::new(4);
		fixture.insert(None, 1);
		let mut store = RingAdapter {
			txn: &mut fixture.txn,
			counters: &fixture.counters,
			storage: storage(),
		};
		store.oldest(None, None, 4).unwrap();
		assert_eq!(fixture.txn.marked, vec![ring_row_range(storage(), None)]);
	}

	#[test]
	fn row_numbers_only_grow_even_through_an_emptied_unpartitioned_ring() {
		// A reused row number would hand a reader two different rows under one id.
		let mut fixture = Fixture::new(2);
		let first = fixture.insert(None, 2);
		fixture.remove(None, &[first[0].row, first[1].row]);
		assert!(fixture.counters.removed.borrow().is_empty(), "an unpartitioned ring keeps its metadata");
		let second = fixture.insert(None, 1);
		assert_eq!(second[0].row.0, first[1].row.0 + 1, "a row number is never handed out twice");
	}

	#[test]
	fn an_emptied_partition_removes_its_metadata_and_rows() {
		// Anything left behind makes a dropped partition look alive and grows storage forever.
		let partition = Some(Partition(5));
		let mut fixture = Fixture::new(2);
		let first = fixture.insert(partition, 2);
		fixture.remove(partition, &[first[0].row]);
		fixture.remove(partition, &[first[1].row]);
		assert_eq!(*fixture.counters.removed.borrow(), vec![partition]);
		assert!(fixture.txn.rows.is_empty(), "no row may survive");
	}

	#[test]
	fn two_partitions_never_get_the_same_row_number() {
		// Flow diffs carry no partition, so a shared row number would merge rows of two partitions downstream.
		let (one, two) = (Some(Partition(1)), Some(Partition(2)));
		let mut fixture = Fixture::new(3);
		let in_one = fixture.insert(one, 3);
		let in_two = fixture.insert(two, 3);
		let again_one = fixture.insert(one, 3);
		let mut rows: Vec<RowNumber> = in_one.iter().chain(&in_two).chain(&again_one).map(|p| p.row).collect();
		rows.sort();
		rows.dedup();
		assert_eq!(rows.len(), 9, "one ring-wide counter must never hand two partitions the same row number");
	}

	#[test]
	fn release_returns_the_bytes_of_held_rows_and_none_for_a_row_that_is_not_stored() {
		// A release of an unknown row must not lower the count of rows the partition holds.
		let mut fixture = Fixture::new(2);
		let placed = fixture.insert(None, 1);
		let removed = fixture.remove(None, &[placed[0].row, RowNumber(placed[0].row.0 + 1_000)]);
		assert_eq!(removed, vec![Some(row(placed[0].row.0)), None]);
	}

	#[test]
	fn save_writes_every_row_and_the_metadata_in_one_call_each() {
		// One call per entry would scale store round trips with every row a batch places.
		let mut fixture = Fixture::new(4);
		let placed = fixture.insert(None, 3);
		let rows = placed
			.iter()
			.map(|p| (ring_row_tagged(storage(), None, p.row), row(p.row.0), SetPre::Absent))
			.collect();
		assert_eq!(fixture.txn.seam, vec![Seam::SetWithPre(rows)]);
		assert_eq!(fixture.counters.saves.get(), 1);
	}

	#[test]
	fn an_emptied_partition_drops_all_its_rows_and_metadata_in_one_call_each() {
		// A remove per entry would leave a half-dropped partition visible if a later call failed.
		let partition = Some(Partition(5));
		let mut fixture = Fixture::new(3);
		let placed = fixture.insert(partition, 3);
		fixture.txn.seam.clear();
		let rows: Vec<RowNumber> = placed.iter().map(|p| p.row).collect();
		fixture.remove(partition, &rows);
		let held = placed.iter().map(|p| (ring_row_tagged(storage(), partition, p.row), row(p.row.0))).collect();
		assert_eq!(fixture.txn.seam, vec![Seam::RemoveWithPre(held)]);
		assert_eq!(fixture.counters.removes.get(), 1);
		assert_eq!(*fixture.counters.removed.borrow(), vec![partition]);
		assert!(fixture.txn.rows.is_empty(), "no row may survive");
	}

	#[test]
	fn an_insert_and_a_delete_carry_the_callers_pre_and_an_update_stays_unknown() {
		// A writer that rewrites the caller's pre makes the lift misreport evictions and deletes.
		let mut txn = Memory::default();
		let mut writer = RingWriter::new(&mut txn);
		let key = |row: u64| ring_row_tagged(storage(), None, RowNumber(row));
		let inserted = writer
			.insert(vec![(key(1), row(1), SetPre::Absent), (key(2), row(3), SetPre::Present(row(9)))])
			.unwrap();
		let updated = writer.update(vec![(key(1), row(2))]).unwrap();
		let deleted = writer.delete(vec![(key(1), row(2))]).unwrap();
		assert_eq!((inserted.written, inserted.replaced), (vec![row(1), row(3)], vec![]));
		assert_eq!((updated.written, updated.replaced), (vec![row(2)], vec![]));
		assert_eq!((deleted.written, deleted.replaced), (vec![], vec![row(2)]));
		assert_eq!(
			txn.seam,
			vec![
				Seam::SetWithPre(vec![
					(key(1), row(1), SetPre::Absent),
					(key(2), row(3), SetPre::Present(row(9)))
				]),
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
		// A partitioned scan merges rows by this sequence, so a mismatch returns rows out of order.
		let mut fixture = Fixture::new(2);
		let placed = fixture.insert(None, 1);
		let stored = fixture.txn.rows[&ring_row_encoded(storage(), None, placed[0].row)].clone();
		assert_eq!(EncodedRingBufferRow::view(&stored).sequence(), placed[0].row.0);
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
