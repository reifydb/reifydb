// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, ops::Bound};

use indexmap::IndexMap;
use reifydb_codec::row::{bytes::EncodedBytes, ringbuffer::EncodedRingBufferRow};
use reifydb_value::{
	Result, reifydb_assertions,
	value::{datetime::DateTime, partition::Partition, row_number::RowNumber},
};

use crate::{interface::catalog::ringbuffer::RingBufferMetadata, return_internal_error};

const ORDER_PAGE: usize = 64;

pub const FREE_PAGE_SLOTS: u64 = 512;

pub trait RingRead {
	fn metadata(&mut self, partition: Option<Partition>) -> Result<Option<RingBufferMetadata>>;

	fn slots(&mut self, partition: Option<Partition>, rows: &[RowNumber]) -> Result<Vec<Option<EncodedBytes>>>;

	fn order(
		&mut self,
		partition: Option<Partition>,
		after: Option<u64>,
		limit: usize,
	) -> Result<Vec<(u64, RowNumber)>>;
}

pub trait RingStore: RingRead {
	fn reserve(&mut self, capacity: u64) -> Result<u64>;

	fn free_pages(&mut self, partition: Option<Partition>, pages: &[u64]) -> Result<Vec<Vec<RowNumber>>>;
}

pub trait RingNewest: RingRead {
	fn newest_order(&mut self, partition: Option<Partition>) -> Result<Option<(u64, RowNumber)>>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct Placement {
	pub row: RowNumber,
	pub sequence: u64,
	pub evicted: Option<EncodedBytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataWrite {
	Save(Option<Partition>, RingBufferMetadata),
	Remove(Option<Partition>),
	Order(Option<Partition>, u64, Option<RowNumber>),
	Free(Option<Partition>, u64, Option<Vec<RowNumber>>),
}

#[derive(Default)]
struct FreePage {
	stored: Vec<RowNumber>,
	slots: Vec<RowNumber>,
}

struct Tracked {
	loaded: Option<RingBufferMetadata>,
	current: Option<RingBufferMetadata>,
	order: BTreeMap<u64, Option<RowNumber>>,
	free_len: u64,
	free_pages: BTreeMap<u64, FreePage>,
}

pub struct Ring {
	capacity: u64,
	tracked: IndexMap<Option<Partition>, Tracked>,
}

impl Ring {
	pub fn new(capacity: u64) -> Self {
		assert!(capacity > 0, "a ring buffer must hold at least one row");
		Self {
			capacity,
			tracked: IndexMap::new(),
		}
	}

	pub fn metadata(
		&mut self,
		store: &mut impl RingStore,
		partition: Option<Partition>,
	) -> Result<Option<RingBufferMetadata>> {
		if let Some(tracked) = self.tracked.get(&partition) {
			return Ok(tracked.current);
		}
		let loaded = store.metadata(partition)?;
		self.tracked.insert(
			partition,
			Tracked {
				loaded,
				current: loaded,
				order: BTreeMap::new(),
				free_len: loaded.map_or(0, |metadata| metadata.filled - metadata.count),
				free_pages: BTreeMap::new(),
			},
		);
		Ok(loaded)
	}

	fn tracked(&mut self, partition: Option<Partition>) -> &mut Tracked {
		self.tracked.get_mut(&partition).expect("metadata is loaded before it is changed")
	}

	fn set(&mut self, partition: Option<Partition>, metadata: RingBufferMetadata) {
		self.tracked(partition).current = Some(metadata);
	}

	fn load_free_pages(
		&mut self,
		store: &mut impl RingStore,
		partition: Option<Partition>,
		first: u64,
		last: u64,
	) -> Result<()> {
		let tracked = self.tracked(partition);
		let missing: Vec<u64> = (first..=last).filter(|page| !tracked.free_pages.contains_key(page)).collect();
		if missing.is_empty() {
			return Ok(());
		}
		let loaded = store.free_pages(partition, &missing)?;
		let tracked = self.tracked(partition);
		for (page, slots) in missing.into_iter().zip(loaded) {
			reifydb_assertions! {
				assert_eq!(
					slots.len() as u64,
					(tracked.free_len - page * FREE_PAGE_SLOTS).min(FREE_PAGE_SLOTS),
					"a ring's free page must hold exactly the free slots its index range reaches"
				);
			}
			tracked.free_pages.insert(
				page,
				FreePage {
					stored: slots.clone(),
					slots,
				},
			);
		}
		Ok(())
	}

	fn load_top_free_page(&mut self, store: &mut impl RingStore, partition: Option<Partition>) -> Result<()> {
		let free_len = self.tracked(partition).free_len;
		if !free_len.is_multiple_of(FREE_PAGE_SLOTS) {
			let top = free_len / FREE_PAGE_SLOTS;
			self.load_free_pages(store, partition, top, top)?;
		}
		Ok(())
	}

	fn release(&mut self, partition: Option<Partition>, sequence: u64, row: RowNumber) {
		let tracked = self.tracked(partition);
		tracked.order.insert(sequence, None);
		let page = tracked.free_len / FREE_PAGE_SLOTS;
		tracked.free_pages.entry(page).or_default().slots.push(row);
		tracked.free_len += 1;
	}

	#[cfg_attr(not(reifydb_assertions), allow(unused_variables))]
	fn assert_free_len(&mut self, partition: Option<Partition>, metadata: RingBufferMetadata) {
		reifydb_assertions! {
			assert_eq!(
				self.tracked(partition).free_len,
				metadata.filled - metadata.count,
				"a ring's free stack must hold exactly the filled slots no row occupies"
			);
		}
	}

	fn live(
		&mut self,
		store: &mut impl RingStore,
		partition: Option<Partition>,
		after: Option<u64>,
		limit: usize,
	) -> Result<Vec<(u64, RowNumber)>> {
		let mut live = Vec::with_capacity(limit);
		if limit == 0 {
			return Ok(live);
		}
		let page = limit.max(ORDER_PAGE);
		let head = self.tracked(partition).current.map_or(0, |metadata| metadata.head);
		let mut cursor = after.or_else(|| head.checked_sub(1));
		loop {
			let stored = store.order(partition, cursor, page)?;
			let exhausted = stored.len() < page;
			let staged = &self.tracked(partition).order;
			for (sequence, row) in stored {
				cursor = Some(sequence);
				if matches!(staged.get(&sequence), Some(None)) {
					continue;
				}
				live.push((sequence, row));
				if live.len() == limit {
					return Ok(live);
				}
			}
			if exhausted {
				break;
			}
		}
		let lower = after.map_or(Bound::Unbounded, Bound::Excluded);
		for (&sequence, entry) in self.tracked(partition).order.range((lower, Bound::Unbounded)) {
			if let Some(row) = entry {
				live.push((sequence, *row));
				if live.len() == limit {
					break;
				}
			}
		}
		Ok(live)
	}

	pub fn place(
		&mut self,
		store: &mut impl RingStore,
		partition: Option<Partition>,
		count: usize,
	) -> Result<Vec<Placement>> {
		assert!(
			count as u64 <= self.capacity,
			"one placement call must not lap the ring, or two rows of the call would share a slot"
		);
		if count == 0 {
			return Ok(Vec::new());
		}
		let mut metadata = match self.metadata(store, partition)? {
			Some(metadata) => metadata,
			None if partition.is_none() => RingBufferMetadata::unpartitioned(),
			None => RingBufferMetadata::reserved(store.reserve(self.capacity)?),
		};
		let free = self.tracked(partition).free_len;
		let pops = free.min(count as u64);
		if pops > 0 {
			self.load_free_pages(
				store,
				partition,
				(free - pops) / FREE_PAGE_SLOTS,
				(free - 1) / FREE_PAGE_SLOTS,
			)?;
		}
		let room = free + (self.capacity - metadata.filled);
		let evictions = (count as u64).saturating_sub(room) as usize;
		let victims = self.live(store, partition, None, evictions)?;
		if victims.len() != evictions {
			return_internal_error!(
				"Ring buffer partition {:?} must evict {} rows but its order index names only {}. This indicates a corrupt ring index.",
				partition,
				evictions,
				victims.len()
			)
		}
		if let Some(&(newest_victim, _)) = victims.last() {
			metadata.head = newest_victim + 1;
		}
		let capacity = self.capacity;
		let tracked = self.tracked(partition);
		let mut victims = victims.into_iter();
		let mut placed: Vec<(RowNumber, u64, bool)> = Vec::with_capacity(count);
		for _ in 0..count {
			let sequence = metadata.next_sequence;
			metadata.next_sequence += 1;
			let (row, evicts) = if tracked.free_len > 0 {
				let page = (tracked.free_len - 1) / FREE_PAGE_SLOTS;
				let popped = tracked.free_pages.get_mut(&page).and_then(|page| page.slots.pop());
				let Some(row) = popped else {
					return_internal_error!(
						"Ring buffer partition {:?} free page {} is shorter than its free count {}. This indicates a corrupt ring index.",
						partition,
						page,
						tracked.free_len
					)
				};
				tracked.free_len -= 1;
				(row, false)
			} else if metadata.filled < capacity {
				let row = RowNumber(metadata.base_rn + metadata.filled);
				metadata.filled += 1;
				(row, false)
			} else {
				let (oldest, row) = victims.next().expect("victims cover every eviction");
				tracked.order.insert(oldest, None);
				(row, true)
			};
			tracked.order.insert(sequence, Some(row));
			if !evicts {
				metadata.count += 1;
			}
			placed.push((row, sequence, evicts));
		}
		let evicted_rows: Vec<RowNumber> =
			placed.iter().filter(|(_, _, evicts)| *evicts).map(|(row, _, _)| *row).collect();
		let mut evicted = store.slots(partition, &evicted_rows)?.into_iter();
		self.assert_free_len(partition, metadata);
		self.set(partition, metadata);
		let mut placements = Vec::with_capacity(count);
		for (row, sequence, evicts) in placed {
			let evicted = match evicts {
				false => None,
				true => match evicted.next().flatten() {
					Some(bytes) => Some(bytes),
					None => return_internal_error!(
						"Ring buffer partition {:?} order index names slot {:?} as live but the slot is empty. This indicates a corrupt ring index.",
						partition,
						row
					),
				},
			};
			placements.push(Placement {
				row,
				sequence,
				evicted,
			});
		}
		Ok(placements)
	}

	pub fn remove(
		&mut self,
		store: &mut impl RingStore,
		partition: Option<Partition>,
		rows: &[RowNumber],
	) -> Result<Vec<Option<EncodedBytes>>> {
		let Some(mut metadata) = self.metadata(store, partition)? else {
			return Ok(vec![None; rows.len()]);
		};
		let held: Vec<RowNumber> =
			rows.iter().copied().filter(|&row| metadata.holds(row, self.capacity)).collect();
		let mut stored = store.slots(partition, &held)?.into_iter();
		let removed: Vec<Option<EncodedBytes>> = rows
			.iter()
			.map(|&row| match metadata.holds(row, self.capacity) {
				true => stored.next().flatten(),
				false => None,
			})
			.collect();
		if removed.iter().any(Option::is_some) {
			self.load_top_free_page(store, partition)?;
		}
		for (&row, bytes) in rows.iter().zip(&removed) {
			if let Some(bytes) = bytes {
				self.release(partition, EncodedRingBufferRow::view(bytes).sequence(), row);
				metadata.count -= 1;
			}
		}
		self.assert_free_len(partition, metadata);
		self.set(partition, metadata);
		Ok(removed)
	}

	pub fn expire(
		&mut self,
		store: &mut impl RingStore,
		partition: Option<Partition>,
		cutoff: DateTime,
		batch: usize,
	) -> Result<Vec<(RowNumber, EncodedBytes)>> {
		let Some(mut metadata) = self.metadata(store, partition)?.filter(|metadata| !metadata.is_empty())
		else {
			return Ok(Vec::new());
		};
		let mut expired: Vec<(u64, RowNumber, EncodedBytes)> = Vec::new();
		let mut after = None;
		'walk: loop {
			let window = self.live(store, partition, after, batch.max(1))?;
			let Some(&(last, _)) = window.last() else {
				break;
			};
			after = Some(last);
			let rows: Vec<RowNumber> = window.iter().map(|&(_, row)| row).collect();
			for ((sequence, row), stored) in window.into_iter().zip(store.slots(partition, &rows)?) {
				let Some(bytes) = stored else {
					return_internal_error!(
						"Ring buffer partition {:?} order index names slot {:?} as live but the slot is empty. This indicates a corrupt ring index.",
						partition,
						row
					)
				};
				if !EncodedRingBufferRow::view(&bytes).time().is_some_and(|time| time < cutoff) {
					break 'walk;
				}
				expired.push((sequence, row, bytes));
			}
		}
		if !expired.is_empty() {
			self.load_top_free_page(store, partition)?;
		}
		for &(sequence, row, _) in &expired {
			self.release(partition, sequence, row);
		}
		if let Some(&(newest_expired, _, _)) = expired.last() {
			metadata.head = newest_expired + 1;
		}
		metadata.count -= expired.len() as u64;
		self.assert_free_len(partition, metadata);
		self.set(partition, metadata);
		Ok(expired.into_iter().map(|(_, row, bytes)| (row, bytes)).collect())
	}

	pub fn writes(&self) -> Vec<MetadataWrite> {
		let mut writes = Vec::new();
		for (&partition, tracked) in &self.tracked {
			let Some(current) = tracked.current.filter(|current| tracked.loaded != Some(*current)) else {
				continue;
			};
			let dropped = partition.is_some() && current.is_empty();
			match (dropped, tracked.loaded) {
				(true, None) => {}
				(true, Some(_)) => writes.push(MetadataWrite::Remove(partition)),
				(false, _) => writes.push(MetadataWrite::Save(partition, current)),
			}
			for (&sequence, &entry) in &tracked.order {
				writes.push(MetadataWrite::Order(partition, sequence, entry));
			}
			if dropped {
				let stored = tracked.loaded.map_or(0, |metadata| metadata.filled - metadata.count);
				for page in 0..stored.div_ceil(FREE_PAGE_SLOTS) {
					writes.push(MetadataWrite::Free(partition, page, None));
				}
				continue;
			}
			for (&page, free) in &tracked.free_pages {
				if free.slots != free.stored {
					reifydb_assertions! {
						assert!(
							free.slots.len() as u64 <= FREE_PAGE_SLOTS,
							"a ring's free page must never hold more than its slot limit"
						);
					}
					let slots = (!free.slots.is_empty()).then(|| free.slots.clone());
					writes.push(MetadataWrite::Free(partition, page, slots));
				}
			}
		}
		writes
	}
}

pub fn newest(store: &mut impl RingNewest, partition: Option<Partition>) -> Result<Option<(RowNumber, EncodedBytes)>> {
	match store.newest_order(partition)? {
		None => Ok(None),
		Some((_, row)) => live_slot(store, partition, row).map(Some),
	}
}

pub fn oldest(store: &mut impl RingRead, partition: Option<Partition>) -> Result<Option<(RowNumber, EncodedBytes)>> {
	let after = head_cursor(store, partition)?;
	match store.order(partition, after, 1)?.pop() {
		None => Ok(None),
		Some((_, row)) => live_slot(store, partition, row).map(Some),
	}
}

pub fn oldest_first(store: &mut impl RingRead, partition: Option<Partition>) -> Result<Vec<RowNumber>> {
	let mut rows = Vec::new();
	let mut after = head_cursor(store, partition)?;
	loop {
		let page = store.order(partition, after, ORDER_PAGE)?;
		let exhausted = page.len() < ORDER_PAGE;
		after = page.last().map(|&(sequence, _)| sequence).or(after);
		rows.extend(page.into_iter().map(|(_, row)| row));
		if exhausted {
			return Ok(rows);
		}
	}
}

fn head_cursor(store: &mut impl RingRead, partition: Option<Partition>) -> Result<Option<u64>> {
	Ok(store.metadata(partition)?.and_then(|metadata| metadata.head.checked_sub(1)))
}

fn live_slot(
	store: &mut impl RingRead,
	partition: Option<Partition>,
	row: RowNumber,
) -> Result<(RowNumber, EncodedBytes)> {
	match store.slots(partition, &[row])?.pop().flatten() {
		Some(bytes) => Ok((row, bytes)),
		None => return_internal_error!(
			"Ring buffer partition {:?} order index names slot {:?} as live but the slot is empty. This indicates a corrupt ring index.",
			partition,
			row
		),
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::HashMap,
		panic::{AssertUnwindSafe, catch_unwind},
	};

	use reifydb_codec::row::shape::{RowFamily, RowShape};
	use reifydb_value::value::value_type::ValueType;

	use super::*;

	#[derive(Default)]
	struct MemoryStore {
		metadata: HashMap<Option<Partition>, RingBufferMetadata>,
		rows: HashMap<(Option<Partition>, RowNumber), EncodedBytes>,
		order: BTreeMap<(Option<Partition>, u64), RowNumber>,
		free: HashMap<(Option<Partition>, u64), Vec<RowNumber>>,
		next_base: u64,
		reads: Vec<(Option<Partition>, RowNumber)>,
		free_reads: usize,
		pages_read: Vec<(Option<Partition>, u64)>,
	}

	impl RingRead for MemoryStore {
		fn metadata(&mut self, partition: Option<Partition>) -> Result<Option<RingBufferMetadata>> {
			Ok(self.metadata.get(&partition).copied())
		}

		fn slots(
			&mut self,
			partition: Option<Partition>,
			rows: &[RowNumber],
		) -> Result<Vec<Option<EncodedBytes>>> {
			self.reads.extend(rows.iter().map(|&row| (partition, row)));
			Ok(rows.iter().map(|&row| self.rows.get(&(partition, row)).cloned()).collect())
		}

		fn order(
			&mut self,
			partition: Option<Partition>,
			after: Option<u64>,
			limit: usize,
		) -> Result<Vec<(u64, RowNumber)>> {
			Ok(self.entries(partition)
				.filter(|&(sequence, _)| after.is_none_or(|after| sequence > after))
				.take(limit)
				.collect())
		}
	}

	impl RingStore for MemoryStore {
		fn reserve(&mut self, capacity: u64) -> Result<u64> {
			let base = self.next_base.max(100);
			self.next_base = base + capacity;
			Ok(base)
		}

		fn free_pages(&mut self, partition: Option<Partition>, pages: &[u64]) -> Result<Vec<Vec<RowNumber>>> {
			self.free_reads += 1;
			self.pages_read.extend(pages.iter().map(|&page| (partition, page)));
			Ok(pages.iter()
				.map(|&page| self.free.get(&(partition, page)).cloned().unwrap_or_default())
				.collect())
		}
	}

	impl RingNewest for MemoryStore {
		fn newest_order(&mut self, partition: Option<Partition>) -> Result<Option<(u64, RowNumber)>> {
			Ok(self.entries(partition).last())
		}
	}

	impl MemoryStore {
		fn entries(
			&self,
			partition: Option<Partition>,
		) -> impl DoubleEndedIterator<Item = (u64, RowNumber)> + '_ {
			self.order
				.iter()
				.filter(move |((owner, _), _)| *owner == partition)
				.map(|(&(_, sequence), &row)| (sequence, row))
		}
	}

	fn row(sequence: u64, time: Option<i64>) -> EncodedBytes {
		let shape = RowShape::testing(RowFamily::RingBuffer, &[ValueType::Int4]);
		let mut builder = shape.allocate_ringbuffer();
		builder.set_sequence(sequence);
		if let Some(time) = time {
			builder.set_time(DateTime::from_millis(time));
		}
		builder.freeze().into_bytes()
	}

	fn sequence_of(bytes: &EncodedBytes) -> u64 {
		EncodedRingBufferRow::view(bytes).sequence()
	}

	fn insert(
		store: &mut MemoryStore,
		capacity: u64,
		partition: Option<Partition>,
		times: &[i64],
	) -> Vec<Placement> {
		let mut ring = Ring::new(capacity);
		let placed = ring.place(store, partition, times.len()).unwrap();
		for (placement, &time) in placed.iter().zip(times) {
			store.rows.insert((partition, placement.row), row(placement.sequence, Some(time)));
		}
		apply(store, &ring);
		placed
	}

	fn apply(store: &mut MemoryStore, ring: &Ring) {
		for write in ring.writes() {
			match write {
				MetadataWrite::Save(partition, metadata) => {
					store.metadata.insert(partition, metadata);
				}
				MetadataWrite::Remove(partition) => {
					store.metadata.remove(&partition);
				}
				MetadataWrite::Order(partition, sequence, Some(row)) => {
					store.order.insert((partition, sequence), row);
				}
				MetadataWrite::Order(partition, sequence, None) => {
					store.order.remove(&(partition, sequence));
				}
				MetadataWrite::Free(partition, page, None) => {
					store.free.remove(&(partition, page));
				}
				MetadataWrite::Free(partition, page, Some(slots)) => {
					store.free.insert((partition, page), slots);
				}
			}
		}
	}

	fn delete(store: &mut MemoryStore, capacity: u64, partition: Option<Partition>, rows: &[RowNumber]) {
		let mut ring = Ring::new(capacity);
		ring.remove(store, partition, rows).unwrap();
		for row in rows {
			store.rows.remove(&(partition, *row));
		}
		apply(store, &ring);
	}

	#[test]
	fn an_insert_into_a_full_partition_overwrites_the_oldest_slot_in_place() {
		// Mutation: appending a fresh row number gives downstream Remove + Insert instead of one Update.
		let mut store = MemoryStore::default();
		let first = insert(&mut store, 1, Some(Partition(1)), &[10]);
		let second = insert(&mut store, 1, Some(Partition(1)), &[20]);
		assert_eq!(second.len(), 1);
		assert_eq!(second[0].row, first[0].row, "the slot keeps its row number");
		assert_eq!(second[0].sequence, first[0].sequence + 1, "#sequence advances by one per write");
		let evicted = second[0].evicted.as_ref().expect("an overwrite names the evicted row as its pre");
		assert_eq!(sequence_of(evicted), first[0].sequence);
		assert_eq!(store.metadata[&Some(Partition(1))].count, 1, "an overwrite keeps the live count");
	}

	#[test]
	fn with_capacity_three_the_overwrite_pre_is_the_oldest_row() {
		// Mutation: overwriting the newest slot would evict the row a reader still expects to see.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 3, None, &[1, 2, 3]);
		let wrapped = insert(&mut store, 3, None, &[4]);
		assert_eq!(wrapped[0].row, placed[0].row, "the fourth write lands in the first slot");
		assert_eq!(sequence_of(wrapped[0].evicted.as_ref().unwrap()), placed[0].sequence);
		assert_eq!(store.metadata[&None].count, 3);
	}

	#[test]
	fn a_delete_reads_only_the_slots_of_its_own_partition() {
		// Mutation: walking every partition shows up as reads of partition 2's slots.
		let mut store = MemoryStore::default();
		let a = insert(&mut store, 4, Some(Partition(1)), &[1, 2, 3, 4]);
		insert(&mut store, 4, Some(Partition(2)), &[1, 2, 3, 4]);
		store.reads.clear();

		delete(&mut store, 4, Some(Partition(1)), &[a[2].row]);
		assert_eq!(store.reads, vec![(Some(Partition(1)), a[2].row)]);
		assert_eq!(store.metadata[&Some(Partition(1))].count, 3);
		assert_eq!(store.metadata[&Some(Partition(2))].count, 4);
	}

	#[test]
	fn a_delete_of_a_row_outside_the_partition_range_reads_nothing() {
		// Mutation: reading a foreign row number could count another partition's row as removed.
		let mut store = MemoryStore::default();
		insert(&mut store, 2, Some(Partition(1)), &[1, 2]);
		store.reads.clear();
		let mut ring = Ring::new(2);
		let removed = ring.remove(&mut store, Some(Partition(1)), &[RowNumber(1)]).unwrap();
		assert_eq!(removed, vec![None]);
		assert!(store.reads.is_empty());
		assert!(ring.writes().is_empty(), "nothing removed, nothing to write");
	}

	#[test]
	fn an_emptied_partition_drops_its_metadata_and_returns_with_a_fresh_range() {
		// Mutation: reusing the old range after removal would repeat (rn, #sequence) pairs.
		let mut store = MemoryStore::default();
		let first = insert(&mut store, 2, Some(Partition(1)), &[1]);
		delete(&mut store, 2, Some(Partition(1)), &[first[0].row]);
		assert!(!store.metadata.contains_key(&Some(Partition(1))));
		let again = insert(&mut store, 2, Some(Partition(1)), &[2]);
		assert_ne!(again[0].row, first[0].row);
	}

	#[test]
	fn an_emptied_unpartitioned_ring_keeps_its_sequence() {
		// Mutation: dropping the metadata would restart #sequence on the same row numbers.
		let mut store = MemoryStore::default();
		let first = insert(&mut store, 2, None, &[1]);
		delete(&mut store, 2, None, &[first[0].row]);
		assert_eq!(store.metadata[&None].count, 0);
		let again = insert(&mut store, 2, None, &[2]);
		assert_eq!(again[0].sequence, first[0].sequence + 1);
	}

	#[test]
	fn rows_in_distinct_partitions_never_share_a_row_number() {
		// Mutation: a shared base would make an rn name two rows across partitions.
		let mut store = MemoryStore::default();
		let a = insert(&mut store, 3, Some(Partition(1)), &[1, 2, 3]);
		let b = insert(&mut store, 3, Some(Partition(2)), &[1, 2, 3]);
		for left in &a {
			assert!(b.iter().all(|right| right.row != left.row));
		}
	}

	#[test]
	fn expiry_walks_from_the_oldest_slot_and_stops_at_the_first_live_row() {
		// Mutation: scanning all slots would also expire the old-stamped row behind a live one.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 4, None, &[10, 20, 30, 15]);
		store.reads.clear();
		let mut ring = Ring::new(4);
		let expired = ring.expire(&mut store, None, DateTime::from_millis(25), 1).unwrap();
		let rows: Vec<RowNumber> = expired.iter().map(|(row, _)| *row).collect();
		assert_eq!(rows, vec![placed[0].row, placed[1].row]);
		assert_eq!(store.reads.len(), 3, "reads stop at the first live row");
		assert_eq!(ring.metadata(&mut store, None).unwrap().unwrap().count, 2);
	}

	#[test]
	fn expiry_skips_holes_and_starts_at_the_oldest_live_sequence_after_a_wrap() {
		// Mutation: starting at slot 0 instead of the head would expire the newest row first.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 3, None, &[10, 20, 30]);
		let wrapped = insert(&mut store, 3, None, &[40]);
		delete(&mut store, 3, None, &[placed[1].row]);
		let mut ring = Ring::new(3);
		let expired = ring.expire(&mut store, None, DateTime::from_millis(35), 8).unwrap();
		let rows: Vec<RowNumber> = expired.iter().map(|(row, _)| *row).collect();
		assert_eq!(rows, vec![placed[2].row], "slot 1 is a hole, slot 2 holds 30, slot 0 holds the live 40");
		assert_ne!(rows[0], wrapped[0].row);
	}

	#[test]
	fn the_newest_row_follows_the_sequence_not_the_highest_row_number() {
		// Mutation: taking the highest row number answers with an older row after a wrap.
		let mut store = MemoryStore::default();
		insert(&mut store, 2, Some(Partition(1)), &[10, 20]);
		let wrapped = insert(&mut store, 2, Some(Partition(1)), &[30]);
		let (row, bytes) = newest(&mut store, Some(Partition(1))).unwrap().unwrap();
		assert_eq!(row, wrapped[0].row);
		assert_eq!(sequence_of(&bytes), wrapped[0].sequence);
	}

	#[test]
	fn the_newest_row_skips_a_deleted_newest_slot() {
		// Mutation: stopping at the newest slot returns nothing although an older row is live.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 3, None, &[10, 20]);
		delete(&mut store, 3, None, &[placed[1].row]);
		let (row, _) = newest(&mut store, None).unwrap().unwrap();
		assert_eq!(row, placed[0].row);
	}

	#[test]
	fn the_oldest_row_is_the_first_live_slot_from_the_head_after_a_wrap() {
		// Mutation: taking the lowest row number arms the expiry timer on the newest row.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 3, None, &[10, 20, 30]);
		insert(&mut store, 3, None, &[40]);
		delete(&mut store, 3, None, &[placed[1].row]);
		let (row, bytes) = oldest(&mut store, None).unwrap().unwrap();
		assert_eq!(row, placed[2].row, "slot 0 now holds the newest row and slot 1 is a hole");
		assert_eq!(sequence_of(&bytes), placed[2].sequence);
	}

	#[test]
	fn an_insert_after_a_delete_fills_the_free_slot_instead_of_evicting_a_live_row() {
		// Mutation: placing at seq % cap evicts the live row under the cursor and leaves the ring under
		// capacity.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 2, None, &[1, 2]);
		delete(&mut store, 2, None, &[placed[0].row]);
		let next = insert(&mut store, 2, None, &[3]);
		assert_eq!(next[0].row, placed[0].row, "the freed slot is reused");
		assert!(next[0].evicted.is_none(), "a free slot exists, so nothing live may be evicted");
		assert_eq!(store.metadata[&None].count, 2, "the ring is full again");
		assert!(store.rows.contains_key(&(None, placed[1].row)), "the surviving row stays live");
	}

	#[test]
	fn a_full_ring_evicts_the_oldest_live_sequence_not_the_slot_under_a_cursor() {
		// Mutation: evicting by slot position instead of sequence drops the newer of the two live rows.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 2, None, &[1, 2]);
		delete(&mut store, 2, None, &[placed[0].row]);
		let refill = insert(&mut store, 2, None, &[3]);
		let overflow = insert(&mut store, 2, None, &[4]);
		assert_eq!(overflow[0].row, placed[1].row, "slot 2 holds the oldest live sequence");
		let evicted = overflow[0].evicted.as_ref().expect("a full ring evicts");
		assert_eq!(sequence_of(evicted), placed[1].sequence);
		assert_ne!(overflow[0].row, refill[0].row);
		assert_eq!(store.metadata[&None].count, 2);
	}

	#[test]
	fn a_batch_larger_than_the_free_room_evicts_only_the_shortfall_oldest_first() {
		// Mutation: evicting before using free slots or fresh room drops rows the ring still has room for.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 4, None, &[1, 2, 3]);
		delete(&mut store, 4, None, &[placed[1].row]);
		let batch = insert(&mut store, 4, None, &[4, 5, 6]);
		let evicted: Vec<u64> = batch.iter().filter_map(|p| p.evicted.as_ref()).map(sequence_of).collect();
		assert_eq!(
			evicted,
			vec![placed[0].sequence],
			"one free slot and one fresh slot leave a single eviction"
		);
		assert_eq!(store.metadata[&None].count, 4);
		let order: Vec<u64> = store.entries(None).map(|(sequence, _)| sequence).collect();
		assert_eq!(order, vec![placed[2].sequence, batch[0].sequence, batch[1].sequence, batch[2].sequence]);
	}

	#[test]
	fn an_emptied_partition_leaves_no_order_or_free_rows_behind() {
		// Mutation: keeping the index of a dropped partition makes a fresh range evict ghost slots.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 2, Some(Partition(1)), &[1, 2]);
		delete(&mut store, 2, Some(Partition(1)), &[placed[0].row]);
		assert_eq!(store.free[&(Some(Partition(1)), 0)], vec![placed[0].row]);
		delete(&mut store, 2, Some(Partition(1)), &[placed[1].row]);
		assert!(!store.metadata.contains_key(&Some(Partition(1))));
		assert_eq!(store.entries(Some(Partition(1))).count(), 0);
		assert!(store.free.keys().all(|(owner, _)| *owner != Some(Partition(1))));
	}

	#[test]
	fn expiry_frees_the_slots_it_drains_so_the_next_insert_does_not_evict() {
		// Mutation: expiring without releasing slots makes the next insert evict a live row.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 2, None, &[10, 20]);
		let mut ring = Ring::new(2);
		ring.expire(&mut store, None, DateTime::from_millis(15), 8).unwrap();
		store.rows.remove(&(None, placed[0].row));
		apply(&mut store, &ring);
		let next = insert(&mut store, 2, None, &[30]);
		assert_eq!(next[0].row, placed[0].row);
		assert!(next[0].evicted.is_none());
		assert_eq!(store.metadata[&None].count, 2);
	}

	#[test]
	fn eviction_moves_the_head_to_the_oldest_live_sequence() {
		// Mutation: a head left at 0 makes every eviction walk the whole deleted order history.
		let mut store = MemoryStore::default();
		insert(&mut store, 3, None, &[1, 2, 3]);
		let wrapped = insert(&mut store, 3, None, &[4, 5]);
		let oldest_live = store.entries(None).next().map(|(sequence, _)| sequence).unwrap();
		assert_eq!(store.metadata[&None].head, oldest_live, "two evictions advance the head past both victims");
		assert_eq!(wrapped.len(), 2);
	}

	#[test]
	fn a_delete_of_the_head_row_keeps_the_head_a_safe_lower_bound() {
		// Mutation: a head moved past a live row would hide that row from eviction and scans.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 3, None, &[1, 2, 3]);
		insert(&mut store, 3, None, &[4]);
		let head = store.metadata[&None].head;
		delete(&mut store, 3, None, &[placed[1].row]);
		assert_eq!(store.metadata[&None].head, head, "a delete never moves the head");
		let (row, _) = oldest(&mut store, None).unwrap().unwrap();
		assert_eq!(row, placed[2].row, "the oldest live row is still found from the head");
		let next = insert(&mut store, 3, None, &[5]);
		assert!(next[0].evicted.is_none(), "the freed slot is used first");
		let overflow = insert(&mut store, 3, None, &[6]);
		assert_eq!(sequence_of(overflow[0].evicted.as_ref().unwrap()), placed[2].sequence);
	}

	#[test]
	fn a_ring_with_no_free_slots_never_reads_the_free_row() {
		// Without this skip an append-only ring pays one storage read per commit for a list known to be empty.
		let mut store = MemoryStore::default();
		insert(&mut store, 2, None, &[1, 2]);
		insert(&mut store, 2, None, &[3]);
		assert_eq!(store.free_reads, 0);
	}

	#[test]
	fn a_ring_with_a_freed_slot_still_reads_and_reuses_it() {
		// The skip must never hide a real free slot, otherwise an insert evicts a live row while a slot is
		// free.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 3, None, &[1, 2, 3]);
		delete(&mut store, 3, None, &[placed[1].row]);
		let reads_before_reuse = store.free_reads;
		let reused = insert(&mut store, 3, None, &[4]);
		assert_eq!(store.free_reads, reads_before_reuse + 1);
		assert_eq!(reused[0].row, placed[1].row);
		assert!(reused[0].evicted.is_none());
	}

	fn writes_of(ring: &Ring) -> Vec<(u64, Option<usize>)> {
		ring.writes()
			.into_iter()
			.filter_map(|write| match write {
				MetadataWrite::Free(_, page, slots) => Some((page, slots.map(|slots| slots.len()))),
				_ => None,
			})
			.collect()
	}

	fn stack(store: &MemoryStore, partition: Option<Partition>) -> Vec<RowNumber> {
		let mut pages: Vec<u64> =
			store.free.keys().filter(|(owner, _)| *owner == partition).map(|(_, page)| *page).collect();
		pages.sort_unstable();
		pages.into_iter().flat_map(|page| store.free[&(partition, page)].iter().copied()).collect()
	}

	fn assert_free_matches_count(store: &MemoryStore, partition: Option<Partition>) {
		// Every page below the top must be full and the top partial, or the page arithmetic points at nothing.
		let metadata = store.metadata[&partition];
		let free = metadata.filled - metadata.count;
		assert_eq!(stack(store, partition).len() as u64, free);
		for page in 0..free.div_ceil(FREE_PAGE_SLOTS) {
			let expected = (free - page * FREE_PAGE_SLOTS).min(FREE_PAGE_SLOTS) as usize;
			assert_eq!(store.free[&(partition, page)].len(), expected, "page {page} of {free} free");
		}
		let keys = store.free.keys().filter(|(owner, _)| *owner == partition).count() as u64;
		assert_eq!(keys, free.div_ceil(FREE_PAGE_SLOTS));
	}

	fn full(capacity: u64, partition: Option<Partition>) -> (MemoryStore, Vec<Placement>) {
		let mut store = MemoryStore::default();
		let times: Vec<i64> = (0..capacity as i64).collect();
		let placed = insert(&mut store, capacity, partition, &times);
		(store, placed)
	}

	fn rows_of(placed: &[Placement]) -> Vec<RowNumber> {
		placed.iter().map(|placement| placement.row).collect()
	}

	#[test]
	fn reuse_is_last_freed_first_within_one_batch_and_across_commits() {
		// Row numbers are user visible, so any other reuse order changes results.
		let (mut store, placed) = full(6, None);
		let rows = rows_of(&placed);
		delete(&mut store, 6, None, &[rows[1], rows[4]]);
		delete(&mut store, 6, None, &[rows[2]]);
		assert_eq!(stack(&store, None), vec![rows[1], rows[4], rows[2]]);

		let batch = insert(&mut store, 6, None, &[10, 11]);
		assert_eq!(rows_of(&batch), vec![rows[2], rows[4]], "within one batch, last freed first");
		let next = insert(&mut store, 6, None, &[12]);
		assert_eq!(rows_of(&next), vec![rows[1]], "across commits, last freed first");
		assert!(store.free.is_empty());
	}

	#[test]
	fn reuse_stays_last_freed_first_across_a_page_boundary() {
		// A page popped in the wrong order would hand out older slots while newer ones sit free.
		let capacity = FREE_PAGE_SLOTS + 3;
		let (mut store, placed) = full(capacity, None);
		let rows = rows_of(&placed);
		delete(&mut store, capacity, None, &rows);
		let times: Vec<i64> = (0..6).collect();
		let reused = insert(&mut store, capacity, None, &times);
		let mut expected: Vec<RowNumber> = rows.clone();
		expected.reverse();
		assert_eq!(rows_of(&reused), expected[..6].to_vec());
		assert_free_matches_count(&store, None);
	}

	#[test]
	fn a_push_then_pop_in_one_session_writes_no_free_key_and_reads_none() {
		// A leak here grows storage on a slot that is immediately busy again.
		let (mut store, placed) = full(4, None);
		let mut ring = Ring::new(4);
		ring.remove(&mut store, None, &[placed[1].row]).unwrap();
		let again = ring.place(&mut store, None, 1).unwrap();
		assert_eq!(again[0].row, placed[1].row);
		assert!(writes_of(&ring).is_empty(), "the stack is back where it started");
		assert_eq!(store.free_reads, 0, "a top page starting at its first index needs no read");
	}

	#[test]
	fn a_push_then_pop_on_a_stored_page_leaves_the_page_unwritten() {
		// The page content is unchanged, so rewriting it would be a write with no effect.
		let (mut store, placed) = full(4, None);
		delete(&mut store, 4, None, &[placed[0].row]);
		store.pages_read.clear();
		let mut ring = Ring::new(4);
		ring.remove(&mut store, None, &[placed[1].row]).unwrap();
		ring.place(&mut store, None, 1).unwrap();
		assert!(writes_of(&ring).is_empty());
		assert_eq!(
			store.pages_read,
			vec![(None, 0)],
			"the page is read once for the push, then cached for the pop"
		);
	}

	#[test]
	fn a_no_free_ring_never_reads_a_page_even_when_it_evicts() {
		// The skip is what keeps an append-only ring free of a read on every commit.
		let (mut store, _) = full(600, None);
		let times: Vec<i64> = (0..3).collect();
		insert(&mut store, 600, None, &times);
		assert!(store.pages_read.is_empty());
		assert!(store.free.is_empty());
	}

	#[test]
	fn a_commit_touches_only_the_pages_its_pushes_and_pops_reach() {
		// A rewrite of untouched pages would bring back the cost of rewriting the whole list.
		let capacity = FREE_PAGE_SLOTS * 3;
		let (mut store, placed) = full(capacity, None);
		let rows = rows_of(&placed);
		delete(&mut store, capacity, None, &rows[..(FREE_PAGE_SLOTS as usize * 2 + 10)]);
		store.pages_read.clear();

		let mut ring = Ring::new(capacity);
		ring.place(&mut store, None, 3).unwrap();
		assert_eq!(store.pages_read, vec![(None, 2)], "only the top page is read");
		assert_eq!(writes_of(&ring), vec![(2, Some(7))], "only the top page is written");

		store.pages_read.clear();
		let mut ring = Ring::new(capacity);
		ring.remove(&mut store, None, &[rows[FREE_PAGE_SLOTS as usize * 2 + 20]]).unwrap();
		assert_eq!(store.pages_read, vec![(None, 2)]);
		assert_eq!(writes_of(&ring), vec![(2, Some(11))]);
	}

	#[test]
	fn a_pop_batch_across_pages_reads_them_in_one_call_and_each_page_once() {
		// A read per slot or per pop would make a batch insert pay one round trip per row.
		let capacity = FREE_PAGE_SLOTS * 3;
		let (mut store, placed) = full(capacity, None);
		let rows = rows_of(&placed);
		delete(&mut store, capacity, None, &rows[..(FREE_PAGE_SLOTS as usize * 2 + 10)]);
		store.free_reads = 0;
		store.pages_read.clear();

		let mut ring = Ring::new(capacity);
		ring.place(&mut store, None, FREE_PAGE_SLOTS as usize + 20).unwrap();
		assert_eq!(store.free_reads, 1);
		assert_eq!(store.pages_read, vec![(None, 0), (None, 1), (None, 2)]);
		assert_eq!(writes_of(&ring), vec![(0, Some(502)), (1, None), (2, None)]);
	}

	#[test]
	fn many_changes_in_one_session_read_and_write_each_page_at_most_once() {
		// Repeated reads would miss the session's own changes, and repeated writes would waste the commit.
		let capacity = FREE_PAGE_SLOTS * 2;
		let (mut store, placed) = full(capacity, None);
		let rows = rows_of(&placed);
		delete(&mut store, capacity, None, &rows[..(FREE_PAGE_SLOTS as usize + 5)]);
		store.pages_read.clear();

		let mut ring = Ring::new(capacity);
		for _ in 0..4 {
			ring.place(&mut store, None, 2).unwrap();
		}
		ring.remove(&mut store, None, &[rows[FREE_PAGE_SLOTS as usize + 6]]).unwrap();
		ring.place(&mut store, None, 2).unwrap();
		let mut read = store.pages_read.clone();
		read.sort();
		read.dedup();
		assert_eq!(read.len(), store.pages_read.len(), "no page is read twice: {:?}", store.pages_read);
		let written: Vec<u64> = writes_of(&ring).iter().map(|(page, _)| *page).collect();
		let mut unique = written.clone();
		unique.dedup();
		assert_eq!(written, unique, "no page is written twice");
	}

	#[test]
	fn pushing_past_a_page_boundary_starts_a_new_page_with_no_read_and_never_overfills() {
		// A slot past the page limit would break the index arithmetic every later session relies on.
		let capacity = FREE_PAGE_SLOTS + 2;
		let (mut store, placed) = full(capacity, None);
		let rows = rows_of(&placed);
		delete(&mut store, capacity, None, &rows[..FREE_PAGE_SLOTS as usize]);
		assert_eq!(store.free[&(None, 0)].len() as u64, FREE_PAGE_SLOTS);
		store.pages_read.clear();

		delete(&mut store, capacity, None, &[rows[FREE_PAGE_SLOTS as usize]]);
		assert!(store.pages_read.is_empty(), "the new page starts at its first index, so nothing is read");
		assert_eq!(store.free[&(None, 0)].len() as u64, FREE_PAGE_SLOTS);
		assert_eq!(store.free[&(None, 1)], vec![rows[FREE_PAGE_SLOTS as usize]]);
		assert!(store.free.values().all(|page| page.len() as u64 <= FREE_PAGE_SLOTS));
		assert_free_matches_count(&store, None);
	}

	#[test]
	fn popping_back_across_a_page_boundary_removes_the_empty_page_and_keeps_the_rest() {
		// A kept empty page would disagree with the free count and leak a key.
		let capacity = FREE_PAGE_SLOTS + 2;
		let (mut store, placed) = full(capacity, None);
		let rows = rows_of(&placed);
		delete(&mut store, capacity, None, &rows[..(FREE_PAGE_SLOTS as usize + 1)]);
		let popped = insert(&mut store, capacity, None, &[10, 11]);
		assert_eq!(popped[0].row, rows[FREE_PAGE_SLOTS as usize]);
		assert_eq!(popped[1].row, rows[FREE_PAGE_SLOTS as usize - 1]);
		assert!(!store.free.contains_key(&(None, 1)), "the emptied page key is removed");
		assert_eq!(store.free[&(None, 0)].len() as u64, FREE_PAGE_SLOTS - 1);
		assert_free_matches_count(&store, None);
	}

	#[test]
	fn the_free_pages_agree_with_the_free_count_through_every_step() {
		// A drifted stack either overwrites a live row or leaks a slot forever.
		let capacity = FREE_PAGE_SLOTS + 40;
		let (mut store, placed) = full(capacity, None);
		let rows = rows_of(&placed);
		for chunk in rows[..(FREE_PAGE_SLOTS as usize + 30)].chunks(97) {
			delete(&mut store, capacity, None, chunk);
			assert_free_matches_count(&store, None);
		}
		for _ in 0..6 {
			let times: Vec<i64> = (0..80).collect();
			insert(&mut store, capacity, None, &times);
			assert_free_matches_count(&store, None);
		}
	}

	#[test]
	fn an_emptied_partition_removes_every_free_page_and_a_new_range_starts_clean() {
		// A page left behind would hand slots of the old range to the partition's next life.
		let partition = Some(Partition(1));
		let capacity = FREE_PAGE_SLOTS * 2 + 10;
		let (mut store, placed) = full(capacity, partition);
		let rows = rows_of(&placed);
		delete(&mut store, capacity, partition, &rows[..rows.len() - 1]);
		assert_eq!(store.free.len(), 3);
		store.pages_read.clear();
		delete(&mut store, capacity, partition, &[rows[rows.len() - 1]]);
		assert!(store.free.is_empty(), "every page of the emptied partition goes");

		let again = insert(&mut store, capacity, partition, &[1]);
		assert!(!rows.contains(&again[0].row), "freed slots of the old range must not come back");
	}

	#[test]
	fn emptying_a_million_slot_partition_writes_about_one_key_per_page() {
		// One key per slot would hit the one million write limit and make the last delete fail for good.
		let partition = Some(Partition(1));
		let capacity = 1_000_000u64;
		let mut store = MemoryStore::default();
		store.metadata.insert(
			partition,
			RingBufferMetadata {
				base_rn: 100,
				next_sequence: capacity,
				count: 1,
				filled: capacity,
				head: capacity - 1,
			},
		);
		let row_number = RowNumber(100 + capacity - 1);
		store.rows.insert((partition, row_number), row(capacity - 1, None));
		store.order.insert((partition, capacity - 1), row_number);
		store.free.insert(
			(partition, (capacity - 2) / FREE_PAGE_SLOTS),
			vec![RowNumber(100); ((capacity - 1) % FREE_PAGE_SLOTS) as usize],
		);
		let mut ring = Ring::new(capacity);
		ring.remove(&mut store, partition, &[row_number]).unwrap();
		let writes = ring.writes();
		let pages = writes.iter().filter(|write| matches!(write, MetadataWrite::Free(_, _, None))).count();
		assert_eq!(pages as u64, (capacity - 1).div_ceil(FREE_PAGE_SLOTS));
		assert_eq!(pages, 1954);
		assert!(writes.len() < 2_000, "got {} writes", writes.len());
	}

	#[test]
	fn a_full_reuse_of_the_top_page_writes_a_removal_not_an_empty_page() {
		// An empty page row would read back as a present page with no slots and hide a corrupt count.
		let (mut store, placed) = full(4, None);
		delete(&mut store, 4, None, &[placed[0].row]);
		let mut ring = Ring::new(4);
		ring.place(&mut store, None, 1).unwrap();
		assert_eq!(writes_of(&ring), vec![(0, None)]);
	}

	#[test]
	fn a_page_shorter_than_the_free_count_fails_instead_of_evicting_a_live_row() {
		// Falling through to fresh slots or eviction would overwrite a row while the index is corrupt.
		let (mut store, placed) = full(4, None);
		delete(&mut store, 4, None, &[placed[0].row]);
		store.free.remove(&(None, 0));
		let mut ring = Ring::new(4);
		let result = catch_unwind(AssertUnwindSafe(|| ring.place(&mut store, None, 1)));
		assert!(!matches!(result, Ok(Ok(_))), "a corrupt free stack must not place a row");
	}
}
