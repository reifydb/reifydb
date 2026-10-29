// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use indexmap::IndexMap;
use reifydb_codec::row::{bytes::EncodedBytes, ringbuffer::EncodedRingBufferRow};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, partition::Partition, row_number::RowNumber},
};

use crate::interface::catalog::ringbuffer::RingBufferMetadata;

pub trait RingStore {
	fn metadata(&mut self, partition: Option<Partition>) -> Result<Option<RingBufferMetadata>>;

	fn reserve(&mut self, capacity: u64) -> Result<u64>;

	fn slots(&mut self, partition: Option<Partition>, rows: &[RowNumber]) -> Result<Vec<Option<EncodedBytes>>>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct Placement {
	pub row: RowNumber,
	pub sequence: u64,
	pub evicted: Option<EncodedBytes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataWrite {
	Save(Option<Partition>, RingBufferMetadata),
	Remove(Option<Partition>),
}

struct Tracked {
	loaded: Option<RingBufferMetadata>,
	current: Option<RingBufferMetadata>,
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
			},
		);
		Ok(loaded)
	}

	fn set(&mut self, partition: Option<Partition>, metadata: RingBufferMetadata) {
		self.tracked.get_mut(&partition).expect("metadata is loaded before it is changed").current =
			Some(metadata);
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
		let sequences: Vec<u64> = (metadata.next_sequence..metadata.next_sequence + count as u64).collect();
		let rows: Vec<RowNumber> =
			sequences.iter().map(|&sequence| metadata.row_of(sequence, self.capacity)).collect();
		let stored = store.slots(partition, &rows)?;
		metadata.next_sequence += count as u64;
		metadata.count += stored.iter().filter(|row| row.is_none()).count() as u64;
		self.set(partition, metadata);
		Ok(rows.into_iter()
			.zip(sequences)
			.zip(stored)
			.map(|((row, sequence), evicted)| Placement {
				row,
				sequence,
				evicted,
			})
			.collect())
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
		metadata.count -= removed.iter().filter(|row| row.is_some()).count() as u64;
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
		let cycle: Vec<RowNumber> = metadata.oldest_first(self.capacity).map(|(_, row)| row).collect();
		let mut expired = Vec::new();
		'walk: for window in cycle.chunks(batch.max(1)) {
			for (&row, stored) in window.iter().zip(store.slots(partition, window)?) {
				let Some(bytes) = stored else {
					continue;
				};
				if !EncodedRingBufferRow::view(&bytes).time().is_some_and(|time| time < cutoff) {
					break 'walk;
				}
				expired.push((row, bytes));
			}
		}
		metadata.count -= expired.len() as u64;
		self.set(partition, metadata);
		Ok(expired)
	}

	pub fn writes(&self) -> Vec<MetadataWrite> {
		self.tracked
			.iter()
			.filter(|(_, tracked)| tracked.current != tracked.loaded)
			.filter_map(|(&partition, tracked)| {
				let current = tracked.current?;
				match (partition, current.is_empty(), tracked.loaded) {
					(Some(_), true, None) => None,
					(Some(_), true, Some(_)) => Some(MetadataWrite::Remove(partition)),
					_ => Some(MetadataWrite::Save(partition, current)),
				}
			})
			.collect()
	}
}

pub fn newest(
	store: &mut impl RingStore,
	partition: Option<Partition>,
	capacity: u64,
) -> Result<Option<(RowNumber, EncodedBytes)>> {
	let Some(metadata) = store.metadata(partition)?.filter(|metadata| !metadata.is_empty()) else {
		return Ok(None);
	};
	for (_, row) in metadata.oldest_first(capacity).rev() {
		if let Some(bytes) = store.slots(partition, &[row])?.pop().flatten() {
			return Ok(Some((row, bytes)));
		}
	}
	Ok(None)
}

pub fn oldest(
	store: &mut impl RingStore,
	partition: Option<Partition>,
	capacity: u64,
) -> Result<Option<(RowNumber, EncodedBytes)>> {
	let Some(metadata) = store.metadata(partition)?.filter(|metadata| !metadata.is_empty()) else {
		return Ok(None);
	};
	for (_, row) in metadata.oldest_first(capacity) {
		if let Some(bytes) = store.slots(partition, &[row])?.pop().flatten() {
			return Ok(Some((row, bytes)));
		}
	}
	Ok(None)
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use reifydb_codec::row::shape::{RowFamily, RowShape};
	use reifydb_value::value::value_type::ValueType;

	use super::*;

	#[derive(Default)]
	struct MemoryStore {
		metadata: HashMap<Option<Partition>, RingBufferMetadata>,
		rows: HashMap<(Option<Partition>, RowNumber), EncodedBytes>,
		next_base: u64,
		reads: Vec<(Option<Partition>, RowNumber)>,
	}

	impl RingStore for MemoryStore {
		fn metadata(&mut self, partition: Option<Partition>) -> Result<Option<RingBufferMetadata>> {
			Ok(self.metadata.get(&partition).copied())
		}

		fn reserve(&mut self, capacity: u64) -> Result<u64> {
			let base = self.next_base.max(100);
			self.next_base = base + capacity;
			Ok(base)
		}

		fn slots(
			&mut self,
			partition: Option<Partition>,
			rows: &[RowNumber],
		) -> Result<Vec<Option<EncodedBytes>>> {
			self.reads.extend(rows.iter().map(|&row| (partition, row)));
			Ok(rows.iter().map(|&row| self.rows.get(&(partition, row)).cloned()).collect())
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
	fn a_hole_is_refilled_only_when_the_cycle_reaches_it_and_capacity_holds() {
		// Mutation: writing into the first free slot breaks slot order == #sequence order.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 3, None, &[1, 2, 3]);
		delete(&mut store, 3, None, &[placed[1].row]);
		assert_eq!(store.metadata[&None].count, 2);

		let next = insert(&mut store, 3, None, &[4]);
		assert_eq!(next[0].row, placed[0].row, "the cycle goes to slot 0, not to the hole in slot 1");
		assert!(next[0].evicted.is_some(), "slot 0 was occupied, so this is an overwrite");
		assert_eq!(store.metadata[&None].count, 2, "the hole stays empty");

		let refill = insert(&mut store, 3, None, &[5]);
		assert_eq!(refill[0].row, placed[1].row, "the cycle reaches the hole next");
		assert!(refill[0].evicted.is_none(), "filling a hole is an insert, not an overwrite");
		assert_eq!(store.metadata[&None].count, 3, "count never exceeds capacity");
		let live = store.rows.keys().filter(|(partition, _)| partition.is_none()).count();
		assert_eq!(live, 3);
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
		let (row, bytes) = newest(&mut store, Some(Partition(1)), 2).unwrap().unwrap();
		assert_eq!(row, wrapped[0].row);
		assert_eq!(sequence_of(&bytes), wrapped[0].sequence);
	}

	#[test]
	fn the_newest_row_skips_a_deleted_newest_slot() {
		// Mutation: stopping at the newest slot returns nothing although an older row is live.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 3, None, &[10, 20]);
		delete(&mut store, 3, None, &[placed[1].row]);
		let (row, _) = newest(&mut store, None, 3).unwrap().unwrap();
		assert_eq!(row, placed[0].row);
	}

	#[test]
	fn the_oldest_row_is_the_first_live_slot_from_the_head_after_a_wrap() {
		// Mutation: taking the lowest row number arms the expiry timer on the newest row.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 3, None, &[10, 20, 30]);
		insert(&mut store, 3, None, &[40]);
		delete(&mut store, 3, None, &[placed[1].row]);
		let (row, bytes) = oldest(&mut store, None, 3).unwrap().unwrap();
		assert_eq!(row, placed[2].row, "slot 0 now holds the newest row and slot 1 is a hole");
		assert_eq!(sequence_of(&bytes), placed[2].sequence);
	}
}
