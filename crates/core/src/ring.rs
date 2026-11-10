// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use indexmap::{IndexMap, IndexSet};
use reifydb_codec::row::{bytes::EncodedBytes, ringbuffer::EncodedRingBufferRow};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, partition::Partition, row_number::RowNumber},
};

use crate::{interface::catalog::ringbuffer::RingBufferMetadata, return_internal_error};

const ROW_PAGE: usize = 64;

pub trait RingRead {
	fn metadata(&mut self, partitions: &[Option<Partition>]) -> Result<Vec<Option<RingBufferMetadata>>>;

	fn rows(&mut self, rows: &[(Option<Partition>, RowNumber)]) -> Result<Vec<Option<EncodedBytes>>>;

	fn oldest(
		&mut self,
		partition: Option<Partition>,
		after: Option<RowNumber>,
		limit: usize,
	) -> Result<Vec<(RowNumber, EncodedBytes)>>;
}

pub trait RingStore: RingRead {
	fn reserve(&mut self, count: u64) -> Result<u64>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct Evicted {
	pub row: RowNumber,
	pub bytes: EncodedBytes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Placement {
	pub row: RowNumber,
	pub evicted: Option<Evicted>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

	pub fn preload(&mut self, store: &mut impl RingStore, partitions: &[Option<Partition>]) -> Result<()> {
		let missing: Vec<Option<Partition>> = partitions
			.iter()
			.copied()
			.filter(|partition| !self.tracked.contains_key(partition))
			.collect::<IndexSet<_>>()
			.into_iter()
			.collect();
		if missing.is_empty() {
			return Ok(());
		}
		let loaded = store.metadata(&missing)?;
		for (partition, loaded) in missing.into_iter().zip(loaded) {
			self.tracked.insert(
				partition,
				Tracked {
					loaded,
					current: loaded,
				},
			);
		}
		Ok(())
	}

	pub fn metadata(
		&mut self,
		store: &mut impl RingStore,
		partition: Option<Partition>,
	) -> Result<Option<RingBufferMetadata>> {
		self.preload(store, &[partition])?;
		Ok(self.tracked[&partition].current)
	}

	fn set(&mut self, partition: Option<Partition>, metadata: RingBufferMetadata) {
		self.tracked.get_mut(&partition).expect("metadata is loaded before it is changed").current =
			Some(metadata);
	}

	fn oldest_live(
		&self,
		store: &mut impl RingStore,
		partition: Option<Partition>,
		metadata: &RingBufferMetadata,
		after: Option<RowNumber>,
		limit: usize,
	) -> Result<Vec<(RowNumber, EncodedBytes)>> {
		if self.capacity > 1 {
			return store.oldest(partition, after, limit);
		}
		let head = RowNumber(metadata.head);
		if limit == 0 || after.is_some_and(|after| after.0 >= head.0) {
			return Ok(Vec::new());
		}
		Ok(store.rows(&[(partition, head)])?.pop().flatten().map(|bytes| (head, bytes)).into_iter().collect())
	}

	pub fn place(
		&mut self,
		store: &mut impl RingStore,
		requests: &[(Option<Partition>, Vec<RowNumber>)],
	) -> Result<Vec<Vec<Placement>>> {
		let partitions: Vec<Option<Partition>> = requests.iter().map(|(partition, _)| *partition).collect();
		assert!(
			distinct(&partitions),
			"one placement call must name each partition once, or a later request would evict a row the earlier one already evicted"
		);
		self.preload(store, &partitions)?;
		requests.iter().map(|(partition, rows)| self.place_partition(store, *partition, rows)).collect()
	}

	fn place_partition(
		&mut self,
		store: &mut impl RingStore,
		partition: Option<Partition>,
		rows: &[RowNumber],
	) -> Result<Vec<Placement>> {
		let count = rows.len();
		assert!(
			count as u64 <= self.capacity,
			"one placement call must not lap the ring, or it would evict a row it just placed"
		);
		if count == 0 {
			return Ok(Vec::new());
		}
		let first = rows[0].0;
		let mut metadata = match self.metadata(store, partition)? {
			Some(metadata) => metadata,
			None => RingBufferMetadata {
				head: first,
				..RingBufferMetadata::default()
			},
		};
		let room = self.capacity - metadata.count;
		let evictions = (count as u64).saturating_sub(room) as usize;
		let victims = if evictions == 0 {
			Vec::new()
		} else {
			self.oldest_live(store, partition, &metadata, head_cursor(&metadata), evictions)?
		};
		if victims.len() != evictions {
			return_internal_error!(
				"Ring buffer partition {:?} must evict {} rows but holds only {} live rows. This indicates corrupt ring metadata.",
				partition,
				evictions,
				victims.len()
			)
		}
		if let Some(&(newest_victim, _)) = victims.last() {
			metadata.head = newest_victim.0 + 1;
		}
		if self.capacity == 1 {
			metadata.head = first;
		}
		let mut victims = victims.into_iter();
		let fresh = count - evictions;
		let mut placements = Vec::with_capacity(count);
		for (offset, &row) in rows.iter().enumerate() {
			let evicted = if offset < fresh {
				metadata.count += 1;
				None
			} else {
				let (victim, bytes) = victims.next().expect("victims cover every eviction");
				Some(Evicted {
					row: victim,
					bytes,
				})
			};
			placements.push(Placement {
				row,
				evicted,
			});
		}
		self.set(partition, metadata);
		Ok(placements)
	}

	pub fn remove(
		&mut self,
		store: &mut impl RingStore,
		requests: &[(Option<Partition>, Vec<RowNumber>)],
	) -> Result<Vec<Vec<Option<EncodedBytes>>>> {
		let partitions: Vec<Option<Partition>> = requests.iter().map(|(partition, _)| *partition).collect();
		assert!(
			distinct(&partitions),
			"one removal call must name each partition once, or a row would be read and released twice"
		);
		self.preload(store, &partitions)?;
		let held: Vec<(Option<Partition>, RowNumber)> = requests
			.iter()
			.filter(|(partition, _)| self.tracked[partition].current.is_some())
			.flat_map(|(partition, rows)| rows.iter().map(move |&row| (*partition, row)))
			.collect();
		let mut stored = store.rows(&held)?.into_iter();
		let mut removals = Vec::with_capacity(requests.len());
		for (partition, rows) in requests {
			let Some(mut metadata) = self.tracked[partition].current else {
				removals.push(vec![None; rows.len()]);
				continue;
			};
			let removed: Vec<Option<EncodedBytes>> = stored.by_ref().take(rows.len()).collect();
			metadata.count -= removed.iter().flatten().count() as u64;
			self.set(*partition, metadata);
			removals.push(removed);
		}
		Ok(removals)
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
		let window = batch.max(1);
		let mut expired: Vec<(RowNumber, EncodedBytes)> = Vec::new();
		let mut after = head_cursor(&metadata);
		'walk: loop {
			let rows = self.oldest_live(store, partition, &metadata, after, window)?;
			let exhausted = rows.len() < window;
			for (row, bytes) in rows {
				if !EncodedRingBufferRow::view(&bytes).time().is_some_and(|time| time < cutoff) {
					break 'walk;
				}
				after = Some(row);
				expired.push((row, bytes));
			}
			if exhausted {
				break;
			}
		}
		if let Some(&(newest_expired, _)) = expired.last() {
			metadata.head = newest_expired.0 + 1;
		}
		metadata.count -= expired.len() as u64;
		self.set(partition, metadata);
		Ok(expired)
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
		}
		writes
	}
}

pub fn oldest(
	store: &mut impl RingRead,
	partitions: &[Option<Partition>],
) -> Result<Vec<Option<(RowNumber, EncodedBytes)>>> {
	let metadata = store.metadata(partitions)?;
	let mut oldest = Vec::with_capacity(partitions.len());
	for (&partition, metadata) in partitions.iter().zip(metadata) {
		oldest.push(match metadata {
			Some(metadata) => store.oldest(partition, head_cursor(&metadata), 1)?.pop(),
			None => None,
		});
	}
	Ok(oldest)
}

pub fn oldest_first(store: &mut impl RingRead, partition: Option<Partition>) -> Result<Vec<RowNumber>> {
	let Some(metadata) = store.metadata(&[partition])?.pop().flatten() else {
		return Ok(Vec::new());
	};
	let mut rows = Vec::new();
	let mut after = head_cursor(&metadata);
	loop {
		let page = store.oldest(partition, after, ROW_PAGE)?;
		let exhausted = page.len() < ROW_PAGE;
		after = page.last().map(|&(row, _)| row).or(after);
		rows.extend(page.into_iter().map(|(row, _)| row));
		if exhausted {
			return Ok(rows);
		}
	}
}

fn distinct(partitions: &[Option<Partition>]) -> bool {
	partitions.iter().collect::<IndexSet<_>>().len() == partitions.len()
}

fn head_cursor(metadata: &RingBufferMetadata) -> Option<RowNumber> {
	metadata.head.checked_sub(1).map(RowNumber)
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
		next_base: u64,
		reads: Vec<(Option<Partition>, RowNumber)>,
		metadata_calls: usize,
		rows_calls: usize,
	}

	impl RingRead for MemoryStore {
		fn metadata(&mut self, partitions: &[Option<Partition>]) -> Result<Vec<Option<RingBufferMetadata>>> {
			self.metadata_calls += 1;
			Ok(partitions.iter().map(|partition| self.metadata.get(partition).copied()).collect())
		}

		fn rows(&mut self, rows: &[(Option<Partition>, RowNumber)]) -> Result<Vec<Option<EncodedBytes>>> {
			self.rows_calls += 1;
			self.reads.extend_from_slice(rows);
			Ok(rows.iter().map(|key| self.rows.get(key).cloned()).collect())
		}

		fn oldest(
			&mut self,
			partition: Option<Partition>,
			after: Option<RowNumber>,
			limit: usize,
		) -> Result<Vec<(RowNumber, EncodedBytes)>> {
			let mut found: Vec<(RowNumber, EncodedBytes)> = self
				.rows
				.iter()
				.filter(|&(&(owner, row), _)| {
					owner == partition && after.is_none_or(|after| row.0 > after.0)
				})
				.map(|(&(_, row), bytes)| (row, bytes.clone()))
				.collect();
			found.sort_by_key(|&(row, _)| row.0);
			found.truncate(limit);
			self.reads.extend(found.iter().map(|&(row, _)| (partition, row)));
			Ok(found)
		}
	}

	impl RingStore for MemoryStore {
		fn reserve(&mut self, batch_count: u64) -> Result<u64> {
			let base = self.next_base.max(100);
			self.next_base = base + batch_count;
			Ok(base)
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
		let first = store.reserve(times.len() as u64).unwrap();
		let rows: Vec<RowNumber> = (first..first + times.len() as u64).map(RowNumber).collect();
		let placed = ring.place(store, &[(partition, rows)]).unwrap().remove(0);
		for (placement, &time) in placed.iter().zip(times) {
			if let Some(evicted) = &placement.evicted {
				store.rows.remove(&(partition, evicted.row));
			}
			store.rows.insert((partition, placement.row), row(placement.row.0, Some(time)));
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
		ring.remove(store, &[(partition, rows.to_vec())]).unwrap();
		for row in rows {
			store.rows.remove(&(partition, *row));
		}
		apply(store, &ring);
	}

	#[test]
	fn an_insert_into_a_full_partition_evicts_the_oldest_row_under_a_fresh_row_number() {
		// Mutation: reusing the victim's row number would hand a reader two different rows under one id.
		let mut store = MemoryStore::default();
		let first = insert(&mut store, 1, Some(Partition(1)), &[10]);
		let second = insert(&mut store, 1, Some(Partition(1)), &[20]);
		assert_eq!(second.len(), 1);
		assert_eq!(second[0].row.0, first[0].row.0 + 1, "an eviction hands the new row a fresh row number");
		let evicted = second[0].evicted.as_ref().expect("an eviction names the evicted row with its bytes");
		assert_eq!(evicted.row, first[0].row, "the victim keeps its own row number");
		assert_eq!(sequence_of(&evicted.bytes), first[0].row.0);
		assert_eq!(store.metadata[&Some(Partition(1))].count, 1, "an eviction keeps the live count");
	}

	#[test]
	fn with_capacity_three_the_evicted_row_is_the_oldest() {
		// Mutation: evicting the newest row would drop the row a reader still expects to see.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 3, None, &[1, 2, 3]);
		let overflow = insert(&mut store, 3, None, &[4]);
		let evicted = overflow[0].evicted.as_ref().unwrap();
		assert_eq!(evicted.row, placed[0].row, "the oldest row is the one evicted");
		assert_eq!(sequence_of(&evicted.bytes), placed[0].row.0);
		assert_eq!(store.metadata[&None].count, 3);
	}

	#[test]
	fn a_delete_reads_only_the_rows_of_its_own_partition() {
		// Mutation: walking every partition shows up as reads of partition 2's rows.
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
	fn a_delete_of_another_partitions_row_removes_nothing() {
		// Mutation: reading a foreign row number could count another partition's row as removed.
		let mut store = MemoryStore::default();
		insert(&mut store, 2, Some(Partition(1)), &[1, 2]);
		let foreign = insert(&mut store, 2, Some(Partition(2)), &[3]);
		let mut ring = Ring::new(2);
		let removed = ring.remove(&mut store, &[(Some(Partition(1)), vec![foreign[0].row])]).unwrap().remove(0);
		assert_eq!(removed, vec![None]);
		assert!(ring.writes().is_empty(), "nothing removed, nothing to write");
	}

	#[test]
	fn an_emptied_partition_drops_its_metadata_and_returns_with_fresh_row_numbers() {
		// Mutation: reusing row numbers after removal would hand a reader two different rows under one id.
		let mut store = MemoryStore::default();
		let first = insert(&mut store, 2, Some(Partition(1)), &[1]);
		delete(&mut store, 2, Some(Partition(1)), &[first[0].row]);
		assert!(!store.metadata.contains_key(&Some(Partition(1))));
		let again = insert(&mut store, 2, Some(Partition(1)), &[2]);
		assert_ne!(again[0].row, first[0].row);
	}

	#[test]
	fn an_emptied_unpartitioned_ring_never_reuses_a_row_number() {
		// Mutation: restarting the counter would hand a reader two different rows under one id.
		let mut store = MemoryStore::default();
		let first = insert(&mut store, 2, None, &[1]);
		delete(&mut store, 2, None, &[first[0].row]);
		assert_eq!(store.metadata[&None].count, 0);
		let again = insert(&mut store, 2, None, &[2]);
		assert_eq!(again[0].row.0, first[0].row.0 + 1, "an emptied ring never hands a row number out twice");
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
	fn expiry_walks_from_the_oldest_row_and_stops_at_the_first_live_one() {
		// Mutation: scanning every row would also expire the old-stamped row behind a live one.
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
	fn expiry_skips_deleted_rows_and_starts_at_the_head_after_an_eviction() {
		// Mutation: expiring an evicted or deleted row would hand back a row that is gone.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 3, None, &[10, 20, 30]);
		let overflow = insert(&mut store, 3, None, &[40]);
		delete(&mut store, 3, None, &[placed[1].row]);
		let mut ring = Ring::new(3);
		let expired = ring.expire(&mut store, None, DateTime::from_millis(35), 8).unwrap();
		let rows: Vec<RowNumber> = expired.iter().map(|(row, _)| *row).collect();
		assert_eq!(
			rows,
			vec![placed[2].row],
			"the evicted and the deleted rows are skipped, and 40 is too new"
		);
		assert_ne!(rows[0], overflow[0].row);
	}

	#[test]
	fn the_oldest_row_is_the_first_live_row_from_the_head_after_an_eviction() {
		// Mutation: starting below the head arms the expiry timer on an evicted or deleted row.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 3, None, &[10, 20, 30]);
		insert(&mut store, 3, None, &[40]);
		delete(&mut store, 3, None, &[placed[1].row]);
		let (row, bytes) = oldest(&mut store, &[None]).unwrap().remove(0).unwrap();
		assert_eq!(row, placed[2].row, "the evicted and the deleted rows are skipped");
		assert_eq!(sequence_of(&bytes), placed[2].row.0);
	}

	#[test]
	fn an_insert_after_a_delete_uses_the_room_instead_of_evicting_a_live_row() {
		// Mutation: ignoring the room a delete made evicts a live row and leaves the ring under capacity.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 2, None, &[1, 2]);
		delete(&mut store, 2, None, &[placed[0].row]);
		let next = insert(&mut store, 2, None, &[3]);
		assert!(next[0].evicted.is_none(), "the delete made room, so nothing live may be evicted");
		assert_eq!(store.metadata[&None].count, 2, "the ring is full again");
		assert!(store.rows.contains_key(&(None, placed[1].row)), "the surviving row stays live");
	}

	#[test]
	fn a_full_ring_evicts_the_oldest_live_row_skipping_deleted_ones() {
		// Mutation: choosing a victim that is already deleted keeps an extra live row past capacity.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 2, None, &[1, 2]);
		delete(&mut store, 2, None, &[placed[0].row]);
		let refill = insert(&mut store, 2, None, &[3]);
		let overflow = insert(&mut store, 2, None, &[4]);
		let evicted = overflow[0].evicted.as_ref().expect("a full ring evicts");
		assert_eq!(evicted.row, placed[1].row, "the oldest live row is evicted");
		assert_eq!(sequence_of(&evicted.bytes), placed[1].row.0);
		assert_ne!(overflow[0].row, refill[0].row);
		assert_eq!(store.metadata[&None].count, 2);
	}

	#[test]
	fn a_batch_larger_than_the_free_room_evicts_only_the_shortfall_oldest_first() {
		// Mutation: evicting before using the free room drops rows the ring still has room for.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 4, None, &[1, 2, 3]);
		delete(&mut store, 4, None, &[placed[1].row]);
		let batch = insert(&mut store, 4, None, &[4, 5, 6]);
		let evicted: Vec<u64> = batch
			.iter()
			.filter_map(|p| p.evicted.as_ref())
			.map(|evicted| sequence_of(&evicted.bytes))
			.collect();
		assert_eq!(evicted, vec![placed[0].row.0], "two rows of room leave a single eviction");
		assert_eq!(store.metadata[&None].count, 4);
		let order: Vec<u64> = oldest_first(&mut store, None).unwrap().into_iter().map(|row| row.0).collect();
		assert_eq!(order, vec![placed[2].row.0, batch[0].row.0, batch[1].row.0, batch[2].row.0]);
	}

	#[test]
	fn expiry_makes_room_so_the_next_insert_does_not_evict() {
		// Mutation: expiring without lowering the count makes the next insert evict a live row.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 2, None, &[10, 20]);
		let mut ring = Ring::new(2);
		ring.expire(&mut store, None, DateTime::from_millis(15), 8).unwrap();
		store.rows.remove(&(None, placed[0].row));
		apply(&mut store, &ring);
		let next = insert(&mut store, 2, None, &[30]);
		assert!(next[0].evicted.is_none());
		assert_eq!(store.metadata[&None].count, 2);
	}

	#[test]
	fn eviction_moves_the_head_to_the_oldest_live_sequence() {
		// Mutation: a head left at 0 makes every eviction walk the whole deleted order history.
		let mut store = MemoryStore::default();
		insert(&mut store, 3, None, &[1, 2, 3]);
		let overflow = insert(&mut store, 3, None, &[4, 5]);
		let oldest_live = oldest_first(&mut store, None).unwrap()[0].0;
		assert_eq!(store.metadata[&None].head, oldest_live, "two evictions advance the head past both victims");
		assert_eq!(overflow.len(), 2);
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
		let (row, _) = oldest(&mut store, &[None]).unwrap().remove(0).unwrap();
		assert_eq!(row, placed[2].row, "the oldest live row is still found from the head");
		let next = insert(&mut store, 3, None, &[5]);
		assert!(next[0].evicted.is_none(), "the delete made room, so nothing is evicted");
		let overflow = insert(&mut store, 3, None, &[6]);
		assert_eq!(sequence_of(&overflow[0].evicted.as_ref().unwrap().bytes), placed[2].row.0);
	}

	#[test]
	fn a_delete_and_an_insert_in_one_session_leave_the_metadata_unwritten() {
		// Mutation: saving metadata that ends as loaded adds a write to every delete-and-insert statement.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 2, None, &[1, 2]);
		let mut ring = Ring::new(2);
		ring.remove(&mut store, &[(None, vec![placed[0].row])]).unwrap();
		store.rows.remove(&(None, placed[0].row));
		let first = store.reserve(1).unwrap();
		let next = ring.place(&mut store, &[(None, vec![RowNumber(first)])]).unwrap().remove(0);
		assert!(next[0].evicted.is_none(), "the delete made room");
		assert!(ring.writes().is_empty(), "the count and the head end as loaded");
		store.rows.insert((None, next[0].row), row(next[0].row.0, Some(3)));
		let order: Vec<u64> = oldest_first(&mut store, None).unwrap().into_iter().map(|row| row.0).collect();
		assert_eq!(order, vec![placed[1].row.0, next[0].row.0], "the deleted row leaves and the new row joins");
	}

	#[test]
	fn with_capacity_one_each_insert_evicts_the_previous_row() {
		// Mutation: a point read off the head misses the live row and fails the insert as corrupt metadata.
		let mut store = MemoryStore::default();
		let first = insert(&mut store, 1, None, &[1]);
		let second = insert(&mut store, 1, None, &[2]);
		let third = insert(&mut store, 1, None, &[3]);
		let evicted = second[0].evicted.as_ref().expect("a full ring of one evicts");
		assert_eq!(evicted.row, first[0].row);
		assert_eq!(sequence_of(&evicted.bytes), first[0].row.0);
		let evicted = third[0].evicted.as_ref().expect("a full ring of one evicts");
		assert_eq!(evicted.row, second[0].row, "each insert evicts the row placed just before it");
		assert_eq!(sequence_of(&evicted.bytes), second[0].row.0);
		assert_eq!(store.metadata[&None].count, 1);
		assert_eq!(store.metadata[&None].head, third[0].row.0, "the head names the only live row");
	}

	#[test]
	fn with_capacity_one_interleaved_partitions_evict_their_own_row() {
		// Mutation: a head left one past the victim lands on another partition's row number and misses the live row.
		let mut store = MemoryStore::default();
		insert(&mut store, 1, Some(Partition(1)), &[1]);
		let other = insert(&mut store, 1, Some(Partition(2)), &[2]);
		let live = insert(&mut store, 1, Some(Partition(1)), &[3]);
		let next = insert(&mut store, 1, Some(Partition(1)), &[4]);
		let evicted = next[0].evicted.as_ref().expect("a full ring of one evicts");
		assert_eq!(evicted.row, live[0].row, "partition 1 evicts its own live row");
		assert_eq!(sequence_of(&evicted.bytes), live[0].row.0);
		assert!(store.rows.contains_key(&(Some(Partition(2)), other[0].row)), "partition 2 keeps its row");
		assert_eq!(store.metadata[&Some(Partition(2))].count, 1);
	}

	#[test]
	fn with_capacity_one_an_insert_after_a_delete_does_not_evict_and_the_next_one_does() {
		// Mutation: a head kept on the deleted row makes the next eviction miss the live row.
		let mut store = MemoryStore::default();
		let first = insert(&mut store, 1, None, &[1]);
		delete(&mut store, 1, None, &[first[0].row]);
		let refill = insert(&mut store, 1, None, &[2]);
		assert!(refill[0].evicted.is_none(), "the delete made room");
		let next = insert(&mut store, 1, None, &[3]);
		let evicted = next[0].evicted.as_ref().expect("a full ring of one evicts");
		assert_eq!(evicted.row, refill[0].row);
		assert_eq!(store.metadata[&None].count, 1);
	}

	#[test]
	fn with_capacity_one_expiry_with_batch_one_expires_the_row_once() {
		// Mutation: reading the head again after it was walked expires the same row twice and never ends the walk.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 1, None, &[10]);
		store.reads.clear();
		let mut ring = Ring::new(1);
		let expired = ring.expire(&mut store, None, DateTime::from_millis(25), 1).unwrap();
		let rows: Vec<RowNumber> = expired.iter().map(|(row, _)| *row).collect();
		assert_eq!(rows, vec![placed[0].row]);
		assert_eq!(store.reads, vec![(None, placed[0].row)], "the single row is read once");
		assert_eq!(ring.metadata(&mut store, None).unwrap().unwrap().count, 0);
	}

	#[test]
	fn with_capacity_one_a_missing_head_row_is_corrupt_metadata() {
		// Mutation: treating the missing row as room lets the count say one while no row is live.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 1, None, &[1]);
		store.rows.remove(&(None, placed[0].row));
		let mut ring = Ring::new(1);
		let first = store.reserve(1).unwrap();
		let err = ring.place(&mut store, &[(None, vec![RowNumber(first)])]).unwrap_err();
		assert_eq!(err.0.code, "INTERNAL_ERROR");
		assert!(err.0.message.contains("must evict 1 rows but holds only 0 live rows"));
	}

	#[test]
	fn placing_rows_with_gaps_evicts_the_oldest_given_row_first() {
		// A head cursor past a given row would skip that live row on the next eviction.
		let mut store = MemoryStore::default();
		insert(&mut store, 2, None, &[1, 2]);
		let mut ring = Ring::new(2);
		let placed = ring.place(&mut store, &[(None, vec![RowNumber(110), RowNumber(113)])]).unwrap().remove(0);
		for placement in &placed {
			store.rows.remove(&(None, placement.evicted.as_ref().expect("a full ring evicts").row));
			store.rows.insert((None, placement.row), row(placement.row.0, Some(3)));
		}
		apply(&mut store, &ring);

		let mut ring = Ring::new(2);
		let next = ring.place(&mut store, &[(None, vec![RowNumber(120)])]).unwrap().remove(0);
		assert_eq!(next[0].evicted.as_ref().map(|evicted| evicted.row), Some(RowNumber(110)));
	}

	fn numbers(store: &mut MemoryStore, count: u64) -> Vec<RowNumber> {
		let first = store.reserve(count).unwrap();
		(first..first + count).map(RowNumber).collect()
	}

	#[test]
	fn a_place_across_partitions_reads_metadata_once() {
		// A read per partition makes an insert over many partitions pay one round trip each.
		let (a, b, fresh) = (Some(Partition(1)), Some(Partition(2)), Some(Partition(3)));
		let mut store = MemoryStore::default();
		let in_a = insert(&mut store, 2, a, &[1, 2]);
		let in_b = insert(&mut store, 2, b, &[1, 2]);
		let rows = numbers(&mut store, 4);
		store.metadata_calls = 0;
		let mut ring = Ring::new(2);
		let placed = ring
			.place(&mut store, &[(a, vec![rows[0]]), (fresh, vec![rows[1], rows[2]]), (b, vec![rows[3]])])
			.unwrap();
		assert_eq!(store.metadata_calls, 1);
		assert_eq!(placed.len(), 3, "one answer per request, in request order");
		assert_eq!(placed[0][0].row, rows[0]);
		assert_eq!(placed[0][0].evicted.as_ref().unwrap().row, in_a[0].row);
		assert_eq!(placed[1].len(), 2);
		assert!(
			placed[1].iter().all(|placement| placement.evicted.is_none()),
			"a fresh partition evicts nothing"
		);
		assert_eq!(placed[2][0].row, rows[3]);
		assert_eq!(placed[2][0].evicted.as_ref().unwrap().row, in_b[0].row);
	}

	#[test]
	fn a_batched_place_matches_one_place_per_partition() {
		// The batch must leave the same rows and metadata behind as separate calls, or row numbers drift.
		let (a, b) = (Some(Partition(1)), Some(Partition(2)));
		let mut batched = MemoryStore::default();
		insert(&mut batched, 3, a, &[1, 2, 3]);
		insert(&mut batched, 3, b, &[1]);
		let mut single = MemoryStore::default();
		insert(&mut single, 3, a, &[1, 2, 3]);
		insert(&mut single, 3, b, &[1]);

		let rows = numbers(&mut batched, 4);
		let mut ring = Ring::new(3);
		let together = ring.place(&mut batched, &[(a, rows[..2].to_vec()), (b, rows[2..].to_vec())]).unwrap();
		apply(&mut batched, &ring);
		let mut apart = Vec::new();
		for partition in [a, b] {
			let rows = numbers(&mut single, 2);
			let mut ring = Ring::new(3);
			apart.push(ring.place(&mut single, &[(partition, rows)]).unwrap().remove(0));
			apply(&mut single, &ring);
		}
		assert_eq!(together, apart);
		assert_eq!(batched.metadata, single.metadata);
	}

	#[test]
	fn a_remove_across_partitions_reads_metadata_once_and_rows_once() {
		// A read per partition makes a delete over many partitions pay one round trip each.
		let (a, b, missing) = (Some(Partition(1)), Some(Partition(2)), Some(Partition(3)));
		let mut store = MemoryStore::default();
		let in_a = insert(&mut store, 3, a, &[1, 2, 3]);
		let in_b = insert(&mut store, 3, b, &[1, 2]);
		store.metadata_calls = 0;
		store.rows_calls = 0;
		let mut ring = Ring::new(3);
		let removed = ring
			.remove(
				&mut store,
				&[
					(a, vec![in_a[1].row, in_b[0].row]),
					(missing, vec![in_a[0].row]),
					(b, vec![in_b[1].row]),
				],
			)
			.unwrap();
		assert_eq!(store.metadata_calls, 1);
		assert_eq!(store.rows_calls, 1);
		assert_eq!(sequence_of(removed[0][0].as_ref().unwrap()), in_a[1].row.0);
		assert!(removed[0][1].is_none(), "a row of another partition is never removed from this one");
		assert_eq!(removed[1], vec![None], "a partition with no metadata holds nothing");
		assert_eq!(sequence_of(removed[2][0].as_ref().unwrap()), in_b[1].row.0);
		for (partition, row) in [(a, in_a[1].row), (b, in_b[1].row)] {
			store.rows.remove(&(partition, row));
		}
		apply(&mut store, &ring);
		assert_eq!(store.metadata[&a].count, 2);
		assert_eq!(store.metadata[&b].count, 1);
	}

	#[test]
	fn a_place_naming_one_partition_twice_is_refused() {
		// The second request would evict the same oldest row before the first request's row is written.
		let mut store = MemoryStore::default();
		insert(&mut store, 2, None, &[1, 2]);
		let rows = numbers(&mut store, 2);
		let mut ring = Ring::new(2);
		let result =
			catch_unwind(AssertUnwindSafe(|| ring.place(&mut store, &[(None, vec![rows[0]]), (None, vec![rows[1]])])));
		assert!(result.is_err());
	}

	#[test]
	fn a_remove_naming_one_partition_twice_is_refused() {
		// A row named in both requests would be read twice and drop the count by two.
		let mut store = MemoryStore::default();
		let placed = insert(&mut store, 2, None, &[1, 2]);
		let mut ring = Ring::new(2);
		let rows = vec![placed[0].row];
		let result = catch_unwind(AssertUnwindSafe(|| {
			ring.remove(&mut store, &[(None, rows.clone()), (None, rows.clone())])
		}));
		assert!(result.is_err());
	}

	#[test]
	fn a_preload_reads_every_partition_in_one_call_and_expiry_reads_no_more_metadata() {
		// An expiry pass over many partitions must not pay one metadata read each.
		let (a, b) = (Some(Partition(1)), Some(Partition(2)));
		let mut store = MemoryStore::default();
		insert(&mut store, 2, a, &[10, 20]);
		insert(&mut store, 2, b, &[10, 20]);
		store.metadata_calls = 0;
		let mut ring = Ring::new(2);
		ring.preload(&mut store, &[a, b, a]).unwrap();
		assert_eq!(store.metadata_calls, 1);
		for partition in [a, b] {
			assert_eq!(ring.expire(&mut store, partition, DateTime::from_millis(15), 8).unwrap().len(), 1);
		}
		ring.preload(&mut store, &[a, b]).unwrap();
		assert_eq!(store.metadata_calls, 1, "tracked partitions are never read again");
	}
}
