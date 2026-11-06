// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	delta::SetPre,
	interface::catalog::{series::SeriesPartitionMetadata, storage::StorageId},
	internal_err,
	key::{
		any::TaggedKey,
		series::{PartitionedSeriesRowKey, SeriesRowKey},
	},
};
use reifydb_value::{Result, value::partition::Partition};

use crate::{
	txn::{FlowStorageTransaction, StorageTransaction},
	writer::Written,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeriesRowAddress {
	pub storage: StorageId,
	pub partition: Option<Partition>,
	pub variant_tag: Option<u8>,
	pub key: u64,
	pub sequence: u64,
}

impl SeriesRowAddress {
	pub fn encoded(self) -> EncodedKey {
		match self.partition {
			Some(partition) => PartitionedSeriesRowKey::encoded(
				self.storage,
				partition,
				self.variant_tag,
				self.key,
				self.sequence,
			),
			None => SeriesRowKey {
				storage: self.storage,
				variant_tag: self.variant_tag,
				key: self.key,
				sequence: self.sequence,
			}
			.encode(),
		}
	}

	pub fn tagged(self) -> TaggedKey {
		match self.partition {
			Some(partition) => PartitionedSeriesRowKey::new(
				self.storage,
				partition,
				self.variant_tag,
				self.key,
				self.sequence,
			)
			.into(),
			None => SeriesRowKey {
				storage: self.storage,
				variant_tag: self.variant_tag,
				key: self.key,
				sequence: self.sequence,
			}
			.into(),
		}
	}
}

pub struct SeriesWriter<'a, T: StorageTransaction> {
	txn: &'a mut T,
}

impl<'a, T: StorageTransaction> SeriesWriter<'a, T> {
	pub fn new(txn: &'a mut T) -> Self {
		Self {
			txn,
		}
	}

	pub fn append(&mut self, rows: Vec<(TaggedKey, EncodedBytes)>) -> Result<Written> {
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

pub struct FlowSeriesWriter<'a, T: FlowStorageTransaction> {
	txn: &'a mut T,
}

impl<'a, T: FlowStorageTransaction> FlowSeriesWriter<'a, T> {
	pub fn new(txn: &'a mut T) -> Self {
		Self {
			txn,
		}
	}

	pub fn append(&mut self, keys: Vec<EncodedKey>, rows: Vec<EncodedBytes>) -> Result<()> {
		self.txn.set(keys, rows)
	}

	pub fn update(
		&mut self,
		pre_keys: Vec<EncodedKey>,
		post_keys: Vec<EncodedKey>,
		rows: Vec<EncodedBytes>,
	) -> Result<()> {
		if pre_keys.len() != post_keys.len() || post_keys.len() != rows.len() {
			return internal_err!(
				"series update got {} pre keys, {} post keys and {} rows",
				pre_keys.len(),
				post_keys.len(),
				rows.len()
			);
		}
		for ((pre_key, post_key), row) in pre_keys.into_iter().zip(post_keys).zip(rows) {
			if pre_key != post_key {
				self.txn.remove(vec![pre_key])?;
			}
			self.txn.set(vec![post_key], vec![row])?;
		}
		Ok(())
	}

	pub fn delete(&mut self, keys: Vec<EncodedKey>) -> Result<()> {
		self.txn.remove(keys)
	}
}

pub struct SeriesDeleteTally {
	pub count: u64,
	pub min_key: u64,
	pub max_key: u64,
}

impl SeriesDeleteTally {
	pub fn new() -> Self {
		Self {
			count: 0,
			min_key: u64::MAX,
			max_key: 0,
		}
	}

	pub fn record(&mut self, key: u64) {
		self.count += 1;
		self.min_key = self.min_key.min(key);
		self.max_key = self.max_key.max(key);
	}
}

impl Default for SeriesDeleteTally {
	fn default() -> Self {
		Self::new()
	}
}

pub fn apply_series_metadata_after_delete(metadata: &mut SeriesPartitionMetadata, tally: &SeriesDeleteTally) {
	metadata.row_count = metadata.row_count.saturating_sub(tally.count);
	if metadata.row_count == 0 {
		metadata.oldest_key = 0;
		metadata.newest_key = 0;
	}
	if tally.count > 0 {
		metadata.dirty_from_key = metadata.dirty_from_key.min(tally.min_key);
		metadata.dirty_to_key = metadata.dirty_to_key.max(tally.max_key.saturating_add(1));
	}
}

pub fn update_series_metadata_for_insert(metadata: &mut SeriesPartitionMetadata, key_value: u64) {
	if metadata.row_count == 0 {
		metadata.oldest_key = key_value;
		metadata.newest_key = key_value;
	} else {
		if key_value < metadata.oldest_key {
			metadata.oldest_key = key_value;
		}
		if key_value > metadata.newest_key {
			metadata.newest_key = key_value;
		}
	}
	metadata.dirty_from_key = metadata.dirty_from_key.min(key_value);
	metadata.dirty_to_key = metadata.dirty_to_key.max(key_value.saturating_add(1));
	metadata.row_count += 1;
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::id::SeriesId;
	use reifydb_value::util::cowvec::CowVec;

	use super::*;
	use crate::txn::{Batch, FlowBatch, FlowGet, Get};

	#[derive(Debug, PartialEq, Eq)]
	enum Call {
		Set(Vec<EncodedKey>, Vec<EncodedBytes>),
		Remove(Vec<EncodedKey>),
		SetWithPre(Vec<(TaggedKey, EncodedBytes, SetPre)>),
		RemoveWithPre(Vec<(TaggedKey, EncodedBytes)>),
		RemoveTagged(Vec<TaggedKey>),
	}

	#[derive(Default)]
	struct Recording {
		calls: Vec<Call>,
		reads: usize,
	}

	impl Get for Recording {
		fn get_tagged(&mut self, keys: &[TaggedKey]) -> Result<Vec<Option<EncodedBytes>>> {
			self.reads += 1;
			Ok(vec![None; keys.len()])
		}
	}

	impl Batch for Recording {
		fn set_with_pre(&mut self, entries: Vec<(TaggedKey, EncodedBytes, SetPre)>) -> Result<()> {
			self.calls.push(Call::SetWithPre(entries));
			Ok(())
		}

		fn remove_with_pre(&mut self, entries: Vec<(TaggedKey, EncodedBytes)>) -> Result<()> {
			self.calls.push(Call::RemoveWithPre(entries));
			Ok(())
		}

		fn remove_tagged(&mut self, keys: Vec<TaggedKey>) -> Result<()> {
			self.calls.push(Call::RemoveTagged(keys));
			Ok(())
		}
	}

	#[derive(Default)]
	struct FlowRecording {
		calls: Vec<Call>,
		reads: usize,
	}

	impl FlowGet for FlowRecording {
		fn get(&mut self, keys: &[EncodedKey]) -> Result<Vec<Option<EncodedBytes>>> {
			self.reads += 1;
			Ok(vec![None; keys.len()])
		}
	}

	impl FlowBatch for FlowRecording {
		fn set(&mut self, keys: Vec<EncodedKey>, rows: Vec<EncodedBytes>) -> Result<()> {
			self.calls.push(Call::Set(keys, rows));
			Ok(())
		}

		fn remove(&mut self, keys: Vec<EncodedKey>) -> Result<()> {
			self.calls.push(Call::Remove(keys));
			Ok(())
		}
	}

	fn address(key: u64) -> SeriesRowAddress {
		SeriesRowAddress {
			storage: StorageId::series(SeriesId(9)),
			partition: None,
			variant_tag: None,
			key,
			sequence: key,
		}
	}

	fn tagged(key: u64) -> TaggedKey {
		address(key).tagged()
	}

	fn encoded(key: u64) -> EncodedKey {
		address(key).encoded()
	}

	fn bytes(value: u8) -> EncodedBytes {
		EncodedBytes(CowVec::new(vec![value]))
	}

	fn metadata() -> SeriesPartitionMetadata {
		SeriesPartitionMetadata {
			row_count: 3,
			oldest_key: 10,
			newest_key: 30,
			sequence_counter: 3,
			dirty_from_key: 15,
			dirty_to_key: 16,
			..SeriesPartitionMetadata::new()
		}
	}

	#[test]
	fn a_delete_with_pre_hands_every_pre_image_to_the_transaction_and_returns_them() {
		// Without the pre the transaction reads its own view of the row, which can differ from committed.
		let mut txn = Recording::default();
		let rows = vec![(tagged(1), bytes(1)), (tagged(2), bytes(2))];
		let written = SeriesWriter::new(&mut txn).delete(rows.clone()).unwrap();
		assert_eq!(txn.calls, vec![Call::RemoveWithPre(rows)]);
		assert_eq!(written.replaced, vec![bytes(1), bytes(2)], "post delete hooks read these pre-images");
		assert_eq!(txn.reads, 0, "the caller owns the committed read, the writer must not read");
	}

	#[test]
	fn a_tagged_append_and_update_hand_rows_over_with_an_unknown_pre_in_one_call() {
		// Series DML has never told the store a pre on a set; absent or present would change the commit log
		// diff.
		let mut txn = Recording::default();
		let mut writer = SeriesWriter::new(&mut txn);
		let appended = writer.append(vec![(tagged(1), bytes(1)), (tagged(2), bytes(2))]).unwrap();
		let updated = writer.update(vec![(tagged(1), bytes(3))]).unwrap();
		assert_eq!(appended.written, vec![bytes(1), bytes(2)], "hooks and the flow diff read the written rows");
		assert_eq!(updated.written, vec![bytes(3)]);
		assert!(appended.replaced.is_empty() && updated.replaced.is_empty());
		assert_eq!(
			txn.calls,
			vec![
				Call::SetWithPre(vec![
					(tagged(1), bytes(1), SetPre::Unknown),
					(tagged(2), bytes(2), SetPre::Unknown),
				]),
				Call::SetWithPre(vec![(tagged(1), bytes(3), SetPre::Unknown)]),
			]
		);
		assert_eq!(txn.reads, 0);
	}

	#[test]
	fn the_flow_paths_never_read_and_never_touch_the_pre_image_seam() {
		// A read here adds store load to every view write that has none today.
		let mut txn = FlowRecording::default();
		let mut writer = FlowSeriesWriter::new(&mut txn);
		writer.append(vec![encoded(1)], vec![bytes(1)]).unwrap();
		writer.update(vec![encoded(1)], vec![encoded(1)], vec![bytes(2)]).unwrap();
		writer.delete(vec![encoded(1)]).unwrap();
		assert_eq!(txn.reads, 0);
		assert_eq!(
			txn.calls,
			vec![
				Call::Set(vec![encoded(1)], vec![bytes(1)]),
				Call::Set(vec![encoded(1)], vec![bytes(2)]),
				Call::Remove(vec![encoded(1)]),
			]
		);
	}

	#[test]
	fn a_flow_update_applies_each_row_in_order_so_a_later_remove_beats_an_earlier_set() {
		// Series views apply row by row, unlike table views; collapsing the writes would keep a row they drop.
		let mut txn = FlowRecording::default();
		FlowSeriesWriter::new(&mut txn)
			.update(vec![encoded(1), encoded(2)], vec![encoded(2), encoded(3)], vec![bytes(12), bytes(23)])
			.unwrap();
		assert_eq!(
			txn.calls,
			vec![
				Call::Remove(vec![encoded(1)]),
				Call::Set(vec![encoded(2)], vec![bytes(12)]),
				Call::Remove(vec![encoded(2)]),
				Call::Set(vec![encoded(3)], vec![bytes(23)]),
			]
		);
	}

	#[test]
	fn a_flow_update_with_mismatched_lengths_fails_and_writes_nothing() {
		// Zipping uneven inputs would silently drop rows from the view.
		for (pre_keys, post_keys, rows) in [
			(vec![encoded(1)], vec![], vec![]),
			(vec![], vec![encoded(1)], vec![]),
			(vec![encoded(1)], vec![encoded(1)], vec![]),
		] {
			let mut txn = FlowRecording::default();
			let error = FlowSeriesWriter::new(&mut txn).update(pre_keys, post_keys, rows).unwrap_err();
			assert_eq!(error.0.code, "INTERNAL_ERROR");
			assert!(txn.calls.is_empty());
		}
	}

	#[test]
	fn the_address_builds_the_same_key_tagged_and_encoded_for_each_form() {
		// DML reads through the tagged key and views through the encoded one; a mismatch splits the row.
		for partition in [None, Some(Partition(5))] {
			for variant_tag in [None, Some(2)] {
				let address = SeriesRowAddress {
					partition,
					variant_tag,
					..address(3)
				};
				assert_eq!(address.tagged().encode().as_slice(), address.encoded().as_slice());
			}
		}
	}

	#[test]
	fn a_partition_changes_the_key_kind_so_a_partitioned_row_never_lands_on_the_plain_key() {
		// The two forms are separate key families, so choosing wrong writes the row where its form never reads.
		let partitioned = SeriesRowAddress {
			partition: Some(Partition(5)),
			..address(3)
		};
		assert!(matches!(partitioned.tagged(), TaggedKey::PartitionedSeriesRow(_)));
		assert!(matches!(address(3).tagged(), TaggedKey::SeriesRow(_)));
	}

	#[test]
	fn an_insert_counts_the_row_widens_the_key_range_and_the_dirty_range() {
		// A miscount here skews the row count and key range kept in the partition metadata row.
		let mut older = metadata();
		update_series_metadata_for_insert(&mut older, 5);
		assert_eq!((older.row_count, older.oldest_key, older.newest_key), (4, 5, 30));
		assert_eq!((older.dirty_from_key, older.dirty_to_key), (5, 16));

		let mut newer = metadata();
		update_series_metadata_for_insert(&mut newer, 40);
		assert_eq!((newer.row_count, newer.oldest_key, newer.newest_key), (4, 10, 40));
		assert_eq!((newer.dirty_from_key, newer.dirty_to_key), (15, 41));

		let mut inside = metadata();
		update_series_metadata_for_insert(&mut inside, 20);
		assert_eq!((inside.row_count, inside.oldest_key, inside.newest_key), (4, 10, 30));
		assert_eq!(inside.sequence_counter, 3, "the caller owns the sequence counter");
	}

	#[test]
	fn the_first_insert_into_an_empty_partition_sets_both_ends_of_the_key_range() {
		// Keeping the zero oldest key would claim rows exist from key 0 upward.
		let mut empty = SeriesPartitionMetadata {
			dirty_from_key: u64::MAX,
			..SeriesPartitionMetadata::new()
		};
		update_series_metadata_for_insert(&mut empty, 7);
		assert_eq!((empty.row_count, empty.oldest_key, empty.newest_key), (1, 7, 7));
		assert_eq!((empty.dirty_from_key, empty.dirty_to_key), (7, 8));
	}

	#[test]
	fn the_top_key_saturates_its_dirty_end_instead_of_wrapping_to_zero() {
		// A wrapped end would mark an empty dirty range for a row that was just written or removed.
		let mut inserted = metadata();
		update_series_metadata_for_insert(&mut inserted, u64::MAX);
		assert_eq!(inserted.dirty_to_key, u64::MAX);

		let mut tally = SeriesDeleteTally::new();
		tally.record(u64::MAX);
		let mut deleted = metadata();
		apply_series_metadata_after_delete(&mut deleted, &tally);
		assert_eq!(deleted.dirty_to_key, u64::MAX);
	}

	#[test]
	fn a_delete_tally_records_count_and_key_bounds_and_shrinks_the_partition() {
		// The key range must stay as it is while rows remain; only the dirty range and the count move.
		let mut tally = SeriesDeleteTally::default();
		tally.record(20);
		tally.record(12);
		tally.record(15);
		assert_eq!((tally.count, tally.min_key, tally.max_key), (3, 12, 20));

		let mut shrunk = SeriesPartitionMetadata {
			row_count: 5,
			..metadata()
		};
		apply_series_metadata_after_delete(&mut shrunk, &tally);
		assert_eq!((shrunk.row_count, shrunk.oldest_key, shrunk.newest_key), (2, 10, 30));
		assert_eq!((shrunk.dirty_from_key, shrunk.dirty_to_key), (12, 21));
	}

	#[test]
	fn deleting_the_last_row_resets_the_key_range_and_never_underflows() {
		// An empty partition must report a zero key range, and a tally above the count must not underflow.
		let mut tally = SeriesDeleteTally::new();
		for key in [10, 20, 30, 40] {
			tally.record(key);
		}
		let mut emptied = metadata();
		apply_series_metadata_after_delete(&mut emptied, &tally);
		assert_eq!((emptied.row_count, emptied.oldest_key, emptied.newest_key), (0, 0, 0));
	}

	#[test]
	fn an_empty_tally_leaves_the_dirty_range_alone() {
		// Its max key is zero, so applying it to a clean partition would mark key 0 as dirty.
		let mut untouched = SeriesPartitionMetadata {
			dirty_from_key: u64::MAX,
			dirty_to_key: 0,
			..metadata()
		};
		apply_series_metadata_after_delete(&mut untouched, &SeriesDeleteTally::new());
		assert_eq!((untouched.row_count, untouched.dirty_from_key, untouched.dirty_to_key), (3, u64::MAX, 0));
	}
}
