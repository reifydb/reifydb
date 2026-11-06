// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	delta::SetPre,
	interface::catalog::storage::StorageId,
	key::{
		any::TaggedKey,
		row::{PartitionedRowKey, RowKey},
		sort_run::SortRun,
	},
};
use reifydb_value::{
	Result,
	value::{partition::Partition, row_number::RowNumber},
};

use crate::{
	operator::sink::view::{partitioned_key, sorted_view_key},
	txn::{FlowStorageTransaction, StorageTransaction},
	writer::Written,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableRowAddress {
	pub storage: StorageId,
	pub partition: Option<Partition>,
	pub row: RowNumber,
}

impl TableRowAddress {
	pub fn encoded(self, run: Option<SortRun>) -> EncodedKey {
		match self.partition {
			Some(partition) => partitioned_key(self.storage, run, partition, self.row),
			None => sorted_view_key(self.storage, run, self.row),
		}
	}

	pub fn tagged(self) -> TaggedKey {
		match self.partition {
			Some(partition) => PartitionedRowKey::new(self.storage, partition, self.row).into(),
			None => RowKey::new(self.storage, self.row).into(),
		}
	}
}

pub struct TableWriter<'a, T: StorageTransaction> {
	txn: &'a mut T,
}

impl<'a, T: StorageTransaction> TableWriter<'a, T> {
	pub fn new(txn: &'a mut T) -> Self {
		Self {
			txn,
		}
	}

	pub fn insert(
		&mut self,
		rows: Vec<(TaggedKey, EncodedBytes)>,
		index: Vec<(TaggedKey, EncodedBytes)>,
	) -> Result<Written> {
		let mut written = Vec::with_capacity(rows.len());
		let mut entries = Vec::with_capacity(rows.len() + index.len());
		for (key, row) in rows {
			written.push(row.clone());
			entries.push((key, row, SetPre::Absent));
		}
		entries.extend(index.into_iter().map(|(key, row)| (key, row, SetPre::Unknown)));
		self.txn.set_with_pre(entries)?;
		Ok(Written {
			written,
			replaced: Vec::new(),
		})
	}

	pub fn update(&mut self, rows: Vec<(TaggedKey, EncodedBytes, SetPre)>) -> Result<Written> {
		let written = rows.iter().map(|(_, row, _)| row.clone()).collect();
		self.txn.set_with_pre(rows)?;
		Ok(Written {
			written,
			replaced: Vec::new(),
		})
	}

	pub fn delete(&mut self, rows: Vec<(TaggedKey, EncodedBytes)>) -> Result<Written> {
		let replaced = rows.iter().map(|(_, pre)| pre.clone()).collect();
		self.txn.remove_with_pre(rows)?;
		Ok(Written {
			written: Vec::new(),
			replaced,
		})
	}
}

pub struct FlowTableWriter<'a, T: FlowStorageTransaction> {
	txn: &'a mut T,
}

impl<'a, T: FlowStorageTransaction> FlowTableWriter<'a, T> {
	pub fn new(txn: &'a mut T) -> Self {
		Self {
			txn,
		}
	}

	pub fn insert(&mut self, keys: Vec<EncodedKey>, rows: Vec<EncodedBytes>) -> Result<()> {
		self.txn.set(keys, rows)
	}

	pub fn update(&mut self, rows: Vec<(EncodedKey, EncodedKey, EncodedBytes)>) -> Result<()> {
		let mut writes = KeyWrites::with_capacity(rows.len());
		for (pre_key, post_key, row) in rows {
			if pre_key.as_slice() != post_key.as_slice() {
				writes.put(pre_key, None);
			}
			writes.put(post_key, Some(row));
		}
		let (removes, set_keys, set_rows) = writes.split();
		if !removes.is_empty() {
			self.txn.remove(removes)?;
		}
		self.txn.set(set_keys, set_rows)
	}

	pub fn delete(&mut self, keys: Vec<EncodedKey>) -> Result<()> {
		self.txn.remove(keys)
	}
}

struct KeyWrites {
	writes: Vec<(EncodedKey, Option<EncodedBytes>)>,
	slots: HashMap<EncodedKey, usize>,
}

impl KeyWrites {
	fn with_capacity(capacity: usize) -> Self {
		Self {
			writes: Vec::with_capacity(capacity * 2),
			slots: HashMap::with_capacity(capacity * 2),
		}
	}

	fn put(&mut self, key: EncodedKey, row: Option<EncodedBytes>) {
		match self.slots.get(&key) {
			Some(&slot) => self.writes[slot].1 = row,
			None => {
				self.slots.insert(key.clone(), self.writes.len());
				self.writes.push((key, row));
			}
		}
	}

	fn split(self) -> (Vec<EncodedKey>, Vec<EncodedKey>, Vec<EncodedBytes>) {
		let mut removes = Vec::new();
		let mut set_keys = Vec::with_capacity(self.writes.len());
		let mut set_rows = Vec::with_capacity(self.writes.len());
		for (key, row) in self.writes {
			match row {
				Some(row) => {
					set_keys.push(key);
					set_rows.push(row);
				}
				None => removes.push(key),
			}
		}
		(removes, set_keys, set_rows)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::id::TableId;
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

	#[derive(Default)]
	struct Defaulted(Vec<Call>);

	impl FlowBatch for Defaulted {
		fn set(&mut self, keys: Vec<EncodedKey>, rows: Vec<EncodedBytes>) -> Result<()> {
			self.0.push(Call::Set(keys, rows));
			Ok(())
		}

		fn remove(&mut self, keys: Vec<EncodedKey>) -> Result<()> {
			self.0.push(Call::Remove(keys));
			Ok(())
		}
	}

	fn storage() -> StorageId {
		StorageId::from(TableId(9))
	}

	fn tagged(row: u64) -> TaggedKey {
		TableRowAddress {
			storage: storage(),
			partition: None,
			row: RowNumber(row),
		}
		.tagged()
	}

	fn encoded(row: u64) -> EncodedKey {
		tagged(row).encode()
	}

	fn bytes(value: u8) -> EncodedBytes {
		EncodedBytes(CowVec::new(vec![value]))
	}

	#[test]
	fn an_update_with_pre_hands_the_exact_pre_image_of_every_row_to_the_transaction() {
		// commit log reads the update pre-image from this seam; a dropped or swapped pre emits a wrong diff.
		let mut txn = Recording::default();
		let rows = vec![
			(tagged(1), bytes(11), SetPre::Present(bytes(1))),
			(tagged(2), bytes(12), SetPre::Unknown),
		];
		let written = TableWriter::new(&mut txn).update(rows.clone()).unwrap();
		assert_eq!(txn.calls, vec![Call::SetWithPre(rows)]);
		assert_eq!(written.written, vec![bytes(11), bytes(12)]);
		assert!(written.replaced.is_empty(), "no update caller reads it, so it must not cost a clone per row");
		assert_eq!(txn.reads, 0, "the caller owns the committed read, the writer must not read");
	}

	#[test]
	fn a_delete_with_pre_hands_every_pre_image_to_the_transaction_and_returns_them() {
		// Without the pre the transaction reads its own view of the row, which can differ from committed.
		let mut txn = Recording::default();
		let rows = vec![(tagged(1), bytes(1)), (tagged(2), bytes(2))];
		let written = TableWriter::new(&mut txn).delete(rows.clone()).unwrap();
		assert_eq!(txn.calls, vec![Call::RemoveWithPre(rows)]);
		assert_eq!(written.replaced, vec![bytes(1), bytes(2)]);
		assert!(written.written.is_empty());
		assert_eq!(txn.reads, 0);
	}

	#[test]
	fn an_insert_with_pre_marks_rows_absent_and_index_entries_unknown_in_one_call() {
		// Rows must be marked absent and index entries unknown, in the one set call DML has always made.
		let mut txn = Recording::default();
		let written = TableWriter::new(&mut txn)
			.insert(vec![(tagged(1), bytes(1))], vec![(tagged(7), bytes(7))])
			.unwrap();
		assert_eq!(
			txn.calls,
			vec![Call::SetWithPre(vec![
				(tagged(1), bytes(1), SetPre::Absent),
				(tagged(7), bytes(7), SetPre::Unknown),
			])]
		);
		assert_eq!(written.written, vec![bytes(1)], "index entries are not rows the caller hooks on");
		assert!(written.replaced.is_empty());
	}

	#[test]
	fn the_flow_paths_never_read_and_never_touch_the_pre_image_seam() {
		// A read here would add store load to every view write that has none today.
		let mut txn = FlowRecording::default();
		let mut writer = FlowTableWriter::new(&mut txn);
		writer.insert(vec![encoded(1)], vec![bytes(1)]).unwrap();
		writer.update(vec![(encoded(1), encoded(1), bytes(2))]).unwrap();
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
	fn a_flow_update_removes_a_moved_key_first_and_lets_a_later_post_key_win_over_an_earlier_remove() {
		// Row 2 moves onto row 1's old key, so removing that key after the set would lose row 2.
		let mut txn = FlowRecording::default();
		FlowTableWriter::new(&mut txn)
			.update(vec![(encoded(1), encoded(2), bytes(21)), (encoded(3), encoded(1), bytes(13))])
			.unwrap();
		assert_eq!(
			txn.calls,
			vec![
				Call::Remove(vec![encoded(3)]),
				Call::Set(vec![encoded(1), encoded(2)], vec![bytes(13), bytes(21)]),
			]
		);
	}

	#[test]
	fn a_transaction_without_pre_images_routes_with_pre_writes_to_its_flow_batch() {
		// A view transaction has no pre-image store, so it must still land the write under the same key.
		let mut txn = Defaulted::default();
		txn.set_with_pre(vec![(tagged(1), bytes(1), SetPre::Present(bytes(9)))]).unwrap();
		txn.remove_with_pre(vec![(tagged(2), bytes(2))]).unwrap();
		assert_eq!(txn.0, vec![Call::Set(vec![encoded(1)], vec![bytes(1)]), Call::Remove(vec![encoded(2)])]);
	}

	#[test]
	fn the_address_builds_the_same_key_tagged_and_encoded_for_each_partition_form() {
		// DML reads through the tagged key and views through the encoded one; a mismatch splits the row.
		for partition in [None, Some(Partition(5))] {
			let address = TableRowAddress {
				storage: storage(),
				partition,
				row: RowNumber(3),
			};
			assert_eq!(address.tagged().encode().as_slice(), address.encoded(None).as_slice());
		}
	}
}
