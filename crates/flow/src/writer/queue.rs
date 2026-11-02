// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{delta::SetPre, interface::catalog::storage::StorageId, key::row::RowKey};
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::txn::StorageTxn;

pub struct QueueWriter<'a, T: StorageTxn> {
	txn: &'a mut T,
	storage: StorageId,
}

impl<'a, T: StorageTxn> QueueWriter<'a, T> {
	pub fn new(txn: &'a mut T, storage: StorageId) -> Self {
		Self {
			txn,
			storage,
		}
	}

	pub fn insert(&mut self, rows: impl IntoIterator<Item = (RowNumber, EncodedBytes)>) -> Result<()> {
		let storage = self.storage;
		self.txn.set_with_pre(
			rows.into_iter()
				.map(|(row, bytes)| (RowKey::new(storage, row).into(), bytes, SetPre::Absent))
				.collect(),
		)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{interface::catalog::id::QueueId, key::any::TaggedKey};
	use reifydb_value::util::cowvec::CowVec;

	use super::*;
	use crate::txn::{Batch, Get};

	#[derive(Default)]
	struct Recording {
		with_pre: Vec<Vec<(TaggedKey, EncodedBytes, SetPre)>>,
		plain: usize,
		reads: usize,
	}

	impl Get for Recording {
		fn get(&mut self, keys: &[EncodedKey]) -> Result<Vec<Option<EncodedBytes>>> {
			self.reads += 1;
			Ok(vec![None; keys.len()])
		}
	}

	impl Batch for Recording {
		fn set(&mut self, _keys: Vec<EncodedKey>, _rows: Vec<EncodedBytes>) -> Result<()> {
			self.plain += 1;
			Ok(())
		}

		fn remove(&mut self, _keys: Vec<EncodedKey>) -> Result<()> {
			self.plain += 1;
			Ok(())
		}

		fn set_with_pre(&mut self, entries: Vec<(TaggedKey, EncodedBytes, SetPre)>) -> Result<()> {
			self.with_pre.push(entries);
			Ok(())
		}
	}

	fn bytes(value: u8) -> EncodedBytes {
		EncodedBytes(CowVec::new(vec![value]))
	}

	fn storage() -> StorageId {
		StorageId::from(QueueId(3))
	}

	fn key(row: u64) -> TaggedKey {
		RowKey {
			storage: storage(),
			row: RowNumber(row),
		}
		.into()
	}

	#[test]
	fn every_row_goes_under_its_own_plain_row_key_with_an_absent_pre_in_one_call() {
		// An unknown pre makes CDC treat a new row as possibly replacing one; a wrong key loses the row.
		let mut txn = Recording::default();
		QueueWriter::new(&mut txn, storage())
			.insert([(RowNumber(1), bytes(1)), (RowNumber(2), bytes(2))])
			.unwrap();
		assert_eq!(
			txn.with_pre,
			vec![vec![(key(1), bytes(1), SetPre::Absent), (key(2), bytes(2), SetPre::Absent)]]
		);
		assert_eq!(txn.plain, 0, "the plain batch carries no pre-image");
		assert_eq!(txn.reads, 0, "a queue insert never reads the row it writes");
	}

	#[test]
	fn the_row_key_belongs_to_the_writers_queue() {
		// A key under another storage would land the row in a different queue.
		let mut txn = Recording::default();
		QueueWriter::new(&mut txn, StorageId::from(QueueId(4))).insert([(RowNumber(1), bytes(1))]).unwrap();
		let other: TaggedKey = RowKey::new(QueueId(4), RowNumber(1)).into();
		assert_eq!(txn.with_pre[0][0].0, other);
		assert_ne!(txn.with_pre[0][0].0, key(1));
	}
}
