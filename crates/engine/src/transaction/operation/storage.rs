// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	delta::SetPre,
	interface::store::MultiVersionRow,
	key::{any::TaggedKey, bound::TaggedKeyBoundRange},
};
use reifydb_flow::txn::{Batch, Get, Range};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::Result;

pub(crate) struct DmlTransaction<'a> {
	pub(crate) tx: Transaction<'a>,
}

impl<'a> DmlTransaction<'a> {
	pub(crate) fn over(tx: Transaction<'a>) -> Self {
		Self {
			tx,
		}
	}
}

impl Get for DmlTransaction<'_> {
	fn get_tagged(&mut self, keys: &[TaggedKey]) -> Result<Vec<Option<EncodedBytes>>> {
		Ok(self.tx.get(keys)?.into_iter().map(|row| row.map(|row| row.bytes)).collect())
	}
}

impl Batch for DmlTransaction<'_> {
	fn set_with_pre(&mut self, entries: Vec<(TaggedKey, EncodedBytes, SetPre)>) -> Result<()> {
		self.tx.set_with_pre(entries)
	}

	fn remove_with_pre(&mut self, entries: Vec<(TaggedKey, EncodedBytes)>) -> Result<()> {
		self.tx.remove(entries.into_iter().map(|(key, pre)| (key, Some(pre))).collect())
	}

	fn remove_tagged(&mut self, keys: Vec<TaggedKey>) -> Result<()> {
		self.tx.remove(keys.into_iter().map(|key| (key, None)).collect())
	}
}

impl Range for DmlTransaction<'_> {
	fn range(
		&mut self,
		range: TaggedKeyBoundRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_>> {
		self.tx.range(range, RangeScope::All, batch_size)
	}

	fn range_rev(
		&mut self,
		range: TaggedKeyBoundRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_>> {
		self.tx.range_rev(range, RangeScope::All, batch_size)
	}

	fn range_rev_untracked(
		&mut self,
		range: TaggedKeyBoundRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_>> {
		self.tx.range_rev_untracked(range, RangeScope::All, batch_size)
	}

	fn mark_range(&mut self, range: TaggedKeyBoundRange) -> Result<()> {
		self.tx.mark_range(range)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		delta::{Delta, RemoveAnnounce},
		interface::catalog::id::TableId,
		key::row::{RowKey, RowKeyRange},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::admin::AdminTransaction;
	use reifydb_value::{util::cowvec::CowVec, value::row_number::RowNumber};

	use super::*;

	fn key(row: u64) -> TaggedKey {
		RowKey::new(TableId(1), RowNumber(row)).into()
	}

	fn bytes(value: u8) -> EncodedBytes {
		EncodedBytes(CowVec::new(vec![value]))
	}

	fn pending(txn: &AdminTransaction, key: &TaggedKey) -> Delta {
		txn.cmd.as_ref().unwrap().pending_writes().get(key).unwrap().delta.clone()
	}

	#[test]
	fn a_set_with_pre_reaches_the_transaction_with_its_pre_image() {
		// commit log builds its update diff from this pre, so the adapter must never fall back to a flow set.
		let mut txn = create_test_admin_transaction();
		DmlTransaction::over(Transaction::Admin(&mut txn))
			.set_with_pre(vec![(key(1), bytes(2), SetPre::Present(bytes(1)))])
			.unwrap();
		assert!(matches!(
			pending(&txn, &key(1)),
			Delta::Set { pre: SetPre::Present(pre), .. } if pre == bytes(1)
		));
	}

	#[test]
	fn a_remove_with_pre_announces_the_callers_pre_image_instead_of_reading_its_own() {
		// No row exists here, so a dropped pre would turn the remove silent and lose the commit log remove.
		let mut txn = create_test_admin_transaction();
		DmlTransaction::over(Transaction::Admin(&mut txn)).remove_with_pre(vec![(key(1), bytes(1))]).unwrap();
		assert!(matches!(
			pending(&txn, &key(1)),
			Delta::Remove { announce: RemoveAnnounce::Announced { pre }, .. } if pre == bytes(1)
		));
	}

	#[test]
	fn a_get_sees_a_set_made_in_the_same_transaction_and_none_for_a_missing_key() {
		// A get that skipped the transaction's own writes would hand a caller a stale row.
		let mut txn = create_test_admin_transaction();
		let mut dml = DmlTransaction::over(Transaction::Admin(&mut txn));
		dml.set_with_pre(vec![(key(1), bytes(1), SetPre::Absent)]).unwrap();
		assert_eq!(dml.get_tagged(&[key(1), key(2)]).unwrap(), vec![Some(bytes(1)), None]);
	}

	#[test]
	fn a_tagged_remove_announces_the_row_it_reads_and_stays_silent_for_a_missing_key() {
		// Order and free rows pass no pre, so the transaction must still find what it removed.
		let mut txn = create_test_admin_transaction();
		let mut dml = DmlTransaction::over(Transaction::Admin(&mut txn));
		dml.set_with_pre(vec![(key(1), bytes(1), SetPre::Absent)]).unwrap();
		dml.remove_tagged(vec![key(1), key(2)]).unwrap();
		assert!(matches!(
			pending(&txn, &key(1)),
			Delta::Remove { announce: RemoveAnnounce::Announced { pre }, .. } if pre == bytes(1)
		));
		assert!(matches!(
			pending(&txn, &key(2)),
			Delta::Remove {
				announce: RemoveAnnounce::Silent,
				..
			}
		));
	}

	#[test]
	fn a_forward_range_sees_rows_written_in_the_same_transaction_in_key_order() {
		// The ring reads its order index here, so a range skipping own writes would miss rows just placed.
		let mut txn = create_test_admin_transaction();
		let mut dml = DmlTransaction::over(Transaction::Admin(&mut txn));
		dml.set_with_pre(vec![(key(2), bytes(2), SetPre::Absent), (key(1), bytes(1), SetPre::Absent)]).unwrap();
		let rows: Vec<EncodedBytes> = dml
			.range(RowKeyRange::storage_scan(TableId(1).into()), 16)
			.unwrap()
			.map(|row| row.unwrap().bytes)
			.collect();
		assert_eq!(rows, vec![bytes(2), bytes(1)], "row keys encode the row number descending");
	}
}
