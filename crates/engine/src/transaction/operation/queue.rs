// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::interface::catalog::queue::Queue;
use reifydb_flow::writer::queue::QueueWriter;
use reifydb_transaction::{
	change::{QueueRowInsertion, RowChange},
	transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction},
};
use reifydb_value::value::{datetime::DateTime, partition::Partition, row_number::RowNumber};

use crate::{
	Result,
	transaction::operation::{storage::DmlTransaction, table::TableDml},
};

#[derive(Debug, Clone)]
pub struct QueueInsertRow {
	pub row_number: RowNumber,
	pub partition: Partition,
	pub key_hash: Option<u64>,
	pub not_before: Option<DateTime>,
	pub encoded: EncodedBytes,
}

pub trait QueueOperations {
	fn insert_queue(&mut self, queue: &Queue, rows: &[QueueInsertRow]) -> Result<()>;
}

fn row_changes(queue: &Queue, rows: &[QueueInsertRow]) -> Vec<RowChange> {
	rows.iter()
		.map(|row| {
			RowChange::QueueInsert(QueueRowInsertion {
				queue_id: queue.id,
				partition: row.partition,
				key_hash: row.key_hash,
				row_number: row.row_number,
				not_before: row.not_before,
				encoded: row.encoded.clone(),
			})
		})
		.collect()
}

fn insert_rows<T: TableDml>(txn: &mut T, queue: &Queue, rows: &[QueueInsertRow]) -> Result<()> {
	if rows.is_empty() {
		return Ok(());
	}

	QueueWriter::new(&mut DmlTransaction::over(txn.transaction()), queue.id.into())
		.insert(rows.iter().map(|row| (row.row_number, row.encoded.clone())))?;

	txn.track_row_change(&row_changes(queue, rows));

	Ok(())
}

impl QueueOperations for CommandTransaction {
	fn insert_queue(&mut self, queue: &Queue, rows: &[QueueInsertRow]) -> Result<()> {
		insert_rows(self, queue, rows)
	}
}

impl QueueOperations for AdminTransaction {
	fn insert_queue(&mut self, queue: &Queue, rows: &[QueueInsertRow]) -> Result<()> {
		insert_rows(self, queue, rows)
	}
}

impl QueueOperations for Transaction<'_> {
	fn insert_queue(&mut self, queue: &Queue, rows: &[QueueInsertRow]) -> Result<()> {
		match self {
			Transaction::Command(txn) => txn.insert_queue(queue, rows),
			Transaction::Admin(txn) => txn.insert_queue(queue, rows),
			Transaction::Test(t) => t.inner.insert_queue(queue, rows),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{Arc, Mutex};

	use reifydb_core::{
		common::TimeSource,
		delta::{Delta, SetPre},
		interface::catalog::{
			id::{NamespaceId, QueueId},
			queue::{QueueDispatch, QueueRetention, QueueRetry},
		},
		key::{any::TaggedKey, row::RowKey},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::interceptor::{WithInterceptors, transaction::post_commit};
	use reifydb_value::util::cowvec::CowVec;

	use super::*;

	fn queue() -> Queue {
		Queue {
			id: QueueId(3),
			namespace: NamespaceId(1),
			name: "q".to_string(),
			columns: vec![],
			partition_by: vec!["tenant".to_string()],
			dispatch: QueueDispatch::default(),
			deduplicate: None,
			retention: QueueRetention::default(),
			retry: QueueRetry::default(),
			time: TimeSource::None,
		}
	}

	fn row(number: u64, partition: u128) -> QueueInsertRow {
		QueueInsertRow {
			row_number: RowNumber(number),
			partition: Partition(partition),
			key_hash: Some(number),
			not_before: None,
			encoded: EncodedBytes(CowVec::new(vec![number as u8])),
		}
	}

	#[test]
	fn a_queue_row_is_written_absent_under_the_plain_row_key_whatever_its_partition() {
		// Readers fetch rows by queue and row number, so a partitioned key would make the row unreadable.
		let mut txn = create_test_admin_transaction();
		txn.insert_queue(&queue(), &[row(1, 7), row(2, 8)]).unwrap();
		for (number, value) in [(1, 1u8), (2, 2u8)] {
			let key: TaggedKey = RowKey::new(QueueId(3), RowNumber(number)).into();
			let delta = txn.cmd.as_ref().unwrap().pending_writes().get(&key).unwrap().delta.clone();
			assert!(
				matches!(
					&delta,
					Delta::Set { pre: SetPre::Absent, bytes, .. }
						if *bytes == EncodedBytes(CowVec::new(vec![value]))
				),
				"a queue insert must tell commit log no row existed, got {delta:?}"
			);
		}
	}

	#[test]
	fn an_empty_insert_touches_no_transaction_so_it_succeeds_on_a_finished_one() {
		// Reaching the writer on no rows would turn a no-op into an inactive transaction error.
		let mut txn = create_test_admin_transaction();
		txn.rollback().unwrap();
		txn.insert_queue(&queue(), &[]).unwrap();
	}

	#[test]
	fn the_caller_tracks_one_queue_insert_per_row_with_its_partition_and_only_on_a_non_empty_insert() {
		// Post commit consumers learn of a row only through these changes, so a lost one hides the item from
		// them.
		let seen: Arc<Mutex<Vec<(u64, u128, Option<u64>)>>> = Arc::new(Mutex::new(Vec::new()));
		let mut txn = create_test_admin_transaction();
		let log = seen.clone();
		txn.post_commit_interceptors().add(Arc::new(post_commit(move |ctx| {
			for change in &ctx.row_changes {
				if let RowChange::QueueInsert(row) = change {
					log.lock().unwrap().push((row.row_number.0, row.partition.0, row.key_hash));
				}
			}
			Ok(())
		})));
		txn.insert_queue(&queue(), &[]).unwrap();
		txn.insert_queue(&queue(), &[row(1, 7), row(2, 8)]).unwrap();
		txn.commit().unwrap();
		assert_eq!(*seen.lock().unwrap(), vec![(1, 7, Some(1)), (2, 8, Some(2))]);
	}
}
