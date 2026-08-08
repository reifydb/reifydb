// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{interface::catalog::queue::Queue, key::row::RowKey};
use reifydb_transaction::{
	change::{QueueRowInsertion, RowChange},
	transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction},
};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::Result;

#[derive(Debug, Clone)]
pub struct QueueInsertRow {
	pub row_number: RowNumber,
	pub partition: u16,
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
				row_number: row.row_number,
				not_before: row.not_before,
				encoded: row.encoded.clone(),
			})
		})
		.collect()
}

impl QueueOperations for CommandTransaction {
	fn insert_queue(&mut self, queue: &Queue, rows: &[QueueInsertRow]) -> Result<()> {
		if rows.is_empty() {
			return Ok(());
		}

		for row in rows {
			self.set(&RowKey::encoded(queue.id, row.row_number), row.encoded.clone())?;
		}

		self.track_row_change(&row_changes(queue, rows));

		Ok(())
	}
}

impl QueueOperations for AdminTransaction {
	fn insert_queue(&mut self, queue: &Queue, rows: &[QueueInsertRow]) -> Result<()> {
		if rows.is_empty() {
			return Ok(());
		}

		for row in rows {
			self.set(&RowKey::encoded(queue.id, row.row_number), row.encoded.clone())?;
		}

		self.track_row_change(&row_changes(queue, rows));

		Ok(())
	}
}

impl QueueOperations for Transaction<'_> {
	fn insert_queue(&mut self, queue: &Queue, rows: &[QueueInsertRow]) -> Result<()> {
		match self {
			Transaction::Command(txn) => txn.insert_queue(queue, rows),
			Transaction::Admin(txn) => txn.insert_queue(queue, rows),
			Transaction::Test(t) => t.inner.insert_queue(queue, rows),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Replica(_) => panic!("Write operations not supported on Replica transaction"),
		}
	}
}
