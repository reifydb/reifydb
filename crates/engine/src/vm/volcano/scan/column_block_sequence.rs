// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use reifydb_core::{error::diagnostic::internal::internal, interface::catalog::column_snapshot::ColumnSnapshot};
use reifydb_store_column::{
	device::BlockKey, predicate::Predicate, reader::SnapshotReader, snapshot::Schema, store::ColumnStore,
};
use reifydb_value::error::Error;

use crate::Result;

pub(crate) struct BlockSequenceReader {
	store: Arc<ColumnStore>,
	snapshots: Vec<ColumnSnapshot>,
	batch_size: usize,
	index: usize,
	current: Option<SnapshotReader>,
	schema: Option<Schema>,
	predicate: Option<Predicate>,
}

impl BlockSequenceReader {
	pub(crate) fn new(store: Arc<ColumnStore>, snapshots: Vec<ColumnSnapshot>, batch_size: usize) -> Self {
		Self {
			store,
			snapshots,
			batch_size,
			index: 0,
			current: None,
			schema: None,
			predicate: None,
		}
	}

	pub(crate) fn with_predicate(mut self, predicate: Option<Predicate>) -> Self {
		self.predicate = predicate;
		self
	}

	pub(crate) fn schema(&self) -> Option<&Schema> {
		self.schema.as_ref()
	}

	pub(crate) fn next(&mut self) -> Result<Option<RecordBatch>> {
		loop {
			if let Some(reader) = self.current.as_mut() {
				match reader.next() {
					Some(batch) => return batch.map(Some),
					None => self.current = None,
				}
			}

			if self.index >= self.snapshots.len() {
				return Ok(None);
			}

			let snapshot = &self.snapshots[self.index];
			self.index += 1;

			let block = Arc::new(
				self.store
					.open(&BlockKey::of(snapshot))?
					.ok_or_else(|| {
						Error(Box::new(internal(format!(
							"column block for snapshot {} is missing from the column store",
							snapshot.id
						))))
					})?
					.read(None)?,
			);

			if self.schema.is_none() {
				self.schema = Some(Arc::clone(&block.schema));
			}

			let reader = SnapshotReader::new(block, self.batch_size, self.store.session().clone());
			self.current = Some(match self.predicate.clone() {
				Some(predicate) => reader.with_predicate(predicate),
				None => reader,
			});
		}
	}
}
