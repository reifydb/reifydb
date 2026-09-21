// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_column::{predicate::Predicate, reader::SnapshotReader, snapshot::Schema};
use reifydb_core::{
	error::diagnostic::internal::internal, interface::catalog::id::ColumnSnapshotId,
	value::column::columns::Columns,
};
use reifydb_store_column::store::ColumnStore;
use reifydb_value::error::Error;

use crate::Result;

pub(crate) struct BlockSequenceReader {
	store: Arc<ColumnStore>,
	snapshots: Vec<ColumnSnapshotId>,
	batch_size: usize,
	index: usize,
	current: Option<SnapshotReader>,
	schema: Option<Schema>,
	predicate: Option<Predicate>,
}

impl BlockSequenceReader {
	pub(crate) fn new(store: Arc<ColumnStore>, snapshots: Vec<ColumnSnapshotId>, batch_size: usize) -> Self {
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

	pub(crate) fn next(&mut self) -> Result<Option<Columns>> {
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

			let id = self.snapshots[self.index];
			self.index += 1;

			let block = self.store.get(id).ok_or_else(|| {
				Error(Box::new(internal(format!(
					"column block for snapshot {} is missing from the column store",
					id
				))))
			})?;

			if self.schema.is_none() {
				self.schema = Some(Arc::clone(&block.schema));
			}

			let reader = SnapshotReader::new(block, self.batch_size);
			self.current = Some(match self.predicate.clone() {
				Some(predicate) => reader.with_predicate(predicate),
				None => reader,
			});
		}
	}
}
