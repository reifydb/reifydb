// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::{row::EncodedRow, shape::RowShape},
	interface::{
		catalog::{ringbuffer::RingBuffer, shape::ShapeId},
		change::{Change, ChangeOrigin, Diff},
	},
	key::row::RowKey,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::{
	interceptor::ringbuffer_row::RingBufferRowInterceptor,
	transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction},
};
use reifydb_type::{
	fragment::Fragment,
	util::cowvec::CowVec,
	value::{datetime::DateTime, row_number::RowNumber},
};
use smallvec::smallvec;

use crate::Result;

fn build_encoded_columns(shape: &RowShape, row_number: RowNumber, encoded: &EncodedRow) -> Columns {
	let fields = shape.fields();

	let mut columns_vec: Vec<ColumnWithName> = Vec::with_capacity(fields.len());
	for field in fields.iter() {
		columns_vec.push(ColumnWithName {
			name: Fragment::internal(&field.name),
			data: ColumnBuffer::with_capacity(field.constraint.get_type(), 1),
		});
	}

	for (i, _) in fields.iter().enumerate() {
		columns_vec[i].data.push_value(shape.get_value(encoded, i));
	}

	Columns::with_system_columns(
		columns_vec,
		vec![row_number],
		vec![DateTime::from_nanos(encoded.created_at_nanos())],
		vec![DateTime::from_nanos(encoded.updated_at_nanos())],
	)
}

fn build_ringbuffer_insert_change(
	rb: &RingBuffer,
	shape: &RowShape,
	row_number: RowNumber,
	encoded: &EncodedRow,
) -> Change {
	Change {
		origin: ChangeOrigin::Shape(ShapeId::ringbuffer(rb.id)),
		version: CommitVersion(0),
		diffs: smallvec![Diff::insert(build_encoded_columns(shape, row_number, encoded))],
		changed_at: DateTime::default(),
	}
}

fn build_ringbuffer_update_change(
	rb: &RingBuffer,
	row_number: RowNumber,
	pre: &EncodedRow,
	post: &EncodedRow,
) -> Change {
	let shape: RowShape = (&rb.columns).into();
	Change {
		origin: ChangeOrigin::Shape(ShapeId::ringbuffer(rb.id)),
		version: CommitVersion(0),
		diffs: smallvec![Diff::update(
			build_encoded_columns(&shape, row_number, pre),
			build_encoded_columns(&shape, row_number, post),
		)],
		changed_at: DateTime::default(),
	}
}

fn build_ringbuffer_remove_change(rb: &RingBuffer, row_number: RowNumber, encoded: &EncodedRow) -> Change {
	let shape: RowShape = (&rb.columns).into();
	Change {
		origin: ChangeOrigin::Shape(ShapeId::ringbuffer(rb.id)),
		version: CommitVersion(0),
		diffs: smallvec![Diff::remove(build_encoded_columns(&shape, row_number, encoded))],
		changed_at: DateTime::default(),
	}
}

pub trait RingBufferOperations {
	fn insert_ringbuffer(&mut self, ringbuffer: RingBuffer, row: EncodedRow) -> Result<RowNumber>;

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		row_number: RowNumber,
		row: EncodedRow,
	) -> Result<EncodedRow>;

	fn update_ringbuffer(&mut self, ringbuffer: RingBuffer, id: RowNumber, row: EncodedRow) -> Result<EncodedRow>;

	fn remove_from_ringbuffer(&mut self, ringbuffer: &RingBuffer, id: RowNumber) -> Result<EncodedRow>;
}

impl RingBufferOperations for CommandTransaction {
	fn insert_ringbuffer(&mut self, _ringbuffer: RingBuffer, _row: EncodedRow) -> Result<RowNumber> {
		// For ring buffers, the row_number is determined by the caller based on ring buffer metadata
		// This is different from tables which use RowSequence::next_row_number
		// The caller must provide the correct row_number based on head/tail position
		unimplemented!(
			"Ring buffer insert must be called with explicit row_number through insert_ringbuffer_at"
		)
	}

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		row_number: RowNumber,
		row: EncodedRow,
	) -> Result<EncodedRow> {
		let key = RowKey::encoded(ringbuffer.id, row_number);

		// Check if we're overwriting existing data (for ring buffer circular behavior)
		let pre = self.get(&key)?.map(|v| v.row);

		// If there's an existing encoded, we need to delete it first with interceptors
		if let Some(ref existing) = pre {
			RingBufferRowInterceptor::pre_delete(self, ringbuffer, row_number)?;
			// Don't actually remove, we'll overwrite
			RingBufferRowInterceptor::post_delete(self, ringbuffer, row_number, existing)?;
		}

		let row = RingBufferRowInterceptor::pre_insert(self, ringbuffer, row)?;

		self.set(&key, row.clone())?;

		RingBufferRowInterceptor::post_insert(self, ringbuffer, row_number, &row)?;

		if let Some(pre_row) = pre.as_ref() {
			self.track_flow_change(build_ringbuffer_update_change(ringbuffer, row_number, pre_row, &row));
		} else {
			self.track_flow_change(build_ringbuffer_insert_change(ringbuffer, shape, row_number, &row));
		}

		Ok(row)
	}

	fn update_ringbuffer(&mut self, ringbuffer: RingBuffer, id: RowNumber, row: EncodedRow) -> Result<EncodedRow> {
		let key = RowKey::encoded(ringbuffer.id, id);

		let pre = match self.get(&key)? {
			Some(v) => v.row,
			None => return Ok(row),
		};

		let row = RingBufferRowInterceptor::pre_update(self, &ringbuffer, id, row)?;

		if self.get_committed(&key)?.is_some() {
			self.mark_preexisting(&key)?;
		}
		self.set(&key, row.clone())?;

		RingBufferRowInterceptor::post_update(self, &ringbuffer, id, &row, &pre)?;

		self.track_flow_change(build_ringbuffer_update_change(&ringbuffer, id, &pre, &row));

		Ok(row)
	}

	fn remove_from_ringbuffer(&mut self, ringbuffer: &RingBuffer, id: RowNumber) -> Result<EncodedRow> {
		let key = RowKey::encoded(ringbuffer.id, id);

		let displayed = match self.get(&key)? {
			Some(v) => v.row,
			None => return Ok(EncodedRow(CowVec::new(vec![]))),
		};
		let committed = self.get_committed(&key)?.map(|v| v.row);

		RingBufferRowInterceptor::pre_delete(self, ringbuffer, id)?;

		let pre_for_cdc = committed.clone().unwrap_or_else(|| displayed.clone());

		if committed.is_some() {
			self.mark_preexisting(&key)?;
		}
		self.unset(&key, pre_for_cdc.clone())?;

		RingBufferRowInterceptor::post_delete(self, ringbuffer, id, &pre_for_cdc)?;

		self.track_flow_change(build_ringbuffer_remove_change(ringbuffer, id, &pre_for_cdc));

		Ok(displayed)
	}
}

impl RingBufferOperations for AdminTransaction {
	fn insert_ringbuffer(&mut self, _ringbuffer: RingBuffer, _row: EncodedRow) -> Result<RowNumber> {
		unimplemented!(
			"Ring buffer insert must be called with explicit row_number through insert_ringbuffer_at"
		)
	}

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		row_number: RowNumber,
		row: EncodedRow,
	) -> Result<EncodedRow> {
		let key = RowKey::encoded(ringbuffer.id, row_number);

		let pre = self.get(&key)?.map(|v| v.row);

		if let Some(ref existing) = pre {
			RingBufferRowInterceptor::pre_delete(self, ringbuffer, row_number)?;
			RingBufferRowInterceptor::post_delete(self, ringbuffer, row_number, existing)?;
		}

		let row = RingBufferRowInterceptor::pre_insert(self, ringbuffer, row)?;

		self.set(&key, row.clone())?;

		RingBufferRowInterceptor::post_insert(self, ringbuffer, row_number, &row)?;

		if let Some(pre_row) = pre.as_ref() {
			self.track_flow_change(build_ringbuffer_update_change(ringbuffer, row_number, pre_row, &row));
		} else {
			self.track_flow_change(build_ringbuffer_insert_change(ringbuffer, shape, row_number, &row));
		}

		Ok(row)
	}

	fn update_ringbuffer(&mut self, ringbuffer: RingBuffer, id: RowNumber, row: EncodedRow) -> Result<EncodedRow> {
		let key = RowKey::encoded(ringbuffer.id, id);

		let pre = match self.get(&key)? {
			Some(v) => v.row,
			None => return Ok(row),
		};

		let row = RingBufferRowInterceptor::pre_update(self, &ringbuffer, id, row)?;

		if self.get_committed(&key)?.is_some() {
			self.mark_preexisting(&key)?;
		}
		self.set(&key, row.clone())?;

		RingBufferRowInterceptor::post_update(self, &ringbuffer, id, &row, &pre)?;

		self.track_flow_change(build_ringbuffer_update_change(&ringbuffer, id, &pre, &row));

		Ok(row)
	}

	fn remove_from_ringbuffer(&mut self, ringbuffer: &RingBuffer, id: RowNumber) -> Result<EncodedRow> {
		let key = RowKey::encoded(ringbuffer.id, id);

		let displayed = match self.get(&key)? {
			Some(v) => v.row,
			None => return Ok(EncodedRow(CowVec::new(vec![]))),
		};
		let committed = self.get_committed(&key)?.map(|v| v.row);

		RingBufferRowInterceptor::pre_delete(self, ringbuffer, id)?;

		let pre_for_cdc = committed.clone().unwrap_or_else(|| displayed.clone());

		if committed.is_some() {
			self.mark_preexisting(&key)?;
		}
		self.unset(&key, pre_for_cdc.clone())?;

		RingBufferRowInterceptor::post_delete(self, ringbuffer, id, &pre_for_cdc)?;

		self.track_flow_change(build_ringbuffer_remove_change(ringbuffer, id, &pre_for_cdc));

		Ok(displayed)
	}
}

impl RingBufferOperations for Transaction<'_> {
	fn insert_ringbuffer(&mut self, _ringbuffer: RingBuffer, _row: EncodedRow) -> Result<RowNumber> {
		unimplemented!(
			"Ring buffer insert must be called with explicit row_number through insert_ringbuffer_at"
		)
	}

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		row_number: RowNumber,
		row: EncodedRow,
	) -> Result<EncodedRow> {
		match self {
			Transaction::Command(txn) => txn.insert_ringbuffer_at(ringbuffer, shape, row_number, row),
			Transaction::Admin(txn) => txn.insert_ringbuffer_at(ringbuffer, shape, row_number, row),
			Transaction::Test(t) => t.inner.insert_ringbuffer_at(ringbuffer, shape, row_number, row),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Replica(_) => panic!("Write operations not supported on Replica transaction"),
		}
	}

	fn update_ringbuffer(&mut self, ringbuffer: RingBuffer, id: RowNumber, row: EncodedRow) -> Result<EncodedRow> {
		match self {
			Transaction::Command(txn) => txn.update_ringbuffer(ringbuffer, id, row),
			Transaction::Admin(txn) => txn.update_ringbuffer(ringbuffer, id, row),
			Transaction::Test(t) => t.inner.update_ringbuffer(ringbuffer, id, row),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Replica(_) => panic!("Write operations not supported on Replica transaction"),
		}
	}

	fn remove_from_ringbuffer(&mut self, ringbuffer: &RingBuffer, id: RowNumber) -> Result<EncodedRow> {
		match self {
			Transaction::Command(txn) => txn.remove_from_ringbuffer(ringbuffer, id),
			Transaction::Admin(txn) => txn.remove_from_ringbuffer(ringbuffer, id),
			Transaction::Test(t) => t.inner.remove_from_ringbuffer(ringbuffer, id),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Replica(_) => panic!("Write operations not supported on Replica transaction"),
		}
	}
}
