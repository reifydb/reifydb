// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::{row::EncodedRow, schema::RowSchema},
	interface::{
		catalog::{ringbuffer::RingBuffer, schema::SchemaId},
		change::{Change, ChangeOrigin, Diff},
	},
	key::row::RowKey,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::{
	interceptor::ringbuffer_row::RingBufferRowInterceptor,
	transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction},
};
use reifydb_type::{fragment::Fragment, util::cowvec::CowVec, value::row_number::RowNumber};

use crate::Result;

fn build_encoded_columns(schema: &RowSchema, row_number: RowNumber, encoded: &EncodedRow) -> Columns {
	let fields = schema.fields();

	let mut columns_vec: Vec<Column> = Vec::with_capacity(fields.len());
	for field in fields.iter() {
		columns_vec.push(Column {
			name: Fragment::internal(&field.name),
			data: ColumnData::with_capacity(field.constraint.get_type(), 1),
		});
	}

	for (i, _) in fields.iter().enumerate() {
		columns_vec[i].data.push_value(schema.get_value(encoded, i));
	}

	Columns {
		row_numbers: CowVec::new(vec![row_number]),
		columns: CowVec::new(columns_vec),
	}
}

fn build_ringbuffer_insert_change(
	rb: &RingBuffer,
	schema: &RowSchema,
	row_number: RowNumber,
	encoded: &EncodedRow,
) -> Change {
	Change {
		origin: ChangeOrigin::Schema(SchemaId::ringbuffer(rb.id)),
		version: CommitVersion(0),
		diffs: vec![Diff::Insert {
			post: build_encoded_columns(schema, row_number, encoded),
		}],
	}
}

fn build_ringbuffer_update_change(
	rb: &RingBuffer,
	row_number: RowNumber,
	pre: &EncodedRow,
	post: &EncodedRow,
) -> Change {
	let schema: RowSchema = (&rb.columns).into();
	Change {
		origin: ChangeOrigin::Schema(SchemaId::ringbuffer(rb.id)),
		version: CommitVersion(0),
		diffs: vec![Diff::Update {
			pre: build_encoded_columns(&schema, row_number, pre),
			post: build_encoded_columns(&schema, row_number, post),
		}],
	}
}

fn build_ringbuffer_remove_change(rb: &RingBuffer, row_number: RowNumber, encoded: &EncodedRow) -> Change {
	let schema: RowSchema = (&rb.columns).into();
	Change {
		origin: ChangeOrigin::Schema(SchemaId::ringbuffer(rb.id)),
		version: CommitVersion(0),
		diffs: vec![Diff::Remove {
			pre: build_encoded_columns(&schema, row_number, encoded),
		}],
	}
}

pub(crate) trait RingBufferOperations {
	fn insert_ringbuffer(&mut self, ringbuffer: RingBuffer, row: EncodedRow) -> Result<RowNumber>;

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: &RingBuffer,
		schema: &RowSchema,
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
		schema: &RowSchema,
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

		if pre.is_some() {
			self.track_flow_change(build_ringbuffer_update_change(
				ringbuffer,
				row_number,
				pre.as_ref().unwrap(),
				&row,
			));
		} else {
			self.track_flow_change(build_ringbuffer_insert_change(ringbuffer, schema, row_number, &row));
		}

		Ok(row)
	}

	fn update_ringbuffer(&mut self, ringbuffer: RingBuffer, id: RowNumber, row: EncodedRow) -> Result<EncodedRow> {
		let key = RowKey::encoded(ringbuffer.id, id);

		// Get the current encoded before updating (for post-update interceptor)
		let pre = self.get(&key)?.map(|v| v.row);

		let row = RingBufferRowInterceptor::pre_update(self, &ringbuffer, id, row)?;

		self.set(&key, row.clone())?;

		if let Some(ref pre) = pre {
			RingBufferRowInterceptor::post_update(self, &ringbuffer, id, &row, pre)?;
			self.track_flow_change(build_ringbuffer_update_change(&ringbuffer, id, pre, &row));
		}

		Ok(row)
	}

	fn remove_from_ringbuffer(&mut self, ringbuffer: &RingBuffer, id: RowNumber) -> Result<EncodedRow> {
		let key = RowKey::encoded(ringbuffer.id, id);

		// Get the encoded before removing (for post-delete interceptor)
		let deleted_row = match self.get(&key)? {
			Some(v) => v.row,
			None => return Ok(EncodedRow(CowVec::new(vec![]))),
		};

		// Execute pre-delete interceptors
		RingBufferRowInterceptor::pre_delete(self, ringbuffer, id)?;

		// Remove the encoded from the database
		self.unset(&key, deleted_row.clone())?;

		RingBufferRowInterceptor::post_delete(self, ringbuffer, id, &deleted_row)?;

		self.track_flow_change(build_ringbuffer_remove_change(ringbuffer, id, &deleted_row));

		Ok(deleted_row)
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
		schema: &RowSchema,
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

		if pre.is_some() {
			self.track_flow_change(build_ringbuffer_update_change(
				ringbuffer,
				row_number,
				pre.as_ref().unwrap(),
				&row,
			));
		} else {
			self.track_flow_change(build_ringbuffer_insert_change(ringbuffer, schema, row_number, &row));
		}

		Ok(row)
	}

	fn update_ringbuffer(&mut self, ringbuffer: RingBuffer, id: RowNumber, row: EncodedRow) -> Result<EncodedRow> {
		let key = RowKey::encoded(ringbuffer.id, id);

		let pre = self.get(&key)?.map(|v| v.row);

		let row = RingBufferRowInterceptor::pre_update(self, &ringbuffer, id, row)?;

		self.set(&key, row.clone())?;

		if let Some(ref pre) = pre {
			RingBufferRowInterceptor::post_update(self, &ringbuffer, id, &row, pre)?;
			self.track_flow_change(build_ringbuffer_update_change(&ringbuffer, id, pre, &row));
		}

		Ok(row)
	}

	fn remove_from_ringbuffer(&mut self, ringbuffer: &RingBuffer, id: RowNumber) -> Result<EncodedRow> {
		let key = RowKey::encoded(ringbuffer.id, id);

		let deleted_row = match self.get(&key)? {
			Some(v) => v.row,
			None => return Ok(EncodedRow(CowVec::new(vec![]))),
		};

		RingBufferRowInterceptor::pre_delete(self, ringbuffer, id)?;

		self.unset(&key, deleted_row.clone())?;

		RingBufferRowInterceptor::post_delete(self, ringbuffer, id, &deleted_row)?;

		self.track_flow_change(build_ringbuffer_remove_change(ringbuffer, id, &deleted_row));

		Ok(deleted_row)
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
		schema: &RowSchema,
		row_number: RowNumber,
		row: EncodedRow,
	) -> Result<EncodedRow> {
		match self {
			Transaction::Command(txn) => txn.insert_ringbuffer_at(ringbuffer, schema, row_number, row),
			Transaction::Admin(txn) => txn.insert_ringbuffer_at(ringbuffer, schema, row_number, row),
			Transaction::Subscription(txn) => {
				txn.as_admin_mut().insert_ringbuffer_at(ringbuffer, schema, row_number, row)
			}
			Transaction::Test(t) => t.inner.insert_ringbuffer_at(ringbuffer, schema, row_number, row),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	fn update_ringbuffer(&mut self, ringbuffer: RingBuffer, id: RowNumber, row: EncodedRow) -> Result<EncodedRow> {
		match self {
			Transaction::Command(txn) => txn.update_ringbuffer(ringbuffer, id, row),
			Transaction::Admin(txn) => txn.update_ringbuffer(ringbuffer, id, row),
			Transaction::Subscription(txn) => txn.as_admin_mut().update_ringbuffer(ringbuffer, id, row),
			Transaction::Test(t) => t.inner.update_ringbuffer(ringbuffer, id, row),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	fn remove_from_ringbuffer(&mut self, ringbuffer: &RingBuffer, id: RowNumber) -> Result<EncodedRow> {
		match self {
			Transaction::Command(txn) => txn.remove_from_ringbuffer(ringbuffer, id),
			Transaction::Admin(txn) => txn.remove_from_ringbuffer(ringbuffer, id),
			Transaction::Subscription(txn) => txn.as_admin_mut().remove_from_ringbuffer(ringbuffer, id),
			Transaction::Test(t) => t.inner.remove_from_ringbuffer(ringbuffer, id),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}
}
