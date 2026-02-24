// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::{encoded::EncodedValues, schema::Schema},
	interface::{
		catalog::{primitive::PrimitiveId, ringbuffer::RingBufferDef},
		change::{Change, ChangeOrigin, Diff},
	},
	key::row::RowKey,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::{
	interceptor::ringbuffer::RingBufferInterceptor,
	transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction},
};
use reifydb_type::{fragment::Fragment, util::cowvec::CowVec, value::row_number::RowNumber};

fn build_encoded_columns(rb: &RingBufferDef, row_number: RowNumber, encoded: &EncodedValues) -> Columns {
	let schema: Schema = (&rb.columns).into();
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

fn build_ringbuffer_insert_change(rb: &RingBufferDef, row_number: RowNumber, encoded: &EncodedValues) -> Change {
	Change {
		origin: ChangeOrigin::Primitive(PrimitiveId::ringbuffer(rb.id)),
		version: CommitVersion(0),
		diffs: vec![Diff::Insert {
			post: build_encoded_columns(rb, row_number, encoded),
		}],
	}
}

fn build_ringbuffer_update_change(
	rb: &RingBufferDef,
	row_number: RowNumber,
	old: &EncodedValues,
	new: &EncodedValues,
) -> Change {
	Change {
		origin: ChangeOrigin::Primitive(PrimitiveId::ringbuffer(rb.id)),
		version: CommitVersion(0),
		diffs: vec![Diff::Update {
			pre: build_encoded_columns(rb, row_number, old),
			post: build_encoded_columns(rb, row_number, new),
		}],
	}
}

fn build_ringbuffer_remove_change(rb: &RingBufferDef, row_number: RowNumber, encoded: &EncodedValues) -> Change {
	Change {
		origin: ChangeOrigin::Primitive(PrimitiveId::ringbuffer(rb.id)),
		version: CommitVersion(0),
		diffs: vec![Diff::Remove {
			pre: build_encoded_columns(rb, row_number, encoded),
		}],
	}
}

pub(crate) trait RingBufferOperations {
	fn insert_ringbuffer(&mut self, ringbuffer: RingBufferDef, row: EncodedValues) -> crate::Result<RowNumber>;

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: RingBufferDef,
		row_number: RowNumber,
		row: EncodedValues,
	) -> crate::Result<()>;

	fn update_ringbuffer(
		&mut self,
		ringbuffer: RingBufferDef,
		id: RowNumber,
		row: EncodedValues,
	) -> crate::Result<()>;

	fn remove_from_ringbuffer(&mut self, ringbuffer: RingBufferDef, id: RowNumber) -> crate::Result<()>;
}

impl RingBufferOperations for CommandTransaction {
	fn insert_ringbuffer(&mut self, _ringbuffer: RingBufferDef, _row: EncodedValues) -> crate::Result<RowNumber> {
		// For ring buffers, the row_number is determined by the caller based on ring buffer metadata
		// This is different from tables which use RowSequence::next_row_number
		// The caller must provide the correct row_number based on head/tail position
		unimplemented!(
			"Ring buffer insert must be called with explicit row_number through insert_ringbuffer_at"
		)
	}

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: RingBufferDef,
		row_number: RowNumber,
		row: EncodedValues,
	) -> crate::Result<()> {
		let key = RowKey::encoded(ringbuffer.id, row_number);

		// Check if we're overwriting existing data (for ring buffer circular behavior)
		let old_row = self.get(&key)?.map(|v| v.values);

		// If there's an existing encoded, we need to delete it first with interceptors
		if let Some(ref existing) = old_row {
			RingBufferInterceptor::pre_delete(self, &ringbuffer, row_number)?;
			// Don't actually remove, we'll overwrite
			RingBufferInterceptor::post_delete(self, &ringbuffer, row_number, existing)?;
		}

		RingBufferInterceptor::pre_insert(self, &ringbuffer, &row)?;

		self.set(&key, row.clone())?;

		RingBufferInterceptor::post_insert(self, &ringbuffer, row_number, &row)?;

		if old_row.is_some() {
			self.track_flow_change(build_ringbuffer_update_change(
				&ringbuffer,
				row_number,
				old_row.as_ref().unwrap(),
				&row,
			));
		} else {
			self.track_flow_change(build_ringbuffer_insert_change(&ringbuffer, row_number, &row));
		}

		Ok(())
	}

	fn update_ringbuffer(
		&mut self,
		ringbuffer: RingBufferDef,
		id: RowNumber,
		row: EncodedValues,
	) -> crate::Result<()> {
		let key = RowKey::encoded(ringbuffer.id, id);

		// Get the current encoded before updating (for post-update interceptor)
		let old_row = self.get(&key)?.map(|v| v.values);

		RingBufferInterceptor::pre_update(self, &ringbuffer, id, &row)?;

		self.set(&key, row.clone())?;

		if let Some(ref old) = old_row {
			RingBufferInterceptor::post_update(self, &ringbuffer, id, &row, old)?;
			self.track_flow_change(build_ringbuffer_update_change(&ringbuffer, id, old, &row));
		}

		Ok(())
	}

	fn remove_from_ringbuffer(&mut self, ringbuffer: RingBufferDef, id: RowNumber) -> crate::Result<()> {
		let key = RowKey::encoded(ringbuffer.id, id);

		// Get the encoded before removing (for post-delete interceptor)
		let deleted_row = match self.get(&key)? {
			Some(v) => v.values,
			None => return Ok(()), // Nothing to delete
		};

		// Execute pre-delete interceptors
		RingBufferInterceptor::pre_delete(self, &ringbuffer, id)?;

		// Remove the encoded from the database
		self.unset(&key, deleted_row.clone())?;

		RingBufferInterceptor::post_delete(self, &ringbuffer, id, &deleted_row)?;

		self.track_flow_change(build_ringbuffer_remove_change(&ringbuffer, id, &deleted_row));

		Ok(())
	}
}

impl RingBufferOperations for AdminTransaction {
	fn insert_ringbuffer(&mut self, _ringbuffer: RingBufferDef, _row: EncodedValues) -> crate::Result<RowNumber> {
		unimplemented!(
			"Ring buffer insert must be called with explicit row_number through insert_ringbuffer_at"
		)
	}

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: RingBufferDef,
		row_number: RowNumber,
		row: EncodedValues,
	) -> crate::Result<()> {
		let key = RowKey::encoded(ringbuffer.id, row_number);

		let old_row = self.get(&key)?.map(|v| v.values);

		if let Some(ref existing) = old_row {
			RingBufferInterceptor::pre_delete(self, &ringbuffer, row_number)?;
			RingBufferInterceptor::post_delete(self, &ringbuffer, row_number, existing)?;
		}

		RingBufferInterceptor::pre_insert(self, &ringbuffer, &row)?;

		self.set(&key, row.clone())?;

		RingBufferInterceptor::post_insert(self, &ringbuffer, row_number, &row)?;

		if old_row.is_some() {
			self.track_flow_change(build_ringbuffer_update_change(
				&ringbuffer,
				row_number,
				old_row.as_ref().unwrap(),
				&row,
			));
		} else {
			self.track_flow_change(build_ringbuffer_insert_change(&ringbuffer, row_number, &row));
		}

		Ok(())
	}

	fn update_ringbuffer(
		&mut self,
		ringbuffer: RingBufferDef,
		id: RowNumber,
		row: EncodedValues,
	) -> crate::Result<()> {
		let key = RowKey::encoded(ringbuffer.id, id);

		let old_row = self.get(&key)?.map(|v| v.values);

		RingBufferInterceptor::pre_update(self, &ringbuffer, id, &row)?;

		self.set(&key, row.clone())?;

		if let Some(ref old) = old_row {
			RingBufferInterceptor::post_update(self, &ringbuffer, id, &row, old)?;
			self.track_flow_change(build_ringbuffer_update_change(&ringbuffer, id, old, &row));
		}

		Ok(())
	}

	fn remove_from_ringbuffer(&mut self, ringbuffer: RingBufferDef, id: RowNumber) -> crate::Result<()> {
		let key = RowKey::encoded(ringbuffer.id, id);

		let deleted_row = match self.get(&key)? {
			Some(v) => v.values,
			None => return Ok(()),
		};

		RingBufferInterceptor::pre_delete(self, &ringbuffer, id)?;

		self.unset(&key, deleted_row.clone())?;

		RingBufferInterceptor::post_delete(self, &ringbuffer, id, &deleted_row)?;

		self.track_flow_change(build_ringbuffer_remove_change(&ringbuffer, id, &deleted_row));

		Ok(())
	}
}

impl RingBufferOperations for Transaction<'_> {
	fn insert_ringbuffer(&mut self, _ringbuffer: RingBufferDef, _row: EncodedValues) -> crate::Result<RowNumber> {
		unimplemented!(
			"Ring buffer insert must be called with explicit row_number through insert_ringbuffer_at"
		)
	}

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: RingBufferDef,
		row_number: RowNumber,
		row: EncodedValues,
	) -> crate::Result<()> {
		match self {
			Transaction::Command(txn) => txn.insert_ringbuffer_at(ringbuffer, row_number, row),
			Transaction::Admin(txn) => txn.insert_ringbuffer_at(ringbuffer, row_number, row),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	fn update_ringbuffer(
		&mut self,
		ringbuffer: RingBufferDef,
		id: RowNumber,
		row: EncodedValues,
	) -> crate::Result<()> {
		match self {
			Transaction::Command(txn) => txn.update_ringbuffer(ringbuffer, id, row),
			Transaction::Admin(txn) => txn.update_ringbuffer(ringbuffer, id, row),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	fn remove_from_ringbuffer(&mut self, ringbuffer: RingBufferDef, id: RowNumber) -> crate::Result<()> {
		match self {
			Transaction::Command(txn) => txn.remove_from_ringbuffer(ringbuffer, id),
			Transaction::Admin(txn) => txn.remove_from_ringbuffer(ringbuffer, id),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}
}
