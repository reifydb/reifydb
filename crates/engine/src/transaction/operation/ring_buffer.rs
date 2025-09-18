// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{
		EncodableKey, RingBufferDef, RowKey, Transaction, VersionedCommandTransaction,
		VersionedQueryTransaction, interceptor::RingBufferInterceptor,
	},
	row::EncodedRow,
};
use reifydb_type::RowNumber;

use crate::StandardCommandTransaction;

pub(crate) trait RingBufferOperations {
	fn insert_into_ring_buffer(&mut self, ring_buffer: RingBufferDef, row: EncodedRow) -> crate::Result<RowNumber>;

	fn insert_into_ring_buffer_at(
		&mut self,
		ring_buffer: RingBufferDef,
		row_number: RowNumber,
		row: EncodedRow,
	) -> crate::Result<()>;

	fn update_ring_buffer(
		&mut self,
		ring_buffer: RingBufferDef,
		id: RowNumber,
		row: EncodedRow,
	) -> crate::Result<()>;

	fn remove_from_ring_buffer(&mut self, ring_buffer: RingBufferDef, id: RowNumber) -> crate::Result<()>;
}

impl<T: Transaction> RingBufferOperations for StandardCommandTransaction<T> {
	fn insert_into_ring_buffer(
		&mut self,
		_ring_buffer: RingBufferDef,
		_row: EncodedRow,
	) -> crate::Result<RowNumber> {
		// For ring buffers, the row_number is determined by the caller based on ring buffer metadata
		// This is different from tables which use RowSequence::next_row_number
		// The caller must provide the correct row_number based on head/tail position
		unimplemented!(
			"Ring buffer insert must be called with explicit row_number through insert_into_ring_buffer_at"
		)
	}

	fn insert_into_ring_buffer_at(
		&mut self,
		ring_buffer: RingBufferDef,
		row_number: RowNumber,
		row: EncodedRow,
	) -> crate::Result<()> {
		let key = RowKey {
			source: ring_buffer.id.into(),
			row: row_number,
		}
		.encode();

		// Check if we're overwriting existing data (for ring buffer circular behavior)
		let old_row = self.get(&key)?.map(|v| v.row);

		// If there's an existing row, we need to delete it first with interceptors
		if let Some(ref existing) = old_row {
			RingBufferInterceptor::pre_delete(self, &ring_buffer, row_number)?;
			// Don't actually remove, we'll overwrite
			RingBufferInterceptor::post_delete(self, &ring_buffer, row_number, existing)?;
		}

		RingBufferInterceptor::pre_insert(self, &ring_buffer, &row)?;

		self.set(&key, row.clone())?;

		RingBufferInterceptor::post_insert(self, &ring_buffer, row_number, &row)?;

		Ok(())
	}

	fn update_ring_buffer(
		&mut self,
		ring_buffer: RingBufferDef,
		id: RowNumber,
		row: EncodedRow,
	) -> crate::Result<()> {
		let key = RowKey {
			source: ring_buffer.id.into(),
			row: id,
		}
		.encode();

		// Get the current row before updating (for post-update interceptor)
		let old_row = self.get(&key)?.map(|v| v.row);

		RingBufferInterceptor::pre_update(self, &ring_buffer, id, &row)?;

		self.set(&key, row.clone())?;

		// Execute post-update interceptors if we had an old row
		if let Some(ref old) = old_row {
			RingBufferInterceptor::post_update(self, &ring_buffer, id, &row, old)?;
		}

		Ok(())
	}

	fn remove_from_ring_buffer(&mut self, ring_buffer: RingBufferDef, id: RowNumber) -> crate::Result<()> {
		let key = RowKey {
			source: ring_buffer.id.into(),
			row: id,
		}
		.encode();

		// Get the row before removing (for post-delete interceptor)
		let deleted_row = match self.get(&key)? {
			Some(v) => v.row,
			None => return Ok(()), // Nothing to delete
		};

		// Execute pre-delete interceptors
		RingBufferInterceptor::pre_delete(self, &ring_buffer, id)?;

		// Remove the row from the database
		self.remove(&key)?;

		RingBufferInterceptor::post_delete(self, &ring_buffer, id, &deleted_row)?;

		Ok(())
	}
}
