// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{encoded::encoded::EncodedValues, interface::catalog::ringbuffer::RingBufferDef, key::row::RowKey};
use reifydb_transaction::{interceptor::ringbuffer::RingBufferInterceptor, transaction::command::CommandTransaction};
use reifydb_type::value::row_number::RowNumber;

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

		Ok(())
	}
}
