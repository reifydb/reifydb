// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{RingBufferDef, RowKey},
	value::encoded::EncodedValues,
};
use reifydb_transaction::interceptor::RingBufferInterceptor;
use reifydb_type::RowNumber;

use crate::StandardCommandTransaction;

pub(crate) trait RingBufferOperations {
	async fn insert_ringbuffer(
		&mut self,
		ringbuffer: RingBufferDef,
		row: EncodedValues,
	) -> crate::Result<RowNumber>;

	async fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: RingBufferDef,
		row_number: RowNumber,
		row: EncodedValues,
	) -> crate::Result<()>;

	async fn update_ringbuffer(
		&mut self,
		ringbuffer: RingBufferDef,
		id: RowNumber,
		row: EncodedValues,
	) -> crate::Result<()>;

	async fn remove_from_ringbuffer(&mut self, ringbuffer: RingBufferDef, id: RowNumber) -> crate::Result<()>;
}

impl RingBufferOperations for StandardCommandTransaction {
	async fn insert_ringbuffer(
		&mut self,
		_ringbuffer: RingBufferDef,
		_row: EncodedValues,
	) -> crate::Result<RowNumber> {
		// For ring buffers, the row_number is determined by the caller based on ring buffer metadata
		// This is different from tables which use RowSequence::next_row_number
		// The caller must provide the correct row_number based on head/tail position
		unimplemented!(
			"Ring buffer insert must be called with explicit row_number through insert_ringbuffer_at"
		)
	}

	async fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: RingBufferDef,
		row_number: RowNumber,
		row: EncodedValues,
	) -> crate::Result<()> {
		let key = RowKey::encoded(ringbuffer.id, row_number);

		// Check if we're overwriting existing data (for ring buffer circular behavior)
		let old_row = self.get(&key).await?.map(|v| v.values);

		// If there's an existing encoded, we need to delete it first with interceptors
		if let Some(ref existing) = old_row {
			RingBufferInterceptor::pre_delete(self, &ringbuffer, row_number).await?;
			// Don't actually remove, we'll overwrite
			RingBufferInterceptor::post_delete(self, &ringbuffer, row_number, existing).await?;
		}

		RingBufferInterceptor::pre_insert(self, &ringbuffer, &row).await?;

		self.set(&key, row.clone()).await?;

		RingBufferInterceptor::post_insert(self, &ringbuffer, row_number, &row).await?;

		Ok(())
	}

	async fn update_ringbuffer(
		&mut self,
		ringbuffer: RingBufferDef,
		id: RowNumber,
		row: EncodedValues,
	) -> crate::Result<()> {
		let key = RowKey::encoded(ringbuffer.id, id);

		// Get the current encoded before updating (for post-update interceptor)
		let old_row = self.get(&key).await?.map(|v| v.values);

		RingBufferInterceptor::pre_update(self, &ringbuffer, id, &row).await?;

		self.set(&key, row.clone()).await?;

		if let Some(ref old) = old_row {
			RingBufferInterceptor::post_update(self, &ringbuffer, id, &row, old).await?;
		}

		Ok(())
	}

	async fn remove_from_ringbuffer(&mut self, ringbuffer: RingBufferDef, id: RowNumber) -> crate::Result<()> {
		let key = RowKey::encoded(ringbuffer.id, id);

		// Get the encoded before removing (for post-delete interceptor)
		let deleted_row = match self.get(&key).await? {
			Some(v) => v.values,
			None => return Ok(()), // Nothing to delete
		};

		// Execute pre-delete interceptors
		RingBufferInterceptor::pre_delete(self, &ringbuffer, id).await?;

		// Remove the encoded from the database
		self.remove(&key).await?;

		RingBufferInterceptor::post_delete(self, &ringbuffer, id, &deleted_row).await?;

		Ok(())
	}
}
