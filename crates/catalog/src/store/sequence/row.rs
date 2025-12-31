// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{CommandTransaction, PrimitiveId, RingBufferId, RowSequenceKey, TableId};
use reifydb_transaction::StandardCommandTransaction;
use reifydb_type::RowNumber;

use crate::store::sequence::generator::u64::GeneratorU64;

pub struct RowSequence {}

impl RowSequence {
	pub async fn next_row_number(txn: &mut StandardCommandTransaction, table: TableId) -> crate::Result<RowNumber> {
		GeneratorU64::next(txn, &RowSequenceKey::encoded(PrimitiveId::from(table)), None).await.map(RowNumber)
	}

	/// Allocates a batch of contiguous row numbers for a table.
	/// Returns a vector containing all allocated row numbers.
	pub async fn next_row_number_batch(
		txn: &mut StandardCommandTransaction,
		table: TableId,
		count: u64,
	) -> crate::Result<Vec<RowNumber>> {
		Self::next_row_number_batch_for_source(txn, PrimitiveId::from(table), count).await
	}

	/// Allocates the next row number for a ring buffer.
	pub async fn next_row_number_for_ringbuffer(
		txn: &mut StandardCommandTransaction,
		ringbuffer: RingBufferId,
	) -> crate::Result<RowNumber> {
		GeneratorU64::next(txn, &RowSequenceKey::encoded(PrimitiveId::from(ringbuffer)), None)
			.await
			.map(RowNumber)
	}

	/// Allocates a batch of contiguous row numbers for a ring buffer.
	/// Returns a vector containing all allocated row numbers.
	pub async fn next_row_number_batch_for_ringbuffer(
		txn: &mut StandardCommandTransaction,
		ringbuffer: RingBufferId,
		count: u64,
	) -> crate::Result<Vec<RowNumber>> {
		Self::next_row_number_batch_for_source(txn, PrimitiveId::from(ringbuffer), count).await
	}

	/// Allocates a batch of contiguous row numbers for any source.
	async fn next_row_number_batch_for_source(
		txn: &mut StandardCommandTransaction,
		source: PrimitiveId,
		count: u64,
	) -> crate::Result<Vec<RowNumber>> {
		let last_row_number =
			GeneratorU64::next_batched(txn, &RowSequenceKey::encoded(source), None, count).await?;

		// Calculate the first row number in the batch
		// next_batched returns the last allocated ID
		let first_row_number = last_row_number.saturating_sub(count - 1);

		// Generate all row numbers in the allocated range
		let row_numbers = (0..count).map(|offset| RowNumber(first_row_number + offset)).collect();

		Ok(row_numbers)
	}
}
