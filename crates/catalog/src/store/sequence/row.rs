// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{RingBufferId, TableId},
		primitive::PrimitiveId,
	},
	key::row_sequence::RowSequenceKey,
};
use reifydb_transaction::standard::command::StandardCommandTransaction;
use reifydb_type::value::row_number::RowNumber;

use crate::store::sequence::generator::u64::GeneratorU64;

pub struct RowSequence {}

impl RowSequence {
	pub(crate) fn next_row_number(
		txn: &mut StandardCommandTransaction,
		table: TableId,
	) -> crate::Result<RowNumber> {
		GeneratorU64::next(txn, &RowSequenceKey::encoded(PrimitiveId::from(table)), None).map(RowNumber)
	}

	/// Allocates a batch of contiguous row numbers for a table.
	/// Returns a vector containing all allocated row numbers.
	pub(crate) fn next_row_number_batch(
		txn: &mut StandardCommandTransaction,
		table: TableId,
		count: u64,
	) -> crate::Result<Vec<RowNumber>> {
		Self::next_row_number_batch_for_source(txn, PrimitiveId::from(table), count)
	}

	/// Allocates the next row number for a ring buffer.
	pub(crate) fn next_row_number_for_ringbuffer(
		txn: &mut StandardCommandTransaction,
		ringbuffer: RingBufferId,
	) -> crate::Result<RowNumber> {
		GeneratorU64::next(txn, &RowSequenceKey::encoded(PrimitiveId::from(ringbuffer)), None).map(RowNumber)
	}

	/// Allocates a batch of contiguous row numbers for a ring buffer.
	/// Returns a vector containing all allocated row numbers.
	pub(crate) fn next_row_number_batch_for_ringbuffer(
		txn: &mut StandardCommandTransaction,
		ringbuffer: RingBufferId,
		count: u64,
	) -> crate::Result<Vec<RowNumber>> {
		Self::next_row_number_batch_for_source(txn, PrimitiveId::from(ringbuffer), count)
	}

	/// Allocates a batch of contiguous row numbers for any source.
	fn next_row_number_batch_for_source(
		txn: &mut StandardCommandTransaction,
		source: PrimitiveId,
		count: u64,
	) -> crate::Result<Vec<RowNumber>> {
		let last_row_number = GeneratorU64::next_batched(txn, &RowSequenceKey::encoded(source), None, count)?;

		// Calculate the first row number in the batch
		// next_batched returns the last allocated ID
		let first_row_number = last_row_number.saturating_sub(count - 1);

		// Generate all row numbers in the allocated range
		let row_numbers = (0..count).map(|offset| RowNumber(first_row_number + offset)).collect();

		Ok(row_numbers)
	}
}
