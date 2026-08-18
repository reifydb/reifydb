// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{QueueId, RingBufferId, TableId},
		storage::StorageId,
	},
	key::row_sequence::RowSequenceKey,
};
use reifydb_value::value::row_number::RowNumber;

use super::generator::SequenceTransaction;
use crate::{Result, store::sequence::generator::u64::GeneratorU64};

pub struct RowSequence {}

impl RowSequence {
	pub(crate) fn next_row_number(txn: &mut impl SequenceTransaction, table: TableId) -> Result<RowNumber> {
		GeneratorU64::next(txn, &RowSequenceKey::encoded(table), None).map(RowNumber)
	}

	pub(crate) fn next_row_number_batch(
		txn: &mut impl SequenceTransaction,
		table: TableId,
		count: u64,
	) -> Result<Vec<RowNumber>> {
		Self::next_row_number_batch_for_source(txn, StorageId::from(table), count)
	}

	pub(crate) fn next_row_number_for_ringbuffer(
		txn: &mut impl SequenceTransaction,
		ringbuffer: RingBufferId,
	) -> Result<RowNumber> {
		GeneratorU64::next(txn, &RowSequenceKey::encoded(ringbuffer), None).map(RowNumber)
	}

	pub(crate) fn next_row_number_batch_for_ringbuffer(
		txn: &mut impl SequenceTransaction,
		ringbuffer: RingBufferId,
		count: u64,
	) -> Result<Vec<RowNumber>> {
		Self::next_row_number_batch_for_source(txn, StorageId::from(ringbuffer), count)
	}

	pub(crate) fn next_row_number_for_queue(
		txn: &mut impl SequenceTransaction,
		queue: QueueId,
	) -> Result<RowNumber> {
		GeneratorU64::next(txn, &RowSequenceKey::encoded(queue), None).map(RowNumber)
	}

	pub(crate) fn next_row_number_batch_for_queue(
		txn: &mut impl SequenceTransaction,
		queue: QueueId,
		count: u64,
	) -> Result<Vec<RowNumber>> {
		Self::next_row_number_batch_for_source(txn, StorageId::from(queue), count)
	}

	fn next_row_number_batch_for_source(
		txn: &mut impl SequenceTransaction,
		storage: StorageId,
		count: u64,
	) -> Result<Vec<RowNumber>> {
		let last_row_number = GeneratorU64::next_batched(txn, &RowSequenceKey::encoded(storage), None, count)?;

		let first_row_number = last_row_number.saturating_sub(count - 1);

		let row_numbers = (0..count).map(|offset| RowNumber(first_row_number + offset)).collect();

		Ok(row_numbers)
	}
}
