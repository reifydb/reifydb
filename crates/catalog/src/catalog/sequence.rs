// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	id::{ColumnId, RingBufferId, SequenceId, TableId},
	primitive::PrimitiveId,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::{Value, row_number::RowNumber};
use tracing::instrument;

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	store::sequence::{Sequence, column::ColumnSequence, generator::SequenceTransaction, row::RowSequence},
};

impl Catalog {
	#[instrument(name = "catalog::sequence::find", level = "trace", skip(self, txn))]
	pub fn find_sequence(&self, txn: &mut Transaction<'_>, id: SequenceId) -> Result<Option<Sequence>> {
		CatalogStore::find_sequence(txn, id)
	}

	#[instrument(name = "catalog::sequence::get", level = "trace", skip(self, txn))]
	pub fn get_sequence(&self, txn: &mut Transaction<'_>, id: SequenceId) -> Result<Sequence> {
		CatalogStore::get_sequence(txn, id)
	}

	#[instrument(name = "catalog::sequence::list", level = "debug", skip(self, txn))]
	pub fn list_sequences(&self, txn: &mut Transaction<'_>) -> Result<Vec<Sequence>> {
		CatalogStore::list_sequences(txn)
	}

	#[instrument(name = "catalog::sequence::next_row_number", level = "trace", skip(self, txn))]
	pub fn next_row_number(&self, txn: &mut impl SequenceTransaction, table: TableId) -> Result<RowNumber> {
		RowSequence::next_row_number(txn, table)
	}

	#[instrument(name = "catalog::sequence::next_row_number_batch", level = "trace", skip(self, txn))]
	pub fn next_row_number_batch(
		&self,
		txn: &mut impl SequenceTransaction,
		table: TableId,
		count: u64,
	) -> Result<Vec<RowNumber>> {
		RowSequence::next_row_number_batch(txn, table, count)
	}

	#[instrument(name = "catalog::sequence::next_row_number_for_ringbuffer", level = "trace", skip(self, txn))]
	pub fn next_row_number_for_ringbuffer(
		&self,
		txn: &mut impl SequenceTransaction,
		ringbuffer: RingBufferId,
	) -> Result<RowNumber> {
		RowSequence::next_row_number_for_ringbuffer(txn, ringbuffer)
	}

	#[instrument(
		name = "catalog::sequence::next_row_number_batch_for_ringbuffer",
		level = "trace",
		skip(self, txn)
	)]
	pub fn next_row_number_batch_for_ringbuffer(
		&self,
		txn: &mut impl SequenceTransaction,
		ringbuffer: RingBufferId,
		count: u64,
	) -> Result<Vec<RowNumber>> {
		RowSequence::next_row_number_batch_for_ringbuffer(txn, ringbuffer, count)
	}

	#[instrument(name = "catalog::sequence::column_sequence_next_value", level = "trace", skip(self, txn, source))]
	pub fn column_sequence_next_value(
		&self,
		txn: &mut impl SequenceTransaction,
		source: impl Into<PrimitiveId>,
		column: ColumnId,
	) -> Result<Value> {
		ColumnSequence::next_value(txn, source, column)
	}

	#[instrument(name = "catalog::sequence::column_sequence_set_value", level = "trace", skip(self, txn, source))]
	pub fn column_sequence_set_value(
		&self,
		txn: &mut impl SequenceTransaction,
		source: impl Into<PrimitiveId>,
		column: ColumnId,
		value: Value,
	) -> Result<()> {
		ColumnSequence::set_value(txn, source, column, value)
	}
}
