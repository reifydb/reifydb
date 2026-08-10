// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::operator::EncodedOperatorRow;
use reifydb_core::value::column::columns::Columns;
use reifydb_value::{Result, util::hash::Hash128, value::row_number::RowNumber};
use tracing::instrument;

use super::hash::{build_shape, columns_from_block, encode_row};
use crate::{
	operator::join::store::{Store, body_bytes},
	transaction::FlowTransaction,
};

#[instrument(name = "flow::operator::join::latest::overwrite_right_slot", level = "trace", skip_all, fields(rows = indices.len()))]
pub(crate) fn overwrite_right_slot(
	txn: &mut FlowTransaction,
	right: &Store,
	key_hash: &Hash128,
	columns: &Columns,
	indices: &[usize],
) -> Result<Option<Columns>> {
	if indices.is_empty() {
		return Ok(None);
	}
	let shape = build_shape(columns);
	right.set_row_shape(txn, &shape)?;
	let mut stored: Option<EncodedOperatorRow> = None;
	for &idx in indices {
		let row = encode_row(&shape, columns, idx, txn.written_at());
		right.put_row(txn, key_hash, RowNumber::MAX, &row)?;
		stored = Some(row);
	}
	match stored {
		Some(row) => Ok(Some(columns_from_block(txn, right, vec![(RowNumber::MAX, body_bytes(&row))])?)),
		None => Ok(None),
	}
}

#[instrument(name = "flow::operator::join::latest::read_right_slot", level = "trace", skip_all)]
pub(crate) fn read_right_slot(txn: &mut FlowTransaction, right: &Store, key_hash: &Hash128) -> Result<Option<Columns>> {
	let Some(group) = right.group_of(txn, key_hash)? else {
		return Ok(None);
	};
	Ok(right.slot(txn, group)?.map(|(_, columns)| columns))
}

#[instrument(name = "flow::operator::join::latest::remove_right_slot", level = "trace", skip_all)]
pub(crate) fn remove_right_slot(txn: &mut FlowTransaction, right: &Store, key_hash: &Hash128) -> Result<()> {
	right.remove_row(txn, key_hash, RowNumber::MAX)?;
	Ok(())
}
