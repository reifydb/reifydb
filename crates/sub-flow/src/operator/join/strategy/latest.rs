// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_runtime::hash::Hash128;
use reifydb_value::{Result, value::row_number::RowNumber};

use super::hash::{build_shape, columns_from_block, encode_row};
use crate::{operator::join::store::Store, transaction::FlowTransaction};

pub(crate) fn overwrite_right_slot(
	txn: &mut FlowTransaction,
	right: &Store,
	key_hash: &Hash128,
	columns: &Columns,
	indices: &[usize],
) -> Result<()> {
	if indices.is_empty() {
		return Ok(());
	}
	let shape = build_shape(columns);
	right.set_row_shape(txn, &shape)?;
	for &idx in indices {
		let encoded = encode_row(&shape, columns, idx);
		right.put_row(txn, key_hash, RowNumber::MAX, &encoded)?;
	}
	Ok(())
}

pub(crate) fn read_right_slot(txn: &mut FlowTransaction, right: &Store, key_hash: &Hash128) -> Result<Option<Columns>> {
	match right.get_row(txn, key_hash, RowNumber::MAX)? {
		Some(row) => Ok(Some(columns_from_block(txn, right, vec![(RowNumber::MAX, row)])?)),
		None => Ok(None),
	}
}

pub(crate) fn remove_right_slot(txn: &mut FlowTransaction, right: &Store, key_hash: &Hash128) -> Result<()> {
	right.remove_row(txn, key_hash, RowNumber::MAX)?;
	Ok(())
}
