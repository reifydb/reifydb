// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	encoded::{
		row::EncodedRow,
		shape::{RowShape, RowShapeField},
	},
	interface::change::Diff,
	internal,
	value::column::columns::Columns,
};
use reifydb_runtime::hash::Hash128;
use reifydb_type::{
	Result,
	error::Error,
	value::{Value, row_number::RowNumber},
};

use crate::{
	operator::join::{operator::JoinOperator, state::JoinSide, store::Store},
	transaction::FlowTransaction,
};

fn build_shape(columns: &Columns) -> RowShape {
	let fields: Vec<RowShapeField> = columns
		.names
		.iter()
		.zip(columns.columns.iter())
		.map(|(name, buf)| RowShapeField::unconstrained(name.text().to_string(), buf.get_type()))
		.collect();
	RowShape::new(fields)
}

fn encode_row(shape: &RowShape, columns: &Columns, row_idx: usize) -> EncodedRow {
	let values: Vec<Value> = columns.columns.iter().map(|buf| buf.get_value(row_idx)).collect();
	let mut encoded = shape.allocate();
	shape.set_values(&mut encoded, &values);
	encoded
}

pub(crate) fn add_to_state_entry_batch(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	columns: &Columns,
	indices: &[usize],
) -> Result<()> {
	if indices.is_empty() {
		return Ok(());
	}
	let shape = build_shape(columns);
	store.set_row_shape(txn, &shape)?;
	for &idx in indices {
		let encoded = encode_row(&shape, columns, idx);
		store.put_row(txn, key_hash, columns.row_numbers[idx], &encoded)?;
	}
	Ok(())
}

pub(crate) fn remove_from_state_entry(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	row_number: RowNumber,
) -> Result<bool> {
	let removed = store.remove_row(txn, key_hash, row_number)?;
	if !removed {
		return Ok(false);
	}
	Ok(!store.contains_key(txn, key_hash)?)
}

pub(crate) fn update_row_in_entry(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	pre_row_number: RowNumber,
	post: &Columns,
	row_idx: usize,
) -> Result<bool> {
	let shape = build_shape(post);
	store.set_row_shape(txn, &shape)?;
	let encoded = encode_row(&shape, post, row_idx);
	let post_row_number = post.row_numbers[row_idx];
	if pre_row_number == post_row_number {
		store.update_row(txn, key_hash, post_row_number, &encoded)
	} else {
		if !store.remove_row(txn, key_hash, pre_row_number)? {
			return Ok(false);
		}
		store.put_row(txn, key_hash, post_row_number, &encoded)?;
		Ok(true)
	}
}

pub(crate) fn is_first_right_row(txn: &mut FlowTransaction, right_store: &Store, key_hash: &Hash128) -> Result<bool> {
	Ok(!right_store.contains_key(txn, key_hash)?)
}

pub(crate) fn pull_from_store(txn: &mut FlowTransaction, store: &Store, key_hash: &Hash128) -> Result<Columns> {
	let rows = store.rows_for_key(txn, key_hash)?;
	if rows.is_empty() {
		return Ok(Columns::empty());
	}
	let ids: Vec<RowNumber> = rows.iter().map(|(rn, _)| *rn).collect();
	let encoded: Vec<EncodedRow> = rows.into_iter().map(|(_, r)| r).collect();
	let fingerprint = encoded[0].fingerprint();
	let shape = store
		.get_row_shape(txn, fingerprint)?
		.ok_or_else(|| Error(Box::new(internal!("Row shape not found in store"))))?;
	Ok(Columns::from_encoded_rows(&shape, &ids, &encoded))
}

pub(crate) struct JoinEmitContext<'a> {
	pub opposite_store: &'a Store,
	pub key_hash: &'a Hash128,
	pub operator: &'a JoinOperator,
}

pub(crate) fn emit_update_joined_columns(
	txn: &mut FlowTransaction,
	pre: &Columns,
	post: &Columns,
	row_idx: usize,
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Option<Diff>> {
	let opposite = pull_from_store(txn, ctx.opposite_store, ctx.key_hash)?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let (pre_joined, post_joined) = match primary_side {
		JoinSide::Left => (
			ctx.operator.join_columns_one_to_many(txn, pre, row_idx, &opposite)?,
			ctx.operator.join_columns_one_to_many(txn, post, row_idx, &opposite)?,
		),
		JoinSide::Right => (
			ctx.operator.join_columns_many_to_one(txn, &opposite, pre, row_idx)?,
			ctx.operator.join_columns_many_to_one(txn, &opposite, post, row_idx)?,
		),
	};

	if pre_joined.is_empty() || post_joined.is_empty() {
		Ok(None)
	} else {
		Ok(Some(Diff::update(pre_joined, post_joined)))
	}
}

pub(crate) fn emit_joined_columns_batch(
	txn: &mut FlowTransaction,
	primary: &Columns,
	primary_indices: &[usize],
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Option<Diff>> {
	if primary_indices.is_empty() {
		return Ok(None);
	}

	let opposite = pull_from_store(txn, ctx.opposite_store, ctx.key_hash)?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let joined = match primary_side {
		JoinSide::Left => {
			let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
			ctx.operator.join_columns_cartesian(
				txn,
				primary,
				primary_indices,
				&opposite,
				&opposite_indices,
			)?
		}
		JoinSide::Right => {
			let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
			ctx.operator.join_columns_cartesian(
				txn,
				&opposite,
				&opposite_indices,
				primary,
				primary_indices,
			)?
		}
	};

	if joined.is_empty() {
		Ok(None)
	} else {
		Ok(Some(Diff::insert(joined)))
	}
}

pub(crate) fn emit_remove_joined_columns_batch(
	txn: &mut FlowTransaction,
	primary: &Columns,
	primary_indices: &[usize],
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Option<Diff>> {
	if primary_indices.is_empty() {
		return Ok(None);
	}

	let opposite = pull_from_store(txn, ctx.opposite_store, ctx.key_hash)?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let joined = match primary_side {
		JoinSide::Left => {
			let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
			ctx.operator.join_columns_cartesian(
				txn,
				primary,
				primary_indices,
				&opposite,
				&opposite_indices,
			)?
		}
		JoinSide::Right => {
			let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
			ctx.operator.join_columns_cartesian(
				txn,
				&opposite,
				&opposite_indices,
				primary,
				primary_indices,
			)?
		}
	};

	if joined.is_empty() {
		Ok(None)
	} else {
		Ok(Some(Diff::remove(joined)))
	}
}

pub(crate) fn pull_left_columns(txn: &mut FlowTransaction, left_store: &Store, key_hash: &Hash128) -> Result<Columns> {
	pull_from_store(txn, left_store, key_hash)
}
