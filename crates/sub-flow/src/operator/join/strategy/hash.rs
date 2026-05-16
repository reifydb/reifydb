// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
	operator::join::{
		operator::JoinOperator,
		state::{JoinSide, JoinSideEntry},
		store::Store,
	},
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

fn decode_entry(txn: &mut FlowTransaction, store: &Store, entry: &JoinSideEntry) -> Result<Columns> {
	if entry.rows.is_empty() {
		return Ok(Columns::empty());
	}
	let shape =
		store.get_row_shape(txn)?.ok_or_else(|| Error(Box::new(internal!("Row shape not found in store"))))?;
	let ids: Vec<RowNumber> = entry.rows.iter().map(|(rn, _)| *rn).collect();
	let encoded: Vec<EncodedRow> = entry.rows.iter().map(|(_, r)| r.clone()).collect();
	Ok(Columns::from_encoded_rows(&shape, &ids, &encoded))
}

pub(crate) fn add_to_state_entry(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	columns: &Columns,
	row_idx: usize,
) -> Result<()> {
	let shape = build_shape(columns);
	store.set_row_shape(txn, &shape)?;
	let encoded = encode_row(&shape, columns, row_idx);
	let mut entry = store.get_or_insert_with(txn, key_hash, JoinSideEntry::default)?;
	entry.rows.push((columns.row_numbers[row_idx], encoded));
	store.set(txn, key_hash, &entry)?;
	Ok(())
}

pub(crate) fn add_to_state_entry_batch(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	columns: &Columns,
	indices: &[usize],
) -> Result<()> {
	let shape = build_shape(columns);
	store.set_row_shape(txn, &shape)?;
	let mut entry = store.get_or_insert_with(txn, key_hash, JoinSideEntry::default)?;
	for &idx in indices {
		entry.rows.push((columns.row_numbers[idx], encode_row(&shape, columns, idx)));
	}
	store.set(txn, key_hash, &entry)?;
	Ok(())
}

pub(crate) fn remove_from_state_entry(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	row_number: RowNumber,
) -> Result<bool> {
	if let Some(mut entry) = store.get(txn, key_hash)? {
		entry.rows.retain(|(rn, _)| *rn != row_number);

		if entry.rows.is_empty() {
			store.remove(txn, key_hash)?;
			Ok(true)
		} else {
			store.set(txn, key_hash, &entry)?;
			Ok(false)
		}
	} else {
		Ok(false)
	}
}

pub(crate) fn update_row_in_entry(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	pre_row_number: RowNumber,
	post: &Columns,
	row_idx: usize,
) -> Result<bool> {
	if let Some(mut entry) = store.get(txn, key_hash)?
		&& let Some(slot) = entry.rows.iter_mut().find(|(rn, _)| *rn == pre_row_number)
	{
		let shape = build_shape(post);
		store.set_row_shape(txn, &shape)?;
		*slot = (post.row_numbers[row_idx], encode_row(&shape, post, row_idx));
		store.set(txn, key_hash, &entry)?;
		return Ok(true);
	}
	Ok(false)
}

pub(crate) fn has_right_rows(txn: &mut FlowTransaction, right_store: &Store, key_hash: &Hash128) -> Result<bool> {
	right_store.contains_key(txn, key_hash)
}

pub(crate) fn is_first_right_row(txn: &mut FlowTransaction, right_store: &Store, key_hash: &Hash128) -> Result<bool> {
	Ok(!right_store.contains_key(txn, key_hash)?)
}

pub(crate) fn pull_from_store(txn: &mut FlowTransaction, store: &Store, key_hash: &Hash128) -> Result<Columns> {
	if let Some(entry) = store.get(txn, key_hash)? {
		decode_entry(txn, store, &entry)
	} else {
		Ok(Columns::empty())
	}
}

pub(crate) struct JoinEmitContext<'a> {
	pub opposite_store: &'a Store,
	pub key_hash: &'a Hash128,
	pub operator: &'a JoinOperator,
}

pub(crate) fn emit_joined_columns(
	txn: &mut FlowTransaction,
	primary: &Columns,
	primary_idx: usize,
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Option<Diff>> {
	let opposite = pull_from_store(txn, ctx.opposite_store, ctx.key_hash)?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let joined = match primary_side {
		JoinSide::Left => ctx.operator.join_columns_one_to_many(txn, primary, primary_idx, &opposite)?,
		JoinSide::Right => ctx.operator.join_columns_many_to_one(txn, &opposite, primary, primary_idx)?,
	};

	if joined.is_empty() {
		Ok(None)
	} else {
		Ok(Some(Diff::insert(joined)))
	}
}

pub(crate) fn emit_remove_joined_columns(
	txn: &mut FlowTransaction,
	primary: &Columns,
	primary_idx: usize,
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Option<Diff>> {
	let opposite = pull_from_store(txn, ctx.opposite_store, ctx.key_hash)?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let joined = match primary_side {
		JoinSide::Left => ctx.operator.join_columns_one_to_many(txn, primary, primary_idx, &opposite)?,
		JoinSide::Right => ctx.operator.join_columns_many_to_one(txn, &opposite, primary, primary_idx)?,
	};

	if joined.is_empty() {
		Ok(None)
	} else {
		Ok(Some(Diff::remove(joined)))
	}
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
