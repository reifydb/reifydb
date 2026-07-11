// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::encoded::{
	row::EncodedRow,
	shape::{RowShape, RowShapeField, fingerprint::RowShapeFingerprint},
};
use reifydb_core::{
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		change::Diff,
	},
	internal,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	util::hash::Hash128,
	value::{Value, datetime::DateTime, row_number::RowNumber, value_type::ValueType},
};

use crate::{
	operator::join::{operator::JoinOperator, state::JoinSide, store::Store},
	transaction::FlowTransaction,
};

#[cfg(test)]
mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowNodeId};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::value::identity::IdentityId;

	use super::*;

	fn h(v: u128) -> Hash128 {
		Hash128(v)
	}

	fn columns_with_fields(fields: &[(&str, i32)], row_number: u64) -> Columns {
		let cols: Vec<ColumnWithName> = fields
			.iter()
			.map(|(name, value)| {
				ColumnWithName::new(Fragment::internal(*name), ColumnBuffer::int4(vec![*value]))
			})
			.collect();
		Columns::new(cols).with_row_numbers(vec![RowNumber(row_number)])
	}

	#[test]
	fn columns_from_block_reads_a_second_key_whose_shape_differs_from_the_first() {
		// Reproduces the production crash end to end through the real join code path:
		// an already-resolved key (e.g. a known token, shape with two fields) is stored
		// first; a freshly-discovered key then arrives with a different field set (e.g.
		// an extra column), giving it a distinct RowShape fingerprint. Reading the
		// second key's rows back must not fail with "Row shape not found in store"
		// just because the first key's shape was the only one this Store instance
		// ever persisted.
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let mut store = Store::new(FlowNodeId(70), JoinSide::Right);

		let key_a = h(0xA);
		let resolved = columns_with_fields(&[("mint", 1), ("decimals", 8)], 1);
		add_to_state_entry_batch(&mut txn, &mut store, &key_a, &resolved, &[0]).unwrap();

		let key_b = h(0xB);
		let freshly_discovered = columns_with_fields(&[("mint", 2), ("decimals", 6), ("bump", 255)], 2);
		add_to_state_entry_batch(&mut txn, &mut store, &key_b, &freshly_discovered, &[0]).unwrap();

		let block_b = store.rows_for_key_block(&mut txn, &key_b, None, 10).unwrap();
		assert_eq!(block_b.len(), 1);
		let read_back =
			columns_from_block(&mut txn, &store, block_b).expect("row shape for key B must be found");
		assert_eq!(read_back.row_count(), 1);
		assert_eq!(read_back.len(), 3, "key B's own 3-field shape must be the one used to decode it");
	}

	#[test]
	fn columns_from_block_decodes_each_row_with_its_own_shape_when_one_key_spans_two_shapes() {
		// Two rows under the SAME join key, written in separate batches whose fields
		// land in a different order (e.g. because the upstream field list is rebuilt
		// per tick, not guaranteed stable), get different RowShapeFingerprints. Each
		// row must be decoded with its own shape, not the first row's.
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let mut store = Store::new(FlowNodeId(71), JoinSide::Right);
		let key = h(0xC);

		// Row 1: written with fields in order [mint, flag].
		let row1 = columns_with_fields(&[("mint", 111), ("flag", 1)], 1);
		add_to_state_entry_batch(&mut txn, &mut store, &key, &row1, &[0]).unwrap();

		// Row 2: same logical fields, but the upstream batch produced them in the
		// OPPOSITE order, giving row 2 a different RowShapeFingerprint even though the
		// column set is identical.
		let row2 = columns_with_fields(&[("flag", 999), ("mint", 222)], 2);
		add_to_state_entry_batch(&mut txn, &mut store, &key, &row2, &[0]).unwrap();

		let block = store.rows_for_key_block(&mut txn, &key, None, 10).unwrap();
		assert_eq!(block.len(), 2);
		let read_back = columns_from_block(&mut txn, &store, block).unwrap();

		let mint = read_back.column("mint").unwrap();
		let flag = read_back.column("flag").unwrap();
		assert_eq!(mint.data().get_value(0), Value::Int4(111));
		assert_eq!(flag.data().get_value(0), Value::Int4(1));
		assert_eq!(
			mint.data().get_value(1),
			Value::Int4(222),
			"row 2's real mint value must be reported under the mint column"
		);
		assert_eq!(
			flag.data().get_value(1),
			Value::Int4(999),
			"row 2's real flag value must be reported under the flag column, not swapped with mint"
		);
	}
}

pub(crate) fn build_shape(columns: &Columns) -> RowShape {
	let fields: Vec<RowShapeField> = columns
		.names
		.iter()
		.zip(columns.columns.iter())
		.map(|(name, buf)| RowShapeField::unconstrained(name.text().to_string(), buf.get_type()))
		.collect();
	RowShape::new(fields)
}

pub(crate) fn encode_row(shape: &RowShape, columns: &Columns, row_idx: usize) -> EncodedRow {
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

fn decode_run(
	txn: &mut FlowTransaction,
	store: &Store,
	fingerprint: RowShapeFingerprint,
	ids: &[RowNumber],
	rows: &[EncodedRow],
) -> Result<Columns> {
	let shape = store
		.get_row_shape(txn, fingerprint)?
		.ok_or_else(|| Error(Box::new(internal!("Row shape not found in store"))))?;
	Ok(Columns::from_encoded_rows(&shape, ids, rows))
}

fn merge_runs(runs: Vec<Columns>) -> Columns {
	let mut names: Vec<String> = Vec::new();
	for run in &runs {
		for name in run.names.iter() {
			let text = name.text().to_string();
			if !names.contains(&text) {
				names.push(text);
			}
		}
	}

	let total: usize = runs.iter().map(|run| run.row_count()).sum();
	let mut result_columns: Vec<ColumnWithName> = Vec::with_capacity(names.len());
	for name in &names {
		let target_type = runs
			.iter()
			.find_map(|run| run.column(name).map(|col| col.data().get_type()))
			.unwrap_or(ValueType::Any);
		let mut buf = ColumnBuffer::with_capacity(target_type, total);
		for run in &runs {
			match run.column(name) {
				Some(col) => {
					for row_idx in 0..run.row_count() {
						buf.push_value(col.data().get_value(row_idx));
					}
				}
				None => {
					for _ in 0..run.row_count() {
						buf.push_value(Value::none());
					}
				}
			}
		}
		result_columns.push(ColumnWithName::new(Fragment::internal(name.as_str()), buf));
	}

	let row_numbers: Vec<RowNumber> = runs.iter().flat_map(|run| run.row_numbers.iter().copied()).collect();
	let created_at: Vec<DateTime> = runs.iter().flat_map(|run| run.created_at.iter().copied()).collect();
	let updated_at: Vec<DateTime> = runs.iter().flat_map(|run| run.updated_at.iter().copied()).collect();

	Columns::with_system_columns(result_columns, row_numbers, created_at, updated_at)
}

pub(crate) fn columns_from_block(
	txn: &mut FlowTransaction,
	store: &Store,
	block: Vec<(RowNumber, EncodedRow)>,
) -> Result<Columns> {
	let mut runs: Vec<Columns> = Vec::new();
	let mut run_fingerprint: Option<RowShapeFingerprint> = None;
	let mut run_ids: Vec<RowNumber> = Vec::new();
	let mut run_rows: Vec<EncodedRow> = Vec::new();

	for (id, row) in block {
		let fingerprint = row.fingerprint();
		if run_fingerprint.is_some_and(|current| current != fingerprint) {
			runs.push(decode_run(txn, store, run_fingerprint.unwrap(), &run_ids, &run_rows)?);
			run_ids.clear();
			run_rows.clear();
		}
		run_fingerprint = Some(fingerprint);
		run_ids.push(id);
		run_rows.push(row);
	}
	if let Some(fingerprint) = run_fingerprint {
		runs.push(decode_run(txn, store, fingerprint, &run_ids, &run_rows)?);
	}

	if runs.len() == 1 {
		return Ok(runs.into_iter().next().unwrap());
	}
	Ok(merge_runs(runs))
}

fn stream_join_blocks<F>(
	txn: &mut FlowTransaction,
	store: &Store,
	key_hash: &Hash128,
	mut join_block: F,
) -> Result<Vec<Diff>>
where
	F: FnMut(&mut FlowTransaction, &Columns) -> Result<Option<Diff>>,
{
	let limit = txn.catalog().get_config_uint8(ConfigKey::FlowJoinProbeBlockSize) as usize;
	let mut out = Vec::new();
	let mut after: Option<RowNumber> = None;
	loop {
		let block = store.rows_for_key_block(txn, key_hash, after.as_ref(), limit)?;
		if block.is_empty() {
			break;
		}
		let last = block.last().unwrap().0;
		let exhausted = block.len() < limit;
		let opposite = columns_from_block(txn, store, block)?;
		if let Some(diff) = join_block(txn, &opposite)? {
			out.push(diff);
		}
		if exhausted {
			break;
		}
		after = Some(last);
	}
	Ok(out)
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
) -> Result<Vec<Diff>> {
	stream_join_blocks(txn, ctx.opposite_store, ctx.key_hash, |txn, opposite| {
		let (pre_joined, post_joined) = match primary_side {
			JoinSide::Left => (
				ctx.operator.join_columns_one_to_many(txn, pre, row_idx, opposite)?,
				ctx.operator.join_columns_one_to_many(txn, post, row_idx, opposite)?,
			),
			JoinSide::Right => (
				ctx.operator.join_columns_many_to_one(txn, opposite, pre, row_idx)?,
				ctx.operator.join_columns_many_to_one(txn, opposite, post, row_idx)?,
			),
		};

		if pre_joined.is_empty() || post_joined.is_empty() {
			Ok(None)
		} else {
			Ok(Some(Diff::update(pre_joined, post_joined)))
		}
	})
}

pub(crate) fn emit_joined_columns_batch(
	txn: &mut FlowTransaction,
	primary: &Columns,
	primary_indices: &[usize],
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Vec<Diff>> {
	if primary_indices.is_empty() {
		return Ok(Vec::new());
	}

	stream_join_blocks(txn, ctx.opposite_store, ctx.key_hash, |txn, opposite| {
		let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
		let joined = match primary_side {
			JoinSide::Left => ctx.operator.join_columns_cartesian(
				txn,
				primary,
				primary_indices,
				opposite,
				&opposite_indices,
			)?,
			JoinSide::Right => ctx.operator.join_columns_cartesian(
				txn,
				opposite,
				&opposite_indices,
				primary,
				primary_indices,
			)?,
		};

		if joined.is_empty() {
			Ok(None)
		} else {
			Ok(Some(Diff::insert(joined)))
		}
	})
}

pub(crate) fn emit_remove_joined_columns_batch(
	txn: &mut FlowTransaction,
	primary: &Columns,
	primary_indices: &[usize],
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Vec<Diff>> {
	if primary_indices.is_empty() {
		return Ok(Vec::new());
	}

	stream_join_blocks(txn, ctx.opposite_store, ctx.key_hash, |txn, opposite| {
		let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
		let joined = match primary_side {
			JoinSide::Left => ctx.operator.join_columns_cartesian(
				txn,
				primary,
				primary_indices,
				opposite,
				&opposite_indices,
			)?,
			JoinSide::Right => ctx.operator.join_columns_cartesian(
				txn,
				opposite,
				&opposite_indices,
				primary,
				primary_indices,
			)?,
		};

		if joined.is_empty() {
			Ok(None)
		} else {
			Ok(Some(Diff::remove(joined)))
		}
	})
}

pub(crate) fn for_each_left_block<F>(
	txn: &mut FlowTransaction,
	left_store: &Store,
	key_hash: &Hash128,
	mut on_block: F,
) -> Result<()>
where
	F: FnMut(&mut FlowTransaction, &Columns) -> Result<()>,
{
	let limit = txn.catalog().get_config_uint8(ConfigKey::FlowJoinProbeBlockSize) as usize;
	let mut after: Option<RowNumber> = None;
	loop {
		let block = left_store.rows_for_key_block(txn, key_hash, after.as_ref(), limit)?;
		if block.is_empty() {
			break;
		}
		let last = block.last().unwrap().0;
		let exhausted = block.len() < limit;
		let left_columns = columns_from_block(txn, left_store, block)?;
		on_block(txn, &left_columns)?;
		if exhausted {
			break;
		}
		after = Some(last);
	}
	Ok(())
}
