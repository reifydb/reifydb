// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{
	bytes::EncodedBytes,
	envelope::{Envelope, EnvelopeBuilder},
	pod::EncodedPodRow,
	shape::{RowFamily, RowShape, RowShapeField, fingerprint::RowShapeFingerprint},
};
use reifydb_core::{
	interface::{catalog::config::ConfigKey, change::Diff},
	internal,
	key::operator_state::GroupId,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	util::{cowvec::CowVec, hash::Hash128},
	value::{
		Value, datetime::DateTime, row_number::RowNumber, system_columns::SystemColumns, value_type::ValueType,
	},
};
use tracing::{Span, instrument};

use crate::operator::{
	host::HostContext,
	join::{Identity, operator::JoinOperator, state::JoinSide, store::Store},
};

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::flow::OperatorId;
	use reifydb_test_harness::engine::TestEngine;

	use super::*;
	use crate::{
		operator::host::TxnHostContext,
		transaction::{deferred::DeferredTransaction, mock::FlowTxn},
	};

	fn h(v: u128) -> Hash128 {
		Hash128(v)
	}

	fn host(txn: &mut DeferredTransaction, operator: OperatorId) -> TxnHostContext<'_, DeferredTransaction> {
		TxnHostContext::new(txn, operator)
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

	fn columns_with_time(fields: &[(&str, i32)], row_number: u64, time: Option<DateTime>) -> Columns {
		// with_row_numbers backfills a default #time, so a timeless row must bypass it or it arrives timed.
		let cols: Vec<ColumnWithName> = fields
			.iter()
			.map(|(name, value)| {
				ColumnWithName::new(Fragment::internal(*name), ColumnBuffer::int4(vec![*value]))
			})
			.collect();
		Columns::with_system(
			cols,
			SystemColumns::new(
				vec![RowNumber(row_number)],
				Vec::new(),
				Vec::new(),
				Vec::new(),
				time.into_iter().collect(),
			),
		)
	}

	#[test]
	fn a_timeless_join_row_pays_for_one_instant_and_decodes_it_into_both_stamps() {
		// Join stores exactly one instant per buffered row; a third envelope field would cost 25 B, not 17.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(72);
		let store = Store::new(JoinSide::Right);

		let now = DateTime::from_nanos(1_700_000_000_000_000_000);
		let columns = columns_with_time(&[("mint", 7)], 1, None);
		let shape = build_shape(&columns);
		store.set_row_shape(&mut host(&mut txn, operator), &shape).unwrap();

		let row = encode_row(&shape, &columns, 0, now);
		let envelope = Envelope::try_view(&row).unwrap();
		assert_eq!(envelope.header_size(), 17, "flags byte plus a fingerprint plus exactly one instant");
		assert_eq!(envelope.fingerprint(), Some(shape.fingerprint()));
		assert_eq!(envelope.created_at(), Some(now));
		assert_eq!(envelope.time(), None);
		assert_eq!(envelope.updated_at(), None, "a second stamp would widen every buffered row by 8 bytes");

		let decoded = decode_run(
			&mut host(&mut txn, operator),
			&store,
			shape.fingerprint(),
			&[RowNumber(1)],
			&[row.into_bytes()],
		)
		.unwrap();
		assert_eq!(decoded.created_at(), &[now][..]);
		assert_eq!(decoded.updated_at(), &[now][..], "both stamps are synthesized from the one stored instant");
		assert!(decoded.time().is_empty(), "a row that carried no #time must not gain one on the way back");
		assert_eq!(decoded.column("mint").unwrap().data().get_value(0), Value::Int4(7));
	}

	#[test]
	fn a_timed_join_row_stores_its_time_in_the_single_instant_slot_and_still_pays_seventeen_bytes() {
		// The row's #time replaces the write stamp rather than joining it, so a timed row is never wider.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(73);
		let store = Store::new(JoinSide::Right);

		let now = DateTime::from_nanos(1_700_000_000_000_000_000);
		let event = DateTime::from_nanos(1_600_000_000_000_000_000);
		let columns = columns_with_time(&[("mint", 9)], 2, Some(event));
		let shape = build_shape(&columns);
		store.set_row_shape(&mut host(&mut txn, operator), &shape).unwrap();

		let row = encode_row(&shape, &columns, 0, now);
		let envelope = Envelope::try_view(&row).unwrap();
		assert_eq!(envelope.header_size(), 17, "a timed row must cost the same as a timeless one");
		assert_eq!(envelope.fingerprint(), Some(shape.fingerprint()));
		assert_eq!(envelope.time(), Some(event));
		assert_eq!(envelope.created_at(), None, "the write stamp is dropped, never stored beside the time");
		assert_eq!(envelope.updated_at(), None);

		let decoded = decode_run(
			&mut host(&mut txn, operator),
			&store,
			shape.fingerprint(),
			&[RowNumber(2)],
			&[row.into_bytes()],
		)
		.unwrap();
		assert_eq!(decoded.created_at(), &[event][..]);
		assert_eq!(decoded.updated_at(), &[event][..]);
		assert_eq!(decoded.time(), &[event][..]);
	}

	#[test]
	fn a_run_mixing_timed_and_timeless_rows_lists_only_the_timed_rows_in_the_time_column() {
		// The time vector is a filter_map over the run, so it stays shorter than the two stamp vectors.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(74);
		let store = Store::new(JoinSide::Right);

		let now = DateTime::from_nanos(1_700_000_000_000_000_000);
		let event = DateTime::from_nanos(1_600_000_000_000_000_000);
		let first = columns_with_time(&[("mint", 1)], 1, None);
		let second = columns_with_time(&[("mint", 2)], 2, Some(event));
		let third = columns_with_time(&[("mint", 3)], 3, None);
		let shape = build_shape(&first);
		store.set_row_shape(&mut host(&mut txn, operator), &shape).unwrap();

		let rows: Vec<EncodedBytes> = [&first, &second, &third]
			.into_iter()
			.map(|columns| encode_row(&shape, columns, 0, now).into_bytes())
			.collect();

		let decoded = decode_run(
			&mut host(&mut txn, operator),
			&store,
			shape.fingerprint(),
			&[RowNumber(1), RowNumber(2), RowNumber(3)],
			&rows,
		)
		.unwrap();
		assert_eq!(decoded.created_at(), &[now, event, now][..]);
		assert_eq!(decoded.updated_at(), &[now, event, now][..]);
		assert_eq!(decoded.time(), &[event][..], "only the row that carried a #time may appear here");
	}

	#[test]
	fn columns_from_block_reads_a_second_key_whose_shape_differs_from_the_first() {
		// A key arriving with an extra column gets its own shape fingerprint, and reading it back
		// must not fail just because the first key's shape was the only one this Store instance
		// ever persisted.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(70);
		let mut store = Store::new(JoinSide::Right);

		let key_a = h(0xA);
		let resolved = columns_with_fields(&[("mint", 1), ("decimals", 8)], 1);
		add_to_state_entry_batch(&mut host(&mut txn, operator), &mut store, &key_a, &resolved, &[0]).unwrap();

		let key_b = h(0xB);
		let freshly_discovered = columns_with_fields(&[("mint", 2), ("decimals", 6), ("bump", 255)], 2);
		add_to_state_entry_batch(&mut host(&mut txn, operator), &mut store, &key_b, &freshly_discovered, &[0])
			.unwrap();

		let block_b = store.rows_for_key(&mut host(&mut txn, operator), &key_b, None, 10).unwrap();
		assert_eq!(block_b.len(), 1);
		let read_back = columns_from_block(&mut host(&mut txn, operator), &store, block_b)
			.expect("row shape for key B must be found");
		assert_eq!(read_back.row_count(), 1);
		assert_eq!(read_back.len(), 3, "key B's own 3-field shape must be the one used to decode it");
	}

	#[test]
	fn columns_from_block_decodes_each_row_with_its_own_shape_when_one_key_spans_two_shapes() {
		// An upstream field list rebuilt per tick is not order-stable, so two rows under one key
		// can carry different shape fingerprints and each must decode with its own.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(71);
		let mut store = Store::new(JoinSide::Right);
		let key = h(0xC);

		let row1 = columns_with_fields(&[("mint", 111), ("flag", 1)], 1);
		add_to_state_entry_batch(&mut host(&mut txn, operator), &mut store, &key, &row1, &[0]).unwrap();

		// The same column set in the opposite order, which is a different fingerprint.
		let row2 = columns_with_fields(&[("flag", 999), ("mint", 222)], 2);
		add_to_state_entry_batch(&mut host(&mut txn, operator), &mut store, &key, &row2, &[0]).unwrap();

		let block = store.rows_for_key(&mut host(&mut txn, operator), &key, None, 10).unwrap();
		assert_eq!(block.len(), 2);
		let read_back = columns_from_block(&mut host(&mut txn, operator), &store, block).unwrap();

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
	RowShape::new(RowFamily::Pod, fields)
}

pub(crate) fn encode_row(shape: &RowShape, columns: &Columns, row_idx: usize, now: DateTime) -> EncodedPodRow {
	let values: Vec<Value> = columns.columns.iter().map(|buf| buf.get_value(row_idx)).collect();
	let mut encoded = shape.allocate_pod();
	shape.set_values(&mut encoded, &values);
	let at = columns.time().get(row_idx).copied();
	let envelope = EnvelopeBuilder::new().fingerprint(shape.fingerprint());
	let envelope = match at {
		Some(time) => envelope.time(time),
		None => envelope.created_at(now),
	};
	envelope.build(encoded.freeze().as_slice())
}

#[instrument(name = "flow::operator::join::add_state_entry", level = "trace", skip_all)]
pub(crate) fn add_to_state_entry_batch(
	host: &mut dyn HostContext,
	store: &mut Store,
	key_hash: &Hash128,
	columns: &Columns,
	indices: &[usize],
) -> Result<()> {
	if indices.is_empty() {
		return Ok(());
	}
	let shape = build_shape(columns);
	store.set_row_shape(host, &shape)?;
	let group = store.group_for(host, key_hash)?;
	for &idx in indices {
		let row = encode_row(&shape, columns, idx, host.written_at());
		store.write_row(host, group, columns.row_numbers()[idx], &row)?;
	}
	Ok(())
}

pub(crate) struct EntryUpdate {
	group: GroupId,
	shape: RowShape,
}

pub(crate) fn prepare_entry_update(
	host: &mut dyn HostContext,
	store: &Store,
	key_hash: &Hash128,
	post: &Columns,
) -> Result<Option<EntryUpdate>> {
	let shape = build_shape(post);
	store.set_row_shape(host, &shape)?;
	let Some(group) = store.group_of(host, key_hash)? else {
		return Ok(None);
	};
	Ok(Some(EntryUpdate {
		group,
		shape,
	}))
}

pub(crate) fn update_row_in_entry(
	host: &mut dyn HostContext,
	store: &Store,
	prepared: &EntryUpdate,
	pre_row_number: RowNumber,
	post: &Columns,
	row_idx: usize,
) -> Result<bool> {
	let row = encode_row(&prepared.shape, post, row_idx, host.written_at());
	let post_row_number = post.row_numbers()[row_idx];
	if pre_row_number == post_row_number {
		store.update_row_in(host, prepared.group, post_row_number, &row)
	} else {
		if store.get_row_in(host, prepared.group, pre_row_number)?.is_none() {
			return Ok(false);
		}
		store.remove_row_in(host, prepared.group, pre_row_number)?;
		store.write_row(host, prepared.group, post_row_number, &row)?;
		Ok(true)
	}
}

pub(crate) fn update_single_row_in_entry(
	host: &mut dyn HostContext,
	store: &Store,
	key_hash: &Hash128,
	pre_row_number: RowNumber,
	post: &Columns,
	row_idx: usize,
) -> Result<bool> {
	let shape = build_shape(post);
	store.set_row_shape(host, &shape)?;
	let row = encode_row(&shape, post, row_idx, host.written_at());
	let post_row_number = post.row_numbers()[row_idx];
	if pre_row_number == post_row_number {
		store.update_row(host, key_hash, post_row_number, &row)
	} else {
		if !store.remove_row(host, key_hash, pre_row_number)? {
			return Ok(false);
		}
		store.put_row(host, key_hash, post_row_number, &row)?;
		Ok(true)
	}
}

pub(crate) fn is_first_right_row(host: &mut dyn HostContext, right_store: &Store, key_hash: &Hash128) -> Result<bool> {
	Ok(!right_store.contains_key(host, key_hash)?)
}

#[instrument(name = "flow::operator::join::decode_run", level = "trace", skip_all, fields(rows = bytes_slice.len()))]
fn decode_run(
	host: &mut dyn HostContext,
	store: &Store,
	fingerprint: RowShapeFingerprint,
	ids: &[RowNumber],
	bytes_slice: &[EncodedBytes],
) -> Result<Columns> {
	let shape = store
		.get_row_shape(host, fingerprint)?
		.ok_or_else(|| Error(Box::new(internal!("Row shape not found in store"))))?;
	let mut envelopes: Vec<&Envelope> = Vec::with_capacity(bytes_slice.len());
	for bytes in bytes_slice {
		envelopes.push(Envelope::try_view(EncodedPodRow::view(bytes))?);
	}
	let bodies: Vec<EncodedBytes> =
		envelopes.iter().map(|envelope| EncodedBytes(CowVec::new(envelope.body().to_vec()))).collect();

	let mut decoded = Columns::from_encoded_bytes(&shape, ids, &bodies);
	let instants: Vec<DateTime> = envelopes
		.iter()
		.map(|envelope| envelope.time().or_else(|| envelope.created_at()).unwrap_or_default())
		.collect();
	let time: Vec<DateTime> = envelopes.iter().filter_map(|envelope| envelope.time()).collect();
	decoded.system = SystemColumns::new(ids.to_vec(), Vec::new(), instants.clone(), instants, time);

	Ok(decoded)
}

#[instrument(name = "flow::operator::join::merge_runs", level = "trace", skip_all, fields(runs = runs.len()))]
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

	let row_numbers: Vec<RowNumber> = runs.iter().flat_map(|run| run.row_numbers().iter().copied()).collect();
	let created_at: Vec<DateTime> = runs.iter().flat_map(|run| run.created_at().iter().copied()).collect();
	let updated_at: Vec<DateTime> = runs.iter().flat_map(|run| run.updated_at().iter().copied()).collect();
	let time: Vec<DateTime> = runs.iter().flat_map(|run| run.time().iter().copied()).collect();

	Columns::with_system(result_columns, SystemColumns::new(row_numbers, Vec::new(), created_at, updated_at, time))
}

#[instrument(name = "flow::operator::join::columns_from_block", level = "trace", skip_all, fields(rows = block.len()))]
pub(crate) fn columns_from_block(
	host: &mut dyn HostContext,
	store: &Store,
	block: Vec<(RowNumber, EncodedBytes)>,
) -> Result<Columns> {
	let mut runs: Vec<Columns> = Vec::new();
	let mut run_fingerprint: Option<RowShapeFingerprint> = None;
	let mut run_ids: Vec<RowNumber> = Vec::new();
	let mut run: Vec<EncodedBytes> = Vec::new();

	for (id, row) in block {
		let fingerprint = Envelope::try_view(EncodedPodRow::view(&row))?
			.fingerprint()
			.ok_or_else(|| Error(Box::new(internal!("Join state row carries no shape fingerprint"))))?;
		if run_fingerprint.is_some_and(|current| current != fingerprint) {
			runs.push(decode_run(host, store, run_fingerprint.unwrap(), &run_ids, &run)?);
			run_ids.clear();
			run.clear();
		}
		run_fingerprint = Some(fingerprint);
		run_ids.push(id);
		run.push(row);
	}
	if let Some(fingerprint) = run_fingerprint {
		runs.push(decode_run(host, store, fingerprint, &run_ids, &run)?);
	}

	if runs.len() == 1 {
		return Ok(runs.into_iter().next().unwrap());
	}
	Ok(merge_runs(runs))
}

fn stream_join_blocks<F>(
	host: &mut dyn HostContext,
	store: &Store,
	key_hash: &Hash128,
	join_block: F,
) -> Result<Vec<Diff>>
where
	F: FnMut(&mut dyn HostContext, &Columns) -> Result<Vec<Diff>>,
{
	let mut join_block = join_block;
	stream_join_blocks_encoded(host, store, key_hash, false, |host, opposite, _| join_block(host, opposite))
}

#[instrument(name = "flow::operator::join::probe", level = "trace", skip_all, fields(blocks = tracing::field::Empty, rows = tracing::field::Empty))]
pub(crate) fn stream_join_blocks_encoded<F>(
	host: &mut dyn HostContext,
	store: &Store,
	key_hash: &Hash128,
	want_encoded: bool,
	mut join_block: F,
) -> Result<Vec<Diff>>
where
	F: FnMut(&mut dyn HostContext, &Columns, &[(RowNumber, EncodedBytes)]) -> Result<Vec<Diff>>,
{
	let limit = host.config_uint8(ConfigKey::FlowJoinProbeBlockSize) as usize;
	let mut out = Vec::new();
	let mut after: Option<RowNumber> = None;
	let mut blocks = 0u64;
	let mut rows = 0u64;
	loop {
		let block = store.rows_for_key(host, key_hash, after.as_ref(), limit)?;
		if block.is_empty() {
			break;
		}
		blocks += 1;
		rows += block.len() as u64;
		let last = block.last().unwrap().0;
		let exhausted = block.len() < limit;
		let encoded = match want_encoded {
			true => block.clone(),
			false => Vec::new(),
		};
		let opposite = columns_from_block(host, store, block)?;
		out.extend(join_block(host, &opposite, &encoded)?);
		if exhausted {
			break;
		}
		after = Some(last);
	}
	let span = Span::current();
	span.record("blocks", blocks);
	span.record("rows", rows);
	Ok(out)
}

pub(crate) struct JoinEmitContext<'a> {
	pub opposite_store: &'a Store,
	pub key_hash: &'a Hash128,
	pub operator: &'a JoinOperator,
}

#[instrument(name = "flow::operator::join::emit_update_joined", level = "trace", skip_all)]
pub(crate) fn emit_update_joined_columns(
	host: &mut dyn HostContext,
	pre: &Columns,
	post: &Columns,
	row_idx: usize,
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Vec<Diff>> {
	stream_join_blocks(host, ctx.opposite_store, ctx.key_hash, |host, opposite| {
		let (pre_joined, post_joined) = match primary_side {
			JoinSide::Left => (
				ctx.operator.join_columns_one_to_many(
					host,
					pre,
					row_idx,
					opposite,
					Identity::Existing,
				)?,
				ctx.operator.join_columns_one_to_many(
					host,
					post,
					row_idx,
					opposite,
					Identity::Existing,
				)?,
			),
			JoinSide::Right => (
				ctx.operator.join_columns_many_to_one(
					host,
					opposite,
					pre,
					row_idx,
					Identity::Existing,
				)?,
				ctx.operator.join_columns_many_to_one(
					host,
					opposite,
					post,
					row_idx,
					Identity::Existing,
				)?,
			),
		};

		if pre_joined.is_empty() || post_joined.is_empty() {
			Ok(Vec::new())
		} else {
			Ok(vec![Diff::update(pre_joined.existing, post_joined.existing)])
		}
	})
}

#[instrument(name = "flow::operator::join::emit_joined", level = "trace", skip_all)]
pub(crate) fn emit_joined_columns_batch(
	host: &mut dyn HostContext,
	primary: &Columns,
	primary_indices: &[usize],
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Vec<Diff>> {
	if primary_indices.is_empty() {
		return Ok(Vec::new());
	}

	stream_join_blocks(host, ctx.opposite_store, ctx.key_hash, |host, opposite| {
		let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
		let joined = match primary_side {
			JoinSide::Left => ctx.operator.join_columns_cartesian(
				host,
				primary,
				primary_indices,
				opposite,
				&opposite_indices,
				Identity::Mint,
			)?,
			JoinSide::Right => ctx.operator.join_columns_cartesian(
				host,
				opposite,
				&opposite_indices,
				primary,
				primary_indices,
				Identity::Mint,
			)?,
		};

		Ok(joined.published())
	})
}

#[instrument(name = "flow::operator::join::emit_remove_joined", level = "trace", skip_all)]
pub(crate) fn emit_remove_joined_columns_batch(
	host: &mut dyn HostContext,
	primary: &Columns,
	primary_indices: &[usize],
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Vec<Diff>> {
	if primary_indices.is_empty() {
		return Ok(Vec::new());
	}

	stream_join_blocks(host, ctx.opposite_store, ctx.key_hash, |host, opposite| {
		let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
		let joined = match primary_side {
			JoinSide::Left => ctx.operator.join_columns_cartesian(
				host,
				primary,
				primary_indices,
				opposite,
				&opposite_indices,
				Identity::Consume,
			)?,
			JoinSide::Right => ctx.operator.join_columns_cartesian(
				host,
				opposite,
				&opposite_indices,
				primary,
				primary_indices,
				Identity::Consume,
			)?,
		};

		Ok(joined.withdrawn().into_iter().collect())
	})
}

#[instrument(name = "flow::operator::join::for_each_left_block", level = "trace", skip_all)]
pub(crate) fn for_each_left_block<F>(
	host: &mut dyn HostContext,
	left_store: &Store,
	key_hash: &Hash128,
	mut on_block: F,
) -> Result<()>
where
	F: FnMut(&mut dyn HostContext, &Columns) -> Result<()>,
{
	let limit = host.config_uint8(ConfigKey::FlowJoinProbeBlockSize) as usize;
	let mut after: Option<RowNumber> = None;
	loop {
		let block = left_store.rows_for_key(host, key_hash, after.as_ref(), limit)?;
		if block.is_empty() {
			break;
		}
		let last = block.last().unwrap().0;
		let exhausted = block.len() < limit;
		let left_columns = columns_from_block(host, left_store, block)?;
		on_block(host, &left_columns)?;
		if exhausted {
			break;
		}
		after = Some(last);
	}
	Ok(())
}
