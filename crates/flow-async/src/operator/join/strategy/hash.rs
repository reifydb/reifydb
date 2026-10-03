// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{iter::repeat_n, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_codec::row::{
	bytes::EncodedBytes,
	envelope::{Envelope, EnvelopeBuilder},
	pod::EncodedPodRow,
	shape::{RowFamily, RowShape, RowShapeField, fingerprint::RowShapeFingerprint},
};
use reifydb_core::{
	interface::{catalog::config::ConfigKey, change::Diff},
	internal,
	key::operator::state::GroupId,
	value::{
		batch::{batch, concat_columns, from_encoded_bytes},
		column::builder::ColumnBuilder,
	},
};
use reifydb_value::{
	Result,
	error::Error,
	util::{cowvec::CowVec, hash::Hash128},
	value::{
		Value,
		column_view::{ColumnView, FromColumnView},
		container::temporal_array::datetime_array,
		datetime::DateTime,
		partition::Partition,
		row_number::RowNumber,
		system_columns::{
			SystemColumn, column_view, require_row_numbers, stamp_system_columns, system_column,
			user_columns,
		},
		value_type::ValueType,
	},
};
use tracing::{Span, instrument};

use crate::operator::{
	host::HostContext,
	join::{Identity, operator::JoinOperator, state::JoinSide, store::Store},
	row_times, time_column,
};

pub(crate) fn build_shape(columns: &RecordBatch) -> Result<RowShape> {
	let fields: Vec<RowShapeField> = user_columns(columns)
		.map(|(field, array)| {
			let view = ColumnView::try_from((array, field.as_ref()))?;
			Ok(RowShapeField::unconstrained(field.name().clone(), view.get_type()))
		})
		.collect::<Result<_>>()?;
	Ok(RowShape::new(RowFamily::Pod, fields))
}

pub(crate) fn encode_row(
	shape: &RowShape,
	columns: &RecordBatch,
	row_idx: usize,
	now: DateTime,
	side: JoinSide,
) -> Result<EncodedPodRow> {
	let values: Vec<Value> = user_columns(columns)
		.map(|(field, array)| Ok(ColumnView::try_from((array, field.as_ref()))?.get_value(row_idx)))
		.collect::<Result<_>>()?;
	let mut encoded = shape.allocate_pod();
	shape.set_values(&mut encoded, &values);
	let envelope = EnvelopeBuilder::new().fingerprint(shape.fingerprint());
	let envelope = match side {
		JoinSide::Left => left_envelope(envelope, columns, row_idx)?,
		JoinSide::Right => match stamp_at::<DateTime>(columns, SystemColumn::Time, row_idx)? {
			Some(time) => envelope.time(time),
			None => envelope.created_at(now),
		},
	};
	Ok(envelope.build(encoded.freeze().as_slice()))
}

fn left_envelope(mut envelope: EnvelopeBuilder, columns: &RecordBatch, row_idx: usize) -> Result<EnvelopeBuilder> {
	if let Some(created_at) = stamp_at::<DateTime>(columns, SystemColumn::CreatedAt, row_idx)? {
		envelope = envelope.created_at(created_at);
	}
	if let Some(updated_at) = stamp_at::<DateTime>(columns, SystemColumn::UpdatedAt, row_idx)? {
		envelope = envelope.updated_at(updated_at);
	}
	if let Some(time) = stamp_at::<DateTime>(columns, SystemColumn::Time, row_idx)? {
		envelope = envelope.time(time);
	}
	if let Some(commit_version) = stamp_at::<u64>(columns, SystemColumn::CommitVersion, row_idx)? {
		envelope = envelope.commit_version(commit_version);
	}
	if let Some(partition) = stamp_at::<u128>(columns, SystemColumn::Partitions, row_idx)? {
		envelope = envelope.partition(Partition(partition));
	}
	Ok(envelope)
}

fn stamp_at<T: FromColumnView>(columns: &RecordBatch, column: SystemColumn, row_idx: usize) -> Result<Option<T>> {
	match column_view(columns, column.name())? {
		Some(view) => view.get_as::<T>(row_idx),
		None => Ok(None),
	}
}

#[instrument(name = "flow::operator::join::add_state_entry", level = "trace", skip_all)]
pub(crate) fn add_to_state_entry_batch(
	host: &mut dyn HostContext,
	store: &mut Store,
	key_hash: &Hash128,
	columns: &RecordBatch,
	indices: &[usize],
) -> Result<()> {
	if indices.is_empty() {
		return Ok(());
	}
	let shape = build_shape(columns)?;
	store.set_row_shape(host, &shape)?;
	let group = store.group_of(key_hash);
	let row_numbers = require_row_numbers(columns)?;
	for &idx in indices {
		let row = encode_row(&shape, columns, idx, host.written_at(), store.side())?;
		store.write_row(host, group, row_numbers[idx], &row)?;
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
	post: &RecordBatch,
) -> Result<EntryUpdate> {
	let shape = build_shape(post)?;
	store.set_row_shape(host, &shape)?;
	Ok(EntryUpdate {
		group: store.group_of(key_hash),
		shape,
	})
}

pub(crate) fn update_row_in_entry(
	host: &mut dyn HostContext,
	store: &Store,
	prepared: &EntryUpdate,
	pre_row_number: RowNumber,
	post: &RecordBatch,
	row_idx: usize,
) -> Result<bool> {
	let row = encode_row(&prepared.shape, post, row_idx, host.written_at(), store.side())?;
	let post_row_number = require_row_numbers(post)?[row_idx];
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
	post: &RecordBatch,
	row_idx: usize,
) -> Result<bool> {
	let shape = build_shape(post)?;
	store.set_row_shape(host, &shape)?;
	let row = encode_row(&shape, post, row_idx, host.written_at(), store.side())?;
	let post_row_number = require_row_numbers(post)?[row_idx];
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
) -> Result<RecordBatch> {
	let shape = store
		.get_row_shape(host, fingerprint)?
		.ok_or_else(|| Error(Box::new(internal!("Row shape not found in store"))))?;
	let mut envelopes: Vec<&Envelope> = Vec::with_capacity(bytes_slice.len());
	for bytes in bytes_slice {
		envelopes.push(Envelope::try_view(EncodedPodRow::view(bytes))?);
	}
	let bodies: Vec<EncodedBytes> =
		envelopes.iter().map(|envelope| EncodedBytes(CowVec::new(envelope.body().to_vec()))).collect();

	let mut decoded = from_encoded_bytes(&shape, ids, &bodies)?;
	let stamps: Vec<(SystemColumn, Option<ArrayRef>)> = match store.side() {
		JoinSide::Left => vec![
			(
				SystemColumn::Partitions,
				left_stamps(&envelopes, SystemColumn::Partitions, |envelope| {
					envelope.partition().map(|partition| Value::Uint16(partition.0))
				}),
			),
			(
				SystemColumn::CreatedAt,
				left_stamps(&envelopes, SystemColumn::CreatedAt, |envelope| {
					envelope.created_at().map(Value::DateTime)
				}),
			),
			(
				SystemColumn::UpdatedAt,
				left_stamps(&envelopes, SystemColumn::UpdatedAt, |envelope| {
					envelope.updated_at().map(Value::DateTime)
				}),
			),
			(
				SystemColumn::Time,
				left_stamps(&envelopes, SystemColumn::Time, |envelope| {
					envelope.time().map(Value::DateTime)
				}),
			),
			(
				SystemColumn::CommitVersion,
				left_stamps(&envelopes, SystemColumn::CommitVersion, |envelope| {
					envelope.commit_version().map(Value::Uint8)
				}),
			),
		],
		JoinSide::Right => {
			let instants: Vec<DateTime> = envelopes
				.iter()
				.map(|envelope| envelope.time().or_else(|| envelope.created_at()).unwrap_or_default())
				.collect();
			let instants: ArrayRef = Arc::new(datetime_array(instants));
			let time = envelopes
				.iter()
				.any(|envelope| envelope.time().is_some())
				.then(|| time_column(envelopes.iter().map(|envelope| envelope.time())));
			vec![
				(SystemColumn::CreatedAt, Some(instants.clone())),
				(SystemColumn::UpdatedAt, Some(instants)),
				(SystemColumn::Time, time),
			]
		}
	};
	decoded = stamp_system_columns(
		decoded,
		stamps.into_iter().filter_map(|(column, array)| array.map(|array| (column, array))).collect(),
	)?;

	Ok(decoded)
}

fn left_stamps(
	envelopes: &[&Envelope],
	column: SystemColumn,
	read: impl Fn(&Envelope) -> Option<Value>,
) -> Option<ArrayRef> {
	let stamps: Vec<Value> = envelopes.iter().filter_map(|envelope| read(envelope)).collect();
	assert_all_or_none(column, stamps.len(), envelopes.len());
	if stamps.is_empty() {
		return None;
	}
	let mut builder = ColumnBuilder::with_capacity(column.ty(), stamps.len());
	for stamp in stamps {
		builder.push_value(stamp);
	}
	Some(builder.finish(column.name()).1)
}

fn assert_all_or_none(column: SystemColumn, stamps: usize, rows: usize) {
	assert!(
		stamps == 0 || stamps == rows,
		"{rows} stored left rows hold {stamps} {} stamps; a join side must stamp all of its rows or none",
		column.name()
	);
}

#[instrument(name = "flow::operator::join::merge_runs", level = "trace", skip_all, fields(runs = runs.len()))]
fn merge_runs(runs: Vec<RecordBatch>, side: JoinSide) -> Result<RecordBatch> {
	let mut names: Vec<String> = Vec::new();
	for run in &runs {
		for (field, _) in user_columns(run) {
			if !names.contains(field.name()) {
				names.push(field.name().clone());
			}
		}
	}

	let total: usize = runs.iter().map(|run| run.num_rows()).sum();
	let mut result_columns: Vec<(FieldRef, ArrayRef)> = Vec::with_capacity(names.len());
	for name in &names {
		let views: Vec<Option<ColumnView>> =
			runs.iter().map(|run| column_view(run, name)).collect::<Result<_>>()?;
		let target_type = views.iter().flatten().next().map(ColumnView::get_type).unwrap_or(ValueType::Any);
		let mut buf = ColumnBuilder::with_capacity(target_type, total);
		for (run, view) in runs.iter().zip(&views) {
			match view {
				Some(view) => {
					for row_idx in 0..run.num_rows() {
						buf.push_value(view.get_value(row_idx));
					}
				}
				None => {
					for _ in 0..run.num_rows() {
						buf.push_value(Value::none());
					}
				}
			}
		}
		result_columns.push(buf.finish(name));
	}
	let mut stamps: Vec<(SystemColumn, ArrayRef)> = Vec::new();

	for column in SystemColumn::ALL {
		if column == SystemColumn::Time {
			continue;
		}
		let parts: Vec<(FieldRef, ArrayRef)> = runs
			.iter()
			.filter_map(|run| {
				let index = run.schema_ref().index_of(column.name()).ok()?;
				Some((run.schema_ref().fields()[index].clone(), run.column(index).clone()))
			})
			.collect();
		if side == JoinSide::Left && column != SystemColumn::RowNumbers {
			let stamps: usize = runs
				.iter()
				.filter(|run| system_column(run, column).is_some())
				.map(|run| run.num_rows())
				.sum();
			assert_all_or_none(column, stamps, total);
		}
		if parts.is_empty() {
			continue;
		}
		stamps.push((column, concat_columns(&parts)?.1));
	}

	let mut times: Vec<Option<DateTime>> = Vec::with_capacity(total);
	for run in &runs {
		let run_times = row_times(run)?;
		match run_times.is_empty() {
			true => times.extend(repeat_n(None, run.num_rows())),
			false => times.extend(run_times),
		}
	}
	let timed = times.iter().filter(|time| time.is_some()).count();
	if side == JoinSide::Left {
		assert_all_or_none(SystemColumn::Time, timed, total);
	}
	if timed > 0 {
		stamps.push((SystemColumn::Time, time_column(times)));
	}

	stamp_system_columns(batch(result_columns)?, stamps)
}

#[instrument(name = "flow::operator::join::columns_from_block", level = "trace", skip_all, fields(rows = block.len()))]
pub(crate) fn columns_from_block(
	host: &mut dyn HostContext,
	store: &Store,
	block: Vec<(RowNumber, EncodedBytes)>,
) -> Result<RecordBatch> {
	let mut runs: Vec<RecordBatch> = Vec::new();
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
	merge_runs(runs, store.side())
}

fn stream_join_blocks<F>(
	host: &mut dyn HostContext,
	store: &Store,
	key_hash: &Hash128,
	join_block: F,
) -> Result<Vec<Diff>>
where
	F: FnMut(&mut dyn HostContext, &RecordBatch) -> Result<Vec<Diff>>,
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
	F: FnMut(&mut dyn HostContext, &RecordBatch, &[(RowNumber, EncodedBytes)]) -> Result<Vec<Diff>>,
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
	pre: &RecordBatch,
	post: &RecordBatch,
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
	primary: &RecordBatch,
	primary_indices: &[usize],
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Vec<Diff>> {
	if primary_indices.is_empty() {
		return Ok(Vec::new());
	}

	stream_join_blocks(host, ctx.opposite_store, ctx.key_hash, |host, opposite| {
		let opposite_indices: Vec<usize> = (0..opposite.num_rows()).collect();
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
	primary: &RecordBatch,
	primary_indices: &[usize],
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Vec<Diff>> {
	if primary_indices.is_empty() {
		return Ok(Vec::new());
	}

	stream_join_blocks(host, ctx.opposite_store, ctx.key_hash, |host, opposite| {
		let opposite_indices: Vec<usize> = (0..opposite.num_rows()).collect();
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
	F: FnMut(&mut dyn HostContext, &RecordBatch) -> Result<()>,
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

#[cfg(test)]
mod tests {
	use arrow_array::UInt64Array;
	use reifydb_core::{interface::catalog::flow::OperatorId, value::column::factory::int4};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::value::system_columns::{created_at, updated_at, with_system_column};

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

	fn columns_with_fields(fields: &[(&str, i32)], row_number: u64) -> RecordBatch {
		let cols: Vec<(FieldRef, ArrayRef)> = fields.iter().map(|(name, value)| int4(name, [*value])).collect();
		with_system_column(
			batch(cols).unwrap(),
			SystemColumn::RowNumbers,
			Arc::new(UInt64Array::from(vec![row_number])),
		)
		.unwrap()
	}

	fn columns_with_time(fields: &[(&str, i32)], row_number: u64, time: Option<DateTime>) -> RecordBatch {
		// A batch with only #rownum carries no #time, so a timed row must add #time as its own column.
		let columns = columns_with_fields(fields, row_number);
		match time {
			Some(time) => with_system_column(columns, SystemColumn::Time, Arc::new(datetime_array([time])))
				.unwrap(),
			None => columns,
		}
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
		let shape = build_shape(&columns).unwrap();
		store.set_row_shape(&mut host(&mut txn, operator), &shape).unwrap();

		let row = encode_row(&shape, &columns, 0, now, JoinSide::Right).unwrap();
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
		assert_eq!(created_at(&decoded).unwrap(), &[now][..]);
		assert_eq!(
			updated_at(&decoded).unwrap(),
			&[now][..],
			"both stamps are synthesized from the one stored instant"
		);
		assert!(
			system_column(&decoded, SystemColumn::Time).is_none(),
			"a row that carried no #time must not gain one on the way back"
		);
		assert_eq!(column_view(&decoded, "mint").unwrap().unwrap().get_value(0), Value::Int4(7));
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
		let shape = build_shape(&columns).unwrap();
		store.set_row_shape(&mut host(&mut txn, operator), &shape).unwrap();

		let row = encode_row(&shape, &columns, 0, now, JoinSide::Right).unwrap();
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
		assert_eq!(created_at(&decoded).unwrap(), &[event][..]);
		assert_eq!(updated_at(&decoded).unwrap(), &[event][..]);
		assert_eq!(row_times(&decoded).unwrap(), vec![Some(event)]);
	}

	#[test]
	fn a_run_mixing_timed_and_timeless_rows_gives_every_row_its_own_time_slot() {
		// Every row must keep its own #time slot (none when timeless), otherwise times land on the wrong rows.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(74);
		let store = Store::new(JoinSide::Right);

		let now = DateTime::from_nanos(1_700_000_000_000_000_000);
		let event = DateTime::from_nanos(1_600_000_000_000_000_000);
		let first = columns_with_time(&[("mint", 1)], 1, None);
		let second = columns_with_time(&[("mint", 2)], 2, Some(event));
		let third = columns_with_time(&[("mint", 3)], 3, None);
		let shape = build_shape(&first).unwrap();
		store.set_row_shape(&mut host(&mut txn, operator), &shape).unwrap();

		let rows: Vec<EncodedBytes> = [&first, &second, &third]
			.into_iter()
			.map(|columns| encode_row(&shape, columns, 0, now, JoinSide::Right).unwrap().into_bytes())
			.collect();

		let decoded = decode_run(
			&mut host(&mut txn, operator),
			&store,
			shape.fingerprint(),
			&[RowNumber(1), RowNumber(2), RowNumber(3)],
			&rows,
		)
		.unwrap();
		assert_eq!(created_at(&decoded).unwrap(), &[now, event, now][..]);
		assert_eq!(updated_at(&decoded).unwrap(), &[now, event, now][..]);
		assert_eq!(
			row_times(&decoded).unwrap(),
			vec![None, Some(event), None],
			"only the row that carried a #time may appear here"
		);
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
		assert_eq!(read_back.num_rows(), 1);
		assert_eq!(
			user_columns(&read_back).count(),
			3,
			"key B's own 3-field shape must be the one used to decode it"
		);
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

		let mint = column_view(&read_back, "mint").unwrap().unwrap();
		let flag = column_view(&read_back, "flag").unwrap().unwrap();
		assert_eq!(mint.get_value(0), Value::Int4(111));
		assert_eq!(flag.get_value(0), Value::Int4(1));
		assert_eq!(
			mint.get_value(1),
			Value::Int4(222),
			"row 2's real mint value must be reported under the mint column"
		);
		assert_eq!(
			flag.get_value(1),
			Value::Int4(999),
			"row 2's real flag value must be reported under the flag column, not swapped with mint"
		);
	}
}
