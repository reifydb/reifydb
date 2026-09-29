// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::cmp::Ordering;

use arrow_array::RecordBatch;
use reifydb_codec::row::{bytes::EncodedBytes, pod::EncodedPodRow};
use reifydb_core::{
	error::diagnostic::operation::join_pick_column_not_found, key::operator::state::GroupId, row::JoinPick,
	sort::SortDirection,
};
use reifydb_value::{
	Result, error,
	fragment::Fragment,
	reifydb_assertions,
	util::hash::Hash128,
	value::{
		Value,
		column_view::ColumnView,
		datetime::TIME_COLUMN_NAME,
		row_number::RowNumber,
		system_columns::{SystemColumn, column_view, created_at, require_row_numbers, user_columns},
	},
};
use tracing::instrument;

use super::hash::{build_shape, columns_from_block, encode_row};
use crate::operator::{host::HostContext, join::store::Store, row_times};

fn instant_values(columns: &RecordBatch) -> Result<Option<Vec<Value>>> {
	let time = row_times(columns)?;
	let created = created_at(columns)?;
	Ok((0..columns.num_rows())
		.map(|idx| time.get(idx).copied().flatten().or_else(|| created.get(idx).copied()).map(Value::DateTime))
		.collect())
}

fn full_system_column<'a>(columns: &'a RecordBatch, name: &str) -> Result<Option<ColumnView<'a>>> {
	let bare = name.strip_prefix('#').unwrap_or(name);
	let Some(column) = SystemColumn::ALL.into_iter().find(|column| &column.name()[1..] == bare) else {
		return Ok(None);
	};
	Ok(column_view(columns, column.name())?.filter(|view| view.none_count() == 0))
}

fn ordering_values(columns: &RecordBatch, pick_column: &Fragment) -> Result<Vec<Value>> {
	let name = pick_column.text();
	let rows = columns.num_rows();
	if let Some((field, array)) = user_columns(columns).find(|(field, _)| field.name() == name) {
		let view = ColumnView::try_from((array, field.as_ref()))?;
		return Ok((0..rows).map(|idx| view.get_value(idx)).collect());
	}
	if let Some(view) = full_system_column(columns, name)? {
		return Ok((0..rows).map(|idx| view.get_value(idx)).collect());
	}
	if name == TIME_COLUMN_NAME {
		if let Some(values) = instant_values(columns)? {
			return Ok(values);
		}
		return Ok(require_row_numbers(columns)?.iter().map(|number| Value::Uint8(number.value())).collect());
	}
	Err(error!(join_pick_column_not_found(pick_column.clone(), "the right side")))
}

fn prefers(ord: Ordering, direction: &SortDirection) -> bool {
	match direction {
		SortDirection::Asc => ord == Ordering::Less,
		SortDirection::Desc => ord == Ordering::Greater,
	}
}

pub(crate) fn winner_index(columns: &RecordBatch, pick: &JoinPick) -> Result<Option<usize>> {
	let rows = columns.num_rows();
	if rows == 0 {
		return Ok(None);
	}
	let ordering: Vec<(Vec<Value>, SortDirection)> = pick
		.keys
		.iter()
		.map(|key| Ok((ordering_values(columns, &key.column)?, key.direction.clone())))
		.collect::<Result<_>>()?;
	let tail = ordering.last().map(|(_, direction)| direction.clone()).unwrap_or(SortDirection::Desc);
	let numbers = require_row_numbers(columns)?;
	let mut winner: Option<usize> = None;
	for idx in 0..rows {
		if ordering.iter().any(|(values, _)| matches!(values[idx], Value::None { .. })) {
			continue;
		}
		let better = match winner {
			None => true,
			Some(best) => {
				let mut decided = None;
				for (values, direction) in &ordering {
					let ord = values[idx].cmp(&values[best]);
					if ord != Ordering::Equal {
						decided = Some(prefers(ord, direction));
						break;
					}
				}
				decided.unwrap_or_else(|| prefers(numbers[idx].cmp(&numbers[best]), &tail))
			}
		};
		if better {
			winner = Some(idx);
		}
	}
	Ok(winner)
}

fn read_slot(host: &mut dyn HostContext, right: &Store, group: GroupId) -> Result<Option<(RowNumber, EncodedBytes)>> {
	let held = right.rows_for_group(host, group, None, 2)?;
	reifydb_assertions! {
		assert!(
			held.len() <= 1,
			"a latest join holds {} right rows under group {:?}, so the slot no longer names one winner and \
			 the loser is never freed",
			held.len(),
			group
		);
	}
	Ok(held.into_iter().next())
}

#[instrument(name = "flow::operator::join::latest::winning_right_row", level = "trace", skip_all)]
pub(crate) fn winning_right_row(
	host: &mut dyn HostContext,
	right: &Store,
	group: GroupId,
) -> Result<Option<(RowNumber, EncodedBytes, RecordBatch)>> {
	let Some((number, content)) = read_slot(host, right, group)? else {
		return Ok(None);
	};
	let columns = columns_from_block(host, right, vec![(number, content.clone())])?;
	Ok(Some((number, content, columns)))
}

#[instrument(name = "flow::operator::join::latest::read_right_slot", level = "trace", skip_all)]
pub(crate) fn read_right_slot(
	host: &mut dyn HostContext,
	right: &Store,
	key_hash: &Hash128,
) -> Result<Option<RecordBatch>> {
	Ok(winning_right_row(host, right, right.group_of(key_hash))?.map(|(_, _, columns)| columns))
}

#[instrument(name = "flow::operator::join::latest::store_right_rows", level = "trace", skip_all, fields(rows = indices.len()))]
pub(crate) fn write_right_rows(
	host: &mut dyn HostContext,
	right: &Store,
	key_hash: &Hash128,
	columns: &RecordBatch,
	indices: &[usize],
	pick: &JoinPick,
) -> Result<()> {
	if indices.is_empty() {
		return Ok(());
	}
	let shape = build_shape(columns)?;
	right.set_row_shape(host, &shape)?;
	let group = right.group_of(key_hash);

	let row_numbers = require_row_numbers(columns)?;
	let mut candidates: Vec<(RowNumber, EncodedBytes)> = Vec::with_capacity(indices.len() + 1);
	for &idx in indices {
		let row = encode_row(&shape, columns, idx, host.written_at(), right.side())?;
		candidates.push((row_numbers[idx], row.into_bytes()));
	}
	let held = read_slot(host, right, group)?;
	if let Some(held) = &held
		&& !candidates.iter().any(|(number, _)| *number == held.0)
	{
		candidates.push(held.clone());
	}

	let all = columns_from_block(host, right, candidates.clone())?;
	let Some(winner) = winner_index(&all, pick)? else {
		return Ok(());
	};
	let (number, content) = candidates[winner].clone();
	if let Some((previous, _)) = held
		&& previous != number
	{
		right.remove_row_in(host, group, previous)?;
	}
	right.write_row(host, group, number, &EncodedPodRow::from(content))
}

pub(crate) fn overwrite_right_slot(
	host: &mut dyn HostContext,
	right: &Store,
	key_hash: &Hash128,
	columns: &RecordBatch,
	indices: &[usize],
	pick: &JoinPick,
) -> Result<Option<RecordBatch>> {
	if indices.is_empty() {
		return Ok(None);
	}
	write_right_rows(host, right, key_hash, columns, indices, pick)?;
	read_right_slot(host, right, key_hash)
}

#[instrument(name = "flow::operator::join::latest::remove_right_rows", level = "trace", skip_all)]
pub(crate) fn remove_right_rows(
	host: &mut dyn HostContext,
	right: &Store,
	key_hash: &Hash128,
	numbers: &[RowNumber],
) -> Result<()> {
	let group = right.group_of(key_hash);
	let Some((held, _)) = read_slot(host, right, group)? else {
		return Ok(());
	};
	if numbers.contains(&held) {
		right.remove_row_in(host, group, held)?;
	}
	Ok(())
}
