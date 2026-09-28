// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::cmp::Ordering;

use reifydb_codec::row::{bytes::EncodedBytes, pod::EncodedPodRow};
use reifydb_core::{
	error::diagnostic::operation::join_pick_column_not_found, key::operator::state::GroupId, row::JoinPick,
	sort::SortDirection, value::column::columns::Columns,
};
use reifydb_value::{
	Result, error,
	fragment::Fragment,
	reifydb_assertions,
	util::hash::Hash128,
	value::{Value, datetime::TIME_COLUMN_NAME, row_number::RowNumber},
};
use tracing::instrument;

use super::hash::{build_shape, columns_from_block, encode_row};
use crate::operator::{host::HostContext, join::store::Store};

fn instant_values(columns: &Columns) -> Option<Vec<Value>> {
	let time = columns.time();
	let created = columns.created_at();
	(0..columns.row_count())
		.map(|idx| time.get(idx).or_else(|| created.get(idx)).map(|stamp| Value::DateTime(*stamp)))
		.collect()
}

fn ordering_values(columns: &Columns, pick_column: &Fragment) -> Result<Vec<Value>> {
	let name = pick_column.text();
	let rows = columns.row_count();
	if let Some(column) = columns.column(name) {
		return Ok((0..rows).map(|idx| column.data().get_value(idx)).collect());
	}
	if let Some(buffer) = columns.system_column(name) {
		return Ok((0..rows).map(|idx| buffer.get_value(idx)).collect());
	}
	if name == TIME_COLUMN_NAME {
		if let Some(values) = instant_values(columns) {
			return Ok(values);
		}
		return Ok(columns.row_numbers().iter().map(|number| Value::Uint8(number.value())).collect());
	}
	Err(error!(join_pick_column_not_found(pick_column.clone(), "the right side")))
}

fn prefers(ord: Ordering, direction: &SortDirection) -> bool {
	match direction {
		SortDirection::Asc => ord == Ordering::Less,
		SortDirection::Desc => ord == Ordering::Greater,
	}
}

pub(crate) fn winner_index(columns: &Columns, pick: &JoinPick) -> Result<Option<usize>> {
	let rows = columns.row_count();
	if rows == 0 {
		return Ok(None);
	}
	let ordering: Vec<(Vec<Value>, SortDirection)> = pick
		.keys
		.iter()
		.map(|key| Ok((ordering_values(columns, &key.column)?, key.direction.clone())))
		.collect::<Result<_>>()?;
	let tail = ordering.last().map(|(_, direction)| direction.clone()).unwrap_or(SortDirection::Desc);
	let numbers = columns.row_numbers();
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
) -> Result<Option<(RowNumber, EncodedBytes, Columns)>> {
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
) -> Result<Option<Columns>> {
	Ok(winning_right_row(host, right, right.group_of(key_hash))?.map(|(_, _, columns)| columns))
}

#[instrument(name = "flow::operator::join::latest::store_right_rows", level = "trace", skip_all, fields(rows = indices.len()))]
pub(crate) fn write_right_rows(
	host: &mut dyn HostContext,
	right: &Store,
	key_hash: &Hash128,
	columns: &Columns,
	indices: &[usize],
	pick: &JoinPick,
) -> Result<()> {
	if indices.is_empty() {
		return Ok(());
	}
	let shape = build_shape(columns);
	right.set_row_shape(host, &shape)?;
	let group = right.group_of(key_hash);

	let mut candidates: Vec<(RowNumber, EncodedBytes)> = Vec::with_capacity(indices.len() + 1);
	for &idx in indices {
		let row = encode_row(&shape, columns, idx, host.written_at(), right.side());
		candidates.push((columns.row_numbers()[idx], row.into_bytes()));
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
	columns: &Columns,
	indices: &[usize],
	pick: &JoinPick,
) -> Result<Option<Columns>> {
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
