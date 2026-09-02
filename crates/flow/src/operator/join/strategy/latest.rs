// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::cmp::Ordering;

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	key::operator::state::GroupId, row::JoinPick, sort::SortDirection, value::column::columns::Columns,
};
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, datetime::TIME_COLUMN_NAME, row_number::RowNumber},
};
use tracing::instrument;

use super::hash::{build_shape, columns_from_block, encode_row};
use crate::operator::{host::HostContext, join::store::Store};

const PAGE: usize = 256;

fn instant_values(columns: &Columns) -> Option<Vec<Value>> {
	let time = columns.time();
	let created = columns.created_at();
	(0..columns.row_count())
		.map(|idx| time.get(idx).or_else(|| created.get(idx)).map(|stamp| Value::DateTime(*stamp)))
		.collect()
}

fn ordering_values(columns: &Columns, name: &str) -> Vec<Value> {
	let rows = columns.row_count();
	if let Some(column) = columns.column(name) {
		return (0..rows).map(|idx| column.data().get_value(idx)).collect();
	}
	if let Some(buffer) = columns.system_column(name) {
		return (0..rows).map(|idx| buffer.get_value(idx)).collect();
	}
	if name == TIME_COLUMN_NAME {
		if let Some(values) = instant_values(columns) {
			return values;
		}
		return columns.row_numbers().iter().map(|number| Value::Uint8(number.value())).collect();
	}
	panic!("join pick orders by column {name}, which the right side does not carry")
}

fn prefers(ord: Ordering, direction: &SortDirection) -> bool {
	match direction {
		SortDirection::Asc => ord == Ordering::Less,
		SortDirection::Desc => ord == Ordering::Greater,
	}
}

pub(crate) fn winner_index(columns: &Columns, pick: &JoinPick) -> Option<usize> {
	let rows = columns.row_count();
	if rows == 0 {
		return None;
	}
	let ordering: Vec<(Vec<Value>, SortDirection)> = pick
		.keys
		.iter()
		.map(|key| (ordering_values(columns, key.column.text()), key.direction.clone()))
		.collect();
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
	winner
}

fn read_all_rows(host: &mut dyn HostContext, right: &Store, group: GroupId) -> Result<Vec<(RowNumber, EncodedBytes)>> {
	let mut entries = Vec::new();
	let mut after: Option<RowNumber> = None;
	loop {
		let block = right.rows_for_group(host, group, after.as_ref(), PAGE)?;
		let short = block.len() < PAGE;
		if let Some(last) = block.last() {
			after = Some(last.0);
		}
		entries.extend(block);
		if short {
			break;
		}
	}
	Ok(entries)
}

#[instrument(name = "flow::operator::join::latest::winning_right_row", level = "trace", skip_all)]
pub(crate) fn winning_right_row(
	host: &mut dyn HostContext,
	right: &Store,
	group: GroupId,
	pick: &JoinPick,
) -> Result<Option<(RowNumber, EncodedBytes, Columns)>> {
	let entries = read_all_rows(host, right, group)?;
	if entries.is_empty() {
		return Ok(None);
	}
	let all = columns_from_block(host, right, entries.clone())?;
	let Some(idx) = winner_index(&all, pick) else {
		return Ok(None);
	};
	let entry = entries[idx].clone();
	let number = entry.0;
	let content = entry.1.clone();
	Ok(Some((number, content, columns_from_block(host, right, vec![entry])?)))
}

#[instrument(name = "flow::operator::join::latest::read_right_slot", level = "trace", skip_all)]
pub(crate) fn read_right_slot(
	host: &mut dyn HostContext,
	right: &Store,
	key_hash: &Hash128,
	pick: &JoinPick,
) -> Result<Option<Columns>> {
	Ok(winning_right_row(host, right, right.group_of(key_hash), pick)?.map(|(_, _, columns)| columns))
}

#[instrument(name = "flow::operator::join::latest::store_right_rows", level = "trace", skip_all, fields(rows = indices.len()))]
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
	let shape = build_shape(columns);
	right.set_row_shape(host, &shape)?;
	let group = right.group_of(key_hash);
	for &idx in indices {
		let row = encode_row(&shape, columns, idx, host.written_at());
		right.write_row(host, group, columns.row_numbers()[idx], &row)?;
	}
	read_right_slot(host, right, key_hash, pick)
}

#[instrument(name = "flow::operator::join::latest::remove_right_rows", level = "trace", skip_all)]
pub(crate) fn remove_right_rows(
	host: &mut dyn HostContext,
	right: &Store,
	key_hash: &Hash128,
	numbers: &[RowNumber],
) -> Result<()> {
	for number in numbers {
		right.remove_row(host, key_hash, *number)?;
	}
	Ok(())
}
