// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, slice::from_ref};

use arrow_array::RecordBatch;
use reifydb_codec::row::{bytes::EncodedBytes, pod::EncodedPodRow};
use reifydb_core::{
	error::diagnostic::operation::join_pick_column_not_found, interface::change::Diff,
	key::operator::state::GroupId, row::JoinPick, sort::SortDirection,
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
		system_columns::{SystemColumn, column_view, created_at, require_row_numbers, sequences, user_columns},
	},
};
use tracing::instrument;

use super::hash::{JoinRows, columns_from_block, merge_runs};
use crate::operator::{
	host::HostContext,
	join::{Identity, operator::JoinOperator, store::Store},
	row_times,
};

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
	let numbers: Vec<u64> = match sequences(columns)? {
		sequences if sequences.len() == rows => sequences.to_vec(),
		_ => require_row_numbers(columns)?.iter().map(|number| number.value()).collect(),
	};
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

pub(crate) struct SlotChange {
	old: Option<RecordBatch>,
	written: Option<(RowNumber, EncodedBytes)>,
}

#[instrument(name = "flow::operator::join::latest::store_right_rows", level = "trace", skip_all, fields(rows = indices.len()))]
pub(crate) fn write_right_rows(
	host: &mut dyn HostContext,
	right: &Store,
	key_hash: &Hash128,
	rows: &mut JoinRows,
	indices: &[usize],
	pick: &JoinPick,
) -> Result<SlotChange> {
	if indices.is_empty() {
		return Ok(SlotChange {
			old: None,
			written: None,
		});
	}
	let group = right.group_of(key_hash);
	let held = read_slot(host, right, group)?;
	let old = held.clone().map(|held| columns_from_block(host, right, vec![held])).transpose()?;
	let encoded = rows.encoded(host, right)?;
	let mut candidates: Vec<(RowNumber, EncodedBytes)> = Vec::with_capacity(indices.len() + 1);
	for &idx in indices {
		candidates.push((encoded.number(idx), encoded.row(idx).clone().into_bytes()));
	}
	let mut all = rows.stored_right_columns(indices)?;
	let mut held_at = None;
	if let (Some(held), Some(held_columns)) = (&held, &old)
		&& !candidates.iter().any(|(number, _)| *number == held.0)
	{
		held_at = Some(candidates.len());
		candidates.push(held.clone());
		all = merge_runs(vec![all, held_columns.clone()], right.side())?;
	}

	let Some(winner) = winner_index(&all, pick)? else {
		return Ok(SlotChange {
			old,
			written: None,
		});
	};
	let (number, content) = candidates[winner].clone();
	if let Some((previous, _)) = &held
		&& *previous != number
	{
		right.remove_row_in(host, group, from_ref(previous))?;
	}
	right.write_row(host, group, number, &EncodedPodRow::from(content.clone()))?;
	Ok(SlotChange {
		old,
		written: (held_at != Some(winner)).then_some((number, content)),
	})
}

pub(crate) fn overwrite_right_slot(
	host: &mut dyn HostContext,
	right: &Store,
	key_hash: &Hash128,
	rows: &mut JoinRows,
	indices: &[usize],
	pick: &JoinPick,
) -> Result<(Option<RecordBatch>, Option<RecordBatch>)> {
	if indices.is_empty() {
		return Ok((read_right_slot(host, right, key_hash)?, None));
	}
	let change = write_right_rows(host, right, key_hash, rows, indices, pick)?;
	let new = match change.written {
		Some(winner) => Some(columns_from_block(host, right, vec![winner])?),
		None => change.old.clone(),
	};
	Ok((change.old, new))
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
		right.remove_row_in(host, group, from_ref(&held))?;
	}
	Ok(())
}

pub(crate) fn republish(
	host: &mut dyn HostContext,
	operator: &JoinOperator,
	pre: (&RecordBatch, Option<&RecordBatch>),
	post: (&RecordBatch, Option<&RecordBatch>),
	indices: &[usize],
) -> Result<Vec<Diff>> {
	let withdrawn = operator.latest_columns(host, pre.0, indices, pre.1, Identity::Existing)?;
	let published = operator.latest_columns(host, post.0, indices, post.1, Identity::Mint)?;
	let mut out = Vec::new();
	if withdrawn.existing.num_rows() > 0 && published.existing.num_rows() > 0 {
		out.push(Diff::update(withdrawn.existing, published.existing));
	}
	if published.fresh.num_rows() > 0 {
		out.push(Diff::insert(published.fresh));
	}
	Ok(out)
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::{ArrayRef, UInt64Array};
	use reifydb_core::value::{batch::batch, column::factory::int8};
	use reifydb_value::value::{
		container::temporal_array::datetime_array, datetime::DateTime, system_columns::with_system_column,
	};

	use super::*;

	fn right_rows(row_numbers: [u64; 2], sequences: Option<[u64; 2]>) -> RecordBatch {
		let now = DateTime::from_nanos(1_000);
		let user = batch(vec![int8("v", [10, 20])]).unwrap();
		let mut system: Vec<(SystemColumn, ArrayRef)> = vec![
			(SystemColumn::RowNumbers, Arc::new(UInt64Array::from(row_numbers.to_vec()))),
			(SystemColumn::CreatedAt, Arc::new(datetime_array([now, now]))),
			(SystemColumn::UpdatedAt, Arc::new(datetime_array([now, now]))),
			(SystemColumn::Time, Arc::new(datetime_array([now, now]))),
		];
		if let Some(sequences) = sequences {
			system.push((SystemColumn::Sequence, Arc::new(UInt64Array::from(sequences.to_vec()))));
		}
		system.into_iter()
			.fold(user, |columns, (column, array)| with_system_column(columns, column, array).unwrap())
	}

	#[test]
	fn an_equal_time_tie_on_a_wrapped_ring_goes_to_the_higher_sequence_not_the_higher_row_number() {
		// After a wrap the newer event sits in the lower slot, so a row-number tie-break would pick the older
		// one.
		let columns = right_rows([1, 3], Some([3, 2]));
		assert_eq!(winner_index(&columns, &JoinPick::latest()).unwrap(), Some(0));
		assert_eq!(winner_index(&columns, &JoinPick::earliest()).unwrap(), Some(1));
	}

	#[test]
	fn an_equal_time_tie_without_a_sequence_column_falls_back_to_the_row_number() {
		// Table sources carry no sequence, so the row number must still break the tie.
		let columns = right_rows([1, 3], None);
		assert_eq!(winner_index(&columns, &JoinPick::latest()).unwrap(), Some(1));
		assert_eq!(winner_index(&columns, &JoinPick::earliest()).unwrap(), Some(0));
	}
}
