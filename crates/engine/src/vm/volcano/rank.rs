// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, sync::Arc};

use arrow_array::UInt32Array;
use arrow_ord::sort::{SortColumn, SortOptions, lexsort_to_indices};
use reifydb_core::{
	internal_error,
	sort::{
		SortDirection,
		SortDirection::{Asc, Desc},
	},
	value::column::buffer::ColumnBuffer,
};

use crate::Result;

pub(crate) fn rank_rows(
	keys: &[(ColumnBuffer, SortDirection)],
	row_count: usize,
	limit: Option<usize>,
) -> Result<Vec<usize>> {
	match sort_columns(keys, row_count) {
		Some(columns) => {
			let indices = lexsort_to_indices(&columns, limit)
				.map_err(|e| internal_error!("Failed to rank sort keys: {}", e))?;
			Ok(indices.values().iter().map(|&index| index as usize).collect())
		}
		None => Ok(compare_rank(keys, row_count, limit)),
	}
}

fn sort_columns(keys: &[(ColumnBuffer, SortDirection)], row_count: usize) -> Option<Vec<SortColumn>> {
	let row_count = u32::try_from(row_count).ok()?;
	if keys.iter().any(|(data, _)| ranks_by_value(data)) {
		return None;
	}
	let mut columns: Vec<SortColumn> = keys
		.iter()
		.map(|(data, direction)| SortColumn {
			values: data.to_array_ref(),
			options: Some(options(direction)),
		})
		.collect();
	columns.push(SortColumn {
		values: Arc::new(UInt32Array::from_iter_values(0..row_count)),
		options: Some(options(&Asc)),
	});
	Some(columns)
}

fn options(direction: &SortDirection) -> SortOptions {
	let descending = matches!(direction, Desc);
	SortOptions {
		descending,
		nulls_first: descending,
	}
}

fn ranks_by_value(data: &ColumnBuffer) -> bool {
	matches!(data, ColumnBuffer::DictionaryId { .. } | ColumnBuffer::Decimal { .. })
}

fn compare_rank(keys: &[(ColumnBuffer, SortDirection)], row_count: usize, limit: Option<usize>) -> Vec<usize> {
	let mut indices: Vec<usize> = (0..row_count).collect();
	indices.sort_by(|&left, &right| compare_rows(keys, left, right));
	if let Some(limit) = limit {
		indices.truncate(limit);
	}
	indices
}

fn compare_rows(keys: &[(ColumnBuffer, SortDirection)], left: usize, right: usize) -> Ordering {
	for (data, direction) in keys {
		let ordering = data.get_value(left).cmp(&data.get_value(right));
		let ordering = match direction {
			Asc => ordering,
			Desc => ordering.reverse(),
		};
		if ordering != Ordering::Equal {
			return ordering;
		}
	}
	Ordering::Equal
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use reifydb_core::{sort::SortDirection, value::column::buffer::ColumnBuffer};
	use reifydb_value::value::{decimal::Decimal, dictionary::DictionaryEntryId};

	use super::rank_rows;

	fn ranked(data: ColumnBuffer, direction: SortDirection) -> Vec<usize> {
		let row_count = data.len();
		rank_rows(&[(data, direction)], row_count, None).expect("ranking must succeed")
	}

	#[test]
	fn a_dictionary_id_key_ranks_by_its_number_not_by_its_stored_bytes() {
		// Entry ids store a width tag then little-endian bytes, so a byte order would sort 5 above 256.
		let data = ColumnBuffer::dictionary_id([
			DictionaryEntryId::U2(256),
			DictionaryEntryId::U1(5),
			DictionaryEntryId::U8(1),
		]);

		assert_eq!(ranked(data, SortDirection::Asc), vec![2, 1, 0]);
	}

	#[test]
	fn a_decimal_key_ranks_numerically_so_equal_values_of_different_scale_tie() {
		// The stored row carries the scale after the value, so a byte order would never tie 1.0 with 1.00.
		let decimal = |text: &str| Decimal::from_str(text).expect("a decimal literal");
		let data = ColumnBuffer::decimal([decimal("1.00"), decimal("0.5"), decimal("1.0")]);

		assert_eq!(ranked(data, SortDirection::Asc), vec![1, 0, 2]);
	}

	#[test]
	fn rows_tied_on_every_key_keep_their_input_position() {
		// Tie order is observable through take, so ranking must be a total order ending in the row index.
		let data = ColumnBuffer::int4([7, 7, 7, 7]);

		assert_eq!(ranked(data.clone(), SortDirection::Asc), vec![0, 1, 2, 3]);
		assert_eq!(ranked(data, SortDirection::Desc), vec![0, 1, 2, 3]);
	}

	#[test]
	fn a_limit_keeps_the_earliest_rows_among_ties() {
		// A later tied row must never displace an earlier one that is already held.
		let data = ColumnBuffer::int4([5, 5, 5, 5, 5]);
		let row_count = data.len();

		let indices = rank_rows(&[(data, SortDirection::Asc)], row_count, Some(3)).expect("ranking");

		assert_eq!(indices, vec![0, 1, 2]);
	}
}
