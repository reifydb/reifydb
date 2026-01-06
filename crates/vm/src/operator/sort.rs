// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::cmp::Ordering;

use reifydb_core::{
	Batch,
	value::column::{ColumnData, Columns},
};

use crate::{
	error::{Result, VmError},
	pipeline::Pipeline,
};

/// Sort specification for a single column.
#[derive(Debug, Clone)]
pub struct SortSpec {
	pub column: String,
	pub order: SortOrder,
}

/// Sort order direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
	Asc,
	Desc,
}

/// Sort operator - sorts rows by specified columns.
/// Note: This is a blocking operation that must collect all input before producing output.
pub struct SortOp {
	pub specs: Vec<SortSpec>,
}

impl SortOp {
	pub fn new(specs: Vec<SortSpec>) -> Self {
		Self {
			specs,
		}
	}

	pub fn apply(&self, input: Pipeline) -> Pipeline {
		Box::new(SortIterator {
			input: Some(input),
			specs: self.specs.clone(),
		})
	}
}

/// Iterator that collects all input, sorts, and emits a single batch
struct SortIterator {
	input: Option<Pipeline>,
	specs: Vec<SortSpec>,
}

impl Iterator for SortIterator {
	type Item = Result<Batch>;

	fn next(&mut self) -> Option<Self::Item> {
		// Take the input (can only iterate once)
		let mut input = self.input.take()?;

		// Collect all input batches
		let mut collected: Option<Columns> = None;

		while let Some(result) = input.next() {
			match result {
				Err(e) => return Some(Err(e)),
				Ok(batch) => {
					// Materialize the batch to Columns
					let columns = batch.into_columns();

					collected = Some(match collected {
						None => columns,
						Some(existing) => match merge_columns(&existing, columns) {
							Ok(merged) => merged,
							Err(e) => return Some(Err(e)),
						},
					});
				}
			}
		}

		// Sort and emit once
		collected.map(|data| {
			let sorted = sort_columns(&data, &self.specs);
			sorted.map(Batch::fully_materialized)
		})
	}
}

/// Merge two Columns batches into one.
fn merge_columns(existing: &Columns, new_batch: Columns) -> Result<Columns> {
	let existing_count = existing.row_count();

	// Create combined columns
	let mut combined_columns = Vec::new();

	for (i, col) in existing.iter().enumerate() {
		let new_col = new_batch.iter().nth(i).ok_or_else(|| VmError::RowCountMismatch {
			expected: existing.len(),
			actual: new_batch.len(),
		})?;

		// Merge the column data
		let merged_data = merge_column_data(col.data(), new_col.data())?;
		combined_columns.push(reifydb_core::value::column::Column::new(col.name().clone(), merged_data));
	}

	// Merge row numbers
	use reifydb_type::RowNumber;
	let mut row_numbers: Vec<RowNumber> = existing.row_numbers.to_vec();
	for rn in new_batch.row_numbers.iter() {
		row_numbers.push(RowNumber(rn.0 + existing_count as u64));
	}

	Ok(Columns::with_row_numbers(combined_columns, row_numbers))
}

/// Merge two ColumnData instances.
fn merge_column_data(a: &ColumnData, b: &ColumnData) -> Result<ColumnData> {
	match (a, b) {
		(ColumnData::Bool(ca), ColumnData::Bool(cb)) => {
			let values: Vec<Option<bool>> = (0..ca.len())
				.map(|i| {
					if ca.is_defined(i) {
						ca.get(i)
					} else {
						None
					}
				})
				.chain((0..cb.len()).map(|i| {
					if cb.is_defined(i) {
						cb.get(i)
					} else {
						None
					}
				}))
				.collect();
			Ok(ColumnData::bool_optional(values))
		}

		(ColumnData::Int8(ca), ColumnData::Int8(cb)) => {
			let values: Vec<Option<i64>> = (0..ca.len())
				.map(|i| {
					if ca.is_defined(i) {
						ca.get(i).copied()
					} else {
						None
					}
				})
				.chain((0..cb.len()).map(|i| {
					if cb.is_defined(i) {
						cb.get(i).copied()
					} else {
						None
					}
				}))
				.collect();
			Ok(ColumnData::int8_optional(values))
		}

		(ColumnData::Float8(ca), ColumnData::Float8(cb)) => {
			let values: Vec<Option<f64>> = (0..ca.len())
				.map(|i| {
					if ca.is_defined(i) {
						ca.get(i).copied()
					} else {
						None
					}
				})
				.chain((0..cb.len()).map(|i| {
					if cb.is_defined(i) {
						cb.get(i).copied()
					} else {
						None
					}
				}))
				.collect();
			Ok(ColumnData::float8_optional(values))
		}

		(
			ColumnData::Utf8 {
				container: ca,
				max_bytes: _ma,
			},
			ColumnData::Utf8 {
				container: cb,
				max_bytes: _mb,
			},
		) => {
			let values: Vec<Option<String>> = (0..ca.len())
				.map(|i| {
					if ca.is_defined(i) {
						ca.get(i).map(|s| s.to_string())
					} else {
						None
					}
				})
				.chain((0..cb.len()).map(|i| {
					if cb.is_defined(i) {
						cb.get(i).map(|s| s.to_string())
					} else {
						None
					}
				}))
				.collect();
			Ok(ColumnData::utf8_optional(values))
		}

		_ => Err(VmError::Internal(format!("cannot merge column data types: {:?} and {:?}", a, b))),
	}
}

/// Sort columns by the given specifications.
fn sort_columns(columns: &Columns, specs: &[SortSpec]) -> Result<Columns> {
	let row_count = columns.row_count();
	if row_count == 0 || specs.is_empty() {
		return Ok(columns.clone());
	}

	// Build sort key indices
	let mut indices: Vec<usize> = (0..row_count).collect();

	// Sort indices based on column values
	indices.sort_by(|&a, &b| {
		for spec in specs {
			let col = columns
				.iter()
				.find(|c| c.name().text() == spec.column)
				.expect("column validated at compile time");

			let ordering = compare_column_values(col.data(), a, b);

			let ordering = match spec.order {
				SortOrder::Asc => ordering,
				SortOrder::Desc => ordering.reverse(),
			};

			if ordering != Ordering::Equal {
				return ordering;
			}
		}
		Ordering::Equal
	});

	// Reorder columns by sorted indices
	Ok(columns.extract_by_indices(&indices))
}

/// Compare two values in a column at the given indices.
fn compare_column_values(data: &ColumnData, a: usize, b: usize) -> Ordering {
	// Handle nulls: nulls sort last in ascending order
	let a_defined = data.is_defined(a);
	let b_defined = data.is_defined(b);

	match (a_defined, b_defined) {
		(false, false) => Ordering::Equal,
		(false, true) => Ordering::Greater, // null > value (nulls last)
		(true, false) => Ordering::Less,
		(true, true) => {
			// Compare actual values
			match data {
				ColumnData::Int8(c) => c.get(a).cmp(&c.get(b)),
				ColumnData::Float8(c) => {
					// Handle NaN: treat as null (sort last)
					let av = c.get(a).unwrap_or(&f64::NAN);
					let bv = c.get(b).unwrap_or(&f64::NAN);
					av.partial_cmp(bv).unwrap_or(Ordering::Equal)
				}
				ColumnData::Utf8 {
					container,
					..
				} => container.get(a).cmp(&container.get(b)),
				ColumnData::Bool(c) => c.get(a).cmp(&c.get(b)),
				_ => Ordering::Equal, // Unsupported types compare equal
			}
		}
	}
}
