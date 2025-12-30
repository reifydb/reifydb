// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	cmp::Ordering,
	pin::Pin,
	task::{Context, Poll},
};

use futures_util::Stream;
use pin_project::pin_project;
use reifydb_core::value::column::{ColumnData, Columns};

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
		Box::pin(SortStream {
			input,
			specs: self.specs.clone(),
			collected: None,
			done: false,
		})
	}
}

#[pin_project]
struct SortStream {
	#[pin]
	input: Pipeline,
	specs: Vec<SortSpec>,
	collected: Option<Columns>,
	done: bool,
}

impl Stream for SortStream {
	type Item = Result<Columns>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let mut this = self.project();

		// If we've already produced output, we're done
		if *this.done {
			return Poll::Ready(None);
		}

		// Collect all input first
		loop {
			match this.input.as_mut().poll_next(cx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(None) => {
					// Input exhausted, sort and return
					*this.done = true;

					if let Some(data) = this.collected.take() {
						match sort_columns(&data, this.specs) {
							Ok(sorted) => return Poll::Ready(Some(Ok(sorted))),
							Err(e) => return Poll::Ready(Some(Err(e))),
						}
					} else {
						return Poll::Ready(None);
					}
				}
				Poll::Ready(Some(Err(e))) => {
					*this.done = true;
					return Poll::Ready(Some(Err(e)));
				}
				Poll::Ready(Some(Ok(batch))) => {
					// Collect this batch
					match this.collected.take() {
						None => {
							*this.collected = Some(batch);
						}
						Some(existing) => match merge_columns(&existing, batch) {
							Ok(merged) => {
								*this.collected = Some(merged);
							}
							Err(e) => {
								*this.done = true;
								return Poll::Ready(Some(Err(e)));
							}
						},
					}
				}
			}
		}
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
