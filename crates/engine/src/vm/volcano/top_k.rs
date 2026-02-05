// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{cmp::Ordering, collections::BinaryHeap};

use reifydb_core::{
	error::diagnostic::query,
	sort::{
		SortDirection,
		SortDirection::{Asc, Desc},
		SortKey,
	},
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{error, util::cowvec::CowVec, value::Value};
use tracing::instrument;

use crate::vm::volcano::query::{QueryContext, QueryNode, QueryPlan};

/// A heap entry that stores a row index and its cached sort key values.
/// The Ord implementation is designed so that BinaryHeap (a max-heap) will
/// have the "largest" element at the top, allowing us to efficiently keep
/// the K "smallest" elements by evicting the largest when a smaller one arrives.
struct HeapEntry {
	row_idx: usize,
	sort_values: Vec<Value>,
	/// Reference to sort keys for comparison directions
	directions: Vec<SortDirection>,
}

impl HeapEntry {
	fn new(row_idx: usize, sort_values: Vec<Value>, directions: Vec<SortDirection>) -> Self {
		Self {
			row_idx,
			sort_values,
			directions,
		}
	}
}

impl PartialEq for HeapEntry {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == Ordering::Equal
	}
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for HeapEntry {
	fn cmp(&self, other: &Self) -> Ordering {
		// Compare each sort key value according to its direction
		for i in 0..self.sort_values.len() {
			let ord = self.sort_values[i].partial_cmp(&other.sort_values[i]).unwrap_or(Ordering::Equal);
			let ord = match self.directions[i] {
				Asc => ord,
				Desc => ord.reverse(),
			};
			if ord != Ordering::Equal {
				return ord;
			}
		}
		Ordering::Equal
	}
}

pub(crate) struct TopKNode {
	input: Box<QueryPlan>,
	by: Vec<SortKey>,
	limit: usize,
	initialized: Option<()>,
}

impl TopKNode {
	pub(crate) fn new(input: Box<QueryPlan>, by: Vec<SortKey>, limit: usize) -> Self {
		Self {
			input,
			by,
			limit,
			initialized: None,
		}
	}
}

impl QueryNode for TopKNode {
	#[instrument(level = "trace", skip_all, name = "volcano::top_k::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()> {
		self.input.initialize(rx, ctx)?;
		self.initialized = Some(());
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::top_k::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		debug_assert!(self.initialized.is_some(), "TopKNode::next() called before initialize()");

		// Handle edge case: limit of 0
		if self.limit == 0 {
			return Ok(None);
		}

		// Collect all input batches into a single Columns structure
		let mut columns_opt: Option<Columns> = None;

		while let Some(columns) = self.input.next(rx, ctx)? {
			if let Some(existing_columns) = &mut columns_opt {
				for (i, col) in columns.into_iter().enumerate() {
					existing_columns[i].data_mut().extend(col.data().clone())?;
				}
			} else {
				columns_opt = Some(columns);
			}
		}

		let mut columns = match columns_opt {
			Some(f) => f,
			None => return Ok(None),
		};

		let row_count = columns.row_count();

		// If we have fewer rows than the limit, just do a regular sort
		if row_count <= self.limit {
			return self.sort_all(&mut columns);
		}

		// Build column references for sort keys
		let key_cols: Vec<_> =
			self.by.iter()
				.map(|key| {
					let col = columns
						.iter()
						.find(|c| c.name() == key.column.fragment())
						.ok_or_else(|| error!(query::column_not_found(key.column.clone())))?;
					Ok::<_, reifydb_type::error::Error>((col.data().clone(), key.direction.clone()))
				})
				.collect::<crate::Result<Vec<_>>>()?;

		let directions: Vec<_> = self.by.iter().map(|k| k.direction.clone()).collect();

		// Use a BinaryHeap to keep the top-k elements
		// BinaryHeap is a max-heap, so the "largest" element is at the top
		// We want to keep the K "smallest" elements, so we evict when a smaller element arrives
		let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(self.limit);

		for row_idx in 0..row_count {
			// Extract sort key values for this row
			let sort_values: Vec<Value> = key_cols.iter().map(|(col, _)| col.get_value(row_idx)).collect();

			let entry = HeapEntry::new(row_idx, sort_values, directions.clone());

			if heap.len() < self.limit {
				heap.push(entry);
			} else if let Some(top) = heap.peek() {
				// If new entry is "smaller" than the largest in heap, replace
				if entry.cmp(top) == Ordering::Less {
					heap.pop();
					heap.push(entry);
				}
			}
		}

		// Extract indices from heap and sort them by the original ordering
		let mut indices: Vec<usize> = heap.into_iter().map(|e| e.row_idx).collect();

		// Sort the selected indices by the sort order (not by row_idx)
		indices.sort_unstable_by(|&l, &r| {
			for (col, dir) in &key_cols {
				let vl = col.get_value(l);
				let vr = col.get_value(r);
				let ord = vl.partial_cmp(&vr).unwrap_or(Ordering::Equal);
				let ord = match dir {
					Asc => ord,
					Desc => ord.reverse(),
				};
				if ord != Ordering::Equal {
					return ord;
				}
			}
			Ordering::Equal
		});

		// Reorder row numbers if present
		if !columns.row_numbers.is_empty() {
			let reordered_row_numbers: Vec<_> = indices.iter().map(|&i| columns.row_numbers[i]).collect();
			columns.row_numbers = CowVec::new(reordered_row_numbers);
		}

		// Reorder columns
		let cols = columns.columns.make_mut();
		for col in cols.iter_mut() {
			col.data_mut().reorder(&indices);
		}

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}

impl TopKNode {
	/// Fallback to regular sorting when row count <= limit
	fn sort_all(&self, columns: &mut Columns) -> crate::Result<Option<Columns>> {
		let key_refs: Vec<_> =
			self.by.iter()
				.map(|key| {
					let col = columns
						.iter()
						.find(|c| c.name() == key.column.fragment())
						.ok_or_else(|| error!(query::column_not_found(key.column.clone())))?;
					Ok::<_, reifydb_type::error::Error>((col.data().clone(), key.direction.clone()))
				})
				.collect::<crate::Result<Vec<_>>>()?;

		let row_count = columns.row_count();
		let mut indices: Vec<usize> = (0..row_count).collect();

		indices.sort_unstable_by(|&l, &r| {
			for (col, dir) in &key_refs {
				let vl = col.get_value(l);
				let vr = col.get_value(r);
				let ord = vl.partial_cmp(&vr).unwrap_or(Ordering::Equal);
				let ord = match dir {
					Asc => ord,
					Desc => ord.reverse(),
				};
				if ord != Ordering::Equal {
					return ord;
				}
			}
			Ordering::Equal
		});

		// Reorder row numbers if present
		if !columns.row_numbers.is_empty() {
			let reordered_row_numbers: Vec<_> = indices.iter().map(|&i| columns.row_numbers[i]).collect();
			columns.row_numbers = CowVec::new(reordered_row_numbers);
		}

		// Reorder columns
		let cols = columns.columns.make_mut();
		for col in cols.iter_mut() {
			col.data_mut().reorder(&indices);
		}

		Ok(Some(columns.clone()))
	}
}
