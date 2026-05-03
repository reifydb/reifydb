// SPDX-License-Identifier: Apache-2.0
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
use reifydb_type::{error, error::Error, util::cowvec::CowVec, value::Value};
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

struct HeapEntry {
	row_idx: usize,
	sort_values: Vec<Value>,

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
	input: Box<dyn QueryNode>,
	by: Vec<SortKey>,
	limit: usize,
	initialized: Option<()>,
}

impl TopKNode {
	pub(crate) fn new(input: Box<dyn QueryNode>, by: Vec<SortKey>, limit: usize) -> Self {
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
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.input.initialize(rx, ctx)?;
		self.initialized = Some(());
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::top_k::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.initialized.is_some(), "TopKNode::next() called before initialize()");

		if self.limit == 0 {
			return Ok(None);
		}

		let mut columns_opt: Option<Columns> = None;

		while let Some(columns) = self.input.next(rx, ctx)? {
			if let Some(existing_columns) = &mut columns_opt {
				existing_columns.row_numbers.make_mut().extend(columns.row_numbers.iter().copied());
				existing_columns.created_at.make_mut().extend(columns.created_at.iter().copied());
				existing_columns.updated_at.make_mut().extend(columns.updated_at.iter().copied());
				for (i, col) in columns.columns.iter().enumerate() {
					existing_columns[i].extend(col.clone())?;
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

		if row_count <= self.limit {
			return self.sort_all(&mut columns);
		}

		let key_cols: Vec<_> =
			self.by.iter()
				.map(|key| {
					let col = columns
						.iter()
						.find(|c| c.name() == key.column.fragment())
						.ok_or_else(|| error!(query::column_not_found(key.column.clone())))?;
					Ok::<_, Error>((col.data().clone(), key.direction.clone()))
				})
				.collect::<Result<Vec<_>>>()?;

		let directions: Vec<_> = self.by.iter().map(|k| k.direction.clone()).collect();

		let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(self.limit);

		for row_idx in 0..row_count {
			let sort_values: Vec<Value> = key_cols.iter().map(|(col, _)| col.get_value(row_idx)).collect();

			let entry = HeapEntry::new(row_idx, sort_values, directions.clone());

			if heap.len() < self.limit {
				heap.push(entry);
			} else if let Some(top) = heap.peek() {
				if entry.cmp(top) == Ordering::Less {
					heap.pop();
					heap.push(entry);
				}
			}
		}

		let mut indices: Vec<usize> = heap.into_iter().map(|e| e.row_idx).collect();

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

		if !columns.row_numbers.is_empty() {
			let reordered_row_numbers: Vec<_> = indices.iter().map(|&i| columns.row_numbers[i]).collect();
			columns.row_numbers = CowVec::new(reordered_row_numbers);
		}

		let cols = columns.columns.make_mut();
		for col in cols.iter_mut() {
			col.reorder(&indices);
		}

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}

impl TopKNode {
	fn sort_all(&self, columns: &mut Columns) -> Result<Option<Columns>> {
		let key_refs: Vec<_> =
			self.by.iter()
				.map(|key| {
					let col = columns
						.iter()
						.find(|c| c.name() == key.column.fragment())
						.ok_or_else(|| error!(query::column_not_found(key.column.clone())))?;
					Ok::<_, Error>((col.data().clone(), key.direction.clone()))
				})
				.collect::<Result<Vec<_>>>()?;

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

		if !columns.row_numbers.is_empty() {
			let reordered_row_numbers: Vec<_> = indices.iter().map(|&i| columns.row_numbers[i]).collect();
			columns.row_numbers = CowVec::new(reordered_row_numbers);
		}

		let cols = columns.columns.make_mut();
		for col in cols.iter_mut() {
			col.reorder(&indices);
		}

		Ok(Some(columns.clone()))
	}
}
