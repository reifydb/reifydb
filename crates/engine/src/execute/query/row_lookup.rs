// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Row-number optimized access nodes for O(1) and O(k) lookups.
//!
//! These nodes bypass full table scans when filtering by row number:
//! - `RowPointLookupNode`: Single row O(1) lookup
//! - `RowListLookupNode`: Multiple discrete rows O(k) lookup
//! - `RowRangeScanNode`: Row number range scan

use std::sync::Arc;

use reifydb_core::{
	interface::{EncodableKey, MultiVersionQueryTransaction, RowKey, catalog::SourceId, resolved::ResolvedSource},
	value::{
		column::{Columns, headers::ColumnHeaders},
		encoded::EncodedValuesLayout,
	},
};
use reifydb_type::{Fragment, RowNumber};
use tracing::instrument;

use crate::execute::{Batch, ExecutionContext, QueryNode};

/// O(1) point lookup by row number
pub(crate) struct RowPointLookupNode<'a> {
	source: ResolvedSource<'a>,
	row_number: u64,
	#[allow(dead_code)]
	context: Option<Arc<ExecutionContext<'a>>>,
	headers: ColumnHeaders<'a>,
	row_layout: EncodedValuesLayout,
	exhausted: bool,
}

impl<'a> RowPointLookupNode<'a> {
	pub fn new(
		source: ResolvedSource<'a>,
		row_number: u64,
		context: Arc<ExecutionContext<'a>>,
	) -> crate::Result<Self> {
		let (headers, row_layout) = build_headers_and_layout(&source)?;

		Ok(Self {
			source,
			row_number,
			context: Some(context),
			headers,
			row_layout,
			exhausted: false,
		})
	}
}

impl<'a> QueryNode<'a> for RowPointLookupNode<'a> {
	#[instrument(name = "query::lookup::point::initialize", level = "trace", skip_all)]
	fn initialize(
		&mut self,
		_rx: &mut crate::StandardTransaction<'a>,
		_ctx: &ExecutionContext<'a>,
	) -> crate::Result<()> {
		Ok(())
	}

	#[instrument(name = "query::lookup::point::next", level = "trace", skip_all)]
	fn next(
		&mut self,
		rx: &mut crate::StandardTransaction<'a>,
		_ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}
		self.exhausted = true;

		let source_id = get_source_id(&self.source)?;
		let row_key = RowKey {
			source: source_id,
			row: RowNumber(self.row_number),
		};
		let encoded_key = row_key.encode();

		// O(1) point lookup
		if let Some(multi_values) = rx.get(&encoded_key)? {
			let mut columns = columns_from_source(&self.source);
			columns.append_rows(
				&self.row_layout,
				std::iter::once(multi_values.values),
				vec![RowNumber(self.row_number)],
			)?;

			Ok(Some(Batch {
				columns,
			}))
		} else {
			// Row not found - return empty result
			Ok(None)
		}
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		Some(self.headers.clone())
	}
}

/// O(k) list lookup by row numbers
pub(crate) struct RowListLookupNode<'a> {
	source: ResolvedSource<'a>,
	row_numbers: Vec<u64>,
	context: Option<Arc<ExecutionContext<'a>>>,
	headers: ColumnHeaders<'a>,
	row_layout: EncodedValuesLayout,
	current_index: usize,
}

impl<'a> RowListLookupNode<'a> {
	pub fn new(
		source: ResolvedSource<'a>,
		row_numbers: Vec<u64>,
		context: Arc<ExecutionContext<'a>>,
	) -> crate::Result<Self> {
		let (headers, row_layout) = build_headers_and_layout(&source)?;

		Ok(Self {
			source,
			row_numbers,
			context: Some(context),
			headers,
			row_layout,
			current_index: 0,
		})
	}
}

impl<'a> QueryNode<'a> for RowListLookupNode<'a> {
	#[instrument(name = "query::lookup::list::initialize", level = "trace", skip_all)]
	fn initialize(
		&mut self,
		_rx: &mut crate::StandardTransaction<'a>,
		_ctx: &ExecutionContext<'a>,
	) -> crate::Result<()> {
		Ok(())
	}

	#[instrument(name = "query::lookup::list::next", level = "trace", skip_all)]
	fn next(
		&mut self,
		rx: &mut crate::StandardTransaction<'a>,
		ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
		let stored_ctx = self.context.as_ref().unwrap();
		let batch_size = stored_ctx.batch_size as usize;

		if self.current_index >= self.row_numbers.len() {
			return Ok(None);
		}

		let source_id = get_source_id(&self.source)?;
		let mut batch_rows = Vec::new();
		let mut found_row_numbers = Vec::new();

		// Process up to batch_size rows
		let end_index = (self.current_index + batch_size).min(self.row_numbers.len());

		for &row_num in &self.row_numbers[self.current_index..end_index] {
			let row_key = RowKey {
				source: source_id,
				row: RowNumber(row_num),
			};
			let encoded_key = row_key.encode();

			// O(1) point lookup for each row
			if let Some(multi_values) = rx.get(&encoded_key)? {
				batch_rows.push(multi_values.values);
				found_row_numbers.push(RowNumber(row_num));
			}
			// Skip rows that don't exist
		}

		self.current_index = end_index;

		if batch_rows.is_empty() {
			// If no rows found in this batch but more to process, try next batch
			if self.current_index < self.row_numbers.len() {
				return self.next(rx, ctx);
			}
			return Ok(None);
		}

		let mut columns = columns_from_source(&self.source);
		columns.append_rows(&self.row_layout, batch_rows.into_iter(), found_row_numbers)?;

		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		Some(self.headers.clone())
	}
}

/// Range scan by row numbers (start..=end)
pub(crate) struct RowRangeScanNode<'a> {
	source: ResolvedSource<'a>,
	#[allow(dead_code)]
	start: u64,
	end: u64,
	context: Option<Arc<ExecutionContext<'a>>>,
	headers: ColumnHeaders<'a>,
	row_layout: EncodedValuesLayout,
	current_row: u64,
	exhausted: bool,
}

impl<'a> RowRangeScanNode<'a> {
	pub fn new(
		source: ResolvedSource<'a>,
		start: u64,
		end: u64,
		context: Arc<ExecutionContext<'a>>,
	) -> crate::Result<Self> {
		let (headers, row_layout) = build_headers_and_layout(&source)?;

		Ok(Self {
			source,
			start,
			end,
			context: Some(context),
			headers,
			row_layout,
			current_row: start,
			exhausted: false,
		})
	}
}

impl<'a> QueryNode<'a> for RowRangeScanNode<'a> {
	#[instrument(name = "query::scan::range::initialize", level = "trace", skip_all)]
	fn initialize(
		&mut self,
		_rx: &mut crate::StandardTransaction<'a>,
		_ctx: &ExecutionContext<'a>,
	) -> crate::Result<()> {
		Ok(())
	}

	#[instrument(name = "query::scan::range::next", level = "trace", skip_all)]
	fn next(
		&mut self,
		rx: &mut crate::StandardTransaction<'a>,
		ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
		let stored_ctx = self.context.as_ref().unwrap();
		let batch_size = stored_ctx.batch_size as usize;

		if self.exhausted || self.current_row > self.end {
			return Ok(None);
		}

		let source_id = get_source_id(&self.source)?;
		let mut batch_rows = Vec::new();
		let mut found_row_numbers = Vec::new();

		// Fetch up to batch_size rows in the range
		let batch_end = (self.current_row + batch_size as u64 - 1).min(self.end);

		for row_num in self.current_row..=batch_end {
			let row_key = RowKey {
				source: source_id,
				row: RowNumber(row_num),
			};
			let encoded_key = row_key.encode();

			if let Some(multi_values) = rx.get(&encoded_key)? {
				batch_rows.push(multi_values.values);
				found_row_numbers.push(RowNumber(row_num));
			}
			// Skip rows that don't exist (sparse storage)
		}

		self.current_row = batch_end + 1;
		if self.current_row > self.end {
			self.exhausted = true;
		}

		if batch_rows.is_empty() {
			// No rows found in this range segment
			if !self.exhausted {
				return self.next(rx, ctx);
			}
			return Ok(None);
		}

		let mut columns = columns_from_source(&self.source);
		columns.append_rows(&self.row_layout, batch_rows.into_iter(), found_row_numbers)?;

		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		Some(self.headers.clone())
	}
}

// Helper functions

fn build_headers_and_layout<'a>(
	source: &ResolvedSource<'a>,
) -> crate::Result<(ColumnHeaders<'a>, EncodedValuesLayout)> {
	let columns = match source {
		ResolvedSource::Table(table) => table.columns(),
		ResolvedSource::View(view) => view.columns(),
		ResolvedSource::RingBuffer(rb) => rb.columns(),
		_ => {
			reifydb_type::return_internal_error!("Row lookup not supported for this source type");
		}
	};

	let data = columns.iter().map(|c| c.constraint.get_type()).collect::<Vec<_>>();
	let row_layout = EncodedValuesLayout::new(&data);

	let headers = ColumnHeaders {
		columns: columns.iter().map(|col| Fragment::owned_internal(&col.name)).collect(),
	};

	Ok((headers, row_layout))
}

fn get_source_id(source: &ResolvedSource<'_>) -> crate::Result<SourceId> {
	match source {
		ResolvedSource::Table(table) => Ok(table.def().id.into()),
		ResolvedSource::View(view) => Ok(view.def().id.into()),
		ResolvedSource::RingBuffer(rb) => Ok(rb.def().id.into()),
		_ => reifydb_type::internal_err!("Row lookup not supported for this source type"),
	}
}

fn columns_from_source<'a>(source: &ResolvedSource<'a>) -> Columns<'a> {
	match source {
		ResolvedSource::Table(table) => Columns::from_table(table),
		ResolvedSource::View(view) => Columns::from_view(view),
		ResolvedSource::RingBuffer(rb) => Columns::from_ringbuffer(rb),
		_ => Columns::empty(),
	}
}
