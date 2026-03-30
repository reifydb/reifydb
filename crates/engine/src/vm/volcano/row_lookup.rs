// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Row-number optimized access nodes for O(1) and O(k) lookups.
//!
//! These nodes bypass full table scans when filtering by row number:
//! - `RowPointLookupNode`: Single row O(1) lookup
//! - `RowListLookupNode`: Multiple discrete rows O(k) lookup
//! - `RowRangeScanNode`: Row number range scan

use std::{iter, sync::Arc};

use reifydb_core::{
	encoded::{row::EncodedRow, shape::RowShape},
	interface::{catalog::shape::ShapeId, resolved::ResolvedShape},
	internal_err, internal_error,
	key::row::RowKey,
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::{row_number::RowNumber, r#type::Type},
};
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

/// O(1) point lookup by row number
pub(crate) struct RowPointLookupNode {
	source: ResolvedShape,
	row_number: u64,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,
	shape: Option<RowShape>,
	exhausted: bool,
}

impl RowPointLookupNode {
	pub fn new(source: ResolvedShape, row_number: u64, context: Arc<QueryContext>) -> Result<Self> {
		let (headers, _) = build_headers_and_storage_types(&source)?;

		Ok(Self {
			source,
			row_number,
			context: Some(context),
			headers,
			shape: None,
			exhausted: false,
		})
	}

	fn get_or_load_shape(&mut self, rx: &mut Transaction, first_row: &EncodedRow) -> Result<RowShape> {
		if let Some(shape) = &self.shape {
			return Ok(shape.clone());
		}

		let fingerprint = first_row.fingerprint();

		let stored_ctx = self.context.as_ref().expect("RowPointLookupNode context not set");
		let shape =
			stored_ctx.services.catalog.shape.get_or_load(fingerprint, rx)?.ok_or_else(|| {
				internal_error!("RowShape with fingerprint {:?} not found", fingerprint)
			})?;

		self.shape = Some(shape.clone());

		Ok(shape)
	}
}

impl QueryNode for RowPointLookupNode {
	#[instrument(name = "volcano::lookup::point::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Ok(())
	}

	#[instrument(name = "volcano::lookup::point::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		if self.exhausted {
			return Ok(None);
		}
		self.exhausted = true;

		let shape_id = get_object_id(&self.source)?;
		let encoded_key = RowKey::encoded(shape_id, RowNumber(self.row_number));

		// O(1) point lookup
		if let Some(multi_values) = rx.get(&encoded_key)? {
			let mut columns = columns_from_shape(&self.source);
			let shape = self.get_or_load_shape(rx, &multi_values.row)?;
			columns.append_rows(&shape, iter::once(multi_values.row), vec![RowNumber(self.row_number)])?;

			Ok(Some(columns))
		} else {
			// Row not found - return empty result
			Ok(None)
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}

/// O(k) list lookup by row numbers
pub(crate) struct RowListLookupNode {
	source: ResolvedShape,
	row_numbers: Vec<u64>,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,
	shape: Option<RowShape>,
	current_index: usize,
}

impl RowListLookupNode {
	pub fn new(source: ResolvedShape, row_numbers: Vec<u64>, context: Arc<QueryContext>) -> Result<Self> {
		let (headers, _) = build_headers_and_storage_types(&source)?;

		Ok(Self {
			source,
			row_numbers,
			context: Some(context),
			headers,
			shape: None,
			current_index: 0,
		})
	}

	fn get_or_load_shape(&mut self, rx: &mut Transaction, first_row: &EncodedRow) -> Result<RowShape> {
		if let Some(shape) = &self.shape {
			return Ok(shape.clone());
		}

		let fingerprint = first_row.fingerprint();

		let stored_ctx = self.context.as_ref().expect("RowListLookupNode context not set");
		let shape =
			stored_ctx.services.catalog.shape.get_or_load(fingerprint, rx)?.ok_or_else(|| {
				internal_error!("RowShape with fingerprint {:?} not found", fingerprint)
			})?;

		self.shape = Some(shape.clone());

		Ok(shape)
	}
}

impl QueryNode for RowListLookupNode {
	#[instrument(name = "volcano::lookup::list::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Ok(())
	}

	#[instrument(name = "volcano::lookup::list::next", level = "trace", skip_all)]
	#[allow(clippy::only_used_in_recursion)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		let stored_ctx = self.context.as_ref().unwrap();
		let batch_size = stored_ctx.batch_size as usize;

		if self.current_index >= self.row_numbers.len() {
			return Ok(None);
		}

		let shape_id = get_object_id(&self.source)?;
		let mut batch_rows = Vec::new();
		let mut found_row_numbers = Vec::new();

		// Process up to batch_size rows
		let end_index = (self.current_index + batch_size).min(self.row_numbers.len());

		for &row_num in &self.row_numbers[self.current_index..end_index] {
			let encoded_key = RowKey::encoded(shape_id, RowNumber(row_num));

			// O(1) point lookup for each row
			if let Some(multi_values) = rx.get(&encoded_key)? {
				batch_rows.push(multi_values.row);
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

		let mut columns = columns_from_shape(&self.source);
		let shape = self.get_or_load_shape(rx, &batch_rows[0])?;
		columns.append_rows(&shape, batch_rows.into_iter(), found_row_numbers)?;

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}

/// Range scan by row numbers (start..=end)
pub(crate) struct RowRangeScanNode {
	source: ResolvedShape,
	#[allow(dead_code)]
	start: u64,
	end: u64,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,
	shape: Option<RowShape>,
	current_row: u64,
	exhausted: bool,
}

impl RowRangeScanNode {
	pub fn new(source: ResolvedShape, start: u64, end: u64, context: Arc<QueryContext>) -> Result<Self> {
		let (headers, _) = build_headers_and_storage_types(&source)?;

		Ok(Self {
			source,
			start,
			end,
			context: Some(context),
			headers,
			shape: None,
			current_row: start,
			exhausted: false,
		})
	}

	fn get_or_load_shape(&mut self, rx: &mut Transaction, first_row: &EncodedRow) -> Result<RowShape> {
		if let Some(shape) = &self.shape {
			return Ok(shape.clone());
		}

		let fingerprint = first_row.fingerprint();

		let stored_ctx = self.context.as_ref().expect("RowRangeScanNode context not set");
		let shape =
			stored_ctx.services.catalog.shape.get_or_load(fingerprint, rx)?.ok_or_else(|| {
				internal_error!("RowShape with fingerprint {:?} not found", fingerprint)
			})?;

		self.shape = Some(shape.clone());

		Ok(shape)
	}
}

impl QueryNode for RowRangeScanNode {
	#[instrument(name = "volcano::scan::range::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Ok(())
	}

	#[instrument(name = "volcano::scan::range::next", level = "trace", skip_all)]
	#[allow(clippy::only_used_in_recursion)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		let stored_ctx = self.context.as_ref().unwrap();
		let batch_size = stored_ctx.batch_size as usize;

		if self.exhausted || self.current_row > self.end {
			return Ok(None);
		}

		let shape_id = get_object_id(&self.source)?;
		let mut batch_rows = Vec::new();
		let mut found_row_numbers = Vec::new();

		// Fetch up to batch_size rows in the range
		let batch_end = (self.current_row + batch_size as u64 - 1).min(self.end);

		for row_num in self.current_row..=batch_end {
			let encoded_key = RowKey::encoded(shape_id, RowNumber(row_num));

			if let Some(multi_values) = rx.get(&encoded_key)? {
				batch_rows.push(multi_values.row);
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

		let mut columns = columns_from_shape(&self.source);
		let shape = self.get_or_load_shape(rx, &batch_rows[0])?;
		columns.append_rows(&shape, batch_rows.into_iter(), found_row_numbers)?;

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}

// Helper functions

fn build_headers_and_storage_types(source: &ResolvedShape) -> Result<(ColumnHeaders, Vec<Type>)> {
	let columns = match source {
		ResolvedShape::Table(table) => table.columns(),
		ResolvedShape::View(view) => view.columns(),
		ResolvedShape::RingBuffer(rb) => rb.columns(),
		_ => {
			unreachable!("Row lookup not supported for this source type");
		}
	};

	let storage_types = columns.iter().map(|c| c.constraint.get_type()).collect::<Vec<_>>();

	let headers = ColumnHeaders {
		columns: columns.iter().map(|col| Fragment::internal(&col.name)).collect(),
	};

	Ok((headers, storage_types))
}

fn get_object_id(source: &ResolvedShape) -> Result<ShapeId> {
	match source {
		ResolvedShape::Table(table) => Ok(table.def().id.into()),
		ResolvedShape::View(view) => Ok(view.def().underlying_id()),
		ResolvedShape::RingBuffer(rb) => Ok(rb.def().id.into()),
		_ => internal_err!("Row lookup not supported for this source type"),
	}
}

fn columns_from_shape(source: &ResolvedShape) -> Columns {
	match source {
		ResolvedShape::Table(table) => Columns::from_resolved_table(table),
		ResolvedShape::View(view) => Columns::from_resolved_view(view),
		ResolvedShape::RingBuffer(rb) => Columns::from_ringbuffer(rb),
		_ => Columns::empty(),
	}
}
