// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Row-number optimized access nodes for O(1) and O(k) lookups.
//!
//! These nodes bypass full table scans when filtering by row number:
//! - `RowPointLookupNode`: Single row O(1) lookup
//! - `RowListLookupNode`: Multiple discrete rows O(k) lookup
//! - `RowRangeScanNode`: Row number range scan

use std::sync::Arc;

use reifydb_core::{
	encoded::{encoded::EncodedValues, schema::Schema},
	interface::{catalog::primitive::PrimitiveId, resolved::ResolvedPrimitive},
	key::row::RowKey,
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::standard::StandardTransaction;
use reifydb_core::error::diagnostic::internal::internal;
use reifydb_type::{
	fragment::Fragment,
	value::{row_number::RowNumber, r#type::Type},
};
use tracing::instrument;

use crate::execute::{Batch, ExecutionContext, QueryNode};

/// O(1) point lookup by row number
pub(crate) struct RowPointLookupNode {
	source: ResolvedPrimitive,
	row_number: u64,
	context: Option<Arc<ExecutionContext>>,
	headers: ColumnHeaders,
	schema: Option<Schema>,
	exhausted: bool,
}

impl<'a> RowPointLookupNode {
	pub fn new(source: ResolvedPrimitive, row_number: u64, context: Arc<ExecutionContext>) -> crate::Result<Self> {
		let (headers, _) = build_headers_and_storage_types(&source)?;

		Ok(Self {
			source,
			row_number,
			context: Some(context),
			headers,
			schema: None,
			exhausted: false,
		})
	}

	fn get_or_load_schema(
		&mut self,
		rx: &mut StandardTransaction,
		first_row: &EncodedValues,
	) -> crate::Result<Schema> {
		if let Some(schema) = &self.schema {
			return Ok(schema.clone());
		}

		let fingerprint = first_row.fingerprint();

		let stored_ctx = self.context.as_ref().expect("RowPointLookupNode context not set");
		let schema = stored_ctx.executor.catalog.schema.get_or_load(fingerprint, rx)?.ok_or_else(|| {
			reifydb_type::error!(reifydb_core::error::diagnostic::internal::internal(format!(
				"Schema with fingerprint {:?} not found",
				fingerprint
			)))
		})?;

		self.schema = Some(schema.clone());

		Ok(schema)
	}
}

impl QueryNode for RowPointLookupNode {
	#[instrument(name = "query::lookup::point::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, _rx: &mut StandardTransaction<'a>, _ctx: &ExecutionContext) -> crate::Result<()> {
		Ok(())
	}

	#[instrument(name = "query::lookup::point::next", level = "trace", skip_all)]
	fn next<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		_ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}
		self.exhausted = true;

		let source_id = get_source_id(&self.source)?;
		let encoded_key = RowKey::encoded(source_id, RowNumber(self.row_number));

		// O(1) point lookup
		if let Some(multi_values) = rx.get(&encoded_key)? {
			let mut columns = columns_from_source(&self.source);
			let schema = self.get_or_load_schema(rx, &multi_values.values)?;
			columns.append_rows(
				&schema,
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

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}

/// O(k) list lookup by row numbers
pub(crate) struct RowListLookupNode {
	source: ResolvedPrimitive,
	row_numbers: Vec<u64>,
	context: Option<Arc<ExecutionContext>>,
	headers: ColumnHeaders,
	schema: Option<Schema>,
	current_index: usize,
}

impl<'a> RowListLookupNode {
	pub fn new(
		source: ResolvedPrimitive,
		row_numbers: Vec<u64>,
		context: Arc<ExecutionContext>,
	) -> crate::Result<Self> {
		let (headers, _) = build_headers_and_storage_types(&source)?;

		Ok(Self {
			source,
			row_numbers,
			context: Some(context),
			headers,
			schema: None,
			current_index: 0,
		})
	}

	fn get_or_load_schema(
		&mut self,
		rx: &mut StandardTransaction,
		first_row: &EncodedValues,
	) -> crate::Result<Schema> {
		if let Some(schema) = &self.schema {
			return Ok(schema.clone());
		}

		let fingerprint = first_row.fingerprint();

		let stored_ctx = self.context.as_ref().expect("RowListLookupNode context not set");
		let schema = stored_ctx.executor.catalog.schema.get_or_load(fingerprint, rx)?.ok_or_else(|| {
			reifydb_type::error!(reifydb_core::error::diagnostic::internal::internal(format!(
				"Schema with fingerprint {:?} not found",
				fingerprint
			)))
		})?;

		self.schema = Some(schema.clone());

		Ok(schema)
	}
}

impl QueryNode for RowListLookupNode {
	#[instrument(name = "query::lookup::list::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, _rx: &mut StandardTransaction<'a>, _ctx: &ExecutionContext) -> crate::Result<()> {
		Ok(())
	}

	#[instrument(name = "query::lookup::list::next", level = "trace", skip_all)]
	fn next<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
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
			let encoded_key = RowKey::encoded(source_id, RowNumber(row_num));

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
		let schema = self.get_or_load_schema(rx, &batch_rows[0])?;
		columns.append_rows(&schema, batch_rows.into_iter(), found_row_numbers)?;

		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}

/// Range scan by row numbers (start..=end)
pub(crate) struct RowRangeScanNode {
	source: ResolvedPrimitive,
	#[allow(dead_code)]
	start: u64,
	end: u64,
	context: Option<Arc<ExecutionContext>>,
	headers: ColumnHeaders,
	schema: Option<Schema>,
	current_row: u64,
	exhausted: bool,
}

impl<'a> RowRangeScanNode {
	pub fn new(
		source: ResolvedPrimitive,
		start: u64,
		end: u64,
		context: Arc<ExecutionContext>,
	) -> crate::Result<Self> {
		let (headers, _) = build_headers_and_storage_types(&source)?;

		Ok(Self {
			source,
			start,
			end,
			context: Some(context),
			headers,
			schema: None,
			current_row: start,
			exhausted: false,
		})
	}

	fn get_or_load_schema(
		&mut self,
		rx: &mut StandardTransaction,
		first_row: &EncodedValues,
	) -> crate::Result<Schema> {
		if let Some(schema) = &self.schema {
			return Ok(schema.clone());
		}

		let fingerprint = first_row.fingerprint();

		let stored_ctx = self.context.as_ref().expect("RowRangeScanNode context not set");
		let schema = stored_ctx.executor.catalog.schema.get_or_load(fingerprint, rx)?.ok_or_else(|| {
			reifydb_type::error!(internal(format!("Schema with fingerprint {:?} not found", fingerprint)))
		})?;

		self.schema = Some(schema.clone());

		Ok(schema)
	}
}

impl QueryNode for RowRangeScanNode {
	#[instrument(name = "query::scan::range::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, _rx: &mut StandardTransaction<'a>, _ctx: &ExecutionContext) -> crate::Result<()> {
		Ok(())
	}

	#[instrument(name = "query::scan::range::next", level = "trace", skip_all)]
	fn next<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
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
			let encoded_key = RowKey::encoded(source_id, RowNumber(row_num));

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
		let schema = self.get_or_load_schema(rx, &batch_rows[0])?;
		columns.append_rows(&schema, batch_rows.into_iter(), found_row_numbers)?;

		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}

// Helper functions

fn build_headers_and_storage_types<'a>(source: &ResolvedPrimitive) -> crate::Result<(ColumnHeaders, Vec<Type>)> {
	let columns = match source {
		ResolvedPrimitive::Table(table) => table.columns(),
		ResolvedPrimitive::View(view) => view.columns(),
		ResolvedPrimitive::RingBuffer(rb) => rb.columns(),
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

fn get_source_id(source: &ResolvedPrimitive) -> crate::Result<PrimitiveId> {
	match source {
		ResolvedPrimitive::Table(table) => Ok(table.def().id.into()),
		ResolvedPrimitive::View(view) => Ok(view.def().id.into()),
		ResolvedPrimitive::RingBuffer(rb) => Ok(rb.def().id.into()),
		_ => reifydb_core::internal_err!("Row lookup not supported for this source type"),
	}
}

fn columns_from_source<'a>(source: &ResolvedPrimitive) -> Columns {
	match source {
		ResolvedPrimitive::Table(table) => Columns::from_table(table),
		ResolvedPrimitive::View(view) => Columns::from_view(view),
		ResolvedPrimitive::RingBuffer(rb) => Columns::from_ringbuffer(rb),
		_ => Columns::empty(),
	}
}
