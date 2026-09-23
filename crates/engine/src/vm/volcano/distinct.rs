// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashSet, sync::Arc};

use arrow_array::{Array, ArrayRef, Float32Array, Float64Array, LargeBinaryArray};
use arrow_row::{Row, RowConverter, SortField};
use reifydb_core::{
	error::diagnostic::operation,
	interface::resolved::ResolvedColumn,
	internal_error,
	value::column::{buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error,
	fragment::Fragment,
	value::{container::bignum_array::decimal_at, decimal::Decimal},
};
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode, charge_query_memory},
};

fn ensure_distinct_keyable(name: &Fragment, data: &ColumnBuffer) -> Result<()> {
	let ty = data.get_type();
	if !ty.is_scalar() {
		return Err(error!(operation::distinct_key_unkeyable(name.clone(), ty)));
	}
	Ok(())
}

macro_rules! canonical_float {
	($value:expr, $ty:ty) => {
		if $value.is_nan() {
			<$ty>::NAN
		} else if $value == 0.0 {
			0.0
		} else {
			$value
		}
	};
}

fn normalized_decimals(container: &LargeBinaryArray) -> ArrayRef {
	let mut values = Vec::with_capacity(container.len());
	let mut bitvec = Vec::with_capacity(container.len());
	for index in 0..container.len() {
		match decimal_at(container, index) {
			Some(value) => {
				values.push(Decimal(value.0.normalized()));
				bitvec.push(true);
			}
			None => {
				values.push(Decimal::default());
				bitvec.push(false);
			}
		}
	}
	ColumnBuffer::decimal_with_bitvec(values, bitvec).to_array_ref()
}

fn key_array(column: &ColumnBuffer) -> ArrayRef {
	match column {
		ColumnBuffer::Decimal {
			container,
			..
		} => normalized_decimals(container),
		ColumnBuffer::Float4(container) => {
			Arc::new(container.iter().map(|v| v.map(|f| canonical_float!(f, f32))).collect::<Float32Array>())
		}
		ColumnBuffer::Float8(container) => {
			Arc::new(container.iter().map(|v| v.map(|f| canonical_float!(f, f64))).collect::<Float64Array>())
		}
		other => other.to_array_ref(),
	}
}

fn key_rows(key_columns: &[&ColumnBuffer]) -> Result<(RowConverter, Vec<ArrayRef>)> {
	let arrays: Vec<ArrayRef> = key_columns.iter().map(|column| key_array(column)).collect();
	let fields = arrays.iter().map(|array| SortField::new(array.data_type().clone())).collect();
	let converter =
		RowConverter::new(fields).map_err(|e| internal_error!("Failed to build distinct keys: {}", e))?;
	Ok((converter, arrays))
}

pub(crate) struct DistinctNode {
	input: Box<dyn QueryNode>,
	columns: Vec<ResolvedColumn>,
	headers: Option<ColumnHeaders>,
}

impl DistinctNode {
	pub fn new(input: Box<dyn QueryNode>, columns: Vec<ResolvedColumn>) -> Self {
		Self {
			input,
			columns,
			headers: None,
		}
	}

	#[instrument(level = "trace", skip_all, name = "volcano::distinct::collect")]
	fn collect_input<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		let mut all_columns: Option<Columns> = None;
		let mut charged = 0usize;

		while let Some(cols) = self.input.next(rx, ctx)? {
			match &mut all_columns {
				None => all_columns = Some(cols),
				Some(existing) => existing.append_columns(cols)?,
			}
			if let Some(acc) = &all_columns {
				charge_query_memory(&ctx.memory, &mut charged, acc)?;
			}
		}

		Ok(all_columns)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::distinct::dedupe")]
	fn dedupe(&self, all_columns: &Columns) -> Result<Vec<usize>> {
		let mut key_columns: Vec<&ColumnBuffer> = Vec::new();
		if self.columns.is_empty() {
			for col in all_columns.iter() {
				ensure_distinct_keyable(col.name(), col.data())?;
				key_columns.push(col.data());
			}
		} else {
			for column in &self.columns {
				if let Some(col) = all_columns.column(column.name()) {
					ensure_distinct_keyable(column.identifier(), col.data())?;
					key_columns.push(col.data());
				}
			}
		}

		let row_count = all_columns.row_count();
		if key_columns.is_empty() {
			return Ok((0..row_count).take(1).collect());
		}

		let (converter, arrays) = key_rows(&key_columns)?;
		let rows = converter
			.convert_columns(&arrays)
			.map_err(|e| internal_error!("Failed to build distinct keys: {}", e))?;

		let mut seen = HashSet::<Row>::with_capacity(row_count);
		let mut kept_indices = Vec::new();
		for row_idx in 0..row_count {
			if seen.insert(rows.row(row_idx)) {
				kept_indices.push(row_idx);
			}
		}

		Ok(kept_indices)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::distinct::extract")]
	fn extract(all_columns: &Columns, kept_indices: &[usize]) -> Columns {
		all_columns.extract_by_indices(kept_indices)
	}
}

impl QueryNode for DistinctNode {
	#[instrument(level = "trace", skip_all, name = "volcano::distinct::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::distinct::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		if self.headers.is_some() {
			return Ok(None);
		}

		let all_columns = self.collect_input(rx, ctx)?;

		let all_columns = match all_columns {
			Some(cols) => cols,
			None => {
				self.headers = Some(ColumnHeaders::empty());
				return Ok(None);
			}
		};

		let kept_indices = self.dedupe(&all_columns)?;

		let result = if kept_indices.is_empty() {
			all_columns
		} else {
			Self::extract(&all_columns, &kept_indices)
		};
		self.headers = Some(ColumnHeaders::from_columns(&result));

		Ok(Some(result))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone().or(self.input.headers())
	}
}
