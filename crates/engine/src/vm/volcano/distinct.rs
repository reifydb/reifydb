// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use reifydb_codec::key::{encoded::EncodedKey, serializer::KeySerializer};
use reifydb_core::{
	error::diagnostic::operation,
	interface::resolved::ResolvedColumn,
	value::column::{buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error, fragment::Fragment};
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

fn row_key(key_columns: &[&ColumnBuffer], row_idx: usize) -> Result<EncodedKey> {
	let mut serializer = KeySerializer::new();
	for column in key_columns {
		column.extend_key(row_idx, &mut serializer)?;
	}
	Ok(serializer.to_encoded_key())
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

		let mut seen = HashSet::<EncodedKey>::new();
		let mut kept_indices = Vec::new();
		for row_idx in 0..all_columns.row_count() {
			let key = row_key(&key_columns, row_idx)?;
			if seen.insert(key) {
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
