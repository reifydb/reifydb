// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	Batch,
	value::column::{Column, Columns},
};
use reifydb_rqlv2::expression::{CompiledExpr, EvalContext};
use reifydb_type::Fragment;

use crate::{error::Result, pipeline::Pipeline};

/// Project operator - adds computed columns using compiled expressions.
pub struct ProjectOp {
	/// New columns to add: (name, compiled_expression)
	pub extensions: Vec<(String, CompiledExpr)>,
	/// Whether to keep original columns
	pub keep_input: bool,
	/// Evaluation context with scope variables
	pub eval_ctx: EvalContext,
}

impl ProjectOp {
	/// Create a project that keeps input columns and adds new ones
	pub fn extend(extensions: Vec<(String, CompiledExpr)>) -> Self {
		Self {
			extensions,
			keep_input: true,
			eval_ctx: EvalContext::new(),
		}
	}

	/// Create a project with context that keeps input columns and adds new ones
	pub fn extend_with_context(extensions: Vec<(String, CompiledExpr)>, eval_ctx: EvalContext) -> Self {
		Self {
			extensions,
			keep_input: true,
			eval_ctx,
		}
	}

	/// Create a project that replaces columns entirely
	pub fn replace(extensions: Vec<(String, CompiledExpr)>) -> Self {
		Self {
			extensions,
			keep_input: false,
			eval_ctx: EvalContext::new(),
		}
	}

	/// Create a project with context that replaces columns entirely
	pub fn replace_with_context(extensions: Vec<(String, CompiledExpr)>, eval_ctx: EvalContext) -> Self {
		Self {
			extensions,
			keep_input: false,
			eval_ctx,
		}
	}

	pub fn apply(&self, input: Pipeline) -> Pipeline {
		Box::new(ProjectIterator {
			input,
			extensions: self.extensions.clone(),
			keep_input: self.keep_input,
			eval_ctx: self.eval_ctx.clone(),
		})
	}
}

/// Iterator that applies projections to each batch
struct ProjectIterator {
	input: Pipeline,
	extensions: Vec<(String, CompiledExpr)>,
	keep_input: bool,
	eval_ctx: EvalContext,
}

impl Iterator for ProjectIterator {
	type Item = Result<Batch>;

	fn next(&mut self) -> Option<Self::Item> {
		let batch = match self.input.next()? {
			Ok(b) => b,
			Err(e) => return Some(Err(e)),
		};

		// Materialize batch for expression evaluation
		let columns = batch.into_columns();
		let mut new_columns: Vec<Column> = Vec::new();

		if self.keep_input {
			// Copy input columns
			new_columns.extend(columns.iter().cloned());
		}

		// Add computed columns
		for (name, compiled_expr) in &self.extensions {
			let col = match compiled_expr.eval(&columns, &self.eval_ctx) {
				Ok(c) => c,
				Err(e) => return Some(Err(e.into())),
			};
			// Set the column name
			let col = Column::new(Fragment::internal(name), col.data().clone());
			new_columns.push(col);
		}

		let result_columns = Columns::with_row_numbers(new_columns, columns.row_numbers.to_vec());
		Some(Ok(Batch::fully_materialized(result_columns)))
	}
}
