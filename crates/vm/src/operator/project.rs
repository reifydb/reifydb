// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::{Column, Columns};
use reifydb_type::Fragment;
use tokio_stream::StreamExt as TokioStreamExt;

use crate::{
	expr::{CompiledExpr, EvalContext},
	pipeline::Pipeline,
};

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
	#[allow(dead_code)]
	pub fn replace(extensions: Vec<(String, CompiledExpr)>) -> Self {
		Self {
			extensions,
			keep_input: false,
			eval_ctx: EvalContext::new(),
		}
	}

	pub fn apply(&self, input: Pipeline) -> Pipeline {
		let extensions = self.extensions.clone();
		let keep_input = self.keep_input;
		let eval_ctx = self.eval_ctx.clone();

		Box::pin(TokioStreamExt::then(input, move |result| {
			let extensions = extensions.clone();
			let eval_ctx = eval_ctx.clone();
			async move {
				let batch = result?;
				let mut new_columns: Vec<Column> = Vec::new();

				if keep_input {
					// Copy input columns
					new_columns.extend(batch.iter().cloned());
				}

				// Add computed columns
				for (name, compiled_expr) in &extensions {
					let col = compiled_expr.eval(&batch, &eval_ctx).await?;
					// Set the column name
					let col = Column::new(Fragment::internal(name), col.data().clone());
					new_columns.push(col);
				}

				Ok(Columns::with_row_numbers(new_columns, batch.row_numbers.to_vec()))
			}
		}))
	}
}
