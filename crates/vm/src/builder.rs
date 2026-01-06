// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::Columns;

use crate::{
	error::Result,
	expr::{Expr, compile_expr, compile_filter},
	operator::{FilterOp, ProjectOp, SelectOp, TakeOp},
	pipeline::{Pipeline, collect},
	source,
};

/// Fluent builder for constructing pipelines.
pub struct PipelineBuilder {
	pipeline: Pipeline,
}

impl PipelineBuilder {
	/// Create a builder from a Columns batch.
	pub fn from_columns(data: Columns) -> Self {
		Self {
			pipeline: source::from_columns(data),
		}
	}

	/// Create a builder from multiple batches.
	pub fn from_batches(batches: Vec<Columns>) -> Self {
		Self {
			pipeline: source::from_batches(batches),
		}
	}

	/// Create a builder from an existing pipeline.
	pub fn from_pipeline(pipeline: Pipeline) -> Self {
		Self {
			pipeline,
		}
	}

	/// Filter rows based on a predicate expression.
	pub fn filter(self, predicate: Expr) -> Self {
		let compiled = compile_filter(predicate);
		Self {
			pipeline: FilterOp::new(compiled).apply(self.pipeline),
		}
	}

	/// Select specific columns by name.
	pub fn select(self, columns: Vec<String>) -> Self {
		Self {
			pipeline: SelectOp::new(columns).apply(self.pipeline),
		}
	}

	/// Select specific columns by name (convenience for string slices).
	pub fn select_cols(self, columns: &[&str]) -> Self {
		let columns: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
		self.select(columns)
	}

	/// Add computed columns while keeping input columns.
	pub fn extend(self, extensions: Vec<(String, Expr)>) -> Self {
		let compiled: Vec<_> = extensions.into_iter().map(|(name, expr)| (name, compile_expr(expr))).collect();
		Self {
			pipeline: ProjectOp::extend(compiled).apply(self.pipeline),
		}
	}

	/// Replace columns with computed columns.
	pub fn project(self, extensions: Vec<(String, Expr)>) -> Self {
		let compiled: Vec<_> = extensions.into_iter().map(|(name, expr)| (name, compile_expr(expr))).collect();
		Self {
			pipeline: ProjectOp::replace(compiled).apply(self.pipeline),
		}
	}

	/// Limit the number of rows returned.
	pub fn take(self, limit: usize) -> Self {
		Self {
			pipeline: TakeOp::new(limit).apply(self.pipeline),
		}
	}

	/// Get the underlying pipeline.
	pub fn build(self) -> Pipeline {
		self.pipeline
	}

	/// Collect all results into a single Columns batch.
	pub fn collect(self) -> Result<Columns> {
		collect(self.pipeline)
	}
}
