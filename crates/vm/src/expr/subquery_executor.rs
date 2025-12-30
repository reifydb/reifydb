// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Runtime subquery executor implementation.

use std::{
	collections::HashMap,
	sync::{Arc, RwLock},
};

use async_trait::async_trait;
use reifydb_core::value::column::Columns;

use super::{EvalContext, SubqueryExecutor};
use crate::{
	bytecode::Program,
	error::{Result, VmError},
	operator::{FilterOp, SelectOp, TakeOp},
	pipeline,
	source::SourceRegistry,
};

/// Runtime implementation of SubqueryExecutor.
///
/// This executor builds and executes subquery pipelines using the compiled
/// SubqueryDef from the Program. It caches results for uncorrelated subqueries
/// and supports per-row execution for correlated subqueries.
pub struct RuntimeSubqueryExecutor {
	program: Arc<Program>,
	sources: Arc<dyn SourceRegistry + Send + Sync>,
	cache: RwLock<HashMap<u16, Columns>>,
}

impl RuntimeSubqueryExecutor {
	/// Create a new runtime subquery executor.
	pub fn new(program: Arc<Program>, sources: Arc<dyn SourceRegistry + Send + Sync>) -> Self {
		Self {
			program,
			sources,
			cache: RwLock::new(HashMap::new()),
		}
	}
}

#[async_trait]
impl SubqueryExecutor for RuntimeSubqueryExecutor {
	async fn execute(&self, index: u16, ctx: &EvalContext) -> Result<Columns> {
		// Get subquery definition
		let subquery_def =
			self.program.subqueries.get(index as usize).ok_or(VmError::InvalidSubqueryIndex {
				index,
			})?;

		// For correlated subqueries, skip cache
		let is_correlated = !subquery_def.outer_refs.is_empty();

		// Check cache first (for uncorrelated subqueries only)
		if !is_correlated {
			if let Some(cached) = self.cache.read().unwrap().get(&index) {
				return Ok(cached.clone());
			}
		}

		// Get source and start pipeline
		let source =
			self.sources.get_source(&subquery_def.source_name).ok_or_else(|| VmError::TableNotFound {
				name: subquery_def.source_name.clone(),
			})?;

		let mut pipeline_stream = source.scan();

		// Apply filter if present
		if let Some(filter_idx) = subquery_def.filter_expr_index {
			let filter = self.program.compiled_filters.get(filter_idx as usize).ok_or(
				VmError::InvalidExpressionIndex {
					index: filter_idx,
				},
			)?;

			// Create context for filter execution
			// For correlated subqueries, outer row values come from ctx.current_row_values
			let filter_ctx = ctx.clone();

			pipeline_stream = FilterOp::with_context(filter.clone(), filter_ctx).apply(pipeline_stream);
		}

		// Apply select if present
		if let Some(select_idx) = subquery_def.select_list_index {
			let columns = self.program.column_lists.get(select_idx as usize).ok_or(
				VmError::InvalidColumnListIndex {
					index: select_idx,
				},
			)?;
			pipeline_stream = SelectOp::new(columns.clone()).apply(pipeline_stream);
		}

		// Apply take if present
		if let Some(limit) = subquery_def.take_limit {
			pipeline_stream = TakeOp::new(limit as usize).apply(pipeline_stream);
		}

		// Execute pipeline - now fully async!
		let result = pipeline::collect(pipeline_stream).await?;

		// Cache result (for uncorrelated only)
		if !is_correlated {
			self.cache.write().unwrap().insert(index, result.clone());
		}

		Ok(result)
	}

	fn is_correlated(&self, index: u16) -> Result<bool> {
		let subquery_def =
			self.program.subqueries.get(index as usize).ok_or(VmError::InvalidSubqueryIndex {
				index,
			})?;

		Ok(!subquery_def.outer_refs.is_empty())
	}
}
