// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::evaluate::TargetColumn, value::column::columns::Columns};
use reifydb_function::registry::Functions;
use reifydb_runtime::clock::Clock;
use reifydb_type::params::Params;

use crate::{
	evaluate::{ColumnEvaluationContext, column::StandardColumnEvaluator},
	vm::stack::SymbolTable,
};

/// Runtime context passed to `CompiledExpr::execute()`.
///
/// Carries everything needed for execution, bridging from the existing
/// `ColumnEvaluationContext` plus resources for evaluator delegation.
pub struct ExecContext<'a> {
	pub target: Option<TargetColumn>,
	pub columns: Columns,
	pub row_count: usize,
	pub take: Option<usize>,
	pub params: &'a Params,
	pub symbol_table: &'a SymbolTable,
	pub is_aggregate_context: bool,
	// Resources for evaluator delegation
	pub functions: &'a Functions,
	pub clock: &'a Clock,
	/// Cached evaluator for delegation to StandardColumnEvaluator methods.
	pub(crate) evaluator: StandardColumnEvaluator,
}

impl<'a> ExecContext<'a> {
	/// Bridge from the existing `ColumnEvaluationContext` to `ExecContext`.
	pub fn from_column_eval_ctx(
		ctx: &'a ColumnEvaluationContext<'a>,
		functions: &'a Functions,
		clock: &'a Clock,
	) -> Self {
		ExecContext {
			target: ctx.target.clone(),
			columns: ctx.columns.clone(),
			row_count: ctx.row_count,
			take: ctx.take,
			params: ctx.params,
			symbol_table: ctx.symbol_table,
			is_aggregate_context: ctx.is_aggregate_context,
			evaluator: StandardColumnEvaluator::new(functions.clone(), clock.clone()),
			functions,
			clock,
		}
	}

	/// Convert back to `ColumnEvaluationContext` for evaluator delegation.
	pub fn to_column_eval_ctx(&self) -> ColumnEvaluationContext<'_> {
		ColumnEvaluationContext {
			target: self.target.clone(),
			columns: self.columns.clone(),
			row_count: self.row_count,
			take: self.take,
			params: self.params,
			symbol_table: self.symbol_table,
			is_aggregate_context: self.is_aggregate_context,
		}
	}
}

/// Compile-time context for resolving functions and UDFs.
pub struct CompileContext<'a> {
	pub functions: &'a Functions,
	pub symbol_table: &'a SymbolTable,
}
