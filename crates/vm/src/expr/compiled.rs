// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Pre-compiled expressions using the closure compilation technique.
//!
//! Instead of interpreting an AST at runtime, we "compile" expressions into
//! nested Rust closures. This eliminates enum dispatch overhead while keeping
//! the code safe (no JIT, no unsafe).
//!
//! Reference: https://blog.cloudflare.com/building-fast-interpreters-in-rust/

use std::sync::Arc;

use reifydb_core::value::column::{Column, Columns};
use reifydb_type::BitVec;

use super::eval::EvalContext;
use crate::error::Result;

/// Pre-compiled expression that evaluates to a Column.
///
/// The closure captures static information (column names, literals, operators)
/// at compile time, and receives dynamic information (columns, context) at
/// evaluation time.
pub struct CompiledExpr(Arc<dyn Fn(&Columns, &EvalContext) -> Result<Column> + Send + Sync>);

impl CompiledExpr {
	/// Create a new compiled expression from a closure.
	pub fn new<F>(f: F) -> Self
	where
		F: Fn(&Columns, &EvalContext) -> Result<Column> + Send + Sync + 'static,
	{
		Self(Arc::new(f))
	}

	/// Evaluate the expression against the given columns and context.
	pub fn eval(&self, columns: &Columns, ctx: &EvalContext) -> Result<Column> {
		(self.0)(columns, ctx)
	}
}

impl Clone for CompiledExpr {
	fn clone(&self) -> Self {
		Self(Arc::clone(&self.0))
	}
}

impl std::fmt::Debug for CompiledExpr {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("CompiledExpr").finish_non_exhaustive()
	}
}

/// Pre-compiled filter expression that evaluates directly to a BitVec mask.
///
/// This is a specialization of CompiledExpr for filter predicates, avoiding
/// the intermediate Column allocation when we only need a boolean mask.
pub struct CompiledFilter(Arc<dyn Fn(&Columns, &EvalContext) -> Result<BitVec> + Send + Sync>);

impl CompiledFilter {
	/// Create a new compiled filter from a closure.
	pub fn new<F>(f: F) -> Self
	where
		F: Fn(&Columns, &EvalContext) -> Result<BitVec> + Send + Sync + 'static,
	{
		Self(Arc::new(f))
	}

	/// Evaluate the filter against the given columns and context.
	/// Returns a BitVec where true = row passes the filter.
	pub fn eval(&self, columns: &Columns, ctx: &EvalContext) -> Result<BitVec> {
		(self.0)(columns, ctx)
	}
}

impl Clone for CompiledFilter {
	fn clone(&self) -> Self {
		Self(Arc::clone(&self.0))
	}
}

impl std::fmt::Debug for CompiledFilter {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("CompiledFilter").finish_non_exhaustive()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::ColumnData;
	use reifydb_type::Fragment;

	use super::*;

	#[test]
	fn test_compiled_expr_clone() {
		let expr = CompiledExpr::new(|columns, _ctx| Ok(columns.iter().next().unwrap().clone()));
		let cloned = expr.clone();

		let columns = Columns::new(vec![Column::new(Fragment::from("x"), ColumnData::int8(vec![1, 2, 3]))]);

		let result1 = expr.eval(&columns, &EvalContext::new()).unwrap();
		let result2 = cloned.eval(&columns, &EvalContext::new()).unwrap();

		assert_eq!(result1.data().len(), result2.data().len());
	}

	#[test]
	fn test_compiled_filter_basic() {
		// Filter that returns all true
		let filter = CompiledFilter::new(|columns, _ctx| {
			let len = columns.row_count();
			Ok(BitVec::from_fn(len, |_| true))
		});

		let columns = Columns::new(vec![Column::new(Fragment::from("x"), ColumnData::int8(vec![1, 2, 3]))]);

		let mask = filter.eval(&columns, &EvalContext::new()).unwrap();
		assert_eq!(mask.len(), 3);
		assert!(mask.iter().all(|b| b));
	}
}
