// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Pre-compiled expressions using the closure compilation technique.
//!
//! Instead of interpreting an AST at runtime, we "compile" expressions into
//! nested Rust closures that return futures. This eliminates enum dispatch
//! overhead while keeping the code safe (no JIT, no unsafe) and supporting
//! async subquery execution.
//!
//! Reference: https://blog.cloudflare.com/building-fast-interpreters-in-rust/

use std::{future::Future, pin::Pin, sync::Arc};

use reifydb_core::value::column::{Column, Columns};
use reifydb_type::BitVec;

use super::eval::EvalContext;

/// Error type for expression evaluation.
#[derive(Debug, Clone)]
pub enum EvalError {
	/// Column not found.
	ColumnNotFound {
		name: String,
	},
	/// Variable not found.
	VariableNotFound {
		id: u32,
	},
	/// Type mismatch.
	TypeMismatch {
		expected: String,
		found: String,
		context: String,
	},
	/// Division by zero.
	DivisionByZero,
	/// Row count mismatch.
	RowCountMismatch {
		expected: usize,
		actual: usize,
	},
	/// Unsupported operation.
	UnsupportedOperation {
		operation: String,
	},
	/// Subquery error.
	SubqueryError {
		message: String,
	},
}

impl std::fmt::Display for EvalError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			EvalError::ColumnNotFound {
				name,
			} => write!(f, "column not found: {}", name),
			EvalError::VariableNotFound {
				id,
			} => write!(f, "variable not found: {}", id),
			EvalError::TypeMismatch {
				expected,
				found,
				context,
			} => write!(f, "type mismatch in {}: expected {}, found {}", context, expected, found),
			EvalError::DivisionByZero => write!(f, "division by zero"),
			EvalError::RowCountMismatch {
				expected,
				actual,
			} => {
				write!(f, "row count mismatch: expected {}, got {}", expected, actual)
			}
			EvalError::UnsupportedOperation {
				operation,
			} => {
				write!(f, "unsupported operation: {}", operation)
			}
			EvalError::SubqueryError {
				message,
			} => write!(f, "subquery error: {}", message),
		}
	}
}

impl std::error::Error for EvalError {}

/// Result type for expression evaluation.
pub type EvalResult<T> = std::result::Result<T, EvalError>;

/// Future type for expression evaluation.
pub type ExprFuture<'a> = Pin<Box<dyn Future<Output = EvalResult<Column>> + Send + 'a>>;

/// Future type for filter evaluation.
pub type FilterFuture<'a> = Pin<Box<dyn Future<Output = EvalResult<BitVec>> + Send + 'a>>;

/// Pre-compiled expression that evaluates to a Column asynchronously.
///
/// The closure captures static information (column names, literals, operators)
/// at compile time, and receives dynamic information (columns, context) at
/// evaluation time. Returns a future to support async subquery execution.
pub struct CompiledExpr(Arc<dyn for<'a> Fn(&'a Columns, &'a EvalContext) -> ExprFuture<'a> + Send + Sync>);

impl CompiledExpr {
	/// Create a new compiled expression from an async closure.
	pub fn new<F>(f: F) -> Self
	where
		F: for<'a> Fn(&'a Columns, &'a EvalContext) -> ExprFuture<'a> + Send + Sync + 'static,
	{
		Self(Arc::new(f))
	}

	/// Evaluate the expression against the given columns and context.
	pub async fn eval(&self, columns: &Columns, ctx: &EvalContext) -> EvalResult<Column> {
		(self.0)(columns, ctx).await
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

/// Pre-compiled filter expression that evaluates directly to a BitVec mask asynchronously.
///
/// This is a specialization of CompiledExpr for filter predicates, avoiding
/// the intermediate Column allocation when we only need a boolean mask.
pub struct CompiledFilter(Arc<dyn for<'a> Fn(&'a Columns, &'a EvalContext) -> FilterFuture<'a> + Send + Sync>);

impl CompiledFilter {
	/// Create a new compiled filter from an async closure.
	pub fn new<F>(f: F) -> Self
	where
		F: for<'a> Fn(&'a Columns, &'a EvalContext) -> FilterFuture<'a> + Send + Sync + 'static,
	{
		Self(Arc::new(f))
	}

	/// Evaluate the filter against the given columns and context.
	/// Returns a BitVec where true = row passes the filter.
	pub async fn eval(&self, columns: &Columns, ctx: &EvalContext) -> EvalResult<BitVec> {
		(self.0)(columns, ctx).await
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

	#[tokio::test]
	async fn test_compiled_expr_clone() {
		let expr = CompiledExpr::new(|columns, _ctx| {
			let col = columns.iter().next().unwrap().clone();
			Box::pin(async move { Ok(col) })
		});
		let cloned = expr.clone();

		let columns = Columns::new(vec![Column::new(Fragment::from("x"), ColumnData::int8(vec![1, 2, 3]))]);

		let result1 = expr.eval(&columns, &EvalContext::new()).await.unwrap();
		let result2 = cloned.eval(&columns, &EvalContext::new()).await.unwrap();

		assert_eq!(result1.data().len(), result2.data().len());
	}

	#[tokio::test]
	async fn test_compiled_filter_basic() {
		// Filter that returns all true
		let filter = CompiledFilter::new(|columns, _ctx| {
			let len = columns.row_count();
			Box::pin(async move { Ok(BitVec::from_fn(len, |_| true)) })
		});

		let columns = Columns::new(vec![Column::new(Fragment::from("x"), ColumnData::int8(vec![1, 2, 3]))]);

		let mask = filter.eval(&columns, &EvalContext::new()).await.unwrap();
		assert_eq!(mask.len(), 3);
		assert!(mask.iter().all(|b| b));
	}
}
