// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod builder;
mod compile;
mod compiled;
mod eval;
mod subquery_executor;
mod types;

pub use builder::{ColumnSchema, ExprBuilder, col, lit};
pub use compile::{compile_expr, compile_filter};
pub use compiled::{CompiledExpr, CompiledFilter};
pub use eval::{EvalContext, EvalValue, SubqueryExecutor};
pub use subquery_executor::RuntimeSubqueryExecutor;
pub use types::{BinaryOp, ColumnRef, Expr, Literal, SubqueryKind, UnaryOp};
