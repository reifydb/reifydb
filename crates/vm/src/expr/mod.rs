// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod builder;
mod compile;
mod compiled;
mod eval;
mod function;
mod types;

pub use builder::{ColumnSchema, ExprBuilder, col, lit};
pub use compile::{compile_expr, compile_filter};
pub use compiled::{CompiledExpr, CompiledFilter};
pub use eval::{EvalContext, EvalValue};
pub use function::{VmFunctionContext, VmFunctionExecutor, VmScalarFn};
pub use types::{BinaryOp, ColumnRef, Expr, Literal, UnaryOp};
