// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Compiled expressions using the closure compilation technique.
//!
//! This module provides:
//! - [`CompiledExpr`] - Pre-compiled expression returning Column
//! - [`CompiledFilter`] - Pre-compiled filter returning BitVec mask
//! - [`EvalContext`] - Evaluation context with variables
//! - [`compile_plan_expr`] / [`compile_plan_filter`] - Compilation functions
//!
//! The closure compilation technique eliminates enum dispatch overhead while
//! keeping the code safe (no JIT, no unsafe) and supporting async subquery execution.
//!
//! Reference: https://blog.cloudflare.com/building-fast-interpreters-in-rust/

pub mod compile;
pub mod eval;
pub mod types;

pub use compile::{compile_plan_expr, compile_plan_filter};
pub use eval::{EvalContext, EvalValue, ScriptFunctionCaller};
pub use types::{CompiledExpr, CompiledFilter, EvalError, EvalResult};
