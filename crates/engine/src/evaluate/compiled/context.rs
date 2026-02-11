// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_function::registry::Functions;

pub use crate::evaluate::EvalContext;
use crate::vm::stack::SymbolTable;

/// Compile-time context for resolving functions and UDFs.
pub struct CompileContext<'a> {
	pub functions: &'a Functions,
	pub symbol_table: &'a SymbolTable,
}
