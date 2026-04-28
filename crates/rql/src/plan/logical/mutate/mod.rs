// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod delete;
pub mod dispatch;
pub mod insert;
pub mod update;

use crate::{
	Result,
	ast::ast::Ast,
	expression::{Expression, ExpressionCompiler},
};

/// Compile an optional `RETURNING` clause shared by insert/update/delete.
pub(crate) fn compile_returning_clause<'bump>(returning: Option<Vec<Ast<'bump>>>) -> Result<Option<Vec<Expression>>> {
	let Some(returning_asts) = returning else {
		return Ok(None);
	};
	let mut exprs = Vec::with_capacity(returning_asts.len());
	for ast_node in returning_asts {
		exprs.push(ExpressionCompiler::compile(ast_node)?);
	}
	Ok(Some(exprs))
}
