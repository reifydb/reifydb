// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::operator_with::WindowWith;
use reifydb_value::fragment::Fragment;

use crate::{
	Result,
	ast::ast::{Ast, AstWindow},
	expression::{Expression, ExpressionCompiler},
	plan::logical::{Compiler, LogicalPlan},
};

#[derive(Debug, Clone)]
pub struct WindowNode {
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub with: WindowWith,
	pub fragment: Fragment,
	pub rql: String,
}

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_window(&self, ast: AstWindow<'bump>) -> Result<LogicalPlan<'bump>> {
		let rql = ast.rql.to_string();
		let fragment = ast.token.fragment.to_owned();

		let group_by = Self::compile_expressions(ast.group_by)?;
		let aggregations = Self::compile_expressions(ast.aggregations)?;
		let with = Self::compile_window_with(ast.kind, ast.with.as_ref(), fragment.clone())?;

		Ok(LogicalPlan::Window(WindowNode {
			group_by,
			aggregations,
			with,
			fragment,
			rql,
		}))
	}

	fn compile_expressions(asts: Vec<Ast<'bump>>) -> Result<Vec<Expression>> {
		let mut expressions = Vec::new();
		for ast in asts {
			expressions.push(ExpressionCompiler::compile(ast)?);
		}
		Ok(expressions)
	}
}
