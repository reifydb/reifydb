// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::{ast::ast::AstDispatch, bump::BumpBox};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::ValueError;

use crate::{
	expression::ExpressionCompiler,
	plan::logical::{Compiler, DispatchNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_dispatch(
		&self,
		ast: AstDispatch<'bump>,
		_tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>, ValueError> {
		let mut fields = Vec::with_capacity(ast.fields.len());
		for (name, expr_box) in ast.fields {
			let expr = ExpressionCompiler::compile(BumpBox::into_inner(expr_box))?;
			fields.push((name, expr));
		}

		Ok(LogicalPlan::Dispatch(DispatchNode {
			on_event: ast.on_event,
			variant: ast.variant,
			fields,
		}))
	}
}
