// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::{
	ast::ast::AstDispatch,
	bump::BumpBox,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, DispatchNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_dispatch(
		&self,
		ast: AstDispatch<'bump>,
		_tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
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
