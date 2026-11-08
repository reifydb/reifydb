// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::ast::ast::AstClosure;
use reifydb_transaction::transaction::Transaction;

use crate::{
	Result, convert_data_type_with_constraints,
	plan::logical::{Compiler, DefineClosureNode, LogicalPlan, function::FunctionParameter},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_closure(
		&self,
		ast: AstClosure<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let mut parameters = Vec::new();
		for param in ast.parameters {
			let param_name = param.variable.token.fragment;
			let type_constraint = if let Some(ref ty) = param.type_annotation {
				Some(convert_data_type_with_constraints(ty)?)
			} else {
				None
			};
			parameters.push(FunctionParameter {
				name: param_name,
				type_constraint,
			});
		}

		let body = self.compile_block(ast.body, tx)?;

		Ok(LogicalPlan::DefineClosure(DefineClosureNode {
			parameters,
			body,
		}))
	}
}
