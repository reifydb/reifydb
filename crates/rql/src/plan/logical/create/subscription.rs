// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::subscription::SubscriptionColumnToCreate;

use crate::{
	ast::ast::{AstCreateSubscription, AstDataType},
	convert_data_type,
	plan::logical::{Compiler, CreateSubscriptionNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_subscription(&self, ast: AstCreateSubscription) -> crate::Result<LogicalPlan> {
		let mut columns = Vec::with_capacity(ast.columns.len());

		for col in ast.columns.iter() {
			let ty = match &col.ty {
				AstDataType::Unconstrained(fragment) => convert_data_type(fragment)?,
				AstDataType::Constrained {
					name,
					..
				} => {
					// Subscriptions use simple types without constraints
					convert_data_type(name)?
				}
			};
			columns.push(SubscriptionColumnToCreate {
				name: col.name.text().to_string(),
				ty,
			});
		}

		// Pass the AS clause through without compiling it (will be compiled in execution layer)
		let as_clause = ast.as_clause;

		Ok(LogicalPlan::CreateSubscription(CreateSubscriptionNode {
			columns,
			as_clause,
		}))
	}
}
