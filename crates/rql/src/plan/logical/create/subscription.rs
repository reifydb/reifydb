// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::subscription::SubscriptionColumnToCreate;
use reifydb_transaction::transaction::AsTransaction;

use crate::{
	ast::ast::{AstCreateSubscription, AstType},
	bump::BumpVec,
	convert_data_type,
	plan::logical::{Compiler, CreateSubscriptionNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_subscription<T: AsTransaction>(
		&self,
		ast: AstCreateSubscription<'bump>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'bump>> {
		let mut columns = Vec::with_capacity(ast.columns.len());

		for col in ast.columns.iter() {
			let ty = match &col.ty {
				AstType::Unconstrained(fragment) => convert_data_type(fragment)?,
				AstType::Constrained {
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

		// Compile the AS clause to logical plans
		let as_clause = if let Some(as_statement) = ast.as_clause {
			self.compile(as_statement, tx)?
		} else {
			BumpVec::new_in(self.bump)
		};

		Ok(LogicalPlan::CreateSubscription(CreateSubscriptionNode {
			columns,
			as_clause,
		}))
	}
}
