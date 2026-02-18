// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::subscription::SubscriptionColumnToCreate;
use reifydb_transaction::transaction::Transaction;

use crate::{
	ast::ast::AstCreateSubscription,
	bump::BumpVec,
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateSubscriptionNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_subscription(
		&self,
		ast: AstCreateSubscription<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		let mut columns = Vec::with_capacity(ast.columns.len());

		for col in ast.columns.iter() {
			let constraint = convert_data_type_with_constraints(&col.ty)?;
			columns.push(SubscriptionColumnToCreate {
				name: col.name.text().to_string(),
				ty: constraint.get_type(),
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
