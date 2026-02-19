// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::policy::{ColumnPolicyKind, ColumnSaturationPolicy};
use reifydb_transaction::transaction::Transaction;

use crate::{
	ast::ast::{AstCreatePolicy, AstPolicyKind},
	plan::logical::{Compiler, CreatePolicyNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_policy(
		&self,
		ast: AstCreatePolicy<'bump>,
		_tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		let policies = ast
			.policies
			.iter()
			.map(|entry| match entry.kind {
				AstPolicyKind::Saturation => {
					if entry.value.is_literal_none() {
						ColumnPolicyKind::Saturation(ColumnSaturationPolicy::None)
					} else {
						let ident = entry.value.as_identifier().text();
						match ident {
							"error" => ColumnPolicyKind::Saturation(
								ColumnSaturationPolicy::Error,
							),
							_ => unimplemented!(),
						}
					}
				}
				AstPolicyKind::Default => unimplemented!(),
			})
			.collect();

		Ok(LogicalPlan::CreatePolicy(CreatePolicyNode {
			column: ast.column,
			policies,
		}))
	}
}
