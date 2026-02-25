// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod flow;
pub mod sequence;

use reifydb_transaction::transaction::Transaction;

use crate::{
	ast::ast::AstAlter,
	plan::logical::{AlterSecurityPolicyNode, Compiler, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_alter(
		&self,
		ast: AstAlter<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		match ast {
			AstAlter::Sequence(node) => self.compile_alter_sequence(node),
			AstAlter::Flow(node) => self.compile_alter_flow(node, tx),
			AstAlter::SecurityPolicy(node) => {
				Ok(LogicalPlan::AlterSecurityPolicy(AlterSecurityPolicyNode {
					target_type: node.target_type,
					name: node.name,
					action: node.action,
				}))
			}
		}
	}
}
