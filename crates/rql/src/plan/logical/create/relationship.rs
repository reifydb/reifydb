// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::{
	Result,
	ast::ast::AstCreateRelationship,
	bump::BumpBox,
	plan::logical::{Compiler, CreateRelationshipNode, LogicalPlan, RelationshipJunctionNode},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_relationship(
		&self,
		ast: AstCreateRelationship<'bump>,
		_tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let node = CreateRelationshipNode {
			name: ast.name,
			source: ast.source,
			source_column: ast.source_column,
			target: ast.target,
			target_column: ast.target_column,
			junction: ast.junction.map(|j| RelationshipJunctionNode {
				table: j.table,
				source_column: j.source_column,
				target_column: j.target_column,
			}),
			cardinality: ast.cardinality,
		};
		Ok(LogicalPlan::CreateRelationship(BumpBox::new_in(node, self.bump)))
	}
}
