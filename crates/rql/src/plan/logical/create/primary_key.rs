// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::{
	Result,
	ast::ast::AstCreatePrimaryKey,
	plan::logical::{Compiler, CreatePrimaryKeyNode, LogicalPlan, PrimaryKeyColumn},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_primary_key(
		&self,
		ast: AstCreatePrimaryKey<'bump>,
		_tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let columns = ast
			.columns
			.into_iter()
			.map(|col| PrimaryKeyColumn {
				column: col.column.name,
				order: col.order,
			})
			.collect();

		Ok(LogicalPlan::CreatePrimaryKey(CreatePrimaryKeyNode {
			table: ast.table,
			columns,
		}))
	}
}
