// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::ast::ast::AstCreatePrimaryKey;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::ValueError;

use crate::plan::logical::{Compiler, CreatePrimaryKeyNode, LogicalPlan, PrimaryKeyColumn};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_primary_key(
		&self,
		ast: AstCreatePrimaryKey<'bump>,
		_tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>, ValueError> {
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
