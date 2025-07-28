// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::AstCreateTable;
use crate::convert_data_type;
use crate::plan::logical::{Compiler, CreateTableNode, LogicalPlan, convert_policy};
use reifydb_catalog::table::ColumnToCreate;
use reifydb_core::interface::ColumnPolicyKind;

impl Compiler {
    pub(crate) fn compile_create_table(ast: AstCreateTable) -> crate::Result<LogicalPlan> {
        let mut columns: Vec<ColumnToCreate> = vec![];

        for col in ast.columns.iter() {
            let column_name = col.name.value().to_string();
            let ty = convert_data_type(&col.ty)?;

            let policies = if let Some(policy_block) = &col.policies {
                policy_block.policies.iter().map(convert_policy).collect::<Vec<ColumnPolicyKind>>()
            } else {
                vec![]
            };

            columns.push(ColumnToCreate { name: column_name, ty, policies });
        }

        Ok(LogicalPlan::CreateTable(CreateTableNode {
            schema: ast.schema.span(),
            table: ast.table.span(),
            if_not_exists: false,
            columns,
        }))
    }
}
