// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::AstCreateTable;
use crate::plan::logical::create::convert_policy;
use crate::plan::logical::{Compiler, CreateTableNode, LogicalPlan};
use reifydb_catalog::column_policy::ColumnPolicyKind;
use reifydb_catalog::table::ColumnToCreate;

impl Compiler {
    pub(crate) fn compile_create_table(ast: AstCreateTable) -> crate::Result<LogicalPlan> {
        let mut columns: Vec<ColumnToCreate> = vec![];

        for col in ast.columns.iter() {
            let column_name = col.name.value().to_string();
            let column_type = col.ty.data_type();

            let policies = if let Some(policy_block) = &col.policies {
                policy_block.policies.iter().map(convert_policy).collect::<Vec<ColumnPolicyKind>>()
            } else {
                vec![]
            };

            columns.push(ColumnToCreate { name: column_name, value: column_type, policies });
        }

        Ok(LogicalPlan::CreateTable(CreateTableNode {
            schema: ast.schema.span(),
            table: ast.table.span(),
            if_not_exists: false,
            columns,
        }))
    }
}
