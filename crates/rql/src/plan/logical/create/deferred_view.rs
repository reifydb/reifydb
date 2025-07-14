// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::AstCreateDeferredView;
use crate::plan::logical::{convert_data_type, convert_policy, Compiler, CreateDeferredViewNode, LogicalPlan};
use reifydb_catalog::column_policy::ColumnPolicyKind;
use reifydb_catalog::table::ColumnToCreate;

impl Compiler {
    pub(crate) fn compile_deferred_view(ast: AstCreateDeferredView) -> crate::Result<LogicalPlan> {
        let mut columns: Vec<ColumnToCreate> = vec![];
        for col in ast.columns.iter() {
            let column_name = col.name.value().to_string();
            let column_type = convert_data_type(&col.data_type)?;

            let policies = if let Some(policy_block) = &col.policies {
                policy_block.policies.iter().map(convert_policy).collect::<Vec<ColumnPolicyKind>>()
            } else {
                vec![]
            };

            columns.push(ColumnToCreate { name: column_name, data_type: column_type, policies });
        }

        Ok(LogicalPlan::CreateDeferredView(CreateDeferredViewNode {
            schema: ast.schema.span(),
            view: ast.view.span(),
            if_not_exists: false,
            columns,
        }))
    }
}
