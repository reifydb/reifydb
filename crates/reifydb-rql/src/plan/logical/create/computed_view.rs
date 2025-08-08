// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::AstCreateComputedView;
use crate::convert_data_type;
use crate::plan::logical::{Compiler, CreateComputedViewNode, LogicalPlan, convert_policy};
use reifydb_catalog::table::ColumnToCreate;
use reifydb_core::interface::ColumnPolicyKind;

impl Compiler {
    pub(crate) fn compile_computed_view(ast: AstCreateComputedView) -> crate::Result<LogicalPlan> {
        let mut columns: Vec<ColumnToCreate> = vec![];
        for col in ast.columns.into_iter() {
            let column_name = col.name.value().to_string();
            let column_type = convert_data_type(&col.ty)?;

            let policies = if let Some(policy_block) = &col.policies {
                policy_block.policies.iter().map(convert_policy).collect::<Vec<ColumnPolicyKind>>()
            } else {
                vec![]
            };

            columns.push(ColumnToCreate {
                name: column_name,
                ty: column_type,
                policies,
                auto_increment: false, // Computed views don't support auto-increment
                span: Some(col.name.span()),
            });
        }

        // Compile the WITH clause if present
        let with = if let Some(with_statement) = ast.with {
            let compiled_plans = Self::compile(with_statement)?;
            Some(compiled_plans)
        } else {
            None
        };

        Ok(LogicalPlan::CreateComputedView(CreateComputedViewNode {
            schema: ast.schema.span(),
            view: ast.view.span(),
            if_not_exists: false,
            columns,
            with,
        }))
    }
}
