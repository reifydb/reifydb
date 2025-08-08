// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::AstSort;
use crate::plan::logical::{Compiler, LogicalPlan, OrderNode};
use reifydb_core::{SortDirection, SortKey};

impl Compiler {
    pub(crate) fn compile_sort(ast: AstSort) -> crate::Result<LogicalPlan> {
        Ok(LogicalPlan::Order(OrderNode {
            by: ast
                .columns
                .into_iter()
                .zip(ast.directions)
                .map(|(column, direction)| {
                    let direction = direction
                        .map(|direction| match direction.value().to_lowercase().as_str() {
                            "asc" => SortDirection::Asc,
                            _ => SortDirection::Desc,
                        })
                        .unwrap_or(SortDirection::Desc);

                    SortKey { column: column.span(), direction }
                })
                .collect(),
        }))
    }
}
