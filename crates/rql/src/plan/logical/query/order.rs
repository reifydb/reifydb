// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::AstOrder;
use crate::plan::logical::{Compiler, LogicalPlan, OrderNode};
use reifydb_core::{OrderDirection, OrderKey};

impl Compiler {
    pub(crate) fn compile_order(ast: AstOrder) -> crate::Result<LogicalPlan> {
        Ok(LogicalPlan::Order(OrderNode {
            by: ast
                .columns
                .into_iter()
                .zip(ast.directions)
                .map(|(column, direction)| {
                    let direction = direction
                        .map(|direction| match direction.value().to_lowercase().as_str() {
                            "asc" => OrderDirection::Asc,
                            _ => OrderDirection::Desc,
                        })
                        .unwrap_or(OrderDirection::Desc);

                    OrderKey { column: column.span(), direction }
                })
                .collect(),
        }))
    }
}
