// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::AstAggregate;
use crate::plan::logical::{AggregateNode, Compiler, LogicalQueryPlan};

impl Compiler {
    pub(crate) fn compile_aggregate(ast: AstAggregate) -> crate::Result<LogicalQueryPlan> {
        Ok(LogicalQueryPlan::Aggregate(AggregateNode {
            by: ast.by.into_iter().map(Self::compile_expression).collect::<Result<Vec<_>, _>>()?,
            select: ast
                .select
                .into_iter()
                .map(Self::compile_expression)
                .collect::<Result<Vec<_>, _>>()?,
        }))
    }
}
