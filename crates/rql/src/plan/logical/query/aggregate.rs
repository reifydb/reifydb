// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::AstAggregate;
use crate::expression::ExpressionCompiler;
use crate::plan::logical::{AggregateNode, Compiler, LogicalPlan};

impl Compiler {
    pub(crate) fn compile_aggregate(ast: AstAggregate) -> crate::Result<LogicalPlan> {
        Ok(LogicalPlan::Aggregate(AggregateNode {
            by: ast
                .by
                .into_iter()
                .map(ExpressionCompiler::compile)
                .collect::<crate::Result<Vec<_>>>()?,
            map: ast
                .map
                .into_iter()
                .map(ExpressionCompiler::compile)
                .collect::<crate::Result<Vec<_>>>()?,
        }))
    }
}
