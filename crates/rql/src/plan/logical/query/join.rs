// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::{Ast, AstJoin};
use crate::plan::expression;
use crate::plan::logical::LogicalQueryPlan::TableScan;
use crate::plan::logical::{Compiler, JoinLeftNode, LogicalQueryPlan, TableScanNode};

impl Compiler {
    pub(crate) fn compile_join(ast: AstJoin) -> crate::Result<LogicalQueryPlan> {
        match ast {
            AstJoin::LeftJoin { with, on, .. } => {
                let Ast::Identifier(identifier) = *with else { panic!() };

                Ok(LogicalQueryPlan::JoinLeft(JoinLeftNode {
                    with: vec![TableScan(TableScanNode { schema: None, table: identifier.span() })],
                    on: on.into_iter().map(expression).collect::<Result<Vec<_>, _>>()?,
                }))
            }
        }
    }
}
