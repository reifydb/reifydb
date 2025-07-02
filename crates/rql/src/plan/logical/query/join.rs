// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::{Ast, AstInfix, AstJoin, InfixOperator};
use crate::plan::logical::LogicalQueryPlan::TableScan;
use crate::plan::logical::{Compiler, JoinLeftNode, LogicalQueryPlan, TableScanNode};

impl Compiler {
    pub(crate) fn compile_join(ast: AstJoin) -> crate::Result<LogicalQueryPlan> {
        match ast {
            AstJoin::LeftJoin { with, on, .. } => {
                let with = match *with {
                    Ast::Identifier(identifier) => {
                        vec![TableScan(TableScanNode { schema: None, table: identifier.span() })]
                    }
                    Ast::Infix(AstInfix { left, operator, right, .. }) => {
                        assert!(matches!(operator, InfixOperator::AccessTable(_)));
                        let Ast::Identifier(schema) = *left else { unreachable!() };
                        let Ast::Identifier(table) = *right else { unreachable!() };
                        vec![TableScan(TableScanNode {
                            schema: Some(schema.span()),
                            table: table.span(),
                        })]
                    }
                    _ => unimplemented!(),
                };
                Ok(LogicalQueryPlan::JoinLeft(JoinLeftNode {
                    with,
                    on: on
                        .into_iter()
                        .map(Self::compile_expression)
                        .collect::<Result<Vec<_>, _>>()?,
                }))
            }
        }
    }
}
