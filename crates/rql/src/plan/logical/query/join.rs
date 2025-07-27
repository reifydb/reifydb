// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{Ast, AstInfix, AstJoin, InfixOperator};
use crate::plan::logical::LogicalPlan::TableScan;
use crate::plan::logical::{
    Compiler, JoinInnerNode, JoinLeftNode, JoinNaturalNode, LogicalPlan, TableScanNode,
};
use reifydb_core::JoinType;

impl Compiler {
    pub(crate) fn compile_join(ast: AstJoin) -> crate::Result<LogicalPlan> {
        match ast {
            AstJoin::InnerJoin { with, on, .. } => {
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
                Ok(LogicalPlan::JoinInner(JoinInnerNode {
                    with,
                    on: on
                        .into_iter()
                        .map(Self::compile_expression)
                        .collect::<crate::Result<Vec<_>>>()?,
                }))
            }
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
                Ok(LogicalPlan::JoinLeft(JoinLeftNode {
                    with,
                    on: on
                        .into_iter()
                        .map(Self::compile_expression)
                        .collect::<crate::Result<Vec<_>>>()?,
                }))
            }
            AstJoin::NaturalJoin { with, join_type, .. } => {
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

                Ok(LogicalPlan::JoinNatural(JoinNaturalNode {
                    with,
                    join_type: join_type.unwrap_or(JoinType::Inner),
                }))
            }
        }
    }
}
