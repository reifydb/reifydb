// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast;
use crate::ast::{Ast, AstInsert, AstLiteral, AstPrefix};
use crate::expression::{ConstantExpression, Expression, PrefixExpression, PrefixOperator};
use crate::plan::logical::{Compiler, InsertNode, LogicalPlan};
use reifydb_core::Span;
use std::collections::HashMap;
use std::mem;
use std::ops::Deref;

impl Compiler {
    pub(crate) fn compile_insert(ast: AstInsert) -> crate::Result<LogicalPlan> {

        // let columns: Vec<Span> = ast
        //     .columns
        //     .nodes
        //     .into_iter()
        //     .map(|column| match column {
        //         Ast::Identifier(ast) => ast.span(),
        //         _ => unimplemented!(),
        //     })
        //     .collect::<Vec<_>>();
        //
        // let insert_index_map: HashMap<_, _> =
        //     columns.iter().enumerate().map(|(i, name)| (name.to_string(), i)).collect();
        //
        // let rows_to_insert = ast
        //     .rows
        //     .into_iter()
        //     .map(|mut row| {
        //         let mut values = vec![None; columns.len()];
        //
        //         for (col_idx, col) in columns.iter().enumerate() {
        //             if let Some(&input_idx) = insert_index_map.get(&col.fragment.to_string()) {
        //                 let expr = mem::replace(&mut row.nodes[input_idx], Ast::Nop);
        //
        //                 let expr = match expr {
        //                     Ast::Literal(AstLiteral::Boolean(ast)) => {
        //                         Expression::Constant(ConstantExpression::Bool { span: ast.0.span })
        //                     }
        //                     Ast::Literal(AstLiteral::Number(ast)) => {
        //                         Expression::Constant(ConstantExpression::Number {
        //                             span: ast.0.span,
        //                         })
        //                     }
        //                     Ast::Literal(AstLiteral::Text(ast)) => {
        //                         Expression::Constant(ConstantExpression::Text { span: ast.0.span })
        //                     }
        //                     Ast::Prefix(AstPrefix { operator, node }) => {
        //                         let a = node.deref();
        //
        //                         let (span, operator) = match operator {
        //                             ast::AstPrefixOperator::Plus(token) => {
        //                                 (token.span.clone(), PrefixOperator::Plus(token.span))
        //                             }
        //                             ast::AstPrefixOperator::Negate(token) => {
        //                                 (token.span.clone(), PrefixOperator::Minus(token.span))
        //                             }
        //                             ast::AstPrefixOperator::Not(_token) => {
        //                                 unimplemented!()
        //                             }
        //                         };
        //
        //                         Expression::Prefix(PrefixExpression {
        //                             operator,
        //                             expression: Box::new(match a {
        //                                 Ast::Literal(lit) => match lit {
        //                                     AstLiteral::Boolean(n) => {
        //                                         Expression::Constant(ConstantExpression::Bool {
        //                                             span: n.0.span.clone(),
        //                                         })
        //                                     }
        //                                     AstLiteral::Number(n) => {
        //                                         Expression::Constant(ConstantExpression::Number {
        //                                             span: n.0.span.clone(),
        //                                         })
        //                                     }
        //                                     AstLiteral::Text(t) => {
        //                                         Expression::Constant(ConstantExpression::Text {
        //                                             span: t.0.span.clone(),
        //                                         })
        //                                     }
        //                                     AstLiteral::Undefined(t) => Expression::Constant(
        //                                         ConstantExpression::Undefined {
        //                                             span: t.0.span.clone(),
        //                                         },
        //                                     ),
        //                                 },
        //                                 _ => unimplemented!(),
        //                             }),
        //                             span,
        //                         })
        //                     }
        //                     Ast::Infix(infix) => Self::compile_expression_infix(infix).unwrap(),
        //                     node => unimplemented!("{node:?}"),
        //                 };
        //
        //                 values[col_idx] = Some(expr);
        //             } else {
        //                 // Not provided in INSERT, use default
        //                 unimplemented!()
        //             }
        //         }
        //
        //         values.into_iter().map(|v| v.unwrap()).collect::<Vec<_>>()
        //     })
        //     .collect::<Vec<_>>();
        //
        //
        // Ok(LogicalPlan::InsertIntoTable(InsertIntoTableNode::Values {
        //     schema: ast.schema.span(),
        //     table: ast.table.span(),
        //     columns,
        //     rows_to_insert,
        // }))

        unimplemented!()
    }
}
