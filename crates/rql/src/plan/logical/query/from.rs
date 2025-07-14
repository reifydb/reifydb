// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{Ast, AstFrom};
use crate::expression::{IdentExpression, KeyedExpression};
use crate::plan::logical::{Compiler, InlineDataNode, LogicalPlan, TableScanNode};
use reifydb_core::{Diagnostic, Error};

impl Compiler {
    pub(crate) fn compile_from(ast: AstFrom) -> crate::Result<LogicalPlan> {
        match ast {
            AstFrom::Table { schema, table, .. } => Ok(LogicalPlan::TableScan(TableScanNode {
                schema: schema.map(|schema| schema.span()),
                table: table.span(),
            })),
            AstFrom::Static { list, .. } => {
                let mut rows = Vec::new();

                for row in list.nodes {
                    match row {
                        Ast::Row(row) => {
                            let mut keyed_fields = Vec::new();
                            for field in row.fields {
                                let key = IdentExpression(field.key.span());
                                let expr = Self::compile_expression(field.value.as_ref().clone())?;

                                let keyed_expr =
                                    KeyedExpression { key, expression: Box::new(expr) };
                                keyed_fields.push(keyed_expr);
                            }
                            rows.push(keyed_fields);
                        }
                        _ => {
                            return Err(Error(Diagnostic {
                                code: "E0001".to_string(),
                                statement: None,
                                message: "Expected row in static data".to_string(),
                                column: None,
                                span: None,
                                label: None,
                                help: None,
                                notes: vec![],
                            }));
                        }
                    }
                }

                Ok(LogicalPlan::InlineData(InlineDataNode { rows }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::lex::lex;
    use crate::ast::parse::parse;
    use crate::expression::{ConstantExpression, Expression};

    #[test]
    fn test_compile_static_single_row() {
        let tokens = lex("from [{id: 1, name: 'Alice'}]").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let from_ast = result[0].first_unchecked().as_from().clone();
        let logical_plan = Compiler::compile_from(from_ast).unwrap();

        match logical_plan {
            LogicalPlan::InlineData(node) => {
                assert_eq!(node.rows.len(), 1); // One row
                assert_eq!(node.rows[0].len(), 2); // Two KeyedExpressions: id and name
                assert_eq!(node.rows[0][0].key.0.fragment, "id");
                assert_eq!(node.rows[0][1].key.0.fragment, "name");
            }
            _ => panic!("Expected InlineData node"),
        }
    }

    #[test]
    fn test_compile_static_multiple_rows_different_columns() {
        let tokens =
            lex("from [{id: 1, name: 'Alice'}, {id: 2, email: 'bob@test.com'}, {name: 'Charlie'}]")
                .unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let from_ast = result[0].first_unchecked().as_from().clone();
        let logical_plan = Compiler::compile_from(from_ast).unwrap();

        match logical_plan {
            LogicalPlan::InlineData(node) => {
                // Should have 3 rows
                assert_eq!(node.rows.len(), 3);

                // First row: id: 1, name: 'Alice'
                assert_eq!(node.rows[0].len(), 2);
                assert_eq!(node.rows[0][0].key.0.fragment, "id");
                assert_eq!(node.rows[0][1].key.0.fragment, "name");

                // Second row: id: 2, email: 'bob@test.com'
                assert_eq!(node.rows[1].len(), 2);
                assert_eq!(node.rows[1][0].key.0.fragment, "id");
                assert_eq!(node.rows[1][1].key.0.fragment, "email");

                // Third row: name: 'Charlie'
                assert_eq!(node.rows[2].len(), 1);
                assert_eq!(node.rows[2][0].key.0.fragment, "name");

                // Check some expression values
                match &*node.rows[0][0].expression {
                    Expression::Constant(ConstantExpression::Number { span }) => {
                        assert_eq!(span.fragment, "1");
                    }
                    _ => panic!("Expected Number for id in first row"),
                }

                match &*node.rows[0][1].expression {
                    Expression::Constant(ConstantExpression::Text { span }) => {
                        assert_eq!(span.fragment, "Alice");
                    }
                    _ => panic!("Expected Text for name in first row"),
                }
            }
            _ => panic!("Expected InlineData node"),
        }
    }

    #[test]
    fn test_compile_static_empty_list() {
        let tokens = lex("from []").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let from_ast = result[0].first_unchecked().as_from().clone();
        let logical_plan = Compiler::compile_from(from_ast).unwrap();

        match logical_plan {
            LogicalPlan::InlineData(node) => {
                assert_eq!(node.rows.len(), 0);
            }
            _ => panic!("Expected InlineData node"),
        }
    }
}
