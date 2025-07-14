// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{Ast, AstFrom};
use crate::expression::{ConstantExpression, Expression};
use crate::plan::logical::{Compiler, InlineDataNode, LogicalPlan, TableScanNode};
use reifydb_core::{Diagnostic, Error, Line, Offset};
use std::collections::{BTreeSet, HashMap};

impl Compiler {
    pub(crate) fn compile_from(ast: AstFrom) -> crate::Result<LogicalPlan> {
        match ast {
            AstFrom::Table { schema, table, .. } => Ok(LogicalPlan::TableScan(TableScanNode {
                schema: schema.map(|schema| schema.span()),
                table: table.span(),
            })),
            AstFrom::Static { list, .. } => {
                // First pass: collect all unique column names from all rows
                let mut all_column_names = BTreeSet::new();
                for row in &list.nodes {
                    match row {
                        Ast::Row(row) => {
                            for field in &row.fields {
                                all_column_names.insert(field.key.value().to_string());
                            }
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

                // Convert to sorted Vec for deterministic ordering
                let column_names: Vec<String> = all_column_names.into_iter().collect();
                let num_columns = column_names.len();
                let num_rows = list.nodes.len();

                // Initialize columnar storage
                let mut columns: Vec<Vec<Expression>> =
                    vec![Vec::with_capacity(num_rows); num_columns];

                // Second pass: populate columnar data
                for row in &list.nodes {
                    match row {
                        Ast::Row(row) => {
                            // Create a map of field name to value for this row
                            let mut row_data: HashMap<String, &Ast> = HashMap::new();
                            for field in &row.fields {
                                row_data
                                    .insert(field.key.value().to_string(), field.value.as_ref());
                            }

                            // For each column, get the value or use Undefined
                            for (col_idx, column_name) in column_names.iter().enumerate() {
                                let expression = if let Some(ast_value) = row_data.get(column_name)
                                {
                                    Self::compile_expression((*ast_value).clone())?
                                } else {
                                    // Column is missing in this row, use Undefined
                                    Expression::Constant(ConstantExpression::Undefined {
                                        span: reifydb_core::Span {
                                            offset: Offset(0),
                                            line: Line(0),
                                            fragment: "Undefined".to_string(),
                                        },
                                    })
                                };
                                columns[col_idx].push(expression);
                            }
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

                Ok(LogicalPlan::InlineData(InlineDataNode { names: column_names, columns }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::lex::lex;
    use crate::ast::parse::parse;

    #[test]
    fn test_compile_static_single_row() {
        let tokens = lex("from [{id: 1, name: 'Alice'}]").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let from_ast = result[0].first_unchecked().as_from().clone();
        let logical_plan = Compiler::compile_from(from_ast).unwrap();

        match logical_plan {
            LogicalPlan::InlineData(node) => {
                assert_eq!(node.names, vec!["id", "name"]);
                assert_eq!(node.columns.len(), 2);
                assert_eq!(node.columns[0].len(), 1); // id column has 1 row
                assert_eq!(node.columns[1].len(), 1); // name column has 1 row
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
                // Should have all columns: age, email, id, name (sorted)
                assert_eq!(node.names, vec!["email", "id", "name"]);
                assert_eq!(node.columns.len(), 3);

                // Each column should have 3 rows
                for column in &node.columns {
                    assert_eq!(column.len(), 3);
                }

                // Email column (index 0)
                match &node.columns[0][0] {
                    Expression::Constant(ConstantExpression::Undefined { .. }) => {}
                    _ => panic!("Expected Undefined for email in row 0"),
                }
                match &node.columns[0][1] {
                    Expression::Constant(ConstantExpression::Text { span }) => {
                        assert_eq!(span.fragment, "bob@test.com");
                    }
                    _ => panic!("Expected Text for email in row 1"),
                }
                match &node.columns[0][2] {
                    Expression::Constant(ConstantExpression::Undefined { .. }) => {}
                    _ => panic!("Expected Undefined for email in row 2"),
                }

                // ID column (index 1)
                match &node.columns[1][0] {
                    Expression::Constant(ConstantExpression::Number { span }) => {
                        assert_eq!(span.fragment, "1");
                    }
                    _ => panic!("Expected Number for id in row 0"),
                }
                match &node.columns[1][1] {
                    Expression::Constant(ConstantExpression::Number { span }) => {
                        assert_eq!(span.fragment, "2");
                    }
                    _ => panic!("Expected Number for id in row 1"),
                }
                match &node.columns[1][2] {
                    Expression::Constant(ConstantExpression::Undefined { .. }) => {}
                    _ => panic!("Expected Undefined for id in row 2"),
                }

                // Name column (index 2)
                match &node.columns[2][0] {
                    Expression::Constant(ConstantExpression::Text { span }) => {
                        assert_eq!(span.fragment, "Alice");
                    }
                    _ => panic!("Expected Text for name in row 0"),
                }
                match &node.columns[2][1] {
                    Expression::Constant(ConstantExpression::Undefined { .. }) => {}
                    _ => panic!("Expected Undefined for name in row 1"),
                }
                match &node.columns[2][2] {
                    Expression::Constant(ConstantExpression::Text { span }) => {
                        assert_eq!(span.fragment, "Charlie");
                    }
                    _ => panic!("Expected Text for name in row 2"),
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
                assert_eq!(node.names.len(), 0);
                assert_eq!(node.columns.len(), 0);
            }
            _ => panic!("Expected InlineData node"),
        }
    }
}
