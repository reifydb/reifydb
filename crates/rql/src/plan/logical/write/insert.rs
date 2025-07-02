// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::plan::logical::Compiler;

impl Compiler{

	pub(crate) fn compile_insert_table(){
		//         Ast::Insert(insert) => {
		//             return match insert {
		//                 AstInsert { schema, store, columns, rows, .. } => {
		//                     let schema = schema.value().to_string();
		//                     let store = store.0.span;
		// 
		//                     let schema = Catalog::get_schema_by_name(rx, &schema).unwrap().unwrap();
		//                     let Some(table) =
		//                         Catalog::get_table_by_name(rx, schema.id, &store.fragment).unwrap()
		//                     else {
		//                         return Err(Error(table_not_found(
		//                             store.clone(),
		//                             &schema.name,
		//                             &store.fragment,
		//                         )));
		//                     };
		// 
		//                     // Get the store schema from the catalog once
		//                     // let store_schema =
		//                     //     catalog.get(&schema).unwrap().get(store.deref()).unwrap();
		// 
		//                     // Build the user-specified column name list
		//                     let insert_column_names: Vec<_> = columns
		//                         .nodes
		//                         .into_iter()
		//                         .map(|column| match column {
		//                             Ast::Identifier(ast) => ast.value().to_string(),
		//                             _ => unimplemented!(),
		//                         })
		//                         .collect::<Vec<_>>();
		// 
		//                     // Lookup actual columns from the store
		//                     let columns_to_insert: Vec<_> = insert_column_names
		//                         .iter()
		//                         .map(|name| {
		//                             Catalog::get_column_by_name(rx, table.id, name.deref())
		//                                 .unwrap()
		//                                 .unwrap()
		//                         })
		//                         .collect::<Vec<_>>();
		// 
		//                     // Create a mapping: column name -> position in insert input
		//                     let insert_index_map: HashMap<_, _> = insert_column_names
		//                         .iter()
		//                         .enumerate()
		//                         .map(|(i, name)| (name.to_string(), i))
		//                         .collect();
		// 
		//                     // Now reorder the row expressions to match store_schema.column order
		//                     let rows_to_insert = rows
		//                         .into_iter()
		//                         .map(|mut row| {
		//                             let mut values = vec![None; columns_to_insert.len()];
		// 
		//                             for (col_idx, col) in table.columns.iter().enumerate() {
		//                                 if let Some(&input_idx) = insert_index_map.get(&col.name) {
		//                                     let expr =
		//                                         mem::replace(&mut row.nodes[input_idx], Ast::Nop);
		// 
		//                                     let expr = match expr {
		//                                         Ast::Literal(AstLiteral::Boolean(ast)) => {
		//                                             Expression::Constant(ConstantExpression::Bool {
		//                                                 span: ast.0.span,
		//                                             })
		//                                         }
		//                                         Ast::Literal(AstLiteral::Number(ast)) => {
		//                                             Expression::Constant(ConstantExpression::Number {
		//                                                 span: ast.0.span,
		//                                             })
		//                                         }
		//                                         Ast::Literal(AstLiteral::Text(ast)) => {
		//                                             Expression::Constant(ConstantExpression::Text {
		//                                                 span: ast.0.span,
		//                                             })
		//                                         }
		//                                         Ast::Prefix(AstPrefix { operator, node }) => {
		//                                             let a = node.deref();
		// 
		//                                             let (span, operator) = match operator {
		//                                                 ast::AstPrefixOperator::Plus(token) => (
		//                                                     token.span.clone(),
		//                                                     PrefixOperator::Plus(token.span),
		//                                                 ),
		//                                                 ast::AstPrefixOperator::Negate(token) => (
		//                                                     token.span.clone(),
		//                                                     PrefixOperator::Minus(token.span),
		//                                                 ),
		//                                                 ast::AstPrefixOperator::Not(_token) => {
		//                                                     unimplemented!()
		//                                                 }
		//                                             };
		// 
		//                                             Expression::Prefix(PrefixExpression {
		//                                                 operator,
		//                                                 expression: Box::new(match a {
		//                                                     Ast::Literal(lit) => match lit {
		//                                                         AstLiteral::Boolean(n) => {
		//                                                             Expression::Constant(
		//                                                                 ConstantExpression::Bool {
		//                                                                     span: n.0.span.clone(),
		//                                                                 },
		//                                                             )
		//                                                         }
		//                                                         AstLiteral::Number(n) => {
		//                                                             Expression::Constant(
		//                                                                 ConstantExpression::Number {
		//                                                                     span: n.0.span.clone(),
		//                                                                 },
		//                                                             )
		//                                                         }
		//                                                         AstLiteral::Text(t) => {
		//                                                             Expression::Constant(
		//                                                                 ConstantExpression::Text {
		//                                                                     span: t.0.span.clone(),
		//                                                                 },
		//                                                             )
		//                                                         }
		//                                                         AstLiteral::Undefined(t) => {
		//                                                             Expression::Constant(
		//                                                                 ConstantExpression::Undefined {
		//                                                                     span: t.0.span.clone(),
		//                                                                 },
		//                                                             )
		//                                                         }
		//                                                     },
		//                                                     _ => unimplemented!(),
		//                                                 }),
		//                                                 span,
		//                                             })
		//                                         }
		//                                         Ast::Infix(infix) => expression_infix(infix).unwrap(),
		//                                         node => unimplemented!("{node:?}"),
		//                                     };
		// 
		//                                     values[col_idx] = Some(expr);
		//                                 } else {
		//                                     // Not provided in INSERT, use default
		//                                     unimplemented!()
		//                                 }
		//                             }
		// 
		//                             values.into_iter().map(|v| v.unwrap()).collect::<Vec<_>>()
		//                         })
		//                         .collect::<Vec<_>>();
		// 
		//                     // let s = catalog.get(&schema).unwrap().get(&store).unwrap();
		// 
		//                     let columns = table.columns;
		// 
		//                     // match s.kind().unwrap() {
		//                     //     StoreKind::Series => {
		//                     //         Ok(PlanTx::InsertIntoSeries(InsertIntoSeriesPlan::Values {
		//                     //             schema: schema.name,
		//                     //             series: store,
		//                     //             columns,
		//                     //             rows_to_insert,
		//                     //         }))
		//                     //     }
		//                     Ok(Some(PlanTx::InsertIntoTable(InsertIntoTablePlan::Values {
		//                         schema: schema.name,
		//                         table: store,
		//                         columns,
		//                         rows_to_insert,
		//                     })))
		todo!()
	}

}
