// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ast::{Ast, AstAlterSequence},
	expression::ExpressionCompiler,
	plan::logical::{AlterSequenceNode, Compiler, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_alter_sequence<'a>(
		ast: AstAlterSequence<'a>,
	) -> crate::Result<LogicalPlan<'a>> {
		Ok(LogicalPlan::AlterSequence(AlterSequenceNode {
			schema: ast.schema.map(|s| s.fragment()),
			table: ast.table.fragment(),
			column: ast.column.fragment(),
			value: ExpressionCompiler::compile(Ast::Literal(
				ast.value,
			))?,
		}))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::evaluate::expression::{
		ConstantExpression, Expression,
	};

	use crate::{
		ast::{parse::parse, tokenize::tokenize},
		plan::logical::{LogicalPlan, compile_logical},
	};

	#[test]
	fn test_with_schema() {
		let tokens =
			tokenize("ALTER SEQUENCE test.users.id SET VALUE 1000")
				.unwrap();
		let ast = parse(tokens).unwrap();

		let plans = compile_logical(ast.into_iter().next().unwrap())
			.unwrap();
		assert_eq!(plans.len(), 1);

		match &plans[0] {
			LogicalPlan::AlterSequence(node) => {
				assert!(node.schema.is_some());
				assert_eq!(
					node.schema
						.as_ref()
						.unwrap()
						.fragment(),
					"test"
				);
				assert_eq!(node.table.fragment(), "users");
				assert_eq!(node.column.fragment(), "id");

				assert!(matches!(
					node.value,
					Expression::Constant(
						ConstantExpression::Number {
							fragment: _
						}
					)
				));
				let fragment = node.value.fragment();
				assert_eq!(fragment.fragment(), "1000");
			}
			_ => panic!("Expected AlterSequence plan"),
		}
	}

	#[test]
	fn test_without_schema() {
		let tokens = tokenize("ALTER SEQUENCE users.id SET VALUE 500")
			.unwrap();
		let ast = parse(tokens).unwrap();

		let plans = compile_logical(ast.into_iter().next().unwrap())
			.unwrap();
		assert_eq!(plans.len(), 1);

		match &plans[0] {
			LogicalPlan::AlterSequence(node) => {
				assert!(node.schema.is_none());
				assert_eq!(node.table.fragment(), "users");
				assert_eq!(node.column.fragment(), "id");

				assert!(matches!(
					node.value,
					Expression::Constant(
						ConstantExpression::Number {
							fragment: _
						}
					)
				));
				let fragment = node.value.fragment();
				assert_eq!(fragment.fragment(), "500");
			}
			_ => panic!("Expected AlterSequence plan"),
		}
	}
}
