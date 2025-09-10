// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::identifier::{
	ColumnIdentifier, ColumnSource, SequenceIdentifier,
};
use reifydb_type::{Fragment, OwnedFragment};

use crate::{
	ast::{Ast, AstAlterSequence},
	expression::ExpressionCompiler,
	plan::logical::{AlterSequenceNode, Compiler, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_alter_sequence<'a>(
		ast: AstAlterSequence<'a>,
	) -> crate::Result<LogicalPlan<'a>> {
		// Convert MaybeQualified to fully qualified
		let schema = ast.sequence.schema.unwrap_or_else(|| {
			Fragment::Owned(OwnedFragment::Internal {
				text: String::from("default"),
			})
		});

		let sequence = SequenceIdentifier::new(
			schema.clone(),
			ast.sequence.name.clone(),
		);

		// Create a fully qualified column identifier
		// The column belongs to the same table as the sequence
		let column = ColumnIdentifier {
			source: ColumnSource::Source {
				schema,
				source: ast.sequence.name,
			},
			name: ast.column.fragment(),
		};

		Ok(LogicalPlan::AlterSequence(AlterSequenceNode {
			sequence,
			column,
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
				assert_eq!(node.sequence.schema.text(), "test");
				assert_eq!(node.sequence.name.text(), "users");
				assert_eq!(node.column.name.text(), "id");

				assert!(matches!(
					node.value,
					Expression::Constant(
						ConstantExpression::Number {
							fragment: _
						}
					)
				));
				let fragment = node.value.full_fragment_owned();
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
				assert_eq!(
					node.sequence.schema.text(),
					"default"
				);
				assert_eq!(node.sequence.name.text(), "users");
				assert_eq!(node.column.name.text(), "id");

				assert!(matches!(
					node.value,
					Expression::Constant(
						ConstantExpression::Number {
							fragment: _
						}
					)
				));
				let fragment = node.value.full_fragment_owned();
				assert_eq!(fragment.fragment(), "500");
			}
			_ => panic!("Expected AlterSequence plan"),
		}
	}
}
