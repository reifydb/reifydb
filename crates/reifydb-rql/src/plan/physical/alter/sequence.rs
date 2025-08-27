// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::QueryTransaction;

use crate::plan::{
	logical::AlterSequenceNode,
	physical::{AlterSequencePlan, Compiler, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_alter_sequence(
		_rx: &mut impl QueryTransaction,
		alter: AlterSequenceNode,
	) -> crate::Result<PhysicalPlan> {
		// For ALTER SEQUENCE, we just pass through the logical plan
		// info The actual execution will happen in the engine
		Ok(PhysicalPlan::AlterSequence(AlterSequencePlan {
			schema: alter.schema,
			table: alter.table,
			column: alter.column,
			value: alter.value,
		}))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::evaluate::expression::{
		ConstantExpression, Expression,
	};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		ast::{parse::parse, tokenize::tokenize},
		plan::{
			logical::compile_logical,
			physical::{PhysicalPlan, compile_physical},
		},
	};

	#[test]
	fn test_compile_alter_sequence_physical() {
		let tokens =
			tokenize("ALTER SEQUENCE test.users.id SET VALUE 1000")
				.unwrap();
		let ast = parse(tokens).unwrap();

		let logical_plans =
			compile_logical(ast.into_iter().next().unwrap())
				.unwrap();

		let mut rx = create_test_command_transaction();
		let physical_plan = compile_physical(&mut rx, logical_plans)
			.unwrap()
			.unwrap();

		match physical_plan {
			PhysicalPlan::AlterSequence(plan) => {
				assert!(plan.schema.is_some());
				assert_eq!(
					plan.schema.as_ref().unwrap().text(),
					"test"
				);
				assert_eq!(plan.table.text(), "users");
				assert_eq!(plan.column.text(), "id");

				assert!(matches!(
					plan.value,
					Expression::Constant(
						ConstantExpression::Number {
							fragment: _
						}
					)
				));
				let fragment = plan.value.fragment();
				assert_eq!(fragment.text(), "1000");
			}
			_ => panic!("Expected AlterSequence physical plan"),
		}
	}
}
