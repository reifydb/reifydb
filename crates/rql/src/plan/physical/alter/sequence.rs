// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::plan::logical::AlterSequenceNode;
use crate::plan::physical::{AlterSequencePlan, Compiler, PhysicalPlan};
use reifydb_core::interface::VersionedReadTransaction;

impl Compiler {
    pub(crate) fn compile_alter_sequence(
        _rx: &mut impl VersionedReadTransaction,
        alter: AlterSequenceNode,
    ) -> crate::Result<PhysicalPlan> {
        // For ALTER SEQUENCE, we just pass through the logical plan info
        // The actual execution will happen in the engine
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
    use crate::ast::lex::lex;
    use crate::ast::parse::parse;
    use crate::expression::{ConstantExpression, Expression};
    use crate::plan::logical::compile_logical;
    use crate::plan::physical::{PhysicalPlan, compile_physical};
    use reifydb_transaction::test_utils::create_test_write_transaction;

    #[test]
    fn test_compile_alter_sequence_physical() {
        let tokens = lex("ALTER SEQUENCE test.users.id SET VALUE 1000").unwrap();
        let ast = parse(tokens).unwrap();

        let logical_plans = compile_logical(ast.into_iter().next().unwrap()).unwrap();

        let mut rx = create_test_write_transaction();
        let physical_plan = compile_physical(&mut rx, logical_plans).unwrap().unwrap();

        match physical_plan {
            PhysicalPlan::AlterSequence(plan) => {
                assert!(plan.schema.is_some());
                assert_eq!(plan.schema.as_ref().unwrap().fragment, "test");
                assert_eq!(plan.table.fragment, "users");
                assert_eq!(plan.column.fragment, "id");

                assert!(matches!(
                    plan.value,
                    Expression::Constant(ConstantExpression::Number { span: _ })
                ));
                let span = plan.value.span();
                assert_eq!(span.fragment, "1000");
            }
            _ => panic!("Expected AlterSequence physical plan"),
        }
    }
}
