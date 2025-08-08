// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{Ast, AstAlterSequence};
use crate::expression::ExpressionCompiler;
use crate::plan::logical::{AlterSequenceNode, Compiler, LogicalPlan};

impl Compiler {
    pub(crate) fn compile_alter_sequence(ast: AstAlterSequence) -> crate::Result<LogicalPlan> {
        Ok(LogicalPlan::AlterSequence(AlterSequenceNode {
            schema: ast.schema.map(|s| s.span()),
            table: ast.table.span(),
            column: ast.column.span(),
            value: ExpressionCompiler::compile(Ast::Literal(ast.value))?,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::lex::lex;
    use crate::ast::parse::parse;
    use crate::expression::{ConstantExpression, Expression};
    use crate::plan::logical::{LogicalPlan, compile_logical};

    #[test]
    fn test_with_schema() {
        let tokens = lex("ALTER SEQUENCE test.users.id SET VALUE 1000").unwrap();
        let ast = parse(tokens).unwrap();

        let plans = compile_logical(ast.into_iter().next().unwrap()).unwrap();
        assert_eq!(plans.len(), 1);

        match &plans[0] {
            LogicalPlan::AlterSequence(node) => {
                assert!(node.schema.is_some());
                assert_eq!(node.schema.as_ref().unwrap().fragment, "test");
                assert_eq!(node.table.fragment, "users");
                assert_eq!(node.column.fragment, "id");

                assert!(matches!(
                    node.value,
                    Expression::Constant(ConstantExpression::Number { span: _ })
                ));
                let span = node.value.span();
                assert_eq!(span.fragment, "1000");
            }
            _ => panic!("Expected AlterSequence plan"),
        }
    }

    #[test]
    fn test_without_schema() {
        let tokens = lex("ALTER SEQUENCE users.id SET VALUE 500").unwrap();
        let ast = parse(tokens).unwrap();

        let plans = compile_logical(ast.into_iter().next().unwrap()).unwrap();
        assert_eq!(plans.len(), 1);

        match &plans[0] {
            LogicalPlan::AlterSequence(node) => {
                assert!(node.schema.is_none());
                assert_eq!(node.table.fragment, "users");
                assert_eq!(node.column.fragment, "id");

                assert!(matches!(
                    node.value,
                    Expression::Constant(ConstantExpression::Number { span: _ })
                ));
                let span = node.value.span();
                assert_eq!(span.fragment, "500");
            }
            _ => panic!("Expected AlterSequence plan"),
        }
    }
}
