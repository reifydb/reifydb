// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{AstAlterSequence, AstLiteral};
use crate::plan::logical::{AlterSequenceNode, Compiler, LogicalPlan};
use reifydb_core::value::number::parse_int;

impl Compiler {
    pub(crate) fn compile_alter_sequence(ast: AstAlterSequence) -> crate::Result<LogicalPlan> {
        // Parse the value from the literal
        let value = match ast.value {
            AstLiteral::Number(num) => parse_int::<i128>(num.0.span)?,
            _ => {
                unimplemented!("ALTER SEQUENCE requires a number literal");
            }
        };

        Ok(LogicalPlan::AlterSequence(AlterSequenceNode {
            schema: ast.schema.map(|s| s.span()),
            table: ast.table.span(),
            column: ast.column.span(),
            value,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::lex::lex;
    use crate::ast::parse::parse;
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
                assert_eq!(node.value, 1000);
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
                assert_eq!(node.value, 500);
            }
            _ => panic!("Expected AlterSequence plan"),
        }
    }
}
