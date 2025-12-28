// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Parallel query execution support.
//!
//! This module provides dependency analysis for query statements to determine
//! if they can be executed in parallel. Statements are considered parallelizable
//! if they don't use any scripting constructs (variables, control flow).

use reifydb_rql::ast::{Ast, AstFrom, AstJoin, AstStatement, InfixOperator};

/// Check if a batch of statements can be executed in parallel.
///
/// Returns `true` if:
/// - There are at least 2 statements
/// - No statement contains scripting constructs (LET, MUT, $variable, IF, :=)
///
/// When `true`, all statements can be executed concurrently since they have
/// no data dependencies between them.
pub fn can_parallelize(statements: &[AstStatement]) -> bool {
	if statements.len() <= 1 {
		return false;
	}
	statements.iter().all(|stmt| !has_scripting(stmt))
}

/// Check if a statement contains any scripting constructs.
fn has_scripting(statement: &AstStatement) -> bool {
	statement.nodes.iter().any(|ast| ast_has_scripting(ast))
}

/// Recursively check if an AST node contains scripting constructs.
fn ast_has_scripting(ast: &Ast) -> bool {
	match ast {
		// Direct scripting constructs - these prevent parallelization
		Ast::Let(_) => true,
		Ast::Variable(_) => true,
		// IF is a scripting construct - always prevents parallelization
		Ast::If(_) => true,

		// Check for assignment operator (:=)
		Ast::Infix(infix) => {
			if matches!(infix.operator, InfixOperator::Assign(_)) {
				return true;
			}
			// Recursively check children
			ast_has_scripting(&infix.left) || ast_has_scripting(&infix.right)
		}

		// Nodes with child expressions to check
		Ast::Aggregate(node) => node.by.iter().any(ast_has_scripting) || node.map.iter().any(ast_has_scripting),
		Ast::Apply(node) => node.expressions.iter().any(ast_has_scripting),
		Ast::Between(node) => {
			ast_has_scripting(&node.value)
				|| ast_has_scripting(&node.lower)
				|| ast_has_scripting(&node.upper)
		}
		Ast::Call(node) => node.arguments.nodes.iter().any(ast_has_scripting),
		Ast::CallFunction(node) => node.arguments.nodes.iter().any(ast_has_scripting),
		Ast::Cast(node) => node.tuple.nodes.iter().any(ast_has_scripting),
		Ast::Distinct(_) => false, // Distinct only has column identifiers, no expressions
		Ast::Filter(node) => ast_has_scripting(&node.node),
		Ast::From(from) => match from {
			AstFrom::Source {
				..
			} => false,
			AstFrom::Variable {
				..
			} => true, // FROM $var is scripting
			AstFrom::Environment {
				..
			} => false,
			AstFrom::Inline {
				..
			} => false,
			AstFrom::Generator(generator) => generator.nodes.iter().any(ast_has_scripting),
		},
		Ast::Join(join) => match join {
			AstJoin::InnerJoin {
				on,
				with,
				..
			} => on.iter().any(ast_has_scripting) || has_scripting(&with.statement),
			AstJoin::LeftJoin {
				on,
				with,
				..
			} => on.iter().any(ast_has_scripting) || has_scripting(&with.statement),
			AstJoin::NaturalJoin {
				with,
				..
			} => has_scripting(&with.statement),
		},
		Ast::Map(node) => node.nodes.iter().any(ast_has_scripting),
		Ast::Extend(node) => node.nodes.iter().any(ast_has_scripting),
		Ast::Merge(node) => has_scripting(&node.with.statement),
		Ast::Prefix(node) => ast_has_scripting(&node.node),
		Ast::Sort(_) => false, // Sort uses column identifiers, not expressions
		Ast::SubQuery(node) => has_scripting(&node.statement),
		Ast::Take(_) => false, // Take only has a count, no expressions
		Ast::Tuple(node) => node.nodes.iter().any(ast_has_scripting),
		Ast::List(node) => node.nodes.iter().any(ast_has_scripting),
		Ast::Window(node) => {
			node.config.iter().any(|c| ast_has_scripting(&c.value))
				|| node.aggregations.iter().any(ast_has_scripting)
				|| node.group_by.iter().any(ast_has_scripting)
		}
		Ast::StatementExpression(node) => ast_has_scripting(&node.expression),
		Ast::Generator(node) => node.nodes.iter().any(ast_has_scripting),
		Ast::Insert(_) => false, // Insert target is an identifier, no expressions here
		Ast::Update(_) => false, // Update target is an identifier, no expressions here
		Ast::Inline(node) => node.keyed_values.iter().any(|kv| ast_has_scripting(&kv.value)),
		Ast::Policy(node) => ast_has_scripting(&node.value),
		Ast::PolicyBlock(node) => node.policies.iter().any(|p| ast_has_scripting(&p.value)),

		// Terminal nodes with no children - these don't prevent parallelization
		Ast::Identifier(_)
		| Ast::Literal(_)
		| Ast::Nop
		| Ast::Wildcard(_)
		| Ast::Environment(_)
		| Ast::Rownum(_)
		| Ast::Create(_)
		| Ast::Alter(_)
		| Ast::Drop(_)
		| Ast::Describe(_)
		| Ast::Delete(_) => false,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_rql::ast::parse_str;

	use super::*;

	#[test]
	fn test_single_statement_not_parallelizable() {
		let statements = parse_str("FROM users").unwrap();
		assert!(!can_parallelize(&statements));
	}

	#[test]
	fn test_multiple_independent_statements() {
		let statements = parse_str("FROM users; FROM orders").unwrap();
		assert!(can_parallelize(&statements));
	}

	#[test]
	fn test_let_prevents_parallelization() {
		let statements = parse_str("LET $x := 1; FROM users").unwrap();
		assert!(!can_parallelize(&statements));
	}

	#[test]
	fn test_variable_reference_prevents_parallelization() {
		let statements = parse_str("FROM users; MAP { count: $x }").unwrap();
		assert!(!can_parallelize(&statements));
	}

	#[test]
	fn test_if_prevents_parallelization() {
		let statements = parse_str("FROM users; IF true { 1 }").unwrap();
		assert!(!can_parallelize(&statements));
	}

	#[test]
	fn test_piped_statement_is_single() {
		// A piped statement is still ONE statement
		let statements = parse_str("FROM users | FILTER age > 21").unwrap();
		assert_eq!(statements.len(), 1);
		assert!(!can_parallelize(&statements));
	}

	#[test]
	fn test_multiple_complex_independent_statements() {
		let statements = parse_str("FROM users | FILTER age > 21; FROM orders | FILTER total > 100").unwrap();
		assert!(can_parallelize(&statements));
	}
}
