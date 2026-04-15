// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::LazyLock;

use bumpalo::Bump;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_rql::ast::ast::{Ast, AstFrom, AstJoin, AstLiteral, AstSkip, AstTake, AstTakeValue, InfixOperator};
use reifydb_rql_graphql::{Compiler, Lexer, Parser};
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, r#type::Type},
};

use crate::routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("graphql::explain"));

/// Procedure that compiles a GraphQL query string and emits the resulting
/// RQL AST nodes as a table - one row per node, columns `idx`, `kind`, `detail`.
pub struct GraphqlExplain;

impl Default for GraphqlExplain {
	fn default() -> Self {
		Self::new()
	}
}

impl GraphqlExplain {
	pub fn new() -> Self {
		Self
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for GraphqlExplain {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}

	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let query = match ctx.params {
			Params::Positional(args) if args.len() == 1 => match &args[0] {
				Value::Utf8(s) => s.as_str().to_string(),
				other => {
					return Err(RoutineError::ProcedureInvalidArgumentType {
						procedure: Fragment::internal("graphql::explain"),
						argument_index: 0,
						expected: vec![Type::Utf8],
						actual: other.get_type(),
					});
				}
			},
			Params::Positional(args) => {
				return Err(RoutineError::ProcedureArityMismatch {
					procedure: Fragment::internal("graphql::explain"),
					expected: 1,
					actual: args.len(),
				});
			}
			_ => {
				return Err(RoutineError::ProcedureArityMismatch {
					procedure: Fragment::internal("graphql::explain"),
					expected: 1,
					actual: 0,
				});
			}
		};

		let bump = Bump::new();
		let tokens = Lexer::new(&bump, &query).tokenize().map_err(|e| RoutineError::ProcedureExecutionFailed {
			procedure: Fragment::internal("graphql::explain"),
			reason: format!("{e}"),
		})?;
		let mut parser = Parser::new(&bump, tokens);
		let operation = parser.parse_operation().map_err(|e| RoutineError::ProcedureExecutionFailed {
			procedure: Fragment::internal("graphql::explain"),
			reason: format!("{e}"),
		})?;
		let statement =
			Compiler::new(&bump).compile(&operation).map_err(|e| RoutineError::ProcedureExecutionFailed {
				procedure: Fragment::internal("graphql::explain"),
				reason: format!("{e}"),
			})?;

		let mut idx_col: Vec<i32> = Vec::with_capacity(statement.nodes.len());
		let mut kind_col: Vec<String> = Vec::with_capacity(statement.nodes.len());
		let mut detail_col: Vec<String> = Vec::with_capacity(statement.nodes.len());

		for (i, node) in statement.nodes.iter().enumerate() {
			idx_col.push(i as i32);
			kind_col.push(kind_of(node).to_string());
			detail_col.push(detail_of(node));
		}

		Ok(Columns::new(vec![
			ColumnWithName::new("idx", ColumnBuffer::int4(idx_col)),
			ColumnWithName::new("kind", ColumnBuffer::utf8(kind_col)),
			ColumnWithName::new("detail", ColumnBuffer::utf8(detail_col)),
		]))
	}
}

fn kind_of(ast: &Ast<'_>) -> &'static str {
	match ast {
		Ast::From(_) => "From",
		Ast::Filter(_) => "Filter",
		Ast::Map(_) => "Map",
		Ast::Take(_) => "Take",
		Ast::Skip(_) => "Skip",
		Ast::Join(_) => "Join",
		Ast::Identifier(_) => "Identifier",
		Ast::Literal(_) => "Literal",
		Ast::Infix(_) => "Infix",
		_ => "Other",
	}
}

fn detail_of(ast: &Ast<'_>) -> String {
	match ast {
		Ast::From(AstFrom::Source {
			source,
			..
		}) => match &source.alias {
			Some(alias) => format!("{} as {}", source.name.text(), alias.text()),
			None => source.name.text().to_string(),
		},
		Ast::From(_) => String::new(),
		Ast::Join(AstJoin::NaturalJoin {
			alias,
			..
		}) => format!("natural {}", alias.text()),
		Ast::Join(AstJoin::InnerJoin {
			..
		}) => "inner".to_string(),
		Ast::Join(AstJoin::LeftJoin {
			..
		}) => "left".to_string(),
		Ast::Take(AstTake {
			take: AstTakeValue::Literal(n),
			..
		}) => n.to_string(),
		Ast::Take(AstTake {
			take: AstTakeValue::Variable(t),
			..
		}) => format!("${}", t.value()),
		Ast::Skip(AstSkip {
			skip: AstTakeValue::Literal(n),
			..
		}) => n.to_string(),
		Ast::Skip(AstSkip {
			skip: AstTakeValue::Variable(t),
			..
		}) => format!("${}", t.value()),
		Ast::Filter(filter) => render(&filter.node),
		Ast::Map(map) => map.nodes.iter().map(render).collect::<Vec<_>>().join(", "),
		other => render(other),
	}
}

fn render(ast: &Ast<'_>) -> String {
	match ast {
		Ast::Identifier(ident) => ident.text().to_string(),
		Ast::Literal(AstLiteral::Number(n)) => n.value().to_string(),
		Ast::Literal(AstLiteral::Text(t)) => format!("'{}'", t.value()),
		Ast::Literal(AstLiteral::Boolean(b)) => b.value().to_string(),
		Ast::Infix(infix) => {
			format!("{} {} {}", render(&infix.left), operator_symbol(&infix.operator), render(&infix.right))
		}
		_ => ast.token().fragment.text().to_string(),
	}
}

fn operator_symbol(op: &InfixOperator<'_>) -> &'static str {
	match op {
		InfixOperator::Add(_) => "+",
		InfixOperator::Subtract(_) => "-",
		InfixOperator::Multiply(_) => "*",
		InfixOperator::Divide(_) => "/",
		InfixOperator::Rem(_) => "%",
		InfixOperator::Equal(_) => "==",
		InfixOperator::NotEqual(_) => "!=",
		InfixOperator::LessThan(_) => "<",
		InfixOperator::LessThanEqual(_) => "<=",
		InfixOperator::GreaterThan(_) => ">",
		InfixOperator::GreaterThanEqual(_) => ">=",
		InfixOperator::And(_) => "and",
		InfixOperator::Or(_) => "or",
		InfixOperator::Xor(_) => "xor",
		InfixOperator::In(_) => "in",
		InfixOperator::NotIn(_) => "not in",
		InfixOperator::Contains(_) => "contains",
		InfixOperator::As(_) => "as",
		InfixOperator::AccessNamespace(_) => "::",
		InfixOperator::AccessTable(_) => ".",
		InfixOperator::Assign(_) => "=",
		InfixOperator::Call(_) => "call",
		InfixOperator::TypeAscription(_) => ":",
	}
}
