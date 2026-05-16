// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::LazyLock;

use bumpalo::Bump;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_rql::ast::ast::{
	Ast, AstAppend, AstAppendSource, AstFor, AstFrom, AstInline, AstJoin, AstLet, AstList, AstLiteral, AstSkip,
	AstTake, AstTakeValue, InfixOperator, LetValue,
};
use reifydb_rql_graphql::{compiler::compiler::Compiler, parse::parser::Parser, token::lexer::Lexer};
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
		let tokens =
			Lexer::new(&bump, &query).tokenize().map_err(|e| RoutineError::ProcedureExecutionFailed {
				procedure: Fragment::internal("graphql::explain"),
				reason: format!("{e}"),
			})?;
		let mut parser = Parser::new(&bump, tokens);
		let operation = parser.parse_operation().map_err(|e| RoutineError::ProcedureExecutionFailed {
			procedure: Fragment::internal("graphql::explain"),
			reason: format!("{e}"),
		})?;
		let statement = Compiler::new(&bump, ctx.catalog.clone()).compile(ctx.tx, &operation).map_err(|e| {
			RoutineError::ProcedureExecutionFailed {
				procedure: Fragment::internal("graphql::explain"),
				reason: format!("{e}"),
			}
		})?;

		let mut rows: Vec<(String, String)> = Vec::new();
		for node in statement.nodes.iter() {
			flatten(node, 0, &mut rows);
		}

		let mut idx_col: Vec<i32> = Vec::with_capacity(rows.len());
		let mut kind_col: Vec<String> = Vec::with_capacity(rows.len());
		let mut detail_col: Vec<String> = Vec::with_capacity(rows.len());
		for (i, (kind, detail)) in rows.into_iter().enumerate() {
			idx_col.push(i as i32);
			kind_col.push(kind);
			detail_col.push(detail);
		}

		Ok(Columns::new(vec![
			ColumnWithName::new("idx", ColumnBuffer::int4(idx_col)),
			ColumnWithName::new("kind", ColumnBuffer::utf8(kind_col)),
			ColumnWithName::new("detail", ColumnBuffer::utf8(detail_col)),
		]))
	}
}

fn flatten(ast: &Ast<'_>, depth: usize, rows: &mut Vec<(String, String)>) {
	let indent = "  ".repeat(depth);
	match ast {
		Ast::For(AstFor {
			variable,
			iterable,
			body,
			..
		}) => {
			rows.push((
				"For".to_string(),
				format!("{}{} in {}", indent, variable.token.value(), render(iterable)),
			));
			for stmt in &body.statements {
				for node in &stmt.nodes {
					flatten(node, depth + 1, rows);
				}
			}
		}
		_ => {
			let detail = detail_of(ast);
			rows.push((kind_of(ast).to_string(), format!("{}{}", indent, detail)));
		}
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
		Ast::Let(_) => "Let",
		Ast::For(_) => "For",
		Ast::Append(_) => "Append",
		Ast::Variable(_) => "Variable",
		Ast::List(_) => "List",
		Ast::Inline(_) => "Inline",
		Ast::SubQuery(_) => "SubQuery",
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
		Ast::From(AstFrom::Variable {
			variable,
			..
		}) => variable.token.value().to_string(),
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
		Ast::Let(AstLet {
			name,
			value,
			..
		}) => match value {
			LetValue::Statement(stmt) => {
				let body = stmt.nodes.iter().map(render_pipeline_node).collect::<Vec<_>>().join(" | ");
				format!("{} = {}", name.text(), body)
			}
			LetValue::Expression(expr) => {
				format!("{} = {}", name.text(), render(expr))
			}
		},
		Ast::For(AstFor {
			variable,
			iterable,
			body,
			..
		}) => {
			let body_str = body
				.statements
				.iter()
				.map(|s| s.nodes.iter().map(render_pipeline_node).collect::<Vec<_>>().join(" | "))
				.collect::<Vec<_>>()
				.join("; ");
			format!("{} in {} {{ {} }}", variable.token.value(), render(iterable), body_str)
		}
		Ast::Append(AstAppend::IntoVariable {
			target,
			source,
			..
		}) => {
			let src_str = match source {
				AstAppendSource::Statement(stmt) => {
					stmt.nodes.iter().map(render_pipeline_node).collect::<Vec<_>>().join(" | ")
				}
				AstAppendSource::Inline(list) => render_list(list),
			};
			format!("{} from {}", target.token.value(), src_str)
		}
		Ast::Append(AstAppend::Query {
			..
		}) => "query".to_string(),
		Ast::Variable(v) => v.token.value().to_string(),
		Ast::List(list) => render_list(list),
		Ast::Inline(inline) => render_inline(inline),
		Ast::SubQuery(sq) => {
			let body = sq.statement.nodes.iter().map(render_pipeline_node).collect::<Vec<_>>().join(" | ");
			format!("({})", body)
		}
		other => render(other),
	}
}

fn render_pipeline_node(ast: &Ast<'_>) -> String {
	match ast {
		Ast::From(AstFrom::Source {
			source,
			..
		}) => format!("from {}", source.name.text()),
		Ast::From(AstFrom::Variable {
			variable,
			..
		}) => format!("from {}", variable.token.value()),
		Ast::Filter(filter) => format!("filter {}", render(&filter.node)),
		Ast::Map(map) => {
			let cols = map.nodes.iter().map(render).collect::<Vec<_>>().join(", ");
			format!("map {}", cols)
		}
		Ast::Take(AstTake {
			take: AstTakeValue::Literal(n),
			..
		}) => format!("take {}", n),
		Ast::Skip(AstSkip {
			skip: AstTakeValue::Literal(n),
			..
		}) => format!("skip {}", n),
		other => render(other),
	}
}

fn render_list(list: &AstList<'_>) -> String {
	let inner = list.nodes.iter().map(render).collect::<Vec<_>>().join(", ");
	format!("[{}]", inner)
}

fn render_inline(inline: &AstInline<'_>) -> String {
	let body = inline
		.keyed_values
		.iter()
		.map(|kv| format!("{}: {}", kv.key.text(), render(&kv.value)))
		.collect::<Vec<_>>()
		.join(", ");
	format!("{{{}}}", body)
}

fn render(ast: &Ast<'_>) -> String {
	match ast {
		Ast::Identifier(ident) => ident.text().to_string(),
		Ast::Literal(AstLiteral::Number(n)) => n.value().to_string(),
		Ast::Literal(AstLiteral::Text(t)) => format!("'{}'", t.value()),
		Ast::Literal(AstLiteral::Boolean(b)) => b.value().to_string(),
		Ast::Infix(infix) => match &infix.operator {
			InfixOperator::AccessTable(_) | InfixOperator::AccessNamespace(_) => format!(
				"{}{}{}",
				render(&infix.left),
				operator_symbol(&infix.operator),
				render(&infix.right)
			),
			_ => format!(
				"{} {} {}",
				render(&infix.left),
				operator_symbol(&infix.operator),
				render(&infix.right)
			),
		},
		Ast::Variable(v) => v.token.value().to_string(),
		Ast::Inline(inline) => render_inline(inline),
		Ast::List(list) => render_list(list),
		Ast::SubQuery(sq) => {
			let body = sq.statement.nodes.iter().map(render_pipeline_node).collect::<Vec<_>>().join(" | ");
			format!("({})", body)
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
