// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use bumpalo::Bump;
use reifydb_core::value::column::{ColumnWithName, columns::Columns};
use reifydb_rql::{
	ast::{
		ast::{Ast, AstAlter, AstCreate, AstFrom, AstJoin},
		identifier::UnqualifiedIdentifier,
		parse_str,
	},
	bump::BumpFragment,
	token::token::{Token, TokenKind},
};
use reifydb_value::value::value_type::ValueType;

use crate::{
	procedure::rql::extract_query,
	routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError},
};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("rql::ast"));

pub struct RqlAst;

impl Default for RqlAst {
	fn default() -> Self {
		Self::new()
	}
}

impl RqlAst {
	pub fn new() -> Self {
		Self
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for RqlAst {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	fn attaches_row_metadata(&self) -> bool {
		false
	}

	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let query = extract_query(ctx.params, "rql::ast")?;

		let bump = Bump::new();
		let statements = parse_str(&bump, query.as_str())?;

		let mut walker = AstWalker::default();
		for statement in &statements {
			for node in &statement.nodes {
				walker.walk(node, 0);
			}
		}

		Ok(walker.into_columns())
	}
}

#[derive(Default)]
struct AstWalker {
	idx: Vec<i32>,
	depth: Vec<i32>,
	kind: Vec<String>,
	detail: Vec<String>,
}

impl AstWalker {
	fn emit(&mut self, depth: i32, kind: &str, detail: String) {
		let next = self.idx.len() as i32;
		self.idx.push(next);
		self.depth.push(depth);
		self.kind.push(kind.to_string());
		self.detail.push(detail);
	}

	fn into_columns(self) -> Columns {
		Columns::new(vec![
			ColumnWithName::int4("idx", self.idx),
			ColumnWithName::int4("depth", self.depth),
			ColumnWithName::utf8("kind", self.kind),
			ColumnWithName::utf8("detail", self.detail),
		])
	}

	fn walk(&mut self, ast: &Ast<'_>, depth: i32) {
		let token = ast.token();
		let fragment = &token.fragment;
		let kind = ast_kind(ast);
		let description = ast_description(ast, kind);
		let detail = format!(
			"{} @ line {}, column {} - \"{}\"",
			description,
			fragment.line().0,
			fragment.column().0,
			fragment.text()
		);
		self.emit(depth, kind, detail);

		let mut owned_children: Vec<Ast<'_>> = vec![];
		let mut ref_children: Vec<&Ast<'_>> = vec![];

		match ast {
			Ast::Tuple(t) => {
				for node in &t.nodes {
					ref_children.push(node);
				}
			}
			Ast::Prefix(p) => ref_children.push(&p.node),
			Ast::Cast(c) => {
				for node in &c.tuple.nodes {
					ref_children.push(node);
				}
			}
			Ast::Filter(f) => ref_children.push(&f.node),
			Ast::Gate(f) => ref_children.push(&f.node),
			Ast::From(from) => match from {
				AstFrom::Source {
					source,
					index_name,
					..
				} => {
					let source_token = Token {
						kind: TokenKind::Identifier,
						fragment: source.name,
					};
					owned_children.push(Ast::Identifier(UnqualifiedIdentifier::new(source_token)));

					if let Some(index) = index_name {
						let index_token = Token {
							kind: TokenKind::Identifier,
							fragment: *index,
						};
						owned_children
							.push(Ast::Identifier(UnqualifiedIdentifier::new(index_token)));
					}
				}
				AstFrom::Inline {
					list: query,
					..
				} => {
					for node in &query.nodes {
						ref_children.push(node);
					}
				}
				AstFrom::Generator(generator_func) => {
					for node in &generator_func.nodes {
						ref_children.push(node);
					}
				}
				AstFrom::Variable {
					variable,
					..
				} => {
					let variable_token = Token {
						kind: TokenKind::Variable,
						fragment: variable.token.fragment,
					};
					owned_children
						.push(Ast::Identifier(UnqualifiedIdentifier::new(variable_token)));
				}
				AstFrom::Environment {
					..
				} => {
					let env_token = Token {
						kind: TokenKind::Variable,
						fragment: BumpFragment::Internal {
							text: "env",
						},
					};
					owned_children.push(Ast::Identifier(UnqualifiedIdentifier::new(env_token)));
				}
			},
			Ast::Aggregate(a) => {
				if !a.map.is_empty() {
					self.emit(depth + 1, "AggregateMap", "Aggregate Map".to_string());
					for child in a.map.iter() {
						self.walk(child, depth + 2);
					}
				}
				if !a.by.is_empty() {
					self.emit(depth + 1, "AggregateBy", "Aggregate By".to_string());
					for child in a.by.iter() {
						self.walk(child, depth + 2);
					}
				} else if a.map.is_empty() {
					self.emit(depth + 1, "AggregateBy", "Aggregate By".to_string());
				}
				return;
			}
			Ast::Insert(_) => {
				return;
			}
			Ast::Join(AstJoin::LeftJoin {
				with,
				using_clause,
				..
			}) => {
				for node in &with.statement.nodes {
					ref_children.push(node);
				}
				for pair in &using_clause.pairs {
					ref_children.push(&pair.first);
					ref_children.push(&pair.second);
				}
			}
			Ast::Map(s) => {
				for node in &s.nodes {
					ref_children.push(node);
				}
			}
			Ast::Generator(s) => {
				for node in &s.nodes {
					ref_children.push(node);
				}
			}
			Ast::Sort(_o) => {}
			Ast::Inline(r) => {
				for field in &r.keyed_values {
					owned_children.push(Ast::Identifier(field.key));
					ref_children.push(&field.value);
				}
			}
			Ast::Infix(i) => {
				ref_children.push(&i.left);
				ref_children.push(&i.right);
			}
			Ast::Alter(_) => {
				return;
			}
			Ast::Patch(p) => {
				for node in &p.assignments {
					ref_children.push(node);
				}
			}
			Ast::Assert(a) => {
				if let Some(ref node) = a.node {
					ref_children.push(node);
				}
			}
			Ast::SubQuery(sq) => {
				for node in &sq.statement.nodes {
					ref_children.push(node);
				}
			}
			_ => {}
		}

		if !owned_children.is_empty() && !ref_children.is_empty() {
			let total = owned_children.len() + ref_children.len();
			let mut oi = 0;
			let mut ri = 0;
			for i in 0..total {
				if i % 2 == 0 && oi < owned_children.len() {
					self.walk(&owned_children[oi], depth + 1);
					oi += 1;
				} else if ri < ref_children.len() {
					self.walk(ref_children[ri], depth + 1);
					ri += 1;
				} else if oi < owned_children.len() {
					self.walk(&owned_children[oi], depth + 1);
					oi += 1;
				}
			}
		} else if !owned_children.is_empty() {
			for child in owned_children.iter() {
				self.walk(child, depth + 1);
			}
		} else {
			for child in ref_children.iter() {
				self.walk(child, depth + 1);
			}
		}
	}
}

fn ast_kind(ast: &Ast<'_>) -> &'static str {
	match ast {
		Ast::Aggregate(_) => "Aggregate",
		Ast::Between(_) => "Between",
		Ast::Block(_) => "Block",
		Ast::Break(_) => "Break",
		Ast::CallFunction(_) => "CallFunction",
		Ast::Continue(_) => "Continue",
		Ast::Inline(_) => "Row",
		Ast::Cast(_) => "Cast",
		Ast::Create(_) => "Create",
		Ast::Alter(_) => "Alter",
		Ast::Drop(_) => "Drop",
		Ast::Describe(_) => "Describe",
		Ast::Filter(_) => "Filter",
		Ast::Gate(_) => "Gate",
		Ast::For(_) => "For",
		Ast::From(_) => "From",
		Ast::Identifier(_) => "Identifier",
		Ast::If(_) => "If",
		Ast::Infix(_) => "Infix",
		Ast::Let(_) => "Let",
		Ast::Loop(_) => "Loop",
		Ast::Delete(_) => "Delete",
		Ast::Insert(_) => "Insert",
		Ast::Update(_) => "Update",
		Ast::Join(_) => "Join",
		Ast::List(_) => "List",
		Ast::Literal(_) => "Literal",
		Ast::Nop => "Nop",
		Ast::Sort(_) => "Sort",
		Ast::Prefix(_) => "Prefix",
		Ast::Map(_) => "Map",
		Ast::Generator(_) => "Generator",
		Ast::Extend(_) => "Extend",
		Ast::Patch(_) => "Patch",
		Ast::Take(_) => "Take",
		Ast::Tuple(_) => "Tuple",
		Ast::While(_) => "While",
		Ast::Wildcard(_) => "Wildcard",
		Ast::Variable(_) => "Variable",
		Ast::Distinct(_) => "Distinct",
		Ast::Apply(_) => "Apply",
		Ast::Call(_) => "Call",
		Ast::SubQuery(_) => "SubQuery",
		Ast::Window(_) => "Window",
		Ast::StatementExpression(_) => "StatementExpression",
		Ast::Environment(_) => "Environment",
		Ast::Rownum(_) => "Rownum",
		Ast::SystemColumn(_) => "SystemColumn",
		Ast::DefFunction(_) => "DefFunction",
		Ast::Return(_) => "Return",
		Ast::Append(_) => "Append",
		Ast::Assert(_) => "Assert",
		Ast::SumTypeConstructor(_) => "SumTypeConstructor",
		Ast::IsVariant(_) => "IsVariant",
		Ast::Match(_) => "Match",
		Ast::Closure(_) => "Closure",
		Ast::Dispatch(_) => "Dispatch",
		Ast::Grant(_) => "Grant",
		Ast::Revoke(_) => "Revoke",
		Ast::Identity(_) => "Identity",
		Ast::Require(_) => "Require",
		Ast::Migrate(_) => "Migrate",
		Ast::RollbackMigration(_) => "RollbackMigration",
		Ast::RunTests(_) => "RunTests",
	}
}

fn ast_description(ast: &Ast<'_>, kind: &str) -> String {
	match ast {
		Ast::Inline(r) => {
			let field_names: Vec<&str> = r.keyed_values.iter().map(|f| f.key.text()).collect();
			format!("{} ({} fields: {})", kind, r.keyed_values.len(), field_names.join(", "))
		}
		Ast::Alter(alter) => match alter {
			AstAlter::Sequence(s) => {
				let namespace = s
					.sequence
					.namespace
					.first()
					.map(|sch| format!("{}.", sch.text()))
					.unwrap_or_default();
				format!("ALTER SEQUENCE {}{}.{}", namespace, s.sequence.name.text(), s.column.text())
			}
			AstAlter::Policy(sp) => {
				format!("ALTER {:?} POLICY {}", sp.target_type, sp.name.text())
			}
			AstAlter::Table(t) => {
				let namespace =
					t.table.namespace.first().map(|s| format!("{}.", s.text())).unwrap_or_default();
				format!("ALTER TABLE {}{}", namespace, t.table.name.text())
			}
			AstAlter::RemoteNamespace(ns) => {
				format!(
					"ALTER REMOTE NAMESPACE {}",
					ns.namespace.segments.iter().map(|s| s.text()).collect::<Vec<_>>().join("::")
				)
			}
			AstAlter::Identity(i) => {
				format!("ALTER USER {}", i.name.text())
			}
		},
		Ast::Create(create) => match create {
			AstCreate::PrimaryKey(pk) => {
				let namespace =
					pk.table.namespace
						.first()
						.map(|s| format!("{}::", s.text()))
						.unwrap_or_default();
				format!("CREATE PRIMARY KEY ON {}{}", namespace, pk.table.name.text())
			}
			AstCreate::ColumnProperty(p) => {
				format!("CREATE COLUMN POLICY ON {}", p.column.name.text())
			}
			_ => kind.to_string(),
		},
		_ => kind.to_string(),
	}
}
