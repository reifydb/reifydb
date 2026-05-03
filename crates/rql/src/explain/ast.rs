// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{Ast, AstAlter, AstCreate, AstFrom, AstJoin},
		identifier::UnqualifiedIdentifier,
		parse::parse,
	},
	bump::{Bump, BumpFragment},
	token::{
		token::{Token, TokenKind},
		tokenize,
	},
};

pub fn explain_ast(query: &str) -> Result<String> {
	let bump = Bump::new();
	let token = tokenize(&bump, query)?;
	let statements = parse(&bump, query, token.into_iter().collect())?;

	let mut result = String::new();
	for statement in &statements {
		for ast in &statement.nodes {
			let mut output = String::new();
			render_ast_tree_inner(ast, "", true, &mut output);
			result += output.as_str();
		}
	}
	Ok(result)
}

fn render_ast_tree_inner(ast: &Ast<'_>, prefix: &str, is_last: bool, output: &mut String) {
	let token = ast.token();
	let fragment = &token.fragment;
	let ty = match ast {
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
	};

	let branch = if is_last {
		"└──"
	} else {
		"├──"
	};

	let description = match ast {
		Ast::Inline(r) => {
			let field_names: Vec<&str> = r.keyed_values.iter().map(|f| f.key.text()).collect();
			format!("{} ({} fields: {})", ty, r.keyed_values.len(), field_names.join(", "))
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
			_ => ty.to_string(),
		},
		_ => ty.to_string(),
	};

	output.push_str(&format!(
		"{}{} {} @ line {}, column {} — \"{}\"\n",
		prefix,
		branch,
		description,
		fragment.line().0,
		fragment.column().0,
		fragment.text()
	));

	let child_prefix = format!(
		"{}{}",
		prefix,
		if is_last {
			"    "
		} else {
			"│   "
		}
	);

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
					owned_children.push(Ast::Identifier(UnqualifiedIdentifier::new(index_token)));
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
				owned_children.push(Ast::Identifier(UnqualifiedIdentifier::new(variable_token)));
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
				output.push_str(&format!("{}├── Aggregate Map\n", child_prefix));
				let map_prefix = format!("{}│   ", child_prefix);
				for (i, child) in a.map.iter().enumerate() {
					let last = i == a.map.len() - 1;
					render_ast_tree_inner(child, &map_prefix, last, output);
				}
			}
			if !a.by.is_empty() {
				output.push_str(&format!("{}└── Aggregate By\n", child_prefix));
				let by_prefix = format!("{}    ", child_prefix);
				for (i, child) in a.by.iter().enumerate() {
					let last = i == a.by.len() - 1;
					render_ast_tree_inner(child, &by_prefix, last, output);
				}
			} else if a.map.is_empty() {
				output.push_str(&format!("{}└── Aggregate By\n", child_prefix));
			}

			return;
		}
		Ast::Insert(_) => {
			unimplemented!()
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
		Ast::Alter(alter) => {
			match alter {
				AstAlter::Sequence(_) => {}
				AstAlter::Policy(_) => {}
				AstAlter::Table(_) => {}
				AstAlter::RemoteNamespace(_) => {}
			}

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
		for idx in 0..total {
			let last = idx == total - 1;

			if idx % 2 == 0 && oi < owned_children.len() {
				render_ast_tree_inner(&owned_children[oi], &child_prefix, last, output);
				oi += 1;
			} else if ri < ref_children.len() {
				render_ast_tree_inner(ref_children[ri], &child_prefix, last, output);
				ri += 1;
			} else if oi < owned_children.len() {
				render_ast_tree_inner(&owned_children[oi], &child_prefix, last, output);
				oi += 1;
			}
		}
	} else if !owned_children.is_empty() {
		for (i, child) in owned_children.iter().enumerate() {
			let last = i == owned_children.len() - 1;
			render_ast_tree_inner(child, &child_prefix, last, output);
		}
	} else {
		for (i, child) in ref_children.iter().enumerate() {
			let last = i == ref_children.len() - 1;
			render_ast_tree_inner(child, &child_prefix, last, output);
		}
	}
}
