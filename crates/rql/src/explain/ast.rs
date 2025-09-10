// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	Ast, AstAlter, AstAlterTableOperation, AstAlterViewOperation, AstFrom,
	AstIdentifier, AstJoin,
	parse::parse,
	tokenize::{Token, TokenKind, tokenize},
};

pub fn explain_ast(query: &str) -> crate::Result<String> {
	let token = tokenize(query)?;
	let statements = parse(token)?;

	let mut result = String::new();
	for statement in statements {
		for ast in statement {
			let mut output = String::new();
			render_ast_tree_inner(ast, "", true, &mut output);
			result += output.as_str();
		}
	}
	Ok(result)
}

fn render_ast_tree_inner(
	ast: Ast,
	prefix: &str,
	is_last: bool,
	output: &mut String,
) {
	let token = ast.token();
	let fragment = &token.fragment;
	let ty = match ast {
		Ast::Aggregate(_) => "Aggregate",
		Ast::Between(_) => "Between",
		Ast::CallFunction(_) => "CallFunction",
		Ast::Inline(_) => "Row",
		Ast::Cast(_) => "Cast",
		Ast::Create(_) => "Create",
		Ast::Alter(_) => "Alter",
		Ast::Describe(_) => "Describe",
		Ast::Filter(_) => "Filter",
		Ast::From(_) => "From",
		Ast::Identifier(_) => "Identifier",
		Ast::Infix(_) => "Infix",
		Ast::AstDelete(_) => "Delete",
		Ast::AstInsert(_) => "Insert",
		Ast::AstUpdate(_) => "Update",
		Ast::Join(_) => "Join",
		Ast::List(_) => "List",
		Ast::Literal(_) => "Literal",
		Ast::Nop => "Nop",
		Ast::Sort(_) => "Sort",
		Ast::Policy(_) => "Policy",
		Ast::PolicyBlock(_) => "PolicyBlock",
		Ast::Prefix(_) => "Prefix",
		Ast::Map(_) => "Map",
		Ast::Extend(_) => "Extend",
		Ast::Take(_) => "Take",
		Ast::Tuple(_) => "Tuple",
		Ast::Wildcard(_) => "Wildcard",
		Ast::ParameterRef(_) => "ParameterRef",
		Ast::Distinct(_) => "Distinct",
	};

	let branch = if is_last {
		"└──"
	} else {
		"├──"
	};

	// Special handling for Row and Alter to show more detail
	let description = match &ast {
		Ast::Inline(r) => {
			let field_names: Vec<&str> = r
				.keyed_values
				.iter()
				.map(|f| f.key.value())
				.collect();
			format!(
				"{} ({} fields: {})",
				ty,
				r.keyed_values.len(),
				field_names.join(", ")
			)
		}
		Ast::Alter(alter) => match alter {
			AstAlter::Table(t) => {
				let schema = t
					.table
					.schema
					.as_ref()
					.map(|s| format!("{}.", s.text()))
					.unwrap_or_default();
				format!(
					"ALTER TABLE {}{}",
					schema,
					t.table.name.text()
				)
			}
			AstAlter::View(v) => {
				let schema = v
					.view
					.schema
					.as_ref()
					.map(|s| format!("{}.", s.text()))
					.unwrap_or_default();
				format!(
					"ALTER VIEW {}{}",
					schema,
					v.view.name.text()
				)
			}
			AstAlter::Sequence(s) => {
				let schema = s
					.sequence
					.schema
					.as_ref()
					.map(|sch| format!("{}.", sch.text()))
					.unwrap_or_default();
				format!(
					"ALTER SEQUENCE {}{}.{}",
					schema,
					s.sequence.name.text(),
					s.column.value()
				)
			}
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
	let mut children: Vec<Ast> = vec![];

	match ast {
		Ast::Tuple(t) => children.extend(t.nodes),
		Ast::Prefix(p) => children.push(*p.node),
		Ast::Cast(c) => children.extend(c.tuple.nodes),
		Ast::Filter(f) => children.push(*f.node),
		Ast::From(from) => match from {
			AstFrom::Source {
				source,
				index_name,
				..
			} => {
				// Create an Identifier AST node for the source
				// name This matches what the test expects
				let source_token = Token {
					kind: TokenKind::Identifier,
					fragment: source.name.clone(),
				};
				children.push(Ast::Identifier(AstIdentifier(
					source_token,
				)));

				// If there's an index directive, add it as a
				// child too
				if let Some(index) = index_name {
					children.push(Ast::Identifier(index));
				}
			}
			AstFrom::Inline {
				list: query,
				..
			} => {
				children.extend(query.nodes);
			}
		},
		Ast::Aggregate(a) => {
			// Show Map and By as labeled branches
			if !a.map.is_empty() {
				// Create a synthetic node for "Aggregate Map"
				// label
				output.push_str(&format!(
					"{}├── Aggregate Map\n",
					child_prefix
				));
				let map_prefix =
					format!("{}│   ", child_prefix);
				for (i, child) in a.map.iter().enumerate() {
					let last = i == a.map.len() - 1;
					render_ast_tree_inner(
						child.clone(),
						&map_prefix,
						last,
						output,
					);
				}
			}
			if !a.by.is_empty() {
				// Create a synthetic node for "Aggregate By"
				// label
				output.push_str(&format!(
					"{}└── Aggregate By\n",
					child_prefix
				));
				let by_prefix = format!("{}    ", child_prefix);
				for (i, child) in a.by.iter().enumerate() {
					let last = i == a.by.len() - 1;
					render_ast_tree_inner(
						child.clone(),
						&by_prefix,
						last,
						output,
					);
				}
			} else if a.map.is_empty() {
				// If both are empty (shouldn't happen), or just
				// By is empty
				output.push_str(&format!(
					"{}└── Aggregate By\n",
					child_prefix
				));
			}
			// Return early since we handled the children
			return;
		}
		Ast::AstInsert(_) => {
			unimplemented!()
		}
		Ast::Join(AstJoin::LeftJoin {
			with,
			on,
			..
		}) => {
			children.push(*with);
			children.extend(on);
		}
		Ast::Map(s) => children.extend(s.nodes),
		Ast::Sort(o) => {
			for col in &o.columns {
				children.push(Ast::Identifier(col.clone()));
			}
		}
		Ast::PolicyBlock(pb) => children.extend(pb
			.policies
			.iter()
			.map(|p| *p.value.clone())
			.collect::<Vec<_>>()),
		Ast::Policy(p) => children.push(*p.value),
		Ast::Inline(r) => {
			// Add each field as a child - they will be displayed as
			// key: value pairs
			for field in &r.keyed_values {
				// Create an infix node to represent "key:
				// value"
				let key_ast =
					Ast::Identifier(field.key.clone());
				let value_ast = *field.value.clone();
				children.push(key_ast);
				children.push(value_ast);
			}
		}
		Ast::Infix(i) => {
			children.push(*i.left);
			children.push(*i.right);
		}
		Ast::Alter(alter) => {
			// Handle ALTER operations as child nodes
			match alter {
				AstAlter::Table(t) => {
					for (i, op) in
						t.operations.iter().enumerate()
					{
						let last = i == t
							.operations
							.len() - 1;
						let op_branch = if last {
							"└──"
						} else {
							"├──"
						};

						match op {
							AstAlterTableOperation::CreatePrimaryKey { name, columns } => {
								// Show the CREATE PRIMARY KEY operation
								let pk_name = name.as_ref()
									.map(|n| format!(" {}", n.value()))
									.unwrap_or_default();
								output.push_str(&format!(
									"{}{}CREATE PRIMARY KEY{}\n",
									child_prefix, op_branch, pk_name
								));

								// Show columns as children of the primary key
								let pk_prefix = format!("{}{}    ", child_prefix, if last { " " } else { "│" });
								for (j, col) in columns.iter().enumerate() {
									let col_last = j == columns.len() - 1;
									let col_branch = if col_last { "└──" } else { "├──" };
									output.push_str(&format!(
										"{}{}Column: {}\n",
										pk_prefix, col_branch, col.column.value()
									));
								}
							}
							AstAlterTableOperation::DropPrimaryKey => {
								output.push_str(&format!(
									"{}{}DROP PRIMARY KEY\n",
									child_prefix, op_branch
								));
							}
						}
					}
				}
				AstAlter::View(v) => {
					for (i, op) in
						v.operations.iter().enumerate()
					{
						let last = i == v
							.operations
							.len() - 1;
						let op_branch = if last {
							"└──"
						} else {
							"├──"
						};

						match op {
							AstAlterViewOperation::CreatePrimaryKey { name, columns } => {
								// Show the CREATE PRIMARY KEY operation
								let pk_name = name.as_ref()
									.map(|n| format!(" {}", n.value()))
									.unwrap_or_default();
								output.push_str(&format!(
									"{}{}CREATE PRIMARY KEY{}\n",
									child_prefix, op_branch, pk_name
								));

								// Show columns as children of the primary key
								let pk_prefix = format!("{}{}    ", child_prefix, if last { " " } else { "│" });
								for (j, col) in columns.iter().enumerate() {
									let col_last = j == columns.len() - 1;
									let col_branch = if col_last { "└──" } else { "├──" };
									output.push_str(&format!(
										"{}{}Column: {}\n",
										pk_prefix, col_branch, col.column.value()
									));
								}
							}
							AstAlterViewOperation::DropPrimaryKey => {
								output.push_str(&format!(
									"{}{}DROP PRIMARY KEY\n",
									child_prefix, op_branch
								));
							}
						}
					}
				}
				AstAlter::Sequence(_) => {
					// Sequence alter doesn't have child
					// operations
				}
			}
			// Return early since we handled the children
			return;
		}
		_ => {}
	}

	for (i, child) in children.iter().enumerate() {
		let last = i == children.len() - 1;
		render_ast_tree_inner(
			child.clone(),
			&child_prefix,
			last,
			output,
		);
	}
}
