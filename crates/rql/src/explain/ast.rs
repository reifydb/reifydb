// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::ast::{
	ast::{Ast, AstAlter, AstAlterTableOperation, AstAlterViewOperation, AstFrom, AstJoin},
	parse::parse,
	tokenize::{
		token::{Token, TokenKind},
		tokenize,
	},
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

fn render_ast_tree_inner(ast: Ast, prefix: &str, is_last: bool, output: &mut String) {
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
		Ast::Merge(_) => "Merge",
		Ast::List(_) => "List",
		Ast::Literal(_) => "Literal",
		Ast::Nop => "Nop",
		Ast::Sort(_) => "Sort",
		Ast::Policy(_) => "Policy",
		Ast::PolicyBlock(_) => "PolicyBlock",
		Ast::Prefix(_) => "Prefix",
		Ast::Map(_) => "Map",
		Ast::Generator(_) => "Generator",
		Ast::Extend(_) => "Extend",
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
	};

	let branch = if is_last {
		"└──"
	} else {
		"├──"
	};

	// Special handling for Row and Alter to show more detail
	let description = match &ast {
		Ast::Inline(r) => {
			let field_names: Vec<&str> = r.keyed_values.iter().map(|f| f.key.text()).collect();
			format!("{} ({} fields: {})", ty, r.keyed_values.len(), field_names.join(", "))
		}
		Ast::Alter(alter) => match alter {
			AstAlter::Table(t) => {
				let namespace =
					t.table.namespace
						.as_ref()
						.map(|s| format!("{}.", s.text()))
						.unwrap_or_default();
				format!("ALTER TABLE {}{}", namespace, t.table.name.text())
			}
			AstAlter::View(v) => {
				let namespace =
					v.view.namespace.as_ref().map(|s| format!("{}.", s.text())).unwrap_or_default();
				format!("ALTER VIEW {}{}", namespace, v.view.name.text())
			}
			AstAlter::Sequence(s) => {
				let namespace = s
					.sequence
					.namespace
					.as_ref()
					.map(|sch| format!("{}.", sch.text()))
					.unwrap_or_default();
				format!("ALTER SEQUENCE {}{}.{}", namespace, s.sequence.name.text(), s.column.text())
			}
			AstAlter::Flow(f) => {
				let namespace =
					f.flow.namespace.as_ref().map(|s| format!("{}.", s.text())).unwrap_or_default();
				format!("ALTER FLOW {}{}", namespace, f.flow.name.text())
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
		Ast::From(from) => {
			match from {
				AstFrom::Source {
					source,
					index_name,
					..
				} => {
					// Create an Identifier AST operator for the
					// source name This matches what
					// the test expects
					let source_token = Token {
						kind: TokenKind::Identifier,
						fragment: source.name.clone(),
					};
					use crate::ast::identifier::UnqualifiedIdentifier;
					children.push(Ast::Identifier(UnqualifiedIdentifier::new(source_token)));

					// If there's an index directive, add it
					// as a child too
					if let Some(index) = index_name {
						use crate::ast::{
							identifier::UnqualifiedIdentifier,
							tokenize::token::{Token, TokenKind},
						};
						let index_token = Token {
							kind: TokenKind::Identifier,
							fragment: index,
						};
						children.push(Ast::Identifier(UnqualifiedIdentifier::new(index_token)));
					}
				}
				AstFrom::Inline {
					list: query,
					..
				} => {
					children.extend(query.nodes);
				}
				AstFrom::Generator(generator_func) => {
					children.extend(generator_func.nodes.clone());
				}
				AstFrom::Variable {
					variable,
					..
				} => {
					// Create an Identifier AST for the variable
					let variable_token = Token {
						kind: TokenKind::Variable,
						fragment: variable.token.fragment.clone(),
					};
					use crate::ast::identifier::UnqualifiedIdentifier;
					children.push(Ast::Identifier(UnqualifiedIdentifier::new(variable_token)));
				}
				AstFrom::Environment {
					..
				} => {
					// Create an Identifier AST for the environment
					let env_token = Token {
						kind: TokenKind::Variable,
						fragment: reifydb_type::fragment::Fragment::internal("env"),
					};
					use crate::ast::identifier::UnqualifiedIdentifier;
					children.push(Ast::Identifier(UnqualifiedIdentifier::new(env_token)));
				}
			}
		}
		Ast::Aggregate(a) => {
			// Show Map and By as labeled branches
			if !a.map.is_empty() {
				// Create a synthetic operator for "Aggregate Map"
				// label
				output.push_str(&format!("{}├── Aggregate Map\n", child_prefix));
				let map_prefix = format!("{}│   ", child_prefix);
				for (i, child) in a.map.iter().enumerate() {
					let last = i == a.map.len() - 1;
					render_ast_tree_inner(child.clone(), &map_prefix, last, output);
				}
			}
			if !a.by.is_empty() {
				// Create a synthetic operator for "Aggregate By"
				// label
				output.push_str(&format!("{}└── Aggregate By\n", child_prefix));
				let by_prefix = format!("{}    ", child_prefix);
				for (i, child) in a.by.iter().enumerate() {
					let last = i == a.by.len() - 1;
					render_ast_tree_inner(child.clone(), &by_prefix, last, output);
				}
			} else if a.map.is_empty() {
				// If both are empty (shouldn't happen), or just
				// By is empty
				output.push_str(&format!("{}└── Aggregate By\n", child_prefix));
			}
			// Return early since we handled the children
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
			// Add the nodes from the subquery statement
			children.extend(with.statement.nodes.clone());
			// Add expressions from using clause pairs
			for pair in &using_clause.pairs {
				children.push((*pair.first).clone());
				children.push((*pair.second).clone());
			}
		}
		Ast::Map(s) => children.extend(s.nodes),
		Ast::Generator(s) => children.extend(s.nodes),
		Ast::Sort(_o) => {
			// Column identifiers are now complex structures, not
			// simple AST nodes Skip adding them as children for
			// explain purposes
		}
		Ast::PolicyBlock(pb) => {
			children.extend(pb.policies.iter().map(|p| *p.value.clone()).collect::<Vec<_>>())
		}
		Ast::Policy(p) => children.push(*p.value),
		Ast::Inline(r) => {
			// Add each field as a child - they will be displayed as
			// key: value pairs
			for field in &r.keyed_values {
				// Create an infix operator to represent "key:
				// value"
				let key_ast = Ast::Identifier(field.key.clone());
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
					for (i, op) in t.operations.iter().enumerate() {
						let last = i == t.operations.len() - 1;
						let op_branch = if last {
							"└──"
						} else {
							"├──"
						};

						match op {
							AstAlterTableOperation::CreatePrimaryKey {
								name,
								columns,
							} => {
								// Show the CREATE PRIMARY KEY operation
								let pk_name = name
									.as_ref()
									.map(|n| format!(" {}", n.text()))
									.unwrap_or_default();
								output.push_str(&format!(
									"{}{}CREATE PRIMARY KEY{}\n",
									child_prefix, op_branch, pk_name
								));

								// Show columns as children of the primary key
								let pk_prefix = format!(
									"{}{}    ",
									child_prefix,
									if last {
										" "
									} else {
										"│"
									}
								);
								for (j, col) in columns.iter().enumerate() {
									let col_last = j == columns.len() - 1;
									let col_branch = if col_last {
										"└──"
									} else {
										"├──"
									};
									output.push_str(&format!(
										"{}{}Column: {}\n",
										pk_prefix,
										col_branch,
										col.column.name.text()
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
					for (i, op) in v.operations.iter().enumerate() {
						let last = i == v.operations.len() - 1;
						let op_branch = if last {
							"└──"
						} else {
							"├──"
						};

						match op {
							AstAlterViewOperation::CreatePrimaryKey {
								name,
								columns,
							} => {
								// Show the CREATE PRIMARY KEY operation
								let pk_name = name
									.as_ref()
									.map(|n| format!(" {}", n.text()))
									.unwrap_or_default();
								output.push_str(&format!(
									"{}{}CREATE PRIMARY KEY{}\n",
									child_prefix, op_branch, pk_name
								));

								// Show columns as children of the primary key
								let pk_prefix = format!(
									"{}{}    ",
									child_prefix,
									if last {
										" "
									} else {
										"│"
									}
								);
								for (j, col) in columns.iter().enumerate() {
									let col_last = j == columns.len() - 1;
									let col_branch = if col_last {
										"└──"
									} else {
										"├──"
									};
									output.push_str(&format!(
										"{}{}Column: {}\n",
										pk_prefix,
										col_branch,
										col.column.name.text()
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
				AstAlter::Flow(_) => {
					// Flow alter doesn't have child operations to display here
					// The action is part of the flow node itself
				}
			}
			// Return early since we handled the children
			return;
		}
		Ast::SubQuery(sq) => {
			// Add the nodes from the subquery statement as children
			children.extend(sq.statement.nodes.clone());
		}
		_ => {}
	}

	for (i, child) in children.iter().enumerate() {
		let last = i == children.len() - 1;
		render_ast_tree_inner(child.clone(), &child_prefix, last, output);
	}
}
