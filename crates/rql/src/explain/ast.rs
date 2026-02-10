// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::{Ast, AstAlter, AstAlterTableOperation, AstAlterViewOperation, AstFrom, AstJoin},
		parse::parse,
	},
	bump::Bump,
	token::{
		token::{Token, TokenKind},
		tokenize,
	},
};

pub fn explain_ast(query: &str) -> crate::Result<String> {
	let bump = Bump::new();
	let token = tokenize(&bump, query)?;
	let statements = parse(&bump, token.into_iter().collect())?;

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
		Ast::Policy(_) => "Policy",
		Ast::PolicyBlock(_) => "PolicyBlock",
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
		Ast::DefFunction(_) => "DefFunction",
		Ast::Return(_) => "Return",
		Ast::Append(_) => "Append",
		Ast::Assert(_) => "Assert",
	};

	let branch = if is_last {
		"└──"
	} else {
		"├──"
	};

	// Special handling for Row and Alter to show more detail
	let description = match ast {
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

	// We collect references to existing children, and also store
	// synthetic owned nodes whose references we need to pass to recursion.
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
		Ast::From(from) => {
			match from {
				AstFrom::Source {
					source,
					index_name,
					..
				} => {
					// Create an Identifier AST node for the source name
					let source_token = Token {
						kind: TokenKind::Identifier,
						fragment: source.name.clone(),
					};
					use crate::ast::identifier::UnqualifiedIdentifier;
					owned_children.push(Ast::Identifier(UnqualifiedIdentifier::new(source_token)));

					// If there's an index directive, add it as a child too
					if let Some(index) = index_name {
						let index_token = Token {
							kind: TokenKind::Identifier,
							fragment: index.clone(),
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
					// Create an Identifier AST for the variable
					let variable_token = Token {
						kind: TokenKind::Variable,
						fragment: variable.token.fragment.clone(),
					};
					use crate::ast::identifier::UnqualifiedIdentifier;
					owned_children
						.push(Ast::Identifier(UnqualifiedIdentifier::new(variable_token)));
				}
				AstFrom::Environment {
					..
				} => {
					// Create an Identifier AST for the environment
					let env_token = Token {
						kind: TokenKind::Variable,
						fragment: crate::bump::BumpFragment::Internal {
							text: "env",
						},
					};
					use crate::ast::identifier::UnqualifiedIdentifier;
					owned_children.push(Ast::Identifier(UnqualifiedIdentifier::new(env_token)));
				}
			}
		}
		Ast::Aggregate(a) => {
			// Show Map and By as labeled branches
			if !a.map.is_empty() {
				// Create a synthetic label for "Aggregate Map"
				output.push_str(&format!("{}├── Aggregate Map\n", child_prefix));
				let map_prefix = format!("{}│   ", child_prefix);
				for (i, child) in a.map.iter().enumerate() {
					let last = i == a.map.len() - 1;
					render_ast_tree_inner(child, &map_prefix, last, output);
				}
			}
			if !a.by.is_empty() {
				// Create a synthetic label for "Aggregate By"
				output.push_str(&format!("{}└── Aggregate By\n", child_prefix));
				let by_prefix = format!("{}    ", child_prefix);
				for (i, child) in a.by.iter().enumerate() {
					let last = i == a.by.len() - 1;
					render_ast_tree_inner(child, &by_prefix, last, output);
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
			for node in &with.statement.nodes {
				ref_children.push(node);
			}
			// Add expressions from using clause pairs
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
		Ast::Sort(_o) => {
			// Column identifiers are now complex structures, not
			// simple AST nodes Skip adding them as children for
			// explain purposes
		}
		Ast::PolicyBlock(pb) => {
			for p in &pb.policies {
				ref_children.push(&p.value);
			}
		}
		Ast::Policy(p) => ref_children.push(&p.value),
		Ast::Inline(r) => {
			// Add each field as a child - they will be displayed as
			// key: value pairs
			for field in &r.keyed_values {
				// Create an infix operator to represent "key: value"
				owned_children.push(Ast::Identifier(field.key.clone()));
				ref_children.push(&field.value);
			}
		}
		Ast::Infix(i) => {
			ref_children.push(&i.left);
			ref_children.push(&i.right);
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
		Ast::Patch(p) => {
			for node in &p.assignments {
				ref_children.push(node);
			}
		}
		Ast::Assert(a) => ref_children.push(&a.node),
		Ast::SubQuery(sq) => {
			// Add the nodes from the subquery statement as children
			for node in &sq.statement.nodes {
				ref_children.push(node);
			}
		}
		_ => {}
	}

	// Render referenced children first, then owned children.
	// For Inline (Row) nodes, owned and ref children alternate (key, value, key, value...),
	// so we need to interleave them. For simplicity and correctness, we handle
	// the general case: if there are only ref_children or only owned_children, render them.
	// If both exist, the Inline case interleaves them, while From pushes only to owned.
	// The ordering: ref_children are added in order, owned_children are added in order.
	// For Inline: owned has keys, ref has values - they alternate.
	// We handle this by merging: for Inline, the order is key0, val0, key1, val1...
	// which means owned[0], ref[0], owned[1], ref[1], etc.
	// For From: only owned_children are used.
	// For most other cases: only ref_children are used.

	if !owned_children.is_empty() && !ref_children.is_empty() {
		// Interleave: owned and ref alternate (for Inline case)
		let total = owned_children.len() + ref_children.len();
		let mut oi = 0;
		let mut ri = 0;
		for idx in 0..total {
			let last = idx == total - 1;
			// For Inline: pattern is owned, ref, owned, ref...
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
