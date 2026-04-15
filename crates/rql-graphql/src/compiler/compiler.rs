// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_rql::{
	ast::{
		ast::{
			Ast, AstFilter, AstFrom, AstInfix, AstJoin, AstMap, AstSkip, AstStatement, AstSubQuery,
			AstTake, AstTakeValue, InfixOperator,
		},
		identifier::{UnqualifiedIdentifier, UnresolvedShapeIdentifier},
	},
	token::token::{Literal as RqlLiteral, Token as RqlToken, TokenKind as RqlTokenKind},
};
use thiserror::Error;

use crate::{
	ast::ast::*,
	bump::{Bump, BumpBox, BumpVec},
	token::token::{Token as GqlToken, TokenKind as GqlTokenKind},
};

#[derive(Error, Debug)]
pub enum CompilerError {
	#[error("Unsupported operation: {0}")]
	UnsupportedOperation(String),
	#[error("Top-level selection must be a field")]
	TopLevelSelectionNotField,
}

pub struct Compiler<'bump> {
	bump: &'bump Bump,
}

impl<'bump> Compiler<'bump> {
	pub fn new(bump: &'bump Bump) -> Self {
		Self {
			bump,
		}
	}

	pub fn compile(&self, operation: &AstOperation<'bump>) -> Result<AstStatement<'bump>, CompilerError> {
		let mut nodes = Vec::new();

		let root_field = match operation.selections.first() {
			Some(AstSelection::Field(field)) => field,
			_ => return Err(CompilerError::TopLevelSelectionNotField),
		};

		let source = UnresolvedShapeIdentifier {
			namespace: Vec::new(),
			name: root_field.name,
			alias: root_field.alias,
		};
		nodes.push(Ast::From(AstFrom::Source {
			token: self.to_rql_token(root_field.token),
			source,
			index_name: None,
		}));

		if let Some(selections) = &root_field.selections {
			for selection in selections {
				let AstSelection::Field(field) = selection;
				if field.selections.is_some() {
					let join_token = self.to_rql_token(field.token);
					let sub_source = UnresolvedShapeIdentifier {
						namespace: Vec::new(),
						name: field.name,
						alias: None,
					};
					let sub_query = AstSubQuery {
						token: join_token,
						statement: AstStatement {
							nodes: vec![Ast::From(AstFrom::Source {
								token: join_token,
								source: sub_source,
								index_name: None,
							})],
							has_pipes: false,
							is_output: false,
							rql: "",
						},
					};
					nodes.push(Ast::Join(AstJoin::NaturalJoin {
						token: join_token,
						with: sub_query,
						join_type: None,
						alias: field.alias.unwrap_or(field.name),
						ttl: None,
						snapshot: false,
						rql: "",
					}));
				}
			}
		}

		if let Some(args) = &root_field.arguments {
			for arg in args {
				match arg.name.text() {
					"where" => {
						if let AstValue::Object(fields) = &arg.value {
							for field in fields {
								nodes.push(self.compile_where_field(field)?);
							}
						}
					}
					"first" => {
						if let AstValue::Int(token) = &arg.value {
							let val = token.value().parse().unwrap_or(0);
							nodes.push(Ast::Take(AstTake {
								token: self.to_rql_token(arg.token),
								take: AstTakeValue::Literal(val),
							}));
						}
					}
					"skip" => {
						if let AstValue::Int(token) = &arg.value {
							let val = token.value().parse().unwrap_or(0);
							nodes.push(Ast::Skip(AstSkip {
								token: self.to_rql_token(arg.token),
								skip: AstTakeValue::Literal(val),
							}));
						}
					}
					_ => {}
				}
			}
		}

		if let Some(selections) = &root_field.selections {
			let mut map_nodes = Vec::new();
			self.collect_map_nodes(selections, None, &mut map_nodes);
			nodes.push(Ast::Map(AstMap {
				token: self.to_rql_token(root_field.token),
				nodes: map_nodes,
				rql: "",
			}));
		}

		Ok(AstStatement {
			nodes,
			has_pipes: false,
			is_output: false,
			rql: "",
		})
	}

	fn collect_map_nodes(
		&self,
		selections: &BumpVec<'bump, AstSelection<'bump>>,
		prefix: Option<&str>,
		map_nodes: &mut Vec<Ast<'bump>>,
	) {
		for selection in selections {
			let AstSelection::Field(field) = selection;
			let field_name = field.name.text();
			let alias_name = field.alias.as_ref().map(|a| a.text());

			if let Some(sub_selections) = &field.selections {
				let effective_name = alias_name.unwrap_or(field_name);
				let new_prefix = if let Some(p) = prefix {
					format!("{}_{}", p, effective_name)
				} else {
					effective_name.to_string()
				};
				self.collect_map_nodes(sub_selections, Some(&new_prefix), map_nodes);
			} else {
				let full_name = if let Some(p) = prefix {
					format!("{}_{}", p, field_name)
				} else {
					field_name.to_string()
				};
				let ident = Ast::Identifier(UnqualifiedIdentifier::from_fragment(
					reifydb_rql::bump::BumpFragment::Internal {
						text: self.bump.alloc_str(&full_name),
					},
				));
				if let Some(alias) = alias_name {
					let alias_ident = Ast::Identifier(UnqualifiedIdentifier::from_fragment(
						reifydb_rql::bump::BumpFragment::Internal {
							text: self.bump.alloc_str(alias),
						},
					));
					let op_token = self.to_rql_token(field.token);
					map_nodes.push(Ast::Infix(AstInfix {
						token: op_token,
						left: BumpBox::new_in(ident, self.bump),
						operator: InfixOperator::As(op_token),
						right: BumpBox::new_in(alias_ident, self.bump),
					}));
				} else {
					map_nodes.push(ident);
				}
			}
		}
	}

	fn compile_where_field(&self, field: &AstObjectField<'bump>) -> Result<Ast<'bump>, CompilerError> {
		let name = field.name.text();
		let rql_token = self.to_rql_token(field.token);

		let (column_name, operator) = if let Some(stripped) = name.strip_suffix("_gt") {
			(stripped, InfixOperator::GreaterThan(rql_token))
		} else if let Some(stripped) = name.strip_suffix("_lt") {
			(stripped, InfixOperator::LessThan(rql_token))
		} else if let Some(stripped) = name.strip_suffix("_gte") {
			(stripped, InfixOperator::GreaterThanEqual(rql_token))
		} else if let Some(stripped) = name.strip_suffix("_lte") {
			(stripped, InfixOperator::LessThanEqual(rql_token))
		} else {
			(name, InfixOperator::Equal(rql_token))
		};

		let left = Ast::Identifier(UnqualifiedIdentifier::from_fragment(
			reifydb_rql::bump::BumpFragment::Internal {
				text: self.bump.alloc_str(column_name),
			},
		));

		let right = match &field.value {
			AstValue::Int(t) => Ast::Literal(reifydb_rql::ast::ast::AstLiteral::Number(
				reifydb_rql::ast::ast::AstLiteralNumber(self.to_rql_token(*t)),
			)),
			AstValue::String(t) => Ast::Literal(reifydb_rql::ast::ast::AstLiteral::Text(
				reifydb_rql::ast::ast::AstLiteralText(self.to_rql_token(*t)),
			)),
			AstValue::Boolean(t) => Ast::Literal(reifydb_rql::ast::ast::AstLiteral::Boolean(
				reifydb_rql::ast::ast::AstLiteralBoolean(self.boolean_rql_token(*t)),
			)),
			_ => return Err(CompilerError::UnsupportedOperation("Complex where values".to_string())),
		};

		let infix = AstInfix {
			token: rql_token,
			left: BumpBox::new_in(left, self.bump),
			operator,
			right: BumpBox::new_in(right, self.bump),
		};

		Ok(Ast::Filter(AstFilter {
			token: rql_token,
			node: BumpBox::new_in(Ast::Infix(infix), self.bump),
			rql: "",
		}))
	}

	fn boolean_rql_token(&self, gql_token: GqlToken<'bump>) -> RqlToken<'bump> {
		let kind = if gql_token.value() == "true" {
			RqlTokenKind::Literal(RqlLiteral::True)
		} else {
			RqlTokenKind::Literal(RqlLiteral::False)
		};
		RqlToken {
			kind,
			fragment: gql_token.fragment,
		}
	}

	fn to_rql_token(&self, gql_token: GqlToken<'bump>) -> RqlToken<'bump> {
		let kind = match gql_token.kind {
			GqlTokenKind::Name => RqlTokenKind::Identifier,
			GqlTokenKind::IntLiteral => RqlTokenKind::Literal(RqlLiteral::Number),
			GqlTokenKind::FloatLiteral => RqlTokenKind::Literal(RqlLiteral::Number),
			GqlTokenKind::StringLiteral => RqlTokenKind::Literal(RqlLiteral::Text),
			GqlTokenKind::BooleanLiteral => {
				if gql_token.value() == "true" {
					RqlTokenKind::Literal(RqlLiteral::True)
				} else {
					RqlTokenKind::Literal(RqlLiteral::False)
				}
			}
			_ => RqlTokenKind::Identifier,
		};
		RqlToken {
			kind,
			fragment: gql_token.fragment,
		}
	}
}
