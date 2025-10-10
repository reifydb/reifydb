// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{diagnostic::ast, return_error};

use crate::ast::{
	Ast, AstFrom, AstVariable, AstWildcard,
	parse::Parser,
	tokenize::{
		Keyword,
		Literal::{False, Number, Temporal, Text, True, Undefined},
		Operator,
		Separator::NewLine,
		TokenKind,
	},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_primary(&mut self) -> crate::Result<Ast<'a>> {
		loop {
			if self.is_eof() {
				return Ok(Ast::Nop);
			}

			let is_new_line = self.current()?.is_separator(NewLine);
			if !is_new_line {
				break;
			}
			let _ = self.advance()?;
		}

		let current = self.current()?;
		match &current.kind {
			TokenKind::Operator(operator) => match operator {
				Operator::Plus | Operator::Minus | Operator::Bang | Operator::Not => {
					self.parse_prefix()
				}
				Operator::Asterisk => Ok(Ast::Wildcard(AstWildcard(self.advance()?))),
				Operator::OpenBracket => Ok(Ast::List(self.parse_list()?)),
				Operator::OpenParen => Ok(Ast::Tuple(self.parse_tuple()?)),
				Operator::OpenCurly => Ok(Ast::Inline(self.parse_inline()?)),
				_ => return_error!(ast::unsupported_token_error(self.advance()?.fragment)),
			},
			TokenKind::Keyword(keyword) => {
				// Keywords that can start statements at the top
				// level
				match keyword {
					Keyword::From => Ok(Ast::From(self.parse_from()?)),
					Keyword::Map => Ok(Ast::Map(self.parse_map()?)),
					Keyword::Select => Ok(Ast::Map(self.parse_select()?)),
					Keyword::Extend => Ok(Ast::Extend(self.parse_extend()?)),
					Keyword::Filter => Ok(Ast::Filter(self.parse_filter()?)),
					Keyword::Aggregate => Ok(Ast::Aggregate(self.parse_aggregate()?)),
					Keyword::Cast => Ok(Ast::Cast(self.parse_cast()?)),
					Keyword::Create => Ok(Ast::Create(self.parse_create()?)),
					Keyword::Alter => Ok(Ast::Alter(self.parse_alter()?)),
					Keyword::Delete => Ok(Ast::Delete(self.parse_delete()?)),
					Keyword::Insert => Ok(Ast::Insert(self.parse_insert()?)),
					Keyword::Update => Ok(Ast::Update(self.parse_update()?)),
					Keyword::Inner => Ok(Ast::Join(self.parse_inner_join()?)),
					Keyword::Join => Ok(Ast::Join(self.parse_join()?)),
					Keyword::Left => Ok(Ast::Join(self.parse_left_join()?)),
					Keyword::Natural => Ok(Ast::Join(self.parse_natural_join()?)),
					Keyword::Take => Ok(Ast::Take(self.parse_take()?)),
					Keyword::Sort => Ok(Ast::Sort(self.parse_sort()?)),
					Keyword::Distinct => Ok(Ast::Distinct(self.parse_distinct()?)),
					Keyword::Apply => Ok(Ast::Apply(self.parse_apply()?)),
					Keyword::Call => Ok(Ast::Call(self.parse_call()?)),
					Keyword::If => Ok(Ast::If(self.parse_if()?)),
					Keyword::Let => Ok(Ast::Let(self.parse_let()?)),
					Keyword::Policy => Ok(Ast::PolicyBlock(self.parse_policy_block()?)),
					Keyword::Describe => Ok(Ast::Describe(self.parse_describe()?)),
					Keyword::Window => Ok(Ast::Window(self.parse_window()?)),
					_ => {
						// Try to parse as statement keyword first, if that fails, treat as
						// identifier
						Ok(Ast::Identifier(self.parse_as_identifier()?))
					}
				}
			}
			_ => match current {
				_ if current.is_literal(Number) => Ok(Ast::Literal(self.parse_literal_number()?)),
				_ if current.is_literal(True) => Ok(Ast::Literal(self.parse_literal_true()?)),
				_ if current.is_literal(False) => Ok(Ast::Literal(self.parse_literal_false()?)),
				_ if current.is_literal(Text) => Ok(Ast::Literal(self.parse_literal_text()?)),
				_ if current.is_literal(Temporal) => Ok(Ast::Literal(self.parse_literal_temporal()?)),
				_ if current.is_literal(Undefined) => Ok(Ast::Literal(self.parse_literal_undefined()?)),
				_ if current.is_identifier() => {
					if self.is_function_call_pattern() {
						Ok(Ast::CallFunction(self.parse_function_call()?))
					} else {
						Ok(Ast::Identifier(self.parse_identifier()?))
					}
				}
				_ => {
					if let TokenKind::Variable = current.kind {
						// Check if there's a pipe ahead - if so, treat as frame source
						if self.has_pipe_ahead() {
							let var_token = self.advance()?;
							let from_token = var_token.clone(); // Create FROM token before moving var_token
							let variable = AstVariable {
								token: var_token,
							};
							// Create a FROM AST node to treat variable as frame source
							Ok(Ast::From(AstFrom::Variable {
								token: from_token,
								variable,
							}))
						} else {
							// No pipe ahead, treat as normal variable expression
							let token = self.advance()?;
							Ok(Ast::Variable(AstVariable {
								token,
							}))
						}
					} else {
						return_error!(ast::unsupported_token_error(self.advance()?.fragment))
					}
				}
			},
		}
	}
}
