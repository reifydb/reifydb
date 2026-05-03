// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::{AstErrorKind, Error, TypeError};

use crate::{
	Result,
	ast::{
		ast::{
			Ast, AstCallFunction, AstEnvironment, AstFrom, AstIdentity, AstRequire, AstRownum, AstRunTests,
			AstSystemColumn, AstVariable, AstWildcard,
		},
		identifier::{
			MaybeQualifiedFunctionIdentifier, MaybeQualifiedNamespaceIdentifier,
			MaybeQualifiedTestIdentifier,
		},
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	diagnostic::AstError,
	token::{
		keyword::Keyword,
		operator::Operator,
		separator::Separator::NewLine,
		token::{
			Literal::{False, None, Number, Temporal, Text, True},
			TokenKind,
		},
	},
};

impl<'bump> Parser<'bump> {
	fn try_parse_keyword_as_function_call(&mut self) -> Result<Ast<'bump>> {
		if self.position + 1 < self.tokens.len()
			&& self.tokens[self.position + 1].is_operator(Operator::OpenParen)
		{
			let first_ident_token = self.consume_name()?;
			let open_paren_token = self.advance()?;
			let arguments = self.parse_tuple_call(open_paren_token)?;
			let function = MaybeQualifiedFunctionIdentifier::new(first_ident_token.fragment);
			Ok(Ast::CallFunction(AstCallFunction {
				token: first_ident_token,
				function,
				arguments,
			}))
		} else {
			Ok(Ast::Identifier(self.parse_as_identifier()?))
		}
	}

	pub(crate) fn parse_primary(&mut self) -> Result<Ast<'bump>> {
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
		match current.kind {
			TokenKind::Operator(operator) => match operator {
				Operator::Plus | Operator::Minus | Operator::Bang | Operator::Not => {
					self.parse_prefix()
				}
				Operator::Asterisk => Ok(Ast::Wildcard(AstWildcard(self.advance()?))),
				Operator::OpenBracket => Ok(Ast::List(self.parse_list()?)),
				Operator::OpenParen => {
					if self.is_closure_pattern() {
						Ok(Ast::Closure(self.parse_closure()?))
					} else {
						Ok(Ast::Tuple(self.parse_tuple()?))
					}
				}
				Operator::OpenCurly => Ok(Ast::Inline(self.parse_inline()?)),
				_ => Err(AstError::UnsupportedToken {
					fragment: self.advance()?.fragment.to_owned(),
				}
				.into()),
			},
			TokenKind::Keyword(keyword) => match keyword {
				Keyword::Append => Ok(Ast::Append(self.parse_append()?)),
				Keyword::Assert => Ok(Ast::Assert(self.parse_assert()?)),
				Keyword::Require => {
					let token = self.consume_keyword(Keyword::Require)?;

					let has_braces =
						!self.is_eof() && self.current()?.is_operator(Operator::OpenCurly);
					if has_braces {
						self.advance()?;
					}
					let body = self.parse_node(Precedence::None)?;
					if has_braces {
						self.consume_operator(Operator::CloseCurly)?;
					}
					Ok(Ast::Require(AstRequire {
						token,
						body: BumpBox::new_in(body, self.bump()),
					}))
				}
				Keyword::From => Ok(Ast::From(self.parse_from()?)),
				Keyword::Map => Ok(Ast::Map(self.parse_map()?)),
				Keyword::Extend => Ok(Ast::Extend(self.parse_extend()?)),
				Keyword::Patch => Ok(Ast::Patch(self.parse_patch()?)),
				Keyword::Filter => Ok(Ast::Filter(self.parse_filter()?)),
				Keyword::Gate => Ok(Ast::Gate(self.parse_gate()?)),
				Keyword::Aggregate => Ok(Ast::Aggregate(self.parse_aggregate()?)),
				Keyword::Cast => Ok(Ast::Cast(self.parse_cast()?)),
				Keyword::Create => Ok(Ast::Create(self.parse_create()?)),
				Keyword::Alter => Ok(Ast::Alter(self.parse_alter()?)),
				Keyword::Drop => Ok(Ast::Drop(self.parse_drop()?)),
				Keyword::Delete | Keyword::Insert | Keyword::Update => {
					if self.position + 1 < self.tokens.len()
						&& (matches!(
							self.tokens[self.position + 1].kind,
							TokenKind::Identifier | TokenKind::Keyword(_)
						) || matches!(
							self.tokens[self.position + 1].kind,
							TokenKind::Operator(Operator::OpenBracket)
								| TokenKind::Operator(Operator::OpenCurly)
								| TokenKind::Variable
						)) {
						match keyword {
							Keyword::Delete => Ok(Ast::Delete(self.parse_delete()?)),
							Keyword::Insert => Ok(Ast::Insert(self.parse_insert()?)),
							Keyword::Update => Ok(Ast::Update(self.parse_update()?)),
							_ => unreachable!(),
						}
					} else {
						self.try_parse_keyword_as_function_call()
					}
				}
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
				Keyword::Match => Ok(Ast::Match(self.parse_match()?)),
				Keyword::Dispatch => Ok(Ast::Dispatch(self.parse_dispatch()?)),
				Keyword::Grant => Ok(Ast::Grant(self.parse_grant()?)),
				Keyword::Revoke => Ok(Ast::Revoke(self.parse_revoke()?)),
				Keyword::Migrate => Ok(Ast::Migrate(self.parse_migrate()?)),
				Keyword::Rollback => Ok(Ast::RollbackMigration(self.parse_rollback_migration()?)),
				Keyword::Run => {
					let token = self.advance()?;
					if (self.consume_if(TokenKind::Keyword(Keyword::Tests))?).is_some() {
						if self.is_eof()
							|| self.current()?.is_separator(NewLine) || self
							.current()?
							.is_operator(Operator::Pipe) || matches!(
							self.current()?.kind,
							TokenKind::Separator(_)
						) {
							return Ok(Ast::RunTests(AstRunTests::All {
								token,
							}));
						}

						let segments = self.parse_double_colon_separated_identifiers()?;
						let namespace = MaybeQualifiedNamespaceIdentifier::new(
							segments.into_iter().map(|s| s.into_fragment()).collect(),
						);
						Ok(Ast::RunTests(AstRunTests::Namespace {
							token,
							namespace,
						}))
					} else if (self.consume_if(TokenKind::Keyword(Keyword::Test))?).is_some() {
						let mut segments = self.parse_double_colon_separated_identifiers()?;
						let name = segments.pop().unwrap().into_fragment();
						let namespace: Vec<_> =
							segments.into_iter().map(|s| s.into_fragment()).collect();
						let test_ident = MaybeQualifiedTestIdentifier::new(name)
							.with_namespace(namespace);
						Ok(Ast::RunTests(AstRunTests::Single {
							token,
							test: test_ident,
						}))
					} else {
						let fragment = self.current()?.fragment.to_owned();
						Err(Error::from(TypeError::Ast {
							kind: AstErrorKind::UnexpectedToken {
								expected: "TESTS or TEST after RUN".to_string(),
							},
							message: format!(
								"expected TESTS or TEST after RUN, found `{}`",
								fragment.text()
							),
							fragment,
						}))
					}
				}
				Keyword::Loop => Ok(Ast::Loop(self.parse_loop()?)),
				Keyword::While => Ok(Ast::While(self.parse_while()?)),
				Keyword::For => Ok(Ast::For(self.parse_for()?)),
				Keyword::Break => Ok(Ast::Break(self.parse_break()?)),
				Keyword::Continue => Ok(Ast::Continue(self.parse_continue()?)),
				Keyword::Let => Ok(Ast::Let(self.parse_let()?)),
				Keyword::Describe => Ok(Ast::Describe(self.parse_describe()?)),
				Keyword::Window => Ok(Ast::Window(self.parse_window()?)),
				Keyword::Udf => {
					if self.position + 1 < self.tokens.len()
						&& matches!(
							self.tokens[self.position + 1].kind,
							TokenKind::Identifier | TokenKind::Keyword(_)
						) {
						Ok(Ast::DefFunction(self.parse_def_function()?))
					} else {
						self.try_parse_keyword_as_function_call()
					}
				}
				Keyword::Return => Ok(Ast::Return(self.parse_return()?)),
				Keyword::Rownum => {
					let token = self.advance()?;
					Ok(Ast::Rownum(AstRownum {
						token,
					}))
				}
				_ => self.try_parse_keyword_as_function_call(),
			},
			_ => match current {
				_ if current.is_literal(Number) => Ok(Ast::Literal(self.parse_literal(Number)?)),
				_ if current.is_literal(True) => Ok(Ast::Literal(self.parse_literal(True)?)),
				_ if current.is_literal(False) => Ok(Ast::Literal(self.parse_literal(False)?)),
				_ if current.is_literal(Text) => Ok(Ast::Literal(self.parse_literal(Text)?)),
				_ if current.is_literal(Temporal) => Ok(Ast::Literal(self.parse_literal(Temporal)?)),
				_ if current.is_literal(None) => Ok(Ast::Literal(self.parse_literal(None)?)),
				_ if current.is_identifier() => {
					if self.is_function_call_pattern() {
						Ok(Ast::CallFunction(self.parse_function_call()?))
					} else {
						Ok(Ast::Identifier(self.parse_identifier()?))
					}
				}
				_ if current.is_system_column() => {
					let token = self.advance()?;
					Ok(Ast::SystemColumn(AstSystemColumn {
						token,
					}))
				}
				_ => {
					if let TokenKind::Variable = current.kind {
						let var_token = self.advance()?;
						if var_token.fragment.text() == "$env" {
							return Ok(Ast::Environment(AstEnvironment {
								token: var_token,
							}));
						}
						if var_token.fragment.text() == "$identity" {
							return Ok(Ast::Identity(AstIdentity {
								token: var_token,
							}));
						}

						if self.has_pipe_ahead() {
							let from_token = var_token;
							let variable = AstVariable {
								token: var_token,
							};

							Ok(Ast::From(AstFrom::Variable {
								token: from_token,
								variable,
							}))
						} else {
							Ok(Ast::Variable(AstVariable {
								token: var_token,
							}))
						}
					} else {
						Err(AstError::UnsupportedToken {
							fragment: self.advance()?.fragment.to_owned(),
						}
						.into())
					}
				}
			},
		}
	}
}
