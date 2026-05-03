// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::{AstErrorKind, Error, TypeError};

use crate::{
	Result,
	ast::{
		ast::{
			AstAlter, AstAlterRemoteNamespace, AstAlterSequence, AstAlterTable, AstAlterTableAction,
			AstLiteral, AstLiteralNumber, AstPolicyTargetType,
		},
		identifier::{
			MaybeQualifiedNamespaceIdentifier, MaybeQualifiedSequenceIdentifier,
			MaybeQualifiedTableIdentifier,
		},
		parse::Parser,
	},
	token::{
		keyword::Keyword,
		operator::Operator,
		separator::Separator,
		token::{Literal, Token, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_alter(&mut self) -> Result<AstAlter<'bump>> {
		let token = self.consume_keyword(Keyword::Alter)?;

		if self.current()?.is_keyword(Keyword::Sequence) {
			self.consume_keyword(Keyword::Sequence)?;
			return self.parse_alter_sequence(token);
		}

		if self.current()?.is_keyword(Keyword::Table) {
			self.consume_keyword(Keyword::Table)?;
			if self.current()?.is_keyword(Keyword::Policy) {
				self.consume_keyword(Keyword::Policy)?;
				return self.parse_alter_policy(token, AstPolicyTargetType::Table);
			}
			return self.parse_alter_table(token);
		}

		if self.current()?.is_keyword(Keyword::View) {
			self.consume_keyword(Keyword::View)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::View);
		}

		if self.current()?.is_keyword(Keyword::Ringbuffer) {
			self.consume_keyword(Keyword::Ringbuffer)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::RingBuffer);
		}

		if self.current()?.is_keyword(Keyword::Remote) {
			self.consume_keyword(Keyword::Remote)?;
			self.consume_keyword(Keyword::Namespace)?;
			return self.parse_alter_remote_namespace(token);
		}

		if self.current()?.is_keyword(Keyword::Namespace) {
			self.consume_keyword(Keyword::Namespace)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::Namespace);
		}

		if self.current()?.is_keyword(Keyword::Procedure) {
			self.consume_keyword(Keyword::Procedure)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::Procedure);
		}

		if self.current()?.is_keyword(Keyword::Function) {
			self.consume_keyword(Keyword::Function)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::Function);
		}

		if self.current()?.is_keyword(Keyword::Session) {
			self.consume_keyword(Keyword::Session)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::Session);
		}

		if self.current()?.is_keyword(Keyword::Series) {
			self.consume_keyword(Keyword::Series)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::Series);
		}

		if self.current()?.is_keyword(Keyword::Dictionary) {
			self.consume_keyword(Keyword::Dictionary)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::Dictionary);
		}

		if self.current()?.is_keyword(Keyword::Subscription) {
			self.consume_keyword(Keyword::Subscription)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::Subscription);
		}

		if self.current()?.is_keyword(Keyword::Feature) {
			self.consume_keyword(Keyword::Feature)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::Feature);
		}

		let fragment = self.current()?.fragment.to_owned();
		Err(Error::from(TypeError::Ast {
			kind: AstErrorKind::UnexpectedToken {
				expected: "SEQUENCE, FLOW, TABLE, or a policy target type after ALTER".to_string(),
			},
			message: format!("Unexpected token after ALTER: {}", fragment.text()),
			fragment,
		}))
	}

	fn parse_alter_sequence(&mut self, token: Token<'bump>) -> Result<AstAlter<'bump>> {
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		if segments.len() < 2 {
			let fragment = token.fragment.to_owned();
			return Err(Error::from(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "table.column or namespace.table.column after ALTER SEQUENCE"
						.to_string(),
				},
				message: "ALTER SEQUENCE requires at least table.column".to_string(),
				fragment,
			}));
		}

		let column_token = segments.pop().unwrap();
		let table_token = segments.pop().unwrap();

		let sequence = if segments.is_empty() {
			MaybeQualifiedSequenceIdentifier::new(table_token.into_fragment())
		} else {
			let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
			MaybeQualifiedSequenceIdentifier::new(table_token.into_fragment()).with_namespace(namespace)
		};

		self.consume_keyword(Keyword::Set)?;
		self.consume_keyword(Keyword::Value)?;
		let value_token = self.consume(TokenKind::Literal(Literal::Number))?;

		let column = column_token.into_fragment();
		let value = AstLiteral::Number(AstLiteralNumber(value_token));

		Ok(AstAlter::Sequence(AstAlterSequence {
			token,
			sequence,
			column,
			value,
		}))
	}

	fn parse_alter_table(&mut self, token: Token<'bump>) -> Result<AstAlter<'bump>> {
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let table = MaybeQualifiedTableIdentifier::new(name).with_namespace(namespace);

		let action = if self.current()?.is_keyword(Keyword::Add) {
			self.consume_keyword(Keyword::Add)?;
			self.consume_keyword(Keyword::Column)?;
			let column = self.parse_column()?;
			AstAlterTableAction::AddColumn {
				column,
			}
		} else if self.current()?.is_keyword(Keyword::Drop) {
			self.consume_keyword(Keyword::Drop)?;
			self.consume_keyword(Keyword::Column)?;
			let col_name = self.consume(TokenKind::Identifier)?;
			AstAlterTableAction::DropColumn {
				column: col_name.fragment,
			}
		} else if self.current()?.is_keyword(Keyword::Rename) {
			self.consume_keyword(Keyword::Rename)?;
			self.consume_keyword(Keyword::Column)?;
			let old_name = self.consume(TokenKind::Identifier)?;
			self.consume_keyword(Keyword::To)?;
			let new_name = self.consume(TokenKind::Identifier)?;
			AstAlterTableAction::RenameColumn {
				old_name: old_name.fragment,
				new_name: new_name.fragment,
			}
		} else {
			let fragment = self.current()?.fragment.to_owned();
			return Err(Error::from(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "ADD, DROP, or RENAME".to_string(),
				},
				message: format!(
					"Unexpected token: expected {}, got {}",
					"ADD COLUMN, DROP COLUMN, or RENAME COLUMN",
					fragment.text()
				),
				fragment,
			}));
		};

		Ok(AstAlter::Table(AstAlterTable {
			token,
			table,
			action,
		}))
	}

	fn parse_alter_remote_namespace(&mut self, token: Token<'bump>) -> Result<AstAlter<'bump>> {
		let segments = self.parse_double_colon_separated_identifiers()?;

		let namespace = MaybeQualifiedNamespaceIdentifier::new(
			segments.into_iter().map(|s| s.into_fragment()).collect(),
		);

		self.consume_keyword(Keyword::With)?;
		self.consume_operator(Operator::OpenCurly)?;

		let mut grpc = None;

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let key = self.consume(TokenKind::Identifier)?;
			self.consume_operator(Operator::Colon)?;

			match key.fragment.text() {
				"grpc" => {
					let value = self.consume_literal(Literal::Text)?;
					grpc = Some(value.fragment);
				}
				_other => {
					let fragment = key.fragment.to_owned();
					return Err(Error::from(TypeError::Ast {
						kind: AstErrorKind::UnexpectedToken {
							expected: "'grpc'".to_string(),
						},
						message: format!(
							"Unexpected token: expected {}, got {}",
							"'grpc'",
							fragment.text()
						),
						fragment,
					}));
				}
			}

			self.skip_new_line()?;

			if self.consume_if(TokenKind::Separator(Separator::Comma))?.is_some() {
				continue;
			}

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;

		let grpc = grpc.ok_or_else(|| {
			Error::from(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "'grpc' key in WITH block".to_string(),
				},
				message: "ALTER REMOTE NAMESPACE requires 'grpc' in WITH block".to_string(),
				fragment: token.fragment.to_owned(),
			})
		})?;

		Ok(AstAlter::RemoteNamespace(AstAlterRemoteNamespace {
			token,
			namespace,
			grpc,
		}))
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{AstAlter, AstAlterSequence, AstLiteral},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_alter_sequence_with_shape() {
		let bump = Bump::new();
		let source = "ALTER SEQUENCE test::users::id SET VALUE 1000";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Sequence(AstAlterSequence {
				sequence,
				column,
				value,
				..
			}) => {
				assert!(!sequence.namespace.is_empty());
				assert_eq!(sequence.namespace[0].text(), "test");
				assert_eq!(sequence.name.text(), "users");
				assert_eq!(column.text(), "id");
				match value {
					AstLiteral::Number(num) => {
						assert_eq!(num.value(), "1000")
					}
					_ => panic!("Expected number literal"),
				}
			}
			_ => panic!("Expected AstAlter::Sequence"),
		}
	}

	#[test]
	fn test_alter_sequence_without_shape() {
		let bump = Bump::new();
		let source = "ALTER SEQUENCE users::id SET VALUE 500";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Sequence(AstAlterSequence {
				sequence,
				column,
				value,
				..
			}) => {
				assert!(sequence.namespace.is_empty());
				assert_eq!(sequence.name.text(), "users");
				assert_eq!(column.text(), "id");
				match value {
					AstLiteral::Number(num) => {
						assert_eq!(num.value(), "500")
					}
					_ => panic!("Expected number literal"),
				}
			}
			_ => panic!("Expected AstAlter::Sequence"),
		}
	}
}
