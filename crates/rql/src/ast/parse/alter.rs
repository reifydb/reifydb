// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::{AstErrorKind, Error, TypeError};

use crate::{
	Result,
	ast::{
		ast::{
			AstAlter, AstAlterSequence, AstAlterTable, AstAlterTableAction, AstLiteral, AstLiteralNumber,
			AstPolicyTargetType,
		},
		identifier::{MaybeQualifiedSequenceIdentifier, MaybeQualifiedTableIdentifier},
		parse::Parser,
	},
	token::{
		keyword::Keyword,
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

		unimplemented!("Only ALTER SEQUENCE, ALTER FLOW, ALTER TABLE, and ALTER <TYPE> POLICY are supported");
	}

	fn parse_alter_sequence(&mut self, token: Token<'bump>) -> Result<AstAlter<'bump>> {
		// Parse [namespace...].table.column (at least 2 segments required)
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		if segments.len() < 2 {
			unimplemented!("ALTER SEQUENCE requires table.column or namespace.table.column");
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
	fn test_alter_sequence_with_schema() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "ALTER SEQUENCE test::users::id SET VALUE 1000").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
	fn test_alter_sequence_without_schema() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "ALTER SEQUENCE users::id SET VALUE 500").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
