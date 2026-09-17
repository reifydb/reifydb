// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use crate::{
	Result,
	ast::{
		ast::{AstOperatorWith, AstOperatorWithEntry, AstOperatorWithKey, AstOperatorWithValue},
		parse::Parser,
	},
	diagnostic::AstError,
	error::RqlError,
	token::{
		keyword::Keyword,
		operator::Operator,
		separator::Separator,
		token::{Token, TokenKind},
	},
};

#[derive(Clone, Copy, PartialEq)]
enum Level {
	Top,
	Block,
}

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_operator_with(&mut self) -> Result<Option<AstOperatorWith<'bump>>> {
		if self.is_eof() || !self.current()?.is_keyword(Keyword::With) {
			return Ok(None);
		}
		let token = self.advance()?;
		let entries = self.parse_operator_with_block(Level::Top)?;
		if !self.is_eof() && self.current()?.is_keyword(Keyword::With) {
			return Err(AstError::UnexpectedToken {
				expected: "a single WITH block".to_string(),
				fragment: self.current()?.fragment.to_owned(),
			}
			.into());
		}
		Ok(Some(AstOperatorWith {
			token,
			entries,
		}))
	}

	fn parse_operator_with_block(&mut self, level: Level) -> Result<Vec<AstOperatorWithEntry<'bump>>> {
		self.consume_operator(Operator::OpenCurly)?;
		let mut entries = Vec::new();
		let mut seen = HashSet::new();

		loop {
			self.skip_new_line()?;
			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let start = self.current()?.fragment.offset();
			let key = self.parse_operator_with_key()?;
			let text = self.source_since(start);
			if !seen.insert(text) {
				return Err(RqlError::OperatorWithDuplicateKey {
					key: text.to_string(),
					fragment: key.fragment(),
				}
				.into());
			}

			let value = if !self.is_eof() && self.current()?.is_operator(Operator::Colon) {
				self.advance()?;
				Some(self.parse_operator_with_value()?)
			} else if level == Level::Block {
				None
			} else {
				return Err(AstError::UnexpectedToken {
					expected: "`:` and a value".to_string(),
					fragment: self.current()?.fragment.to_owned(),
				}
				.into());
			};

			entries.push(AstOperatorWithEntry {
				key,
				value,
			});

			self.skip_new_line()?;
			if self.consume_if(TokenKind::Separator(Separator::Comma))?.is_some() {
				continue;
			}
			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}
			if self.tokens[self.position - 1].kind == TokenKind::Separator(Separator::NewLine) {
				continue;
			}
			return Err(AstError::UnexpectedToken {
				expected: ", or }".to_string(),
				fragment: self.current()?.fragment.to_owned(),
			}
			.into());
		}

		self.consume_operator(Operator::CloseCurly)?;
		Ok(entries)
	}

	fn parse_operator_with_key(&mut self) -> Result<AstOperatorWithKey<'bump>> {
		let qualified = self.tokens.get(self.position + 1).is_some_and(|next: &Token<'bump>| {
			next.is_operator(Operator::Dot) || next.is_operator(Operator::DoubleColon)
		});
		if qualified {
			return Ok(AstOperatorWithKey::Column(self.parse_column_identifier()?));
		}
		Ok(AstOperatorWithKey::Word(self.consume_identifier()?))
	}

	fn parse_operator_with_value(&mut self) -> Result<AstOperatorWithValue<'bump>> {
		let current = self.current()?;
		if current.is_operator(Operator::OpenCurly) {
			return Ok(AstOperatorWithValue::Block(self.parse_operator_with_block(Level::Block)?));
		}
		match current.kind {
			TokenKind::Literal(_) => Ok(AstOperatorWithValue::Literal(self.advance()?)),
			TokenKind::Identifier | TokenKind::Keyword(_) => {
				Ok(AstOperatorWithValue::Word(self.advance()?))
			}
			_ => Err(AstError::UnexpectedToken {
				expected: "a literal, a word or a { } block".to_string(),
				fragment: current.fragment.to_owned(),
			}
			.into()),
		}
	}
}
