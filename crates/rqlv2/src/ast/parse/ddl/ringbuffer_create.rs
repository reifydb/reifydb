// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CREATE RINGBUFFER statement parsing.
//!
//! RQL syntax:
//! - `CREATE RINGBUFFER namespace.name { columns } WITH { capacity: n }`

use crate::{
	ast::{
		Statement,
		parse::{ParseError, ParseErrorKind, Parser},
		stmt::ddl::{CreateRingBuffer, CreateStmt},
	},
	token::{
		keyword::Keyword, literal::LiteralKind, operator::Operator, punctuation::Punctuation, span::Span,
		token::TokenKind,
	},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse CREATE RINGBUFFER statement.
	///
	/// Syntax: `CREATE RINGBUFFER namespace.name { columns } WITH { capacity: n }`
	///
	/// # Examples
	///
	/// ```rql
	/// CREATE RINGBUFFER test.events { id: Int4, data: Utf8 } WITH { capacity: 100 }
	/// ```
	pub(in crate::ast::parse) fn parse_create_ringbuffer(
		&mut self,
		start: Span,
	) -> Result<Statement<'bump>, ParseError> {
		// Parse namespace.name
		let (namespace, name) = self.parse_required_qualified_identifier()?;

		// Parse columns in braces
		let columns = self.parse_column_definitions()?;

		// Parse WITH block
		self.expect_keyword(Keyword::With)?;
		let capacity = self.parse_with_capacity()?;

		let end_span = self.current().span;
		let span = start.merge(&end_span);

		Ok(Statement::Create(CreateStmt::RingBuffer(CreateRingBuffer::new(
			Some(namespace),
			name,
			columns,
			capacity,
			false, // if_not_exists not supported for ringbuffers
			span,
		))))
	}

	/// Parse WITH block for capacity: `WITH { capacity: n }`
	fn parse_with_capacity(&mut self) -> Result<u64, ParseError> {
		self.expect_punct(Punctuation::OpenCurly)?;
		self.skip_newlines();

		// Expect 'capacity:' field
		let field_token = self.current();
		if !matches!(field_token.kind, TokenKind::Identifier) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}
		let field_name = self.token_text(field_token);
		if field_name != "capacity" {
			return Err(self.error(ParseErrorKind::ExpectedKeyword(Keyword::With)));
		}
		self.advance();

		// Expect colon
		self.expect_operator(Operator::Colon)?;

		// Parse capacity value
		let capacity_token = self.current();
		let capacity = match capacity_token.kind {
			TokenKind::Literal(LiteralKind::Integer) => {
				let text = self.token_text(capacity_token);
				text.parse::<u64>().map_err(|_| {
					self.error(ParseErrorKind::Custom("invalid integer".to_string()))
				})?
			}
			_ => return Err(self.error(ParseErrorKind::Custom("expected integer literal".to_string()))),
		};
		self.advance();

		self.skip_newlines();
		self.expect_punct(Punctuation::CloseCurly)?;

		Ok(capacity)
	}
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use crate::{ast::Statement, token::tokenize};

	#[test]
	fn test_create_ringbuffer_basic() {
		let bump = Bump::new();
		let source = "CREATE RINGBUFFER test.events { id: Int4 } WITH { capacity: 100 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::RingBuffer(r)) => {
				assert_eq!(r.namespace, Some("test"));
				assert_eq!(r.name, "events");
				assert_eq!(r.columns.len(), 1);
				assert_eq!(r.columns[0].name, "id");
				assert_eq!(r.columns[0].data_type, "Int4");
				assert_eq!(r.capacity, 100);
			}
			_ => panic!("Expected CREATE RINGBUFFER statement"),
		}
	}

	#[test]
	fn test_create_ringbuffer_multiple_columns() {
		let bump = Bump::new();
		let source = "CREATE RINGBUFFER test.logs { timestamp: Int8, message: Text } WITH { capacity: 1000 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::RingBuffer(r)) => {
				assert_eq!(r.namespace, Some("test"));
				assert_eq!(r.name, "logs");
				assert_eq!(r.columns.len(), 2);
				assert_eq!(r.columns[0].name, "timestamp");
				assert_eq!(r.columns[1].name, "message");
				assert_eq!(r.capacity, 1000);
			}
			_ => panic!("Expected CREATE RINGBUFFER statement"),
		}
	}

	#[test]
	fn test_create_ringbuffer_lowercase() {
		let bump = Bump::new();
		let source = "create ringbuffer myns.buffer { data: blob } with { capacity: 50 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::RingBuffer(r)) => {
				assert_eq!(r.namespace, Some("myns"));
				assert_eq!(r.name, "buffer");
				assert_eq!(r.capacity, 50);
			}
			_ => panic!("Expected CREATE RINGBUFFER statement"),
		}
	}
}
