// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CREATE DICTIONARY statement parsing.
//!
//! RQL syntax:
//! - `CREATE DICTIONARY [IF NOT EXISTS] [namespace.]name FOR <value_type> AS <id_type>`

use crate::{
	ast::{
		Statement,
		parse::{ParseError, ParseErrorKind, Parser},
		stmt::ddl::{CreateDictionary, CreateStmt},
	},
	token::{Keyword, Operator, TokenKind},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse CREATE DICTIONARY statement.
	///
	/// Syntax: `CREATE DICTIONARY [IF NOT EXISTS] [namespace.]name FOR <value_type> AS <id_type>`
	///
	/// # Examples
	///
	/// ```rql
	/// CREATE DICTIONARY token_mints FOR Text AS Uint4
	/// CREATE DICTIONARY analytics.hashes FOR Blob AS Uint8
	/// CREATE DICTIONARY IF NOT EXISTS my_dict FOR Utf8 AS Int4
	/// ```
	pub(in crate::ast::parse) fn parse_create_dictionary(
		&mut self,
		start: crate::token::Span,
	) -> Result<Statement<'bump>, ParseError> {
		// Check for IF NOT EXISTS
		let if_not_exists = self.try_parse_if_not_exists();

		// Parse dictionary name: [namespace.]name
		let (namespace, name) = self.parse_qualified_identifier()?;

		// Parse FOR <value_type>
		self.expect_keyword(Keyword::For)?;
		let value_type_token = self.current();
		if !matches!(value_type_token.kind, TokenKind::Identifier) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}
		let value_type = self.token_text(value_type_token);
		self.advance();

		// Parse AS <id_type>
		self.expect_operator(Operator::As)?;
		let id_type_token = self.current();
		if !matches!(id_type_token.kind, TokenKind::Identifier) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}
		let id_type = self.token_text(id_type_token);
		let end_span = id_type_token.span;
		self.advance();

		let span = start.merge(&end_span);

		Ok(Statement::Create(CreateStmt::Dictionary(CreateDictionary::new(
			namespace,
			name,
			value_type,
			id_type,
			if_not_exists,
			span,
		))))
	}
}

#[cfg(test)]
mod tests {
	use bumpalo::Bump;

	use crate::{ast::Statement, token::tokenize};

	#[test]
	fn test_create_dictionary_simple() {
		let bump = Bump::new();
		let source = "CREATE DICTIONARY token_mints FOR Text AS Uint4";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Dictionary(d)) => {
				assert!(d.namespace.is_none());
				assert_eq!(d.name, "token_mints");
				assert_eq!(d.value_type, "Text");
				assert_eq!(d.id_type, "Uint4");
				assert!(!d.if_not_exists);
			}
			_ => panic!("Expected CREATE DICTIONARY statement"),
		}
	}

	#[test]
	fn test_create_dictionary_qualified() {
		let bump = Bump::new();
		let source = "CREATE DICTIONARY analytics.hashes FOR Blob AS Uint8";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Dictionary(d)) => {
				assert_eq!(d.namespace, Some("analytics"));
				assert_eq!(d.name, "hashes");
				assert_eq!(d.value_type, "Blob");
				assert_eq!(d.id_type, "Uint8");
			}
			_ => panic!("Expected CREATE DICTIONARY statement"),
		}
	}

	#[test]
	fn test_create_dictionary_if_not_exists() {
		let bump = Bump::new();
		let source = "CREATE DICTIONARY IF NOT EXISTS my_dict FOR Utf8 AS Int4";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Dictionary(d)) => {
				assert!(d.if_not_exists);
				assert!(d.namespace.is_none());
				assert_eq!(d.name, "my_dict");
			}
			_ => panic!("Expected CREATE DICTIONARY statement"),
		}
	}

	#[test]
	fn test_create_dictionary_lowercase() {
		let bump = Bump::new();
		let source = "create dictionary test FOR text as int4";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Dictionary(d)) => {
				assert_eq!(d.name, "test");
				assert_eq!(d.value_type, "text");
				assert_eq!(d.id_type, "int4");
			}
			_ => panic!("Expected CREATE DICTIONARY statement"),
		}
	}
}
