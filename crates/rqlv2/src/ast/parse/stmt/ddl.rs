// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DDL statement parsing (CREATE, ALTER, DROP).
//!
//! The actual implementations are in the `parse::ddl` module:
//! - `parse::ddl::namespace_create` - CREATE NAMESPACE parsing
//! - `parse::ddl::table_create` - CREATE TABLE parsing
//! - `parse::ddl::alter` - ALTER parsing
//! - `parse::ddl::drop` - DROP parsing

use crate::{
	ast::{
		Statement,
		parse::{
			Parser,
			error::{ParseError, ParseErrorKind},
		},
	},
	token::{keyword::Keyword, operator::Operator},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse CREATE statement.
	///
	/// Dispatches to specific CREATE parsers based on the object type.
	pub(in crate::ast::parse) fn parse_create(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start = self.expect_keyword(Keyword::Create)?;

		// Check for OR REPLACE (for FLOW)
		let or_replace = if self.check_operator(Operator::Or) {
			self.advance();
			self.expect_keyword(Keyword::Replace)?;
			true
		} else {
			false
		};

		// CREATE OR REPLACE is only valid for FLOW
		if or_replace {
			if self.check_keyword(Keyword::Flow) {
				self.advance();
				return self.parse_create_flow(start, true);
			}
			return Err(self.error(ParseErrorKind::Custom(
				"OR REPLACE is only valid for CREATE FLOW".to_string(),
			)));
		}

		// Dispatch based on object type
		if self.check_keyword(Keyword::Namespace) {
			self.advance();
			return self.parse_create_namespace(start);
		}

		if self.check_keyword(Keyword::Table) {
			self.advance();
			return self.parse_create_table(start);
		}

		if self.check_keyword(Keyword::View) {
			self.advance();
			return self.parse_create_view(start);
		}

		if self.check_keyword(Keyword::Deferred) {
			self.advance();
			return self.parse_create_deferred_view(start);
		}

		if self.check_keyword(Keyword::Transactional) {
			self.advance();
			return self.parse_create_transactional_view(start);
		}

		if self.check_keyword(Keyword::Unique) {
			self.advance();
			return self.parse_create_index(start, true);
		}

		if self.check_keyword(Keyword::Index) {
			return self.parse_create_index(start, false);
		}

		if self.check_keyword(Keyword::Dictionary) {
			self.advance();
			return self.parse_create_dictionary(start);
		}

		if self.check_keyword(Keyword::Ringbuffer) {
			self.advance();
			return self.parse_create_ringbuffer(start);
		}

		if self.check_keyword(Keyword::Flow) {
			self.advance();
			return self.parse_create_flow(start, false);
		}

		if self.check_keyword(Keyword::Series) {
			self.advance();
			return self.parse_create_series(start);
		}

		if self.check_keyword(Keyword::Subscription) {
			self.advance();
			return self.parse_create_subscription(start);
		}

		// TODO: Add other CREATE variants
		// - CREATE SEQUENCE

		Err(self.error(ParseErrorKind::NotImplemented("CREATE (unsupported type)")))
	}
}
