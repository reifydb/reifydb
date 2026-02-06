// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::operation::distinct_missing_braces;
use reifydb_type::return_error;

use crate::{
	ast::{ast::AstDistinct, identifier::MaybeQualifiedColumnIdentifier, parse::Parser},
	token::{keyword::Keyword, operator::Operator, separator::Separator},
};

impl Parser {
	pub(crate) fn parse_distinct(&mut self) -> crate::Result<AstDistinct> {
		let token = self.consume_keyword(Keyword::Distinct)?;

		let (columns, has_braces) = self.parse_identifiers()?;

		if !has_braces {
			return_error!(distinct_missing_braces(token.fragment));
		}

		Ok(AstDistinct {
			token,
			columns,
		})
	}

	fn parse_identifiers(&mut self) -> crate::Result<(Vec<MaybeQualifiedColumnIdentifier>, bool)> {
		if self.is_eof() || !self.current()?.is_operator(Operator::OpenCurly) {
			return Ok((vec![], false));
		}

		self.advance()?;

		let mut identifiers = Vec::new();

		if self.current()?.is_operator(Operator::CloseCurly) {
			self.advance()?;
			return Ok((identifiers, true));
		}

		loop {
			identifiers.push(self.parse_column_identifier_or_keyword()?);

			if self.is_eof() {
				break;
			}

			if self.current()?.is_operator(Operator::CloseCurly) {
				self.advance()?;
				break;
			}

			if self.current()?.is_separator(Separator::Comma) {
				self.advance()?;
			} else {
				break;
			}
		}

		Ok((identifiers, true))
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::token::tokenize;

	#[test]
	fn test_distinct_empty_braces() {
		let tokens = tokenize("DISTINCT {}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		if let crate::ast::ast::Ast::Distinct(distinct) = result.first_unchecked() {
			assert_eq!(distinct.columns.len(), 0);
		} else {
			panic!("Expected Distinct operator");
		}
	}

	#[test]
	fn test_distinct_single_column() {
		let tokens = tokenize("DISTINCT {name}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		if let crate::ast::ast::Ast::Distinct(distinct) = result.first_unchecked() {
			assert_eq!(distinct.columns.len(), 1);
			assert_eq!(distinct.columns[0].name.text(), "name");
		} else {
			panic!("Expected Distinct operator");
		}
	}

	#[test]
	fn test_distinct_multiple_columns() {
		let tokens = tokenize("DISTINCT {name, age}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		if let crate::ast::ast::Ast::Distinct(distinct) = result.first_unchecked() {
			assert_eq!(distinct.columns.len(), 2);
			assert_eq!(distinct.columns[0].name.text(), "name");
			assert_eq!(distinct.columns[1].name.text(), "age");
		} else {
			panic!("Expected Distinct operator");
		}
	}

	#[test]
	fn test_distinct_without_braces_fails() {
		let tokens = tokenize("DISTINCT name").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse();

		assert!(result.is_err());
		assert_eq!(result.unwrap_err().code, "DISTINCT_002");
	}
}
