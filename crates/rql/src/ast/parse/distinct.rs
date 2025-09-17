// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{diagnostic::operation::distinct_multiple_columns_without_braces, return_error};

use crate::ast::{
	AstDistinct, TokenKind,
	identifier::MaybeQualifiedColumnIdentifier,
	parse::Parser,
	tokenize::{Keyword, Operator, Separator},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_distinct(&mut self) -> crate::Result<AstDistinct<'a>> {
		let token = self.consume_keyword(Keyword::Distinct)?;

		let (columns, has_braces) = self.parse_identifiers()?;

		// Validate multiple columns require braces
		if columns.len() > 1 && !has_braces {
			return_error!(distinct_multiple_columns_without_braces(token.fragment));
		}

		Ok(AstDistinct {
			token,
			columns,
		})
	}

	/// Parse a comma-separated list of column identifiers with optional
	/// braces Returns (identifiers, had_braces) tuple
	fn parse_identifiers(&mut self) -> crate::Result<(Vec<MaybeQualifiedColumnIdentifier<'a>>, bool)> {
		if self.is_eof() {
			return Ok((vec![], false));
		}

		let has_braces = self.current()?.is_operator(Operator::OpenCurly);
		if has_braces {
			self.advance()?; // consume opening brace
		}

		let mut identifiers = Vec::new();

		// Check if empty list or next statement keyword
		if self.should_stop_identifier_parsing(has_braces)? {
			if has_braces && !self.is_eof() && self.current()?.is_operator(Operator::CloseCurly) {
				self.advance()?; // consume closing brace
			}
			return Ok((identifiers, has_braces));
		}

		// Parse column identifiers
		loop {
			identifiers.push(self.parse_column_identifier_or_keyword()?);

			if self.is_eof() {
				break;
			}

			// Check for closing brace if we have braces
			if has_braces && self.current()?.is_operator(Operator::CloseCurly) {
				self.advance()?; // consume closing brace
				break;
			}

			// Check for comma continuation
			if self.current()?.is_separator(Separator::Comma) {
				self.advance()?;
			} else {
				break;
			}
		}

		Ok((identifiers, has_braces))
	}

	/// Check if we should stop parsing identifiers based on next token
	fn should_stop_identifier_parsing(&mut self, has_braces: bool) -> crate::Result<bool> {
		if self.is_eof() {
			return Ok(true);
		}

		let current = self.current()?;

		// If we have braces, only stop on closing brace
		if has_braces {
			return Ok(current.is_operator(Operator::CloseCurly));
		}

		Ok(matches!(current.kind, TokenKind::Keyword(_)))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ast::tokenize::tokenize;

	#[test]
	fn test_distinct_no_args() {
		let tokens = tokenize("DISTINCT").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		if let crate::ast::Ast::Distinct(distinct) = result.first_unchecked() {
			assert_eq!(distinct.columns.len(), 0);
		} else {
			panic!("Expected Distinct node");
		}
	}

	#[test]
	fn test_distinct_single_column() {
		let tokens = tokenize("DISTINCT name").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		if let crate::ast::Ast::Distinct(distinct) = result.first_unchecked() {
			assert_eq!(distinct.columns.len(), 1);
			assert_eq!(distinct.columns[0].name.text(), "name");
		} else {
			panic!("Expected Distinct node");
		}
	}

	#[test]
	fn test_distinct_multiple_columns() {
		let tokens = tokenize("DISTINCT {name, age}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		if let crate::ast::Ast::Distinct(distinct) = result.first_unchecked() {
			assert_eq!(distinct.columns.len(), 2);
			assert_eq!(distinct.columns[0].name.text(), "name");
			assert_eq!(distinct.columns[1].name.text(), "age");
		} else {
			panic!("Expected Distinct node");
		}
	}

	#[test]
	fn test_distinct_multiple_columns_without_braces_fails() {
		let tokens = tokenize("DISTINCT name, age").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse();

		assert!(result.is_err(), "Expected error for multiple columns without braces");
	}
}
