// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{ast::AstDistinct, identifier::MaybeQualifiedColumnIdentifier, parse::Parser},
	token::{keyword::Keyword, operator::Operator, separator::Separator},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_distinct(&mut self) -> crate::Result<AstDistinct<'bump>> {
		let token = self.consume_keyword(Keyword::Distinct)?;

		let (columns, _has_braces) = self.parse_identifiers()?;

		Ok(AstDistinct {
			token,
			columns,
		})
	}

	fn parse_identifiers(&mut self) -> crate::Result<(Vec<MaybeQualifiedColumnIdentifier<'bump>>, bool)> {
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
	use crate::{bump::Bump, token::tokenize};

	#[test]
	fn test_distinct_empty_braces() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DISTINCT {}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DISTINCT {name}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DISTINCT {name, age}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
	fn test_distinct_bare_at_eof() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DISTINCT").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		if let crate::ast::ast::Ast::Distinct(distinct) = result.first_unchecked() {
			assert_eq!(distinct.columns.len(), 0);
		} else {
			panic!("Expected Distinct operator");
		}
	}

	#[test]
	fn test_distinct_bare_followed_by_operator() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DISTINCT FROM users").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse().unwrap();

		// Should parse as one statement with two nodes: DISTINCT (bare, 0 columns) then FROM users
		let statement = &result[0];
		assert!(statement.nodes.len() >= 2);
		if let crate::ast::ast::Ast::Distinct(distinct) = &statement.nodes[0] {
			assert_eq!(distinct.columns.len(), 0);
		} else {
			panic!("Expected Distinct operator");
		}
	}
}
