// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{ast::AstSort, parse::Parser},
	error::{OperationKind, RqlError},
	token::{
		keyword::Keyword,
		operator::Operator::{CloseCurly, Colon, OpenCurly},
		separator::Separator::Comma,
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_sort(&mut self) -> Result<AstSort<'bump>> {
		let token = self.consume_keyword(Keyword::Sort)?;

		// Always require opening curly brace
		if self.is_eof() || !self.current()?.is_operator(OpenCurly) {
			return Err(RqlError::OperatorMissingBraces {
				kind: OperationKind::Sort,
				fragment: token.fragment.to_owned(),
			}
			.into());
		}
		self.advance()?; // consume opening brace

		let mut columns = Vec::new();
		let mut directions = Vec::new();

		// Handle empty braces case
		if !self.is_eof() && self.current()?.is_operator(CloseCurly) {
			self.advance()?; // consume closing brace
			return Ok(AstSort {
				token,
				columns,
				directions,
			});
		}

		loop {
			columns.push(self.parse_column_identifier_or_keyword()?);

			// Check for direction specifier
			if !self.is_eof()
				&& !self.current()?.is_separator(Comma)
				&& !self.current()?.is_operator(CloseCurly)
			{
				// Colon-based syntax: {column: asc}
				if self.current()?.is_operator(Colon) {
					self.advance()?; // consume colon
					if self.current()?.is_keyword(Keyword::Asc)
						|| self.current()?.is_keyword(Keyword::Desc)
					{
						let token = self.current()?;
						self.advance()?;
						directions.push(Some(token.fragment));
					} else {
						directions.push(None);
					}
				} else {
					directions.push(None);
				}
			} else {
				directions.push(None);
			}

			if self.is_eof() {
				break;
			}

			// Look for closing brace
			if self.current()?.is_operator(CloseCurly) {
				self.advance()?; // consume closing brace
				break;
			}

			// consume comma and continue
			if self.current()?.is_separator(Comma) {
				self.advance()?;
			} else {
				break;
			}
		}

		Ok(AstSort {
			token,
			columns,
			directions,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::{bump::Bump, token::tokenize};

	#[test]
	fn test_single_column() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SORT {name}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.directions.len(), 1);

		assert_eq!(sort.columns[0].name.text(), "name");
		assert_eq!(sort.directions[0].as_ref(), None);
	}

	#[test]
	fn test_keyword() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SORT {value: ASC}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.directions.len(), 1);

		assert_eq!(sort.columns[0].name.text(), "value");
		assert_eq!(sort.directions[0].as_ref().unwrap().text(), "ASC");
	}

	#[test]
	fn test_single_column_asc() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SORT {name: ASC}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.directions.len(), 1);

		assert_eq!(sort.columns[0].name.text(), "name");
		assert_eq!(sort.directions[0].as_ref().unwrap().text(), "ASC");
	}

	#[test]
	fn test_single_column_desc() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SORT {name: DESC}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.directions.len(), 1);

		assert_eq!(sort.columns[0].name.text(), "name");
		assert_eq!(sort.directions[0].as_ref().unwrap().text(), "DESC");
	}

	#[test]
	fn test_multiple_columns() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SORT {name, age}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 2);
		assert_eq!(sort.directions.len(), 2);

		assert_eq!(sort.columns[0].name.text(), "name");
		assert_eq!(sort.directions[0].as_ref(), None);

		assert_eq!(sort.columns[1].name.text(), "age");
		assert_eq!(sort.directions[1].as_ref(), None);
	}

	#[test]
	fn test_multiple_columns_asc_desc() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SORT {name: ASC, age: DESC}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 2);
		assert_eq!(sort.directions.len(), 2);

		assert_eq!(sort.columns[0].name.text(), "name");
		assert_eq!(sort.directions[0].as_ref().unwrap().text(), "ASC");

		assert_eq!(sort.columns[1].name.text(), "age");
		assert_eq!(sort.directions[1].as_ref().unwrap().text(), "DESC");
	}

	#[test]
	fn test_empty_braces() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SORT {}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 0);
		assert_eq!(sort.directions.len(), 0);
	}

	#[test]
	fn test_without_braces_fails() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SORT name").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse();

		assert!(result.is_err(), "Expected error for SORT without braces");
		let err = result.unwrap_err();
		assert_eq!(err.code, "SORT_001");
	}

	#[test]
	fn test_space_syntax_rejected() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SORT {name DESC}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse();

		assert!(result.is_err(), "Expected error for space-separated sort direction");
	}

	#[test]
	fn test_colon_syntax_asc() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SORT {name: asc}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.directions.len(), 1);

		assert_eq!(sort.columns[0].name.text(), "name");
		assert_eq!(sort.directions[0].as_ref().unwrap().text(), "asc");
	}

	#[test]
	fn test_colon_syntax_desc() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SORT {name: desc}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.directions.len(), 1);

		assert_eq!(sort.columns[0].name.text(), "name");
		assert_eq!(sort.directions[0].as_ref().unwrap().text(), "desc");
	}

	#[test]
	fn test_colon_syntax_multiple_columns() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SORT {name: asc, age: desc}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 2);
		assert_eq!(sort.directions.len(), 2);

		assert_eq!(sort.columns[0].name.text(), "name");
		assert_eq!(sort.directions[0].as_ref().unwrap().text(), "asc");

		assert_eq!(sort.columns[1].name.text(), "age");
		assert_eq!(sort.directions[1].as_ref().unwrap().text(), "desc");
	}

	#[test]
	fn test_colon_syntax_mixed() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SORT {name: asc, age}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 2);
		assert_eq!(sort.directions.len(), 2);

		assert_eq!(sort.columns[0].name.text(), "name");
		assert_eq!(sort.directions[0].as_ref().unwrap().text(), "asc");

		assert_eq!(sort.columns[1].name.text(), "age");
		assert_eq!(sort.directions[1].as_ref(), None);
	}
}
