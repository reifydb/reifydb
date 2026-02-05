// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::operation::sort_missing_braces;
use reifydb_type::return_error;

use crate::{
	ast::{ast::AstSort, parse::Parser},
	token::{
		keyword::Keyword,
		operator::Operator::{CloseCurly, Colon, OpenCurly},
		separator::Separator::Comma,
	},
};

impl Parser {
	pub(crate) fn parse_sort(&mut self) -> crate::Result<AstSort> {
		let token = self.consume_keyword(Keyword::Sort)?;

		// Always require opening curly brace
		if self.is_eof() || !self.current()?.is_operator(OpenCurly) {
			return_error!(sort_missing_braces(token.fragment));
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
						let token = self.current()?.clone();
						self.advance()?;
						directions.push(Some(token.fragment));
					} else {
						directions.push(None);
					}
				}
				// Space-based syntax: {column ASC}
				else if self.current()?.is_keyword(Keyword::Asc)
					|| self.current()?.is_keyword(Keyword::Desc)
				{
					let token = self.current()?.clone();
					self.advance()?;
					directions.push(Some(token.fragment));
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
	use crate::token::tokenize;

	#[test]
	fn test_single_column() {
		let tokens = tokenize("SORT {name}").unwrap();
		let mut parser = Parser::new(tokens);
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
		let tokens = tokenize("SORT {value ASC}").unwrap();
		let mut parser = Parser::new(tokens);
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
		let tokens = tokenize("SORT {name ASC}").unwrap();
		let mut parser = Parser::new(tokens);
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
		let tokens = tokenize("SORT {name DESC}").unwrap();
		let mut parser = Parser::new(tokens);
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
		let tokens = tokenize("SORT {name, age}").unwrap();
		let mut parser = Parser::new(tokens);
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
		let tokens = tokenize("SORT {name ASC, age DESC}").unwrap();
		let mut parser = Parser::new(tokens);
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
		let tokens = tokenize("SORT {}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 0);
		assert_eq!(sort.directions.len(), 0);
	}

	#[test]
	fn test_without_braces_fails() {
		let tokens = tokenize("SORT name").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse();

		assert!(result.is_err(), "Expected error for SORT without braces");
		let err = result.unwrap_err();
		assert_eq!(err.code, "SORT_001");
	}

	#[test]
	fn test_colon_syntax_asc() {
		let tokens = tokenize("SORT {name: asc}").unwrap();
		let mut parser = Parser::new(tokens);
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
		let tokens = tokenize("SORT {name: desc}").unwrap();
		let mut parser = Parser::new(tokens);
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
		let tokens = tokenize("SORT {name: asc, age: desc}").unwrap();
		let mut parser = Parser::new(tokens);
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
		let tokens = tokenize("SORT {name: asc, age}").unwrap();
		let mut parser = Parser::new(tokens);
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
