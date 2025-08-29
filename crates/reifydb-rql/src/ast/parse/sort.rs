// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	result::error::diagnostic::ast::multiple_expressions_without_braces,
	return_error,
};

use crate::ast::{
	AstIdentifier, AstSort,
	parse::Parser,
	tokenize::{
		Keyword,
		Operator::{CloseCurly, OpenCurly},
		Separator::Comma,
	},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_sort(&mut self) -> crate::Result<AstSort<'a>> {
		let token = self.consume_keyword(Keyword::Sort)?;

		let has_braces = self.current()?.is_operator(OpenCurly);

		if has_braces {
			self.advance()?;
		}

		let mut columns = Vec::new();
		let mut directions = Vec::new();

		loop {
			columns.push(self.parse_as_identifier()?);

			if !self.is_eof()
				&& !self.current()?.is_separator(Comma)
				&& (!has_braces
					|| !self.current()?
						.is_operator(CloseCurly))
			{
				if self.current()?.is_keyword(Keyword::Asc)
					|| self.current()?
						.is_keyword(Keyword::Desc)
				{
					let token = self.current()?.clone();
					self.advance()?;
					directions.push(Some(AstIdentifier(
						token,
					)));
				} else {
					directions.push(None);
				}
			} else {
				directions.push(None);
			}

			if self.is_eof() {
				break;
			}

			// If we have braces, look for closing brace
			if has_braces && self.current()?.is_operator(CloseCurly)
			{
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

		if columns.len() > 1 && !has_braces {
			return_error!(multiple_expressions_without_braces(
				token.fragment
			));
		}

		Ok(AstSort {
			token,
			columns,
			directions,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ast::tokenize::tokenize;

	#[test]
	fn test_single_column() {
		let tokens = tokenize("SORT name").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.directions.len(), 1);

		assert_eq!(sort.columns[0].value(), "name");
		assert_eq!(sort.directions[0].as_ref(), None);
	}

	#[test]
	fn test_keyword() {
		let tokens = tokenize("SORT value ASC").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.directions.len(), 1);

		assert_eq!(sort.columns[0].value(), "value");
		assert_eq!(sort.directions[0].as_ref().unwrap().value(), "ASC");
	}

	#[test]
	fn test_single_column_asc() {
		let tokens = tokenize("SORT name ASC").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.directions.len(), 1);

		assert_eq!(sort.columns[0].value(), "name");
		assert_eq!(sort.directions[0].as_ref().unwrap().value(), "ASC");
	}

	#[test]
	fn test_single_column_desc() {
		let tokens = tokenize("SORT name DESC").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.directions.len(), 1);

		assert_eq!(sort.columns[0].value(), "name");
		assert_eq!(
			sort.directions[0].as_ref().unwrap().value(),
			"DESC"
		);
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

		assert_eq!(sort.columns[0].value(), "name");
		assert_eq!(sort.directions[0].as_ref(), None);

		assert_eq!(sort.columns[1].value(), "age");
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

		assert_eq!(sort.columns[0].value(), "name");
		assert_eq!(sort.directions[0].as_ref().unwrap().value(), "ASC");

		assert_eq!(sort.columns[1].value(), "age");
		assert_eq!(
			sort.directions[1].as_ref().unwrap().value(),
			"DESC"
		);
	}

	#[test]
	fn test_single_column_with_braces() {
		let tokens = tokenize("SORT {name}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let sort = result.first_unchecked().as_sort();
		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.directions.len(), 1);

		assert_eq!(sort.columns[0].value(), "name");
		assert_eq!(sort.directions[0].as_ref(), None);
	}

	#[test]
	fn test_multiple_columns_without_braces_fails() {
		let tokens = tokenize("SORT name, age").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse();

		assert!(
			result.is_err(),
			"Expected error for multiple columns without braces"
		);
	}
}
