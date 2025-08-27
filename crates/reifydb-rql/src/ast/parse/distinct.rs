// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	result::error::diagnostic::operation::distinct_multiple_columns_without_braces,
	return_error,
};

use crate::ast::{
	AstDistinct,
	parse::Parser,
	tokenize::{
		Keyword,
		Operator::{CloseCurly, OpenCurly},
		Separator::Comma,
	},
};

impl Parser {
	pub(crate) fn parse_distinct(&mut self) -> crate::Result<AstDistinct> {
		let token = self.consume_keyword(Keyword::Distinct)?;

		// Check if we have an opening brace
		let has_braces = self.current()?.is_operator(OpenCurly);

		if has_braces {
			self.advance()?; // consume opening brace
		}

		let mut columns = Vec::new();

		// Check if no arguments (uses primary key)
		if self.is_eof()
			|| (!has_braces
				&& (self.current()?.is_keyword(Keyword::From)
					|| self.current()?
						.is_keyword(Keyword::Map) || self
					.current()?
					.is_keyword(Keyword::Select) || self
					.current()?
					.is_keyword(Keyword::Filter) || self
					.current()?
					.is_keyword(Keyword::Aggregate) || self
					.current()?
					.is_keyword(Keyword::Sort) || self
					.current()?
					.is_keyword(Keyword::Take) || self
					.current()?
					.is_keyword(Keyword::Join) || self
					.current()?
					.is_keyword(Keyword::Inner) || self
					.current()?
					.is_keyword(Keyword::Left)))
			|| (has_braces
				&& self.current()?.is_operator(CloseCurly))
		{
			// No arguments - will use primary key
			if has_braces && self.current()?.is_operator(CloseCurly)
			{
				self.advance()?; // consume closing brace
			}
			return Ok(AstDistinct {
				token,
				columns,
			});
		}

		// Parse column arguments
		loop {
			columns.push(self.parse_as_identifier()?);

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
			return_error!(
				distinct_multiple_columns_without_braces(
					token.fragment
				)
			);
		}

		Ok(AstDistinct {
			token,
			columns,
		})
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
		if let crate::ast::Ast::Distinct(distinct) =
			result.first_unchecked()
		{
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
		if let crate::ast::Ast::Distinct(distinct) =
			result.first_unchecked()
		{
			assert_eq!(distinct.columns.len(), 1);
			assert_eq!(distinct.columns[0].value(), "name");
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
		if let crate::ast::Ast::Distinct(distinct) =
			result.first_unchecked()
		{
			assert_eq!(distinct.columns.len(), 2);
			assert_eq!(distinct.columns[0].value(), "name");
			assert_eq!(distinct.columns[1].value(), "age");
		} else {
			panic!("Expected Distinct node");
		}
	}

	#[test]
	fn test_distinct_multiple_columns_without_braces_fails() {
		let tokens = tokenize("DISTINCT name, age").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse();

		assert!(
			result.is_err(),
			"Expected error for multiple columns without braces"
		);
	}
}
