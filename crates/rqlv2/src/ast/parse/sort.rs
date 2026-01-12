// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! SORT expression parsing.

use bumpalo::collections::Vec as BumpVec;

use super::{
	Parser,
	error::{ParseError, ParseErrorKind},
	pratt::Precedence,
};
use crate::{
	ast::{Expr, expr::*},
	token::{Keyword, Operator, Punctuation},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse SORT expression.
	/// Syntax: SORT { key: ASC, key2: DESC } or SORT { key } (defaults to ASC)
	pub(super) fn parse_sort(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume SORT

		// Require opening brace
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut columns = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let expr = self.parse_expr(Precedence::Comparison)?;

			// Check for colon followed by direction, or just use default
			let direction = if self.try_consume_operator(Operator::Colon) {
				if self.try_consume_keyword(Keyword::Asc) {
					Some(SortDirection::Asc)
				} else if self.try_consume_keyword(Keyword::Desc) {
					Some(SortDirection::Desc)
				} else {
					return Err(self.error(ParseErrorKind::ExpectedKeyword(Keyword::Asc)));
				}
			} else {
				None // Default direction
			};

			columns.push(SortColumn::new(expr, direction));

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(self.alloc(Expr::Sort(SortExpr::new(columns.into_bump_slice(), start_span.merge(&end_span)))))
	}
}

#[cfg(test)]
mod tests {
	use bumpalo::Bump;

	use crate::{ast::Expr, ast::expr::SortDirection, token::tokenize};

	fn get_first_expr<'a>(stmt: crate::ast::Statement<'a>) -> &'a Expr<'a> {
		match stmt {
			crate::ast::Statement::Pipeline(p) => {
				assert!(!p.stages.is_empty());
				&p.stages[0]
			}
			crate::ast::Statement::Expression(e) => e.expr,
			_ => panic!("Expected Pipeline or Expression statement"),
		}
	}

	fn extract_sort<'a>(stmt: crate::ast::Statement<'a>) -> &'a crate::ast::expr::SortExpr<'a> {
		let expr = get_first_expr(stmt);
		match expr {
			Expr::Sort(s) => s,
			_ => panic!("Expected SORT expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_sort_single_column() {
		let bump = Bump::new();
		let source = "SORT { name }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let sort = extract_sort(stmt);

		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.columns[0].direction, None);

		match sort.columns[0].expr {
			Expr::Identifier(id) => assert_eq!(id.name, "name"),
			_ => panic!("Expected identifier"),
		}
	}

	#[test]
	fn test_sort_keyword_column() {
		let bump = Bump::new();
		let source = "SORT { value: ASC }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let sort = extract_sort(stmt);

		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.columns[0].direction, Some(SortDirection::Asc));

		match sort.columns[0].expr {
			Expr::Identifier(id) => assert_eq!(id.name, "value"),
			_ => panic!("Expected identifier"),
		}
	}

	#[test]
	fn test_sort_single_column_asc() {
		let bump = Bump::new();
		let source = "SORT { name: ASC }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let sort = extract_sort(stmt);

		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.columns[0].direction, Some(SortDirection::Asc));
	}

	#[test]
	fn test_sort_single_column_desc() {
		let bump = Bump::new();
		let source = "SORT { name: DESC }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let sort = extract_sort(stmt);

		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.columns[0].direction, Some(SortDirection::Desc));
	}

	#[test]
	fn test_sort_multiple_columns() {
		let bump = Bump::new();
		let source = "SORT { name, age }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let sort = extract_sort(stmt);

		assert_eq!(sort.columns.len(), 2);

		match sort.columns[0].expr {
			Expr::Identifier(id) => assert_eq!(id.name, "name"),
			_ => panic!("Expected identifier"),
		}

		match sort.columns[1].expr {
			Expr::Identifier(id) => assert_eq!(id.name, "age"),
			_ => panic!("Expected identifier"),
		}
	}

	#[test]
	fn test_sort_multiple_columns_asc_desc() {
		let bump = Bump::new();
		let source = "SORT { name: ASC, age: DESC }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let sort = extract_sort(stmt);

		assert_eq!(sort.columns.len(), 2);

		assert_eq!(sort.columns[0].direction, Some(SortDirection::Asc));
		match sort.columns[0].expr {
			Expr::Identifier(id) => assert_eq!(id.name, "name"),
			_ => panic!("Expected identifier"),
		}

		assert_eq!(sort.columns[1].direction, Some(SortDirection::Desc));
		match sort.columns[1].expr {
			Expr::Identifier(id) => assert_eq!(id.name, "age"),
			_ => panic!("Expected identifier"),
		}
	}

	#[test]
	fn test_sort_lowercase() {
		let bump = Bump::new();
		let source = "sort { name }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let sort = extract_sort(stmt);

		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.columns[0].direction, None);

		// Verify column name
		match sort.columns[0].expr {
			Expr::Identifier(id) => assert_eq!(id.name, "name"),
			_ => panic!("Expected identifier"),
		}
	}

	#[test]
	fn test_sort_lowercase_direction() {
		let bump = Bump::new();
		let source = "sort { name: asc }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let sort = extract_sort(stmt);

		assert_eq!(sort.columns.len(), 1);
		assert_eq!(sort.columns[0].direction, Some(SortDirection::Asc));

		// Verify column name
		match sort.columns[0].expr {
			Expr::Identifier(id) => assert_eq!(id.name, "name"),
			_ => panic!("Expected identifier"),
		}
	}
}
