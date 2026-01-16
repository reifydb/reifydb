// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CREATE SERIES statement parsing.
//!
//! RQL syntax:
//! - `CREATE SERIES namespace.name { columns }`

use crate::{
	ast::{
		Statement,
		parse::{ParseError, Parser},
		stmt::ddl::{CreateSeries, CreateStmt},
	},
	token::span::Span,
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse CREATE SERIES statement.
	///
	/// Syntax: `CREATE SERIES namespace.name { columns }`
	///
	/// # Examples
	///
	/// ```rql
	/// CREATE SERIES test.metrics { value: Int2 }
	/// CREATE SERIES analytics.events { timestamp: Int8, data: Text }
	/// ```
	pub(in crate::ast::parse) fn parse_create_series(
		&mut self,
		start: Span,
	) -> Result<Statement<'bump>, ParseError> {
		// Parse namespace.name (required qualified identifier for series)
		let (namespace, name) = self.parse_required_qualified_identifier()?;

		// Parse column definitions
		let columns = self.parse_column_definitions()?;

		let end_span = self.current().span;
		let span = start.merge(&end_span);

		Ok(Statement::Create(CreateStmt::Series(CreateSeries::new(Some(namespace), name, columns, span))))
	}
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use crate::{ast::Statement, token::tokenize};

	#[test]
	fn test_create_series_basic() {
		let bump = Bump::new();
		let source = "CREATE SERIES test.metrics { value: Int2 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Series(s)) => {
				assert_eq!(s.namespace, Some("test"));
				assert_eq!(s.name, "metrics");
				assert_eq!(s.columns.len(), 1);
				assert_eq!(s.columns[0].name, "value");
				assert_eq!(s.columns[0].data_type, "Int2");
			}
			_ => panic!("Expected CREATE SERIES statement"),
		}
	}

	#[test]
	fn test_create_series_multiple_columns() {
		let bump = Bump::new();
		let source = "CREATE SERIES analytics.events { timestamp: Int8, data: Text }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Series(s)) => {
				assert_eq!(s.namespace, Some("analytics"));
				assert_eq!(s.name, "events");
				assert_eq!(s.columns.len(), 2);
				assert_eq!(s.columns[0].name, "timestamp");
				assert_eq!(s.columns[0].data_type, "Int8");
				assert_eq!(s.columns[1].name, "data");
				assert_eq!(s.columns[1].data_type, "Text");
			}
			_ => panic!("Expected CREATE SERIES statement"),
		}
	}

	#[test]
	fn test_create_series_lowercase() {
		let bump = Bump::new();
		let source = "create series myns.timeseries { value: float8 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Series(s)) => {
				assert_eq!(s.namespace, Some("myns"));
				assert_eq!(s.name, "timeseries");
				assert_eq!(s.columns.len(), 1);
			}
			_ => panic!("Expected CREATE SERIES statement"),
		}
	}
}
