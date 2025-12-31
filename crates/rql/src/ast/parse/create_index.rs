// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{IndexType, SortDirection};

use crate::ast::{
	AstCreate, AstCreateIndex, AstIndexColumn,
	parse::{Parser, Precedence},
	tokenize::{
		Keyword::{Asc, Desc, Filter, Index, Map, On, Unique},
		Operator,
		Separator::Comma,
		Token, TokenKind,
	},
};

impl Parser {
	pub(crate) fn peek_is_index_creation(&mut self) -> crate::Result<bool> {
		Ok(matches!(self.current()?.kind, TokenKind::Keyword(Index) | TokenKind::Keyword(Unique)))
	}

	pub(crate) fn parse_create_index(&mut self, create_token: Token) -> crate::Result<AstCreate> {
		let index_type = self.parse_index_type()?;

		let name_token = self.consume(TokenKind::Identifier)?;

		self.consume_keyword(On)?;

		let namespace_token = self.consume(TokenKind::Identifier)?;
		self.consume_operator(Operator::Dot)?;
		let table_token = self.consume(TokenKind::Identifier)?;

		// Create MaybeQualifiedIndexIdentifier
		use crate::ast::identifier::MaybeQualifiedIndexIdentifier;
		let index =
			MaybeQualifiedIndexIdentifier::new(table_token.fragment.clone(), name_token.fragment.clone())
				.with_schema(namespace_token.fragment.clone());

		let columns = self.parse_index_columns()?;

		let mut filters = Vec::new();
		while self.consume_if(TokenKind::Keyword(Filter))?.is_some() {
			filters.push(Box::new(self.parse_node(Precedence::None)?));
		}

		let map = if self.consume_if(TokenKind::Keyword(Map))?.is_some() {
			Some(Box::new(self.parse_node(Precedence::None)?))
		} else {
			None
		};

		Ok(AstCreate::Index(AstCreateIndex {
			token: create_token,
			index_type,
			index,
			columns,
			filters,
			map,
		}))
	}

	fn parse_index_type(&mut self) -> crate::Result<IndexType> {
		if self.consume_if(TokenKind::Keyword(Unique))?.is_some() {
			self.consume_keyword(Index)?;
			Ok(IndexType::Unique)
		} else {
			self.consume_keyword(Index)?;
			Ok(IndexType::Index)
		}
	}

	fn parse_index_columns(&mut self) -> crate::Result<Vec<AstIndexColumn>> {
		let mut columns = Vec::new();

		self.consume_operator(Operator::OpenCurly)?;

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let column = self.parse_column_identifier()?;

			let order = if self.consume_if(TokenKind::Keyword(Asc))?.is_some() {
				Some(SortDirection::Asc)
			} else if self.consume_if(TokenKind::Keyword(Desc))?.is_some() {
				Some(SortDirection::Desc)
			} else {
				None
			};

			columns.push(AstIndexColumn {
				column,
				order,
			});

			if self.consume_if(TokenKind::Separator(Comma))?.is_none() {
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;

		Ok(columns)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{IndexType, SortDirection};

	use crate::ast::{AstCreate, AstCreateIndex, parse::Parser, tokenize};

	#[test]
	fn test_create_index() {
		let tokens = tokenize(r#"create index idx_email on test.users {email}"#).unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Index(AstCreateIndex {
				index_type,
				index,
				columns,
				filters,
				..
			}) => {
				assert_eq!(*index_type, IndexType::Index);
				assert_eq!(index.name.text(), "idx_email");
				assert_eq!(index.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(index.table.text(), "users");
				assert_eq!(columns.len(), 1);
				assert_eq!(columns[0].column.name.text(), "email");
				assert!(columns[0].order.is_none());
				assert_eq!(filters.len(), 0);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_unique_index() {
		let tokens = tokenize(r#"create unique index idx_email on test.users {email}"#).unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Index(AstCreateIndex {
				index_type,
				index,
				columns,
				filters,
				..
			}) => {
				assert_eq!(*index_type, IndexType::Unique);
				assert_eq!(index.name.text(), "idx_email");
				assert_eq!(index.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(index.table.text(), "users");
				assert_eq!(columns.len(), 1);
				assert_eq!(columns[0].column.name.text(), "email");
				assert_eq!(filters.len(), 0);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_composite_index() {
		let tokens = tokenize(r#"create index idx_name on test.users {last_name, first_name}"#).unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Index(AstCreateIndex {
				columns,
				filters,
				..
			}) => {
				assert_eq!(columns.len(), 2);
				assert_eq!(columns[0].column.name.text(), "last_name");
				assert_eq!(columns[1].column.name.text(), "first_name");
				assert_eq!(filters.len(), 0);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_index_with_ordering() {
		let tokens =
			tokenize(r#"create index idx_status on test.users {created_at desc, status asc}"#).unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Index(AstCreateIndex {
				columns,
				filters,
				..
			}) => {
				assert_eq!(columns.len(), 2);
				assert_eq!(columns[0].column.name.text(), "created_at");
				assert_eq!(columns[0].order, Some(SortDirection::Desc));
				assert_eq!(columns[1].column.name.text(), "status");
				assert_eq!(columns[1].order, Some(SortDirection::Asc));
				assert_eq!(filters.len(), 0);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_index_with_single_filter() {
		let tokens = tokenize(r#"create index idx_active_email on test.users {email} filter active == true"#)
			.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Index(AstCreateIndex {
				columns,
				filters,
				..
			}) => {
				assert_eq!(columns.len(), 1);
				assert_eq!(columns[0].column.name.text(), "email");
				assert_eq!(filters.len(), 1);
				// Verify filter contains a comparison
				// expression
				assert!(filters[0].is_infix());
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_index_with_multiple_filters() {
		let tokens = tokenize(
			r#"create index idx_filtered on test.users {email} filter active == true filter age > 18 filter country == "US""#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Index(AstCreateIndex {
				columns,
				filters,
				..
			}) => {
				assert_eq!(columns.len(), 1);
				assert_eq!(columns[0].column.name.text(), "email");
				assert_eq!(filters.len(), 3);
				// Verify each filter is an infix expression
				assert!(filters[0].is_infix());
				assert!(filters[1].is_infix());
				assert!(filters[2].is_infix());
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_index_with_filters_and_map() {
		let tokens = tokenize(
			r#"create index idx_comptokenize on test.users {email} filter active == true filter age > 18 map email"#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Index(AstCreateIndex {
				columns,
				filters,
				map,
				..
			}) => {
				assert_eq!(columns.len(), 1);
				assert_eq!(columns[0].column.name.text(), "email");
				assert_eq!(filters.len(), 2);
				assert!(filters[0].is_infix());
				assert!(filters[1].is_infix());
				assert!(map.is_some());
			}
			_ => unreachable!(),
		}
	}
}
