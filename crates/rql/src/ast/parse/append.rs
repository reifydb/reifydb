// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{Parser, Precedence};
use crate::{
	ast::ast::{Ast, AstAppend, AstAppendSource, AstFrom, AstStatement, AstVariable},
	token::{keyword::Keyword, operator::Operator, separator::Separator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_append(&mut self) -> crate::Result<AstAppend<'bump>> {
		let token = self.current()?;
		// Consume APPEND keyword
		self.advance()?;

		// Check if next token is '{' — if so, this is the query form (APPEND { subquery })
		if !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly) {
			let with = self.parse_sub_query()?;
			return Ok(AstAppend::Query {
				token,
				with,
			});
		}

		// Otherwise, imperative form: APPEND $target FROM <source>
		// Parse target variable ($name)
		let variable_token = self.current()?;
		if !matches!(variable_token.kind, TokenKind::Variable) {
			return Err(reifydb_type::error::Error(
				reifydb_type::error::diagnostic::ast::unexpected_token_error(
					"expected variable name starting with '$'",
					variable_token.fragment.to_owned(),
				),
			));
		}
		let var_token = self.advance()?;
		let target = AstVariable {
			token: var_token,
		};

		// Consume FROM keyword
		self.consume_keyword(Keyword::From)?;

		// Dispatch on next token
		let source = if !self.is_eof() && self.current()?.is_operator(Operator::OpenBracket) {
			// Inline: APPEND $x FROM [{...}]
			AstAppendSource::Inline(self.parse_list()?)
		} else if !self.is_eof() && matches!(self.current()?.kind, TokenKind::Variable) {
			// Variable source: always treat as frame source, then parse any pipe continuation
			let src_token = self.advance()?;
			let variable = AstVariable {
				token: src_token,
			};
			let first_node = Ast::From(AstFrom::Variable {
				token: src_token,
				variable,
			});

			let mut nodes = vec![first_node];
			let mut has_pipes = false;

			// Check for pipe continuation (e.g., $data | filter { ... })
			while !self.is_eof() {
				if let Ok(current) = self.current() {
					if current.is_separator(Separator::Semicolon) {
						break;
					}
				}
				if self.current()?.is_operator(Operator::Pipe) {
					self.advance()?; // consume the pipe
					has_pipes = true;
					nodes.push(self.parse_node(Precedence::None)?);
				} else {
					break;
				}
			}

			AstAppendSource::Statement(AstStatement {
				nodes,
				has_pipes,
				is_output: false,
			})
		} else {
			// Statement: APPEND $x FROM table | FILTER ...
			let statement = self.parse_statement_content()?;
			AstAppendSource::Statement(statement)
		};

		Ok(AstAppend::IntoVariable {
			token,
			target,
			source,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{Ast, AstAppend, AstFrom},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_append_query_basic() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "append { from test.orders }").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let node = result.first_unchecked();
		if let Ast::Append(AstAppend::Query {
			with,
			..
		}) = node
		{
			let first_node = with.statement.nodes.first().expect("Expected node in subquery");
			if let Ast::From(AstFrom::Source {
				source,
				..
			}) = first_node
			{
				assert_eq!(source.namespace[0].text(), "test");
				assert_eq!(source.name.text(), "orders");
			} else {
				panic!("Expected From node in subquery");
			}
		} else {
			panic!("Expected Append::Query");
		}
	}

	#[test]
	fn test_append_query_with_from() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "from test.source1 append { from test.source2 }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let statement = &result[0];
		assert_eq!(statement.nodes.len(), 2);

		// First should be FROM
		assert!(statement.nodes[0].is_from());

		// Second should be APPEND Query
		if let Ast::Append(AstAppend::Query {
			with,
			..
		}) = &statement.nodes[1]
		{
			let first_node = with.statement.nodes.first().expect("Expected node in subquery");
			if let Ast::From(AstFrom::Source {
				source,
				..
			}) = first_node
			{
				assert_eq!(source.namespace[0].text(), "test");
				assert_eq!(source.name.text(), "source2");
			} else {
				panic!("Expected From node in subquery");
			}
		} else {
			panic!("Expected Append::Query");
		}
	}

	#[test]
	fn test_append_query_chained() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "from test.source1 append { from test.source2 } append { from test.source3 }")
				.unwrap()
				.into_iter()
				.collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let statement = &result[0];
		assert_eq!(statement.nodes.len(), 3);

		assert!(statement.nodes[0].is_from());
		assert!(matches!(statement.nodes[1], Ast::Append(AstAppend::Query { .. })));
		assert!(matches!(statement.nodes[2], Ast::Append(AstAppend::Query { .. })));
	}
}
