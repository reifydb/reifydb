// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{AstUnion, parse::Parser, tokenize::Keyword::Union};

impl<'a> Parser<'a> {
	pub(crate) fn parse_union(&mut self) -> crate::Result<AstUnion<'a>> {
		let token = self.consume_keyword(Union)?;
		let with = self.parse_sub_query()?;
		Ok(AstUnion {
			token,
			with,
		})
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{Ast, AstFrom, parse::Parser, tokenize::tokenize};

	#[test]
	fn test_union_basic() {
		let tokens = tokenize("union { from test.orders }").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let union = result.first_unchecked().as_union();

		let first_node = union.with.statement.nodes.first().expect("Expected node in subquery");
		if let Ast::From(AstFrom::Source {
			source,
			..
		}) = first_node
		{
			assert_eq!(source.namespace.as_ref().unwrap().text(), "test");
			assert_eq!(source.name.text(), "orders");
		} else {
			panic!("Expected From node in subquery");
		}
	}

	#[test]
	fn test_union_with_query() {
		let tokens = tokenize("from test.source1 union { from test.source2 }").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let statement = &result[0];
		assert_eq!(statement.nodes.len(), 2);

		// First should be FROM
		assert!(statement.nodes[0].is_from());

		// Second should be UNION
		assert!(statement.nodes[1].is_union());
		let union = statement.nodes[1].as_union();
		let first_node = union.with.statement.nodes.first().expect("Expected node in subquery");
		if let Ast::From(AstFrom::Source {
			source,
			..
		}) = first_node
		{
			assert_eq!(source.namespace.as_ref().unwrap().text(), "test");
			assert_eq!(source.name.text(), "source2");
		} else {
			panic!("Expected From node in subquery");
		}
	}

	#[test]
	fn test_union_chained() {
		let tokens =
			tokenize("from test.source1 union { from test.source2 } union { from test.source3 }").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let statement = &result[0];
		assert_eq!(statement.nodes.len(), 3);

		assert!(statement.nodes[0].is_from());
		assert!(statement.nodes[1].is_union());
		assert!(statement.nodes[2].is_union());
	}
}
