// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{ast::AstMerge, parse::Parser},
	token::keyword::Keyword::Merge,
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_merge(&mut self) -> crate::Result<AstMerge<'bump>> {
		let token = self.consume_keyword(Merge)?;
		let with = self.parse_sub_query()?;
		Ok(AstMerge {
			token,
			with,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{Ast, AstFrom},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_merge_basic() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "merge { from test.orders }").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let merge = result.first_unchecked().as_merge();

		let first_node = merge.with.statement.nodes.first().expect("Expected node in subquery");
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
	fn test_merge_with_query() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "from test.source1 merge { from test.source2 }").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let statement = &result[0];
		assert_eq!(statement.nodes.len(), 2);

		// First should be FROM
		assert!(statement.nodes[0].is_from());

		// Second should be MERGE
		assert!(statement.nodes[1].is_merge());
		let merge = statement.nodes[1].as_merge();
		let first_node = merge.with.statement.nodes.first().expect("Expected node in subquery");
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
	fn test_merge_chained() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "from test.source1 merge { from test.source2 } merge { from test.source3 }")
				.unwrap()
				.into_iter()
				.collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let statement = &result[0];
		assert_eq!(statement.nodes.len(), 3);

		assert!(statement.nodes[0].is_from());
		assert!(statement.nodes[1].is_merge());
		assert!(statement.nodes[2].is_merge());
	}
}
