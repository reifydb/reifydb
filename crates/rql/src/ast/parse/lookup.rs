// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::JoinType;

use crate::{
	Result,
	ast::{ast::AstLookup, parse::Parser},
	token::{
		keyword::Keyword::{Inner, Left, Lookup},
		operator::Operator::As,
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_lookup(&mut self, join_type: JoinType) -> Result<AstLookup<'bump>> {
		let start = self.current()?.fragment.offset();
		let token = match join_type {
			JoinType::Inner => self.consume_keyword(Inner)?,
			JoinType::Left => self.consume_keyword(Left)?,
		};
		self.consume_keyword(Lookup)?;

		let subquery = self.parse_sub_query()?;

		self.consume_operator(As)?;
		let alias = self.consume_identifier()?.fragment;

		let using_clause = self.parse_using_clause()?;
		let with = self.parse_operator_with()?;

		Ok(AstLookup {
			token,
			join_type,
			subquery,
			using_clause,
			alias,
			with,
			rql: self.source_since(start),
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::common::JoinType;

	use crate::{
		ast::{
			ast::{Ast, AstFrom, AstLookup, AstOperatorWithValue, InfixOperator, JoinConnector},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	fn only_lookup<'a, 'b>(nodes: &'a [Ast<'b>]) -> &'a AstLookup<'b> {
		match nodes.first() {
			Some(Ast::Lookup(lookup)) => lookup,
			other => panic!("expected a lookup node, got {:?}", other),
		}
	}

	#[test]
	fn test_inner_lookup_with_retention() {
		// The inner form must parse to its own node carrying JoinType::Inner, never to a join.
		let bump = Bump::new();
		let source = "inner lookup { from polaris::pool } as info using (pool, info.pool) and (pair, info.pair) with { retention: { left: 10s } }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let lookup = only_lookup(&result[0].nodes);
		assert_eq!(lookup.join_type, JoinType::Inner);
		assert_eq!(lookup.alias.text(), "info");
		assert_eq!(lookup.rql, source);

		let first_node = lookup.subquery.statement.nodes.first().expect("the block must hold a from");
		let Ast::From(AstFrom::Source {
			source,
			..
		}) = first_node
		else {
			panic!("expected a from source in the block, got {:?}", first_node);
		};
		assert_eq!(source.namespace[0].text(), "polaris");
		assert_eq!(source.name.text(), "pool");
		assert_eq!(lookup.subquery.statement.nodes.len(), 1);

		assert_eq!(lookup.using_clause.pairs.len(), 2);
		assert_eq!(lookup.using_clause.pairs[0].connector, Some(JoinConnector::And));
		assert_eq!(lookup.using_clause.pairs[1].connector, None);
		assert_eq!(lookup.using_clause.pairs[0].first.as_identifier().text(), "pool");
		let second = lookup.using_clause.pairs[1].second.as_infix();
		assert_eq!(second.left.as_identifier().text(), "info");
		assert!(matches!(second.operator, InfixOperator::AccessTable(_)));
		assert_eq!(second.right.as_identifier().text(), "pair");

		let with = lookup.with.as_ref().expect("the with block must be kept");
		assert_eq!(with.entries.len(), 1);
		assert_eq!(with.entries[0].key.word(), Some("retention"));
		let Some(AstOperatorWithValue::Block(sides)) = &with.entries[0].value else {
			panic!("retention must hold a block, got {:?}", with.entries[0].value);
		};
		assert_eq!(sides.len(), 1);
		assert_eq!(sides[0].key.word(), Some("left"));
	}

	#[test]
	fn test_left_lookup_with_retention() {
		// The left form must keep JoinType::Left so a miss keeps the left row with none columns.
		let bump = Bump::new();
		let source = "left lookup { from polaris::price::usd } as base using (info_base_mint, base.mint) with { retention: { left: 10s } }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let lookup = only_lookup(&result[0].nodes);
		assert_eq!(lookup.join_type, JoinType::Left);
		assert_eq!(lookup.alias.text(), "base");
		assert_eq!(lookup.using_clause.pairs.len(), 1);
		assert_eq!(lookup.using_clause.pairs[0].first.as_identifier().text(), "info_base_mint");
		assert!(lookup.with.is_some());
	}

	#[test]
	fn test_lookup_after_from_in_pipeline() {
		// A lookup must chain after a from like a join does, as the second node of the statement.
		let bump = Bump::new();
		let source = "from test::orders inner lookup { from test::users } as u using (user_id, u.id) with { retention: { left: 1m } }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);
		assert_eq!(result[0].nodes.len(), 2);
		assert!(matches!(result[0].nodes[0], Ast::From(_)));
		let lookup = only_lookup(&result[0].nodes[1..]);
		assert_eq!(lookup.join_type, JoinType::Inner);
		assert_eq!(lookup.alias.text(), "u");
	}

	#[test]
	fn test_inner_join_still_parses_as_join() {
		// The lookup arms must not capture a plain inner join.
		let bump = Bump::new();
		let source = "inner join { from test::users } as u using (user_id, u.id)";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();
		assert!(matches!(result[0].nodes.first(), Some(Ast::Join(_))));
	}

	#[test]
	fn test_lookup_without_alias_fails() {
		// The alias is required, as for a join; without it the right columns cannot be named.
		let bump = Bump::new();
		let source = "inner lookup { from test::users } using (user_id, u.id)";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		assert!(parser.parse().is_err());
	}

	#[test]
	fn test_lookup_without_using_fails() {
		// A lookup matches only on using columns; a missing using clause must not parse.
		let bump = Bump::new();
		let source = "inner lookup { from test::users } as u";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		assert!(parser.parse().is_err());
	}
}
