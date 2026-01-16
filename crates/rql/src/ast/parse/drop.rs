// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::ast::{
	ast::{AstDrop, AstDropFlow},
	identifier::MaybeQualifiedFlowIdentifier,
	parse::Parser,
	tokenize::{
		keyword::Keyword,
		operator::Operator,
		token::{Token, TokenKind},
	},
};

impl Parser {
	pub(crate) fn parse_drop(&mut self) -> crate::Result<AstDrop> {
		let token = self.consume_keyword(Keyword::Drop)?;

		// Check what we're dropping
		if (self.consume_if(TokenKind::Keyword(Keyword::Flow))?).is_some() {
			return self.parse_drop_flow(token);
		}

		// Future: Add other DROP variants here (TABLE, VIEW, etc.)
		Err(reifydb_type::error::Error(reifydb_type::error::diagnostic::ast::unexpected_token_error(
			"FLOW, TABLE, VIEW, or other droppable object",
			self.current()?.fragment.clone(),
		)))
	}

	fn parse_drop_flow(&mut self, token: Token) -> crate::Result<AstDrop> {
		// Check for IF EXISTS
		let if_exists = if (self.consume_if(TokenKind::Keyword(Keyword::If))?).is_some() {
			self.consume_keyword(Keyword::Exists)?;
			true
		} else {
			false
		};

		// Parse the flow identifier (namespace.name or just name)
		let first_token = self.consume(TokenKind::Identifier)?;

		let flow = if (self.consume_if(TokenKind::Operator(Operator::Dot))?).is_some() {
			// namespace.name format
			let second_token = self.consume(TokenKind::Identifier)?;
			MaybeQualifiedFlowIdentifier::new(second_token.fragment.clone())
				.with_namespace(first_token.fragment.clone())
		} else {
			// just name format
			MaybeQualifiedFlowIdentifier::new(first_token.fragment.clone())
		};

		// Check for CASCADE or RESTRICT
		let cascade = if (self.consume_if(TokenKind::Keyword(Keyword::Cascade))?).is_some() {
			true
		} else if (self.consume_if(TokenKind::Keyword(Keyword::Restrict))?).is_some() {
			false
		} else {
			// Default to RESTRICT if neither is specified
			false
		};

		Ok(AstDrop::Flow(AstDropFlow {
			token,
			if_exists,
			flow,
			cascade,
		}))
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::ast::{parse::Parser, tokenize::tokenize};

	#[test]
	fn test_drop_flow_basic() {
		let tokens = tokenize("DROP FLOW my_flow").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse_drop().unwrap();

		match result {
			AstDrop::Flow(drop) => {
				assert!(!drop.if_exists);
				assert_eq!(drop.flow.name.text(), "my_flow");
				assert!(drop.flow.namespace.is_none());
				assert!(!drop.cascade); // Default is RESTRICT
			}
		}
	}

	#[test]
	fn test_drop_flow_if_exists() {
		let tokens = tokenize("DROP FLOW IF EXISTS my_flow").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse_drop().unwrap();

		match result {
			AstDrop::Flow(drop) => {
				assert!(drop.if_exists);
				assert_eq!(drop.flow.name.text(), "my_flow");
			}
		}
	}

	#[test]
	fn test_drop_flow_qualified() {
		let tokens = tokenize("DROP FLOW analytics.sales_flow").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse_drop().unwrap();

		match result {
			AstDrop::Flow(drop) => {
				assert_eq!(drop.flow.namespace.as_ref().unwrap().text(), "analytics");
				assert_eq!(drop.flow.name.text(), "sales_flow");
			}
		}
	}

	#[test]
	fn test_drop_flow_cascade() {
		let tokens = tokenize("DROP FLOW my_flow CASCADE").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse_drop().unwrap();

		match result {
			AstDrop::Flow(drop) => {
				assert_eq!(drop.flow.name.text(), "my_flow");
				assert!(drop.cascade);
			}
		}
	}

	#[test]
	fn test_drop_flow_restrict() {
		let tokens = tokenize("DROP FLOW my_flow RESTRICT").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse_drop().unwrap();

		match result {
			AstDrop::Flow(drop) => {
				assert_eq!(drop.flow.name.text(), "my_flow");
				assert!(!drop.cascade);
			}
		}
	}

	#[test]
	fn test_drop_flow_if_exists_cascade() {
		let tokens = tokenize("DROP FLOW IF EXISTS test.my_flow CASCADE").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse_drop().unwrap();

		match result {
			AstDrop::Flow(drop) => {
				assert!(drop.if_exists);
				assert_eq!(drop.flow.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(drop.flow.name.text(), "my_flow");
				assert!(drop.cascade);
			}
		}
	}
}
