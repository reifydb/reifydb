// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::{AstErrorKind, Error, TypeError};

use crate::{
	ast::{
		ast::{AstDrop, AstDropFlow},
		identifier::MaybeQualifiedFlowIdentifier,
		parse::Parser,
	},
	token::{
		keyword::Keyword,
		token::{Token, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_drop(&mut self) -> crate::Result<AstDrop<'bump>> {
		let token = self.consume_keyword(Keyword::Drop)?;

		// Check what we're dropping
		if (self.consume_if(TokenKind::Keyword(Keyword::Flow))?).is_some() {
			return self.parse_drop_flow(token);
		}

		// Future: Add other DROP variants here (TABLE, VIEW, etc.)
		let fragment = self.current()?.fragment.to_owned();
		Err(Error::from(TypeError::Ast {
			kind: AstErrorKind::UnexpectedToken {
				expected: "FLOW, TABLE, VIEW, or other droppable object".to_string(),
			},
			message: format!(
				"Unexpected token: expected {}, got {}",
				"FLOW, TABLE, VIEW, or other droppable object",
				fragment.text()
			),
			fragment,
		}))
	}

	fn parse_drop_flow(&mut self, token: Token<'bump>) -> crate::Result<AstDrop<'bump>> {
		// Check for IF EXISTS
		let if_exists = if (self.consume_if(TokenKind::Keyword(Keyword::If))?).is_some() {
			self.consume_keyword(Keyword::Exists)?;
			true
		} else {
			false
		};

		let mut segments = self.parse_dot_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let flow = if namespace.is_empty() {
			MaybeQualifiedFlowIdentifier::new(name)
		} else {
			MaybeQualifiedFlowIdentifier::new(name).with_namespace(namespace)
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
	use crate::{ast::parse::Parser, bump::Bump, token::tokenize};

	#[test]
	fn test_drop_flow_basic() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP FLOW my_flow").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		match result {
			AstDrop::Flow(drop) => {
				assert!(!drop.if_exists);
				assert_eq!(drop.flow.name.text(), "my_flow");
				assert!(drop.flow.namespace.is_empty());
				assert!(!drop.cascade); // Default is RESTRICT
			}
		}
	}

	#[test]
	fn test_drop_flow_if_exists() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP FLOW IF EXISTS my_flow").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP FLOW analytics.sales_flow").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		match result {
			AstDrop::Flow(drop) => {
				assert_eq!(drop.flow.namespace[0].text(), "analytics");
				assert_eq!(drop.flow.name.text(), "sales_flow");
			}
		}
	}

	#[test]
	fn test_drop_flow_cascade() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP FLOW my_flow CASCADE").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP FLOW my_flow RESTRICT").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP FLOW IF EXISTS test.my_flow CASCADE").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		match result {
			AstDrop::Flow(drop) => {
				assert!(drop.if_exists);
				assert_eq!(drop.flow.namespace[0].text(), "test");
				assert_eq!(drop.flow.name.text(), "my_flow");
				assert!(drop.cascade);
			}
		}
	}
}
