// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{AstCreate, AstCreateIdentity, AstCreateRole, AstDrop, AstDropIdentity, AstDropRole},
		parse::Parser,
	},
	token::token::{Token, TokenKind},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_create_identity(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		let name_token = self.consume(TokenKind::Identifier)?;

		Ok(AstCreate::Identity(AstCreateIdentity {
			token,
			name: name_token.fragment,
		}))
	}

	pub(crate) fn parse_create_role(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		let name_token = self.consume(TokenKind::Identifier)?;

		Ok(AstCreate::Role(AstCreateRole {
			token,
			name: name_token.fragment,
		}))
	}

	pub(crate) fn parse_drop_identity(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;
		let name_token = self.consume(TokenKind::Identifier)?;

		Ok(AstDrop::Identity(AstDropIdentity {
			token,
			name: name_token.fragment,
			if_exists,
		}))
	}

	pub(crate) fn parse_drop_role(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;
		let name_token = self.consume(TokenKind::Identifier)?;

		Ok(AstDrop::Role(AstDropRole {
			token,
			name: name_token.fragment,
			if_exists,
		}))
	}
}

#[cfg(test)]
mod tests {
	use crate::{
		ast::{
			ast::{Ast, AstCreate, AstDrop},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_create_user() {
		let bump = Bump::new();
		let source = "CREATE USER alice";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let AstCreate::Identity(identity) = node.as_create() else {
			panic!("expected CreateIdentity")
		};
		assert_eq!(identity.name.text(), "alice");
	}

	#[test]
	fn test_create_role() {
		let bump = Bump::new();
		let source = "CREATE ROLE analyst";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let AstCreate::Role(role) = node.as_create() else {
			panic!("expected CreateRole")
		};
		assert_eq!(role.name.text(), "analyst");
	}

	#[test]
	fn test_drop_user() {
		let bump = Bump::new();
		let source = "DROP USER alice";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let drop = match node {
			Ast::Drop(d) => d,
			_ => panic!("expected Drop"),
		};
		let AstDrop::Identity(identity) = drop else {
			panic!("expected DropIdentity")
		};
		assert_eq!(identity.name.text(), "alice");
		assert!(!identity.if_exists);
	}

	#[test]
	fn test_drop_user_if_exists() {
		let bump = Bump::new();
		let source = "DROP USER IF EXISTS alice";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		let drop = match node {
			Ast::Drop(d) => d,
			_ => panic!("expected Drop"),
		};
		let AstDrop::Identity(identity) = drop else {
			panic!("expected DropIdentity")
		};
		assert_eq!(identity.name.text(), "alice");
		assert!(identity.if_exists);
	}

	#[test]
	fn test_drop_role() {
		let bump = Bump::new();
		let source = "DROP ROLE analyst";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		let drop = match node {
			Ast::Drop(d) => d,
			_ => panic!("expected Drop"),
		};
		let AstDrop::Role(role) = drop else {
			panic!("expected DropRole")
		};
		assert_eq!(role.name.text(), "analyst");
		assert!(!role.if_exists);
	}
}
