// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::{AstCreate, AstCreateRole, AstCreateUser, AstDrop, AstDropRole, AstDropUser},
		parse::Parser,
	},
	token::token::{Token, TokenKind},
};

impl<'bump> Parser<'bump> {
	/// Parse `CREATE USER name`
	pub(crate) fn parse_create_user(&mut self, token: Token<'bump>) -> crate::Result<AstCreate<'bump>> {
		let name_token = self.consume(TokenKind::Identifier)?;

		Ok(AstCreate::User(AstCreateUser {
			token,
			name: name_token.fragment,
		}))
	}

	/// Parse `CREATE ROLE name`
	pub(crate) fn parse_create_role(&mut self, token: Token<'bump>) -> crate::Result<AstCreate<'bump>> {
		let name_token = self.consume(TokenKind::Identifier)?;

		Ok(AstCreate::Role(AstCreateRole {
			token,
			name: name_token.fragment,
		}))
	}

	/// Parse `DROP USER [IF EXISTS] name`
	pub(crate) fn parse_drop_user(&mut self, token: Token<'bump>) -> crate::Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;
		let name_token = self.consume(TokenKind::Identifier)?;

		Ok(AstDrop::User(AstDropUser {
			token,
			name: name_token.fragment,
			if_exists,
		}))
	}

	/// Parse `DROP ROLE [IF EXISTS] name`
	pub(crate) fn parse_drop_role(&mut self, token: Token<'bump>) -> crate::Result<AstDrop<'bump>> {
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
			ast::{AstCreate, AstDrop},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_create_user() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE USER alice").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let AstCreate::User(user) = node.as_create() else {
			panic!("expected CreateUser")
		};
		assert_eq!(user.name.text(), "alice");
	}

	#[test]
	fn test_create_role() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE ROLE analyst").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
		let tokens = tokenize(&bump, "DROP USER alice").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let drop = match node {
			crate::ast::ast::Ast::Drop(d) => d,
			_ => panic!("expected Drop"),
		};
		let AstDrop::User(user) = drop else {
			panic!("expected DropUser")
		};
		assert_eq!(user.name.text(), "alice");
		assert!(!user.if_exists);
	}

	#[test]
	fn test_drop_user_if_exists() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP USER IF EXISTS alice").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		let drop = match node {
			crate::ast::ast::Ast::Drop(d) => d,
			_ => panic!("expected Drop"),
		};
		let AstDrop::User(user) = drop else {
			panic!("expected DropUser")
		};
		assert_eq!(user.name.text(), "alice");
		assert!(user.if_exists);
	}

	#[test]
	fn test_drop_role() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP ROLE analyst").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		let drop = match node {
			crate::ast::ast::Ast::Drop(d) => d,
			_ => panic!("expected Drop"),
		};
		let AstDrop::Role(role) = drop else {
			panic!("expected DropRole")
		};
		assert_eq!(role.name.text(), "analyst");
		assert!(!role.if_exists);
	}
}
