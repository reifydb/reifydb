// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{AstGrant, AstRevoke},
		parse::Parser,
	},
	token::{keyword::Keyword, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_grant(&mut self) -> Result<AstGrant<'bump>> {
		let token = self.consume_keyword(Keyword::Grant)?;
		let role_token = self.consume(TokenKind::Identifier)?;
		self.consume_keyword(Keyword::To)?;
		let user_token = self.consume(TokenKind::Identifier)?;

		Ok(AstGrant {
			token,
			role: role_token.fragment,
			user: user_token.fragment,
		})
	}

	pub(crate) fn parse_revoke(&mut self) -> Result<AstRevoke<'bump>> {
		let token = self.consume_keyword(Keyword::Revoke)?;
		let role_token = self.consume(TokenKind::Identifier)?;
		self.consume_keyword(Keyword::From)?;
		let user_token = self.consume(TokenKind::Identifier)?;

		Ok(AstRevoke {
			token,
			role: role_token.fragment,
			user: user_token.fragment,
		})
	}
}

#[cfg(test)]
mod tests {
	use crate::{
		ast::{ast::Ast, parse::Parser},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_grant_basic() {
		let bump = Bump::new();
		let source = "GRANT analyst TO alice";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let grant = match node {
			Ast::Grant(g) => g,
			_ => panic!("expected Grant"),
		};
		assert_eq!(grant.role.text(), "analyst");
		assert_eq!(grant.user.text(), "alice");
	}

	#[test]
	fn test_revoke_basic() {
		let bump = Bump::new();
		let source = "REVOKE analyst FROM alice";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let revoke = match node {
			Ast::Revoke(r) => r,
			_ => panic!("expected Revoke"),
		};
		assert_eq!(revoke.role.text(), "analyst");
		assert_eq!(revoke.user.text(), "alice");
	}
}
