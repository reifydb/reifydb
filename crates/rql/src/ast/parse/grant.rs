// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::{AstGrant, AstRevoke},
		parse::Parser,
	},
	token::{keyword::Keyword, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	/// Parse `GRANT role TO user`
	pub(crate) fn parse_grant(&mut self) -> crate::Result<AstGrant<'bump>> {
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

	/// Parse `REVOKE role FROM user`
	pub(crate) fn parse_revoke(&mut self) -> crate::Result<AstRevoke<'bump>> {
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
	use crate::{ast::parse::Parser, bump::Bump, token::tokenize};

	#[test]
	fn test_grant_basic() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "GRANT analyst TO alice").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let grant = match node {
			crate::ast::ast::Ast::Grant(g) => g,
			_ => panic!("expected Grant"),
		};
		assert_eq!(grant.role.text(), "analyst");
		assert_eq!(grant.user.text(), "alice");
	}

	#[test]
	fn test_revoke_basic() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "REVOKE analyst FROM alice").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let revoke = match node {
			crate::ast::ast::Ast::Revoke(r) => r,
			_ => panic!("expected Revoke"),
		};
		assert_eq!(revoke.role.text(), "analyst");
		assert_eq!(revoke.user.text(), "alice");
	}
}
