// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{AstAuthenticationEntry, AstCreate, AstCreateAuthentication, AstDrop, AstDropAuthentication},
		parse::{Parser, Precedence},
	},
	bump::BumpFragment,
	token::{
		keyword::Keyword,
		operator::Operator,
		separator::Separator,
		token::{Token, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	/// Parse `CREATE AUTHENTICATION FOR user { key: value; ... }`
	pub(crate) fn parse_create_authentication(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		self.consume_keyword(Keyword::For)?;
		let user_token = self.consume(TokenKind::Identifier)?;
		let entries = self.parse_authentication_body()?;

		Ok(AstCreate::Authentication(AstCreateAuthentication {
			token,
			user: user_token.fragment,
			entries,
		}))
	}

	/// Parse `DROP AUTHENTICATION [IF EXISTS] FOR user { method: <method> }`
	pub(crate) fn parse_drop_authentication(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;
		self.consume_keyword(Keyword::For)?;
		let user_token = self.consume(TokenKind::Identifier)?;
		let entries = self.parse_authentication_body()?;

		// Extract method from entries
		let method = entries
			.iter()
			.find(|e| e.key.text() == "method")
			.map(|e| e.value.as_identifier().token.fragment)
			.unwrap_or(BumpFragment::None);

		Ok(AstDrop::Authentication(AstDropAuthentication {
			token,
			user: user_token.fragment,
			if_exists,
			method,
		}))
	}

	/// Parse `{ key: value; key: value; ... }`
	fn parse_authentication_body(&mut self) -> Result<Vec<AstAuthenticationEntry<'bump>>> {
		self.consume_operator(Operator::OpenCurly)?;
		self.skip_new_line()?;

		let mut entries = Vec::new();
		loop {
			if self.is_eof() {
				break;
			}
			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			// key is an identifier or keyword-as-ident
			let key_token = self.consume_name()?;
			self.consume_operator(Operator::Colon)?;
			let value = self.parse_node(Precedence::None)?;

			entries.push(AstAuthenticationEntry {
				key: key_token.fragment,
				value,
			});

			// Skip semicolons and newlines between entries
			self.consume_if(TokenKind::Separator(Separator::Semicolon))?;
			self.skip_new_line()?;
		}

		self.consume_operator(Operator::CloseCurly)?;
		Ok(entries)
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
	fn test_create_authentication_password() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "CREATE AUTHENTICATION FOR alice { method: password; password: 'secret' }")
				.unwrap()
				.into_iter()
				.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let AstCreate::Authentication(auth) = node.as_create() else {
			panic!("expected CreateAuthentication")
		};
		assert_eq!(auth.user.text(), "alice");
		assert_eq!(auth.entries.len(), 2);
		assert_eq!(auth.entries[0].key.text(), "method");
		assert_eq!(auth.entries[1].key.text(), "password");
	}

	#[test]
	fn test_create_authentication_token() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE AUTHENTICATION FOR alice { method: token }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let AstCreate::Authentication(auth) = node.as_create() else {
			panic!("expected CreateAuthentication")
		};
		assert_eq!(auth.user.text(), "alice");
		assert_eq!(auth.entries.len(), 1);
		assert_eq!(auth.entries[0].key.text(), "method");
	}

	#[test]
	fn test_drop_authentication() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP AUTHENTICATION FOR alice { method: password }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let drop = match node {
			Ast::Drop(d) => d,
			_ => panic!("expected Drop"),
		};
		let AstDrop::Authentication(auth) = drop else {
			panic!("expected DropAuthentication")
		};
		assert_eq!(auth.user.text(), "alice");
		assert!(!auth.if_exists);
		assert_eq!(auth.method.text(), "password");
	}

	#[test]
	fn test_drop_authentication_if_exists() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP AUTHENTICATION IF EXISTS FOR alice { method: token }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		let drop = match node {
			Ast::Drop(d) => d,
			_ => panic!("expected Drop"),
		};
		let AstDrop::Authentication(auth) = drop else {
			panic!("expected DropAuthentication")
		};
		assert_eq!(auth.user.text(), "alice");
		assert!(auth.if_exists);
	}
}
