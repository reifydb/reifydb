// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::error::diagnostic::query::duplicate_field;
use reifydb_value::{
	error::{AstErrorKind, Error, TypeError},
	return_error,
};

use crate::{
	Result,
	ast::{
		ast::{
			AstAlter, AstAlterAuthenticationMethod, AstAuthenticationMethodSetting, AstBodyEntry,
			AstCreate, AstCreateAuthentication, AstCreateAuthenticationMethod, AstDrop,
			AstDropAuthentication, AstDropAuthenticationMethod,
		},
		parse::{Parser, Precedence},
	},
	bump::BumpFragment,
	token::{
		keyword::Keyword,
		operator::Operator,
		separator::Separator,
		token::{Literal, Token, TokenKind},
	},
};

const METHOD: &str = "method";

struct MethodSettings<'bump> {
	ttl: Option<Token<'bump>>,
	promote_guest: Option<Token<'bump>>,
	settings: Vec<AstAuthenticationMethodSetting<'bump>>,
}

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_create_authentication(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		self.consume_keyword(Keyword::For)?;
		let user_token = self.consume_identifier()?;
		let entries = self.parse_body_entries()?;

		Ok(AstCreate::Authentication(AstCreateAuthentication {
			token,
			user: user_token.fragment,
			entries,
		}))
	}

	pub(crate) fn parse_drop_authentication(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;
		self.consume_keyword(Keyword::For)?;
		let user_token = self.consume_identifier()?;
		let entries = self.parse_body_entries()?;

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

	pub(crate) fn consume_method_word(&mut self) -> Result<bool> {
		let current = self.current()?;
		if current.kind == TokenKind::Identifier && current.fragment.text().eq_ignore_ascii_case(METHOD) {
			self.advance()?;
			return Ok(true);
		}
		Ok(false)
	}

	pub(crate) fn expect_method_word(&mut self) -> Result<()> {
		if self.consume_method_word()? {
			return Ok(());
		}
		let fragment = self.current()?.fragment.to_owned();
		Err(Error::from(TypeError::Ast {
			kind: AstErrorKind::UnexpectedToken {
				expected: "'method'".to_string(),
			},
			message: format!("expected `METHOD` after `ALTER AUTHENTICATION`, found `{}`", fragment.text()),
			fragment,
		}))
	}

	pub(crate) fn parse_create_authentication_method(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		let name = self.consume_identifier()?;
		self.consume_keyword(Keyword::With)?;
		let settings = self.parse_authentication_method_settings()?;

		let Some(ttl) = settings.ttl else {
			let fragment = name.fragment.to_owned();
			return Err(Error::from(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "'ttl' is required for an authentication method".to_string(),
				},
				message: format!(
					"'ttl' is required for authentication method `{}` (use a duration or `forever`)",
					fragment.text()
				),
				fragment,
			}));
		};

		Ok(AstCreate::AuthenticationMethod(AstCreateAuthenticationMethod {
			token,
			name: name.fragment,
			ttl,
			promote_guest: settings.promote_guest,
			settings: settings.settings,
		}))
	}

	pub(crate) fn parse_alter_authentication_method(&mut self, token: Token<'bump>) -> Result<AstAlter<'bump>> {
		let name = self.consume_identifier()?;
		self.consume_keyword(Keyword::With)?;
		let settings = self.parse_authentication_method_settings()?;

		Ok(AstAlter::AuthenticationMethod(AstAlterAuthenticationMethod {
			token,
			name: name.fragment,
			ttl: settings.ttl,
			promote_guest: settings.promote_guest,
			settings: settings.settings,
		}))
	}

	pub(crate) fn parse_drop_authentication_method(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;
		let name = self.consume_identifier()?;

		Ok(AstDrop::AuthenticationMethod(AstDropAuthenticationMethod {
			token,
			name: name.fragment,
			if_exists,
		}))
	}

	fn parse_authentication_method_settings(&mut self) -> Result<MethodSettings<'bump>> {
		self.consume_operator(Operator::OpenCurly)?;

		let mut seen: Vec<Token<'bump>> = Vec::new();
		let mut parsed = MethodSettings {
			ttl: None,
			promote_guest: None,
			settings: Vec::new(),
		};

		loop {
			self.skip_new_line()?;
			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let key = self.consume_identifier()?;
			if seen.iter().any(|earlier| earlier.fragment.text() == key.fragment.text()) {
				return_error!(duplicate_field(key.fragment.to_owned(), key.fragment.text()));
			}
			self.consume_operator(Operator::Colon)?;

			match key.fragment.text() {
				"ttl" => parsed.ttl = Some(self.consume_duration_or_forever()?),
				"promote_guest" => parsed.promote_guest = Some(self.consume_boolean("promote_guest")?),
				_ => {
					let value = self.consume_setting_text(&key)?;
					parsed.settings.push(AstAuthenticationMethodSetting {
						key: key.fragment,
						value: value.fragment,
					});
				}
			}
			seen.push(key);

			self.skip_new_line()?;
			if self.consume_if(TokenKind::Separator(Separator::Comma))?.is_some() {
				continue;
			}
			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;
		Ok(parsed)
	}

	fn consume_setting_text(&mut self, key: &Token<'bump>) -> Result<Token<'bump>> {
		let current = self.current()?;
		if current.kind == TokenKind::Literal(Literal::Text) {
			return self.advance();
		}
		let fragment = current.fragment.to_owned();
		Err(Error::from(TypeError::Ast {
			kind: AstErrorKind::UnexpectedToken {
				expected: "a string literal".to_string(),
			},
			message: format!(
				"expected string literal for '{}', found `{}`",
				key.fragment.text(),
				fragment.text()
			),
			fragment,
		}))
	}

	pub(crate) fn parse_body_entries(&mut self) -> Result<Vec<AstBodyEntry<'bump>>> {
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

			let key_token = self.consume_identifier()?;
			self.consume_operator(Operator::Colon)?;
			let value = self.parse_node(Precedence::None)?;

			entries.push(AstBodyEntry {
				key: key_token.fragment,
				value,
			});

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
			ast::{Ast, AstAlter, AstCreate, AstDrop},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_create_authentication_password() {
		let bump = Bump::new();
		let source = "CREATE AUTHENTICATION FOR alice { method: password; password: 'secret' }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].nodes.first().unwrap();
		let Ast::Create(create) = node else {
			panic!("not create")
		};
		let AstCreate::Authentication(auth) = &**create else {
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
		let source = "CREATE AUTHENTICATION FOR alice { method: token }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].nodes.first().unwrap();
		let Ast::Create(create) = node else {
			panic!("not create")
		};
		let AstCreate::Authentication(auth) = &**create else {
			panic!("expected CreateAuthentication")
		};
		assert_eq!(auth.user.text(), "alice");
		assert_eq!(auth.entries.len(), 1);
		assert_eq!(auth.entries[0].key.text(), "method");
	}

	#[test]
	fn test_drop_authentication() {
		let bump = Bump::new();
		let source = "DROP AUTHENTICATION FOR alice { method: password }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].nodes.first().unwrap();
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
		let source = "DROP AUTHENTICATION IF EXISTS FOR alice { method: token }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].nodes.first().unwrap();
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

	fn parse_error(source: &str) -> String {
		let bump = Bump::new();
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let err = parser.parse().unwrap_err().diagnostic();
		format!("{} {}", err.code, err.message)
	}

	#[test]
	fn create_method_keeps_ttl_promote_guest_and_unquoted_settings() {
		let bump = Bump::new();
		let source = "CREATE AUTHENTICATION METHOD github WITH { ttl: 30d, promote_guest: true, client_id: 'abc', redirect_uri: 'https://app/cb' }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		let Ast::Create(create) = stmts[0].nodes.first().unwrap() else {
			panic!("not create")
		};
		let AstCreate::AuthenticationMethod(method) = &**create else {
			panic!("expected CreateAuthenticationMethod")
		};

		assert_eq!(method.name.text(), "github");
		assert_eq!(method.ttl.fragment.text(), "30d");
		assert_eq!(method.promote_guest.as_ref().unwrap().fragment.text(), "true");
		let settings: Vec<(&str, &str)> =
			method.settings.iter().map(|s| (s.key.text(), s.value.text())).collect();
		assert_eq!(settings, vec![("client_id", "abc"), ("redirect_uri", "https://app/cb")]);
	}

	#[test]
	fn create_method_accepts_forever() {
		let bump = Bump::new();
		let source = "CREATE AUTHENTICATION METHOD token WITH { ttl: forever }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		let Ast::Create(create) = stmts[0].nodes.first().unwrap() else {
			panic!("not create")
		};
		let AstCreate::AuthenticationMethod(method) = &**create else {
			panic!("expected CreateAuthenticationMethod")
		};

		assert_eq!(method.ttl.fragment.text(), "forever");
		assert!(method.promote_guest.is_none());
		assert!(method.settings.is_empty());
	}

	#[test]
	fn create_method_without_ttl_is_refused() {
		// A method without ttl would issue sessions whose lifetime nobody declared.
		let err = parse_error("CREATE AUTHENTICATION METHOD github WITH { client_id: 'x' }");
		assert!(err.contains("'ttl' is required"), "{err}");
	}

	#[test]
	fn alter_method_may_omit_ttl() {
		let bump = Bump::new();
		let source = "ALTER AUTHENTICATION METHOD github WITH { redirect_uri: 'y' }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		let Ast::Alter(alter) = stmts[0].nodes.first().unwrap() else {
			panic!("not alter")
		};
		let AstAlter::AuthenticationMethod(method) = &**alter else {
			panic!("expected AlterAuthenticationMethod")
		};

		assert_eq!(method.name.text(), "github");
		assert!(method.ttl.is_none());
		assert_eq!(method.settings.len(), 1);
		assert_eq!(method.settings[0].key.text(), "redirect_uri");
	}

	#[test]
	fn drop_method_with_and_without_if_exists() {
		for (source, expected) in [
			("DROP AUTHENTICATION METHOD github", false),
			("DROP AUTHENTICATION METHOD IF EXISTS github", true),
		] {
			let bump = Bump::new();
			let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
			let mut parser = Parser::new(&bump, source, tokens);
			let stmts = parser.parse().unwrap();
			let Ast::Drop(drop) = stmts[0].nodes.first().unwrap() else {
				panic!("not drop")
			};
			let AstDrop::AuthenticationMethod(method) = drop else {
				panic!("expected DropAuthenticationMethod for {source}")
			};
			assert_eq!(method.name.text(), "github");
			assert_eq!(method.if_exists, expected, "{source}");
		}
	}

	#[test]
	fn a_setting_given_twice_is_refused() {
		// Silently keeping the last value would hide which ttl the author meant.
		let err = parse_error("CREATE AUTHENTICATION METHOD token WITH { ttl: 1h, ttl: 2h }");
		assert!(err.starts_with("QUERY_009"), "{err}");
	}

	#[test]
	fn an_unquoted_provider_setting_is_refused() {
		let err = parse_error("CREATE AUTHENTICATION METHOD github WITH { ttl: 1h, client_id: abc }");
		assert!(err.contains("expected string literal for 'client_id'"), "{err}");
	}

	#[test]
	fn an_unknown_setting_reaches_the_provider_check() {
		// The parser must not own provider knowledge; rejecting keys is configure_method's job.
		let bump = Bump::new();
		let source = "CREATE AUTHENTICATION METHOD github WITH { ttl: 1h, scope: 'x' }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		let Ast::Create(create) = stmts[0].nodes.first().unwrap() else {
			panic!("not create")
		};
		let AstCreate::AuthenticationMethod(method) = &**create else {
			panic!("expected CreateAuthenticationMethod")
		};
		assert_eq!(method.settings[0].key.text(), "scope");
	}

	#[test]
	fn the_method_word_is_case_insensitive() {
		let bump = Bump::new();
		let source = "create authentication method token with { ttl: forever }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		let Ast::Create(create) = stmts[0].nodes.first().unwrap() else {
			panic!("not create")
		};
		assert!(matches!(&**create, AstCreate::AuthenticationMethod(_)));
	}
}
