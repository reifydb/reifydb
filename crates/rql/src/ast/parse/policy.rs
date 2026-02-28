// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{
			Ast, AstAlter, AstAlterPolicy, AstAlterPolicyAction, AstCreate, AstCreatePolicy, AstDrop,
			AstDropPolicy, AstPolicyOperationEntry, AstPolicyScope, AstPolicyTargetType,
		},
		parse::{Parser, Precedence},
	},
	token::{
		keyword::Keyword,
		operator::Operator,
		separator::Separator,
		token::{Token, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	/// Parse `CREATE <TYPE> POLICY [name] [ON scope] { operations }`
	pub(crate) fn parse_create_policy(
		&mut self,
		token: Token<'bump>,
		target_type: AstPolicyTargetType,
	) -> Result<AstCreate<'bump>> {
		// Optionally read policy name (identifier before ON or {)
		let name = if !self.is_eof()
			&& self.current()?.is_identifier()
			&& !self.current()?.is_keyword(Keyword::On)
		{
			Some(self.advance()?.fragment)
		} else {
			None
		};

		// Parse scope
		let scope = if target_type == AstPolicyTargetType::Session {
			// SESSION POLICY has no ON clause
			AstPolicyScope::Global
		} else if (self.consume_if(TokenKind::Keyword(Keyword::On))?).is_some() {
			self.parse_policy_scope()?
		} else {
			AstPolicyScope::Global
		};

		// Parse { operation: body, ... }
		self.skip_new_line()?;
		self.consume_operator(Operator::OpenCurly)?;

		let mut operations = Vec::new();
		loop {
			self.skip_new_line()?;
			if self.is_eof() || self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			// Parse operation label (identifier or keyword used as identifier)
			let op_token = if self.current()?.is_identifier() {
				self.advance()?
			} else {
				// Allow keywords like "read", "write", etc. as operation names
				self.consume_keyword_as_ident()?
			};

			// Consume colon
			self.consume_operator(Operator::Colon)?;
			self.skip_new_line()?;

			// Parse body wrapped in { ... }
			self.consume_operator(Operator::OpenCurly)?;
			let body_start_pos = self.position;

			let body = self.parse_policy_body()?;

			// Capture body source by slicing the original source between { and }
			let body_end_pos = self.position;
			let body_source = if body_start_pos < body_end_pos {
				let start = self.tokens[body_start_pos].fragment.offset();
				let end = self.tokens[body_end_pos - 1].fragment.offset()
					+ self.tokens[body_end_pos - 1].fragment.text().len();
				self.source[start..end].trim().to_string()
			} else {
				String::new()
			};

			self.consume_operator(Operator::CloseCurly)?;

			operations.push(AstPolicyOperationEntry {
				operation: op_token.fragment,
				body,
				body_source,
			});

			// Skip optional newlines/commas between operations
			self.skip_new_line()?;
			self.consume_if(TokenKind::Separator(Separator::Comma))?;
		}

		self.consume_operator(Operator::CloseCurly)?;

		Ok(AstCreate::Policy(AstCreatePolicy {
			token,
			name,
			target_type,
			scope,
			operations,
		}))
	}

	/// Parse policy scope: `ns::object` (specific) or `ns` (namespace-wide)
	fn parse_policy_scope(&mut self) -> Result<AstPolicyScope<'bump>> {
		let segments = self.parse_double_colon_separated_identifiers()?;
		let fragments: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();

		if fragments.len() == 1 {
			Ok(AstPolicyScope::NamespaceWide(fragments.into_iter().next().unwrap()))
		} else {
			Ok(AstPolicyScope::Specific(fragments))
		}
	}

	/// Parse a policy body: RQL nodes inside { ... } until closing }
	fn parse_policy_body(&mut self) -> Result<Vec<Ast<'bump>>> {
		let mut nodes = Vec::new();

		loop {
			self.skip_new_line()?;

			if self.is_eof() || self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let node = self.parse_node(Precedence::None)?;
			nodes.push(node);

			// Consume pipe if present
			if !self.is_eof() && self.current()?.is_operator(Operator::Pipe) {
				self.advance()?;
			}
		}

		Ok(nodes)
	}

	/// Parse `ALTER <TYPE> POLICY name ENABLE|DISABLE`
	pub(crate) fn parse_alter_policy(
		&mut self,
		token: Token<'bump>,
		target_type: AstPolicyTargetType,
	) -> Result<AstAlter<'bump>> {
		let name_token = self.consume(TokenKind::Identifier)?;

		let action = if (self.consume_if(TokenKind::Keyword(Keyword::Enable))?).is_some() {
			AstAlterPolicyAction::Enable
		} else {
			self.consume_keyword(Keyword::Disable)?;
			AstAlterPolicyAction::Disable
		};

		Ok(AstAlter::Policy(AstAlterPolicy {
			token,
			target_type,
			name: name_token.fragment,
			action,
		}))
	}

	/// Parse `DROP <TYPE> POLICY [IF EXISTS] name`
	pub(crate) fn parse_drop_policy(
		&mut self,
		token: Token<'bump>,
		target_type: AstPolicyTargetType,
	) -> Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;
		let name_token = self.consume(TokenKind::Identifier)?;

		Ok(AstDrop::Policy(AstDropPolicy {
			token,
			target_type,
			name: name_token.fragment,
			if_exists,
		}))
	}
}

#[cfg(test)]
mod tests {
	use crate::{
		ast::{
			ast::{Ast, AstCreate, AstDrop, AstPolicyTargetType},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_create_table_policy() {
		let bump = Bump::new();
		let src = r#"CREATE TABLE POLICY tenant_isolation ON app::projects {
    read: { filter { org_id == $identity.org_id } }
}"#;
		let tokens = tokenize(&bump, src).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, src, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let AstCreate::Policy(policy) = node.as_create() else {
			panic!("expected Policy")
		};
		assert_eq!(policy.target_type, AstPolicyTargetType::Table);
		assert_eq!(policy.name.unwrap().text(), "tenant_isolation");
		assert_eq!(policy.operations.len(), 1);
		assert_eq!(policy.operations[0].operation.text(), "read");
		assert_eq!(policy.operations[0].body_source, "filter { org_id == $identity.org_id }");
	}

	#[test]
	fn test_create_namespace_policy() {
		let bump = Bump::new();
		let src = r#"CREATE NAMESPACE POLICY finance_access ON finance {
    read: { filter { true } }
}"#;
		let tokens = tokenize(&bump, src).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, src, tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		let AstCreate::Policy(policy) = node.as_create() else {
			panic!("expected Policy")
		};
		assert_eq!(policy.target_type, AstPolicyTargetType::Namespace);
		assert_eq!(policy.name.unwrap().text(), "finance_access");
	}

	#[test]
	fn test_create_session_policy() {
		let bump = Bump::new();
		let src = r#"CREATE SESSION POLICY session_control {
    query: { filter { true } }
}"#;
		let tokens = tokenize(&bump, src).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, src, tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		let AstCreate::Policy(policy) = node.as_create() else {
			panic!("expected Policy")
		};
		assert_eq!(policy.target_type, AstPolicyTargetType::Session);
	}

	#[test]
	fn test_create_procedure_policy() {
		let bump = Bump::new();
		let src = r#"CREATE PROCEDURE POLICY ON finance::close_quarter {
    execute: { filter { true } }
}"#;
		let tokens = tokenize(&bump, src).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, src, tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		let AstCreate::Policy(policy) = node.as_create() else {
			panic!("expected Policy")
		};
		assert_eq!(policy.target_type, AstPolicyTargetType::Procedure);
		assert!(policy.name.is_none());
	}

	#[test]
	fn test_drop_table_policy() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP TABLE POLICY tenant_isolation").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		let drop = match node {
			Ast::Drop(d) => d,
			_ => panic!("expected Drop"),
		};
		let AstDrop::Policy(sp) = drop else {
			panic!("expected Policy")
		};
		assert_eq!(sp.target_type, AstPolicyTargetType::Table);
		assert_eq!(sp.name.text(), "tenant_isolation");
		assert!(!sp.if_exists);
	}

	#[test]
	fn test_drop_table_policy_if_exists() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "DROP TABLE POLICY IF EXISTS tenant_isolation").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		let drop = match node {
			Ast::Drop(d) => d,
			_ => panic!("expected Drop"),
		};
		let AstDrop::Policy(sp) = drop else {
			panic!("expected Policy")
		};
		assert!(sp.if_exists);
	}
}
