// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::{
			AstAlter, AstAlterPolicyAction, AstAlterSecurityPolicy, AstCreate, AstCreateSecurityPolicy,
			AstDrop, AstDropSecurityPolicy, AstPolicyOperationEntry, AstPolicyScope, AstPolicyTargetType,
		},
		parse::Parser,
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
	pub(crate) fn parse_create_security_policy(
		&mut self,
		token: Token<'bump>,
		target_type: AstPolicyTargetType,
	) -> crate::Result<AstCreate<'bump>> {
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

			// Parse body - a sequence of pipe-separated nodes until the next operation or }
			let body = self.parse_policy_body()?;

			operations.push(AstPolicyOperationEntry {
				operation: op_token.fragment,
				body,
			});

			// Skip optional newlines/commas between operations
			self.skip_new_line()?;
			self.consume_if(TokenKind::Separator(Separator::Comma))?;
		}

		self.consume_operator(Operator::CloseCurly)?;

		Ok(AstCreate::SecurityPolicy(AstCreateSecurityPolicy {
			token,
			name,
			target_type,
			scope,
			operations,
		}))
	}

	/// Parse policy scope: `ns::object` (specific) or `ns` (namespace-wide)
	fn parse_policy_scope(&mut self) -> crate::Result<AstPolicyScope<'bump>> {
		let segments = self.parse_double_colon_separated_identifiers()?;
		let fragments: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();

		if fragments.len() == 1 {
			Ok(AstPolicyScope::NamespaceWide(fragments.into_iter().next().unwrap()))
		} else {
			Ok(AstPolicyScope::Specific(fragments))
		}
	}

	/// Parse a policy body: RQL nodes until we hit a newline followed by an identifier+colon (next operation) or }
	fn parse_policy_body(&mut self) -> crate::Result<Vec<crate::ast::ast::Ast<'bump>>> {
		let mut nodes = Vec::new();

		loop {
			self.skip_new_line()?;

			if self.is_eof() || self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			// Check if next token is an operation label (identifier followed by colon)
			if self.is_next_operation_label()? {
				break;
			}

			let node = self.parse_node(crate::ast::parse::Precedence::None)?;
			nodes.push(node);

			// Consume pipe if present
			if !self.is_eof() && self.current()?.is_operator(Operator::Pipe) {
				self.advance()?;
			}
		}

		Ok(nodes)
	}

	/// Check if current position is at an operation label (identifier/keyword followed by colon)
	fn is_next_operation_label(&self) -> crate::Result<bool> {
		if self.position + 1 >= self.tokens.len() {
			return Ok(false);
		}
		let current = self.tokens[self.position];
		let next = self.tokens[self.position + 1];

		let is_label = (current.is_identifier() || matches!(current.kind, TokenKind::Keyword(_)))
			&& next.is_operator(Operator::Colon);
		Ok(is_label)
	}

	/// Parse `ALTER <TYPE> POLICY name ENABLE|DISABLE`
	pub(crate) fn parse_alter_security_policy(
		&mut self,
		token: Token<'bump>,
		target_type: AstPolicyTargetType,
	) -> crate::Result<AstAlter<'bump>> {
		let name_token = self.consume(TokenKind::Identifier)?;

		let action = if (self.consume_if(TokenKind::Keyword(Keyword::Enable))?).is_some() {
			AstAlterPolicyAction::Enable
		} else {
			self.consume_keyword(Keyword::Disable)?;
			AstAlterPolicyAction::Disable
		};

		Ok(AstAlter::SecurityPolicy(AstAlterSecurityPolicy {
			token,
			target_type,
			name: name_token.fragment,
			action,
		}))
	}

	/// Parse `DROP <TYPE> POLICY [IF EXISTS] name`
	pub(crate) fn parse_drop_security_policy(
		&mut self,
		token: Token<'bump>,
		target_type: AstPolicyTargetType,
	) -> crate::Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;
		let name_token = self.consume(TokenKind::Identifier)?;

		Ok(AstDrop::SecurityPolicy(AstDropSecurityPolicy {
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
			ast::{AstCreate, AstDrop, AstPolicyTargetType},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_create_table_policy() {
		let bump = Bump::new();
		let src = r#"CREATE TABLE POLICY tenant_isolation ON app::projects {
    read: filter { org_id == $identity.org_id }
}"#;
		let tokens = tokenize(&bump, src).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, src, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let AstCreate::SecurityPolicy(policy) = node.as_create() else {
			panic!("expected SecurityPolicy")
		};
		assert_eq!(policy.target_type, AstPolicyTargetType::Table);
		assert_eq!(policy.name.unwrap().text(), "tenant_isolation");
		assert_eq!(policy.operations.len(), 1);
		assert_eq!(policy.operations[0].operation.text(), "read");
	}

	#[test]
	fn test_create_namespace_policy() {
		let bump = Bump::new();
		let src = r#"CREATE NAMESPACE POLICY finance_access ON finance {
    read: require { true }
}"#;
		let tokens = tokenize(&bump, src).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, src, tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		let AstCreate::SecurityPolicy(policy) = node.as_create() else {
			panic!("expected SecurityPolicy")
		};
		assert_eq!(policy.target_type, AstPolicyTargetType::Namespace);
		assert_eq!(policy.name.unwrap().text(), "finance_access");
	}

	#[test]
	fn test_create_session_policy() {
		let bump = Bump::new();
		let src = r#"CREATE SESSION POLICY session_control {
    query: require { true }
}"#;
		let tokens = tokenize(&bump, src).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, src, tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		let AstCreate::SecurityPolicy(policy) = node.as_create() else {
			panic!("expected SecurityPolicy")
		};
		assert_eq!(policy.target_type, AstPolicyTargetType::Session);
	}

	#[test]
	fn test_create_procedure_policy() {
		let bump = Bump::new();
		let src = r#"CREATE PROCEDURE POLICY ON finance::close_quarter {
    execute: require { true }
}"#;
		let tokens = tokenize(&bump, src).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, src, tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		let AstCreate::SecurityPolicy(policy) = node.as_create() else {
			panic!("expected SecurityPolicy")
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
			crate::ast::ast::Ast::Drop(d) => d,
			_ => panic!("expected Drop"),
		};
		let AstDrop::SecurityPolicy(sp) = drop else {
			panic!("expected SecurityPolicy")
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
			crate::ast::ast::Ast::Drop(d) => d,
			_ => panic!("expected Drop"),
		};
		let AstDrop::SecurityPolicy(sp) = drop else {
			panic!("expected SecurityPolicy")
		};
		assert!(sp.if_exists);
	}
}
