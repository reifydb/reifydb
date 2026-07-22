// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::identity::IdentityKind;

use crate::{
	Result,
	ast::{
		ast::{
			AstAlter, AstAlterIdentity, AstCreate, AstCreateIdentity, AstCreateIdentityAttribute,
			AstCreateRole, AstDrop, AstDropIdentity, AstDropIdentityAttribute, AstDropRole,
		},
		parse::Parser,
	},
	token::{operator::Operator, token::Token},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_create_identity(
		&mut self,
		token: Token<'bump>,
		kind: IdentityKind,
	) -> Result<AstCreate<'bump>> {
		let name_token = self.consume_identifier()?;

		let entries = if !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly) {
			self.parse_body_entries()?
		} else {
			Vec::new()
		};

		Ok(AstCreate::Identity(AstCreateIdentity {
			token,
			name: name_token.fragment,
			kind,
			entries,
		}))
	}

	pub(crate) fn parse_alter_identity(&mut self, token: Token<'bump>) -> Result<AstAlter<'bump>> {
		let name_token = self.consume_identifier()?;
		let entries = self.parse_body_entries()?;

		Ok(AstAlter::Identity(AstAlterIdentity {
			token,
			name: name_token.fragment,
			entries,
		}))
	}

	pub(crate) fn parse_create_identity_attribute(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		let name_token = self.consume_identifier()?;
		self.consume_operator(Operator::Colon)?;
		let value_type = self.parse_type()?;

		Ok(AstCreate::IdentityAttribute(AstCreateIdentityAttribute {
			token,
			name: name_token.fragment,
			value_type,
		}))
	}

	pub(crate) fn parse_create_role(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		let name_token = self.consume_identifier()?;

		Ok(AstCreate::Role(AstCreateRole {
			token,
			name: name_token.fragment,
		}))
	}

	pub(crate) fn parse_drop_identity(
		&mut self,
		token: Token<'bump>,
		kind: IdentityKind,
	) -> Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;
		let name_token = self.consume_identifier()?;

		Ok(AstDrop::Identity(AstDropIdentity {
			token,
			name: name_token.fragment,
			kind,
			if_exists,
		}))
	}

	pub(crate) fn parse_drop_identity_attribute(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;
		let name_token = self.consume_identifier()?;

		Ok(AstDrop::IdentityAttribute(AstDropIdentityAttribute {
			token,
			name: name_token.fragment,
			if_exists,
		}))
	}

	pub(crate) fn parse_drop_role(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;
		let name_token = self.consume_identifier()?;

		Ok(AstDrop::Role(AstDropRole {
			token,
			name: name_token.fragment,
			if_exists,
		}))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::identity::IdentityKind;

	use crate::{
		ast::{
			ast::{Ast, AstAlter, AstCreate, AstDrop, AstType},
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
		assert!(identity.entries.is_empty());
	}

	#[test]
	fn test_create_user_yields_user_kind() {
		// Bare CREATE USER must keep meaning kind=User. Every existing
		// CREATE USER statement in the repo depends on this default; if it
		// ever changed they would all silently create the wrong kind.
		let bump = Bump::new();
		let source = "CREATE USER alice";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		let AstCreate::Identity(identity) = node.as_create() else {
			panic!("expected CreateIdentity")
		};
		assert_eq!(identity.kind, IdentityKind::User);
	}

	#[test]
	fn test_create_service() {
		// CREATE SERVICE is a parser alias onto the same node as CREATE USER,
		// differing only in the kind it pre-fills.
		let bump = Bump::new();
		let source = "CREATE SERVICE probe_a";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let AstCreate::Identity(identity) = node.as_create() else {
			panic!("expected CreateIdentity")
		};
		assert_eq!(identity.name.text(), "probe_a");
		assert_eq!(identity.kind, IdentityKind::Service);
		assert!(identity.entries.is_empty());
	}

	#[test]
	fn test_create_service_with_attribute_body() {
		// The attribute body must work identically for both spellings, since
		// they share one parse function.
		let bump = Bump::new();
		let source = "CREATE SERVICE probe_a { org_id: 'acme' }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		let AstCreate::Identity(identity) = node.as_create() else {
			panic!("expected CreateIdentity")
		};
		assert_eq!(identity.kind, IdentityKind::Service);
		assert_eq!(identity.entries.len(), 1);
		assert_eq!(identity.entries[0].key.text(), "org_id");
	}

	// Regression: the SERVICE arm sits next to the USER arm in the same flat
	// dispatch chain, so it must not shadow the USER / USER ATTRIBUTE split.
	#[test]
	fn test_service_arm_does_not_disturb_user_dispatch() {
		let bump = Bump::new();
		for (source, expected) in
			[("CREATE USER alice", IdentityKind::User), ("CREATE SERVICE probe_a", IdentityKind::Service)]
		{
			let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
			let mut parser = Parser::new(&bump, source, tokens);
			let stmts = parser.parse().unwrap();
			let node = stmts[0].first_unchecked();
			let AstCreate::Identity(identity) = node.as_create() else {
				panic!("expected CreateIdentity for {source}")
			};
			assert_eq!(identity.kind, expected, "wrong kind for {source}");
		}

		let source = "CREATE USER ATTRIBUTE org_id: utf8";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		assert!(
			matches!(node.as_create(), AstCreate::IdentityAttribute(_)),
			"CREATE USER ATTRIBUTE must still reach the attribute parser"
		);
	}

	#[test]
	fn test_create_user_with_attribute_body() {
		let bump = Bump::new();
		let source = "CREATE USER alice { org_id: 'acme' }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let AstCreate::Identity(identity) = node.as_create() else {
			panic!("expected CreateIdentity")
		};
		assert_eq!(identity.name.text(), "alice");
		assert_eq!(identity.entries.len(), 1);
		assert_eq!(identity.entries[0].key.text(), "org_id");
	}

	#[test]
	fn test_create_user_with_two_attribute_body() {
		let bump = Bump::new();
		let source = "CREATE USER alice { org_id: 'acme'; tier: 'pro' }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let AstCreate::Identity(identity) = node.as_create() else {
			panic!("expected CreateIdentity")
		};
		assert_eq!(identity.entries.len(), 2);
		assert_eq!(identity.entries[0].key.text(), "org_id");
		assert_eq!(identity.entries[1].key.text(), "tier");
	}

	#[test]
	fn test_alter_user_with_attribute_body() {
		// The body is mandatory: an ALTER USER without assignments would be a silent no-op.
		let bump = Bump::new();
		let source = "ALTER USER alice { org_id: 'acme' }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let AstAlter::Identity(identity) = node.as_alter() else {
			panic!("expected AlterIdentity")
		};
		assert_eq!(identity.name.text(), "alice");
		assert_eq!(identity.entries.len(), 1);
		assert_eq!(identity.entries[0].key.text(), "org_id");
	}

	#[test]
	fn test_alter_user_with_two_attribute_body() {
		let bump = Bump::new();
		let source = "ALTER USER alice { org_id: 'acme'; tier: 'pro' }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let AstAlter::Identity(identity) = node.as_alter() else {
			panic!("expected AlterIdentity")
		};
		assert_eq!(identity.entries.len(), 2);
		assert_eq!(identity.entries[0].key.text(), "org_id");
		assert_eq!(identity.entries[1].key.text(), "tier");
	}

	#[test]
	fn test_alter_user_without_body_is_rejected() {
		let bump = Bump::new();
		let source = "ALTER USER alice";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		assert!(parser.parse().is_err());
	}

	#[test]
	fn test_alter_user_without_name_is_rejected() {
		let bump = Bump::new();
		let source = "ALTER USER { org_id: 'acme' }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		assert!(parser.parse().is_err());
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
	fn test_create_user_attribute() {
		let bump = Bump::new();
		let source = "CREATE USER ATTRIBUTE org_id: utf8";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let AstCreate::IdentityAttribute(attribute) = node.as_create() else {
			panic!("expected CreateIdentityAttribute")
		};
		assert_eq!(attribute.name.text(), "org_id");
		let AstType::Unconstrained(ty) = &attribute.value_type else {
			panic!("expected unconstrained type")
		};
		assert_eq!(ty.text(), "utf8");
	}

	#[test]
	fn test_create_user_attribute_lowercase() {
		let bump = Bump::new();
		let source = "create user attribute org_id: int4";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let AstCreate::IdentityAttribute(attribute) = node.as_create() else {
			panic!("expected CreateIdentityAttribute")
		};
		assert_eq!(attribute.name.text(), "org_id");
		let AstType::Unconstrained(ty) = &attribute.value_type else {
			panic!("expected unconstrained type")
		};
		assert_eq!(ty.text(), "int4");
	}

	#[test]
	fn test_drop_user_attribute() {
		let bump = Bump::new();
		let source = "DROP USER ATTRIBUTE org_id";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		assert_eq!(stmts.len(), 1);
		let node = stmts[0].first_unchecked();
		let drop = match node {
			Ast::Drop(d) => d,
			_ => panic!("expected Drop"),
		};
		let AstDrop::IdentityAttribute(attribute) = drop else {
			panic!("expected DropIdentityAttribute")
		};
		assert_eq!(attribute.name.text(), "org_id");
		assert!(!attribute.if_exists);
	}

	#[test]
	fn test_drop_user_attribute_if_exists() {
		let bump = Bump::new();
		let source = "DROP USER ATTRIBUTE IF EXISTS org_id";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		let drop = match node {
			Ast::Drop(d) => d,
			_ => panic!("expected Drop"),
		};
		let AstDrop::IdentityAttribute(attribute) = drop else {
			panic!("expected DropIdentityAttribute")
		};
		assert_eq!(attribute.name.text(), "org_id");
		assert!(attribute.if_exists);
	}

	#[test]
	fn test_create_user_still_parses_as_identity() {
		// Without ATTRIBUTE the dispatch has only two keywords to go on, so USER must still reach identity
		// parsing.
		let bump = Bump::new();
		let source = "CREATE USER alice";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let stmts = parser.parse().unwrap();
		let node = stmts[0].first_unchecked();
		assert!(matches!(node.as_create(), AstCreate::Identity(_)));
	}

	#[test]
	fn test_drop_user_still_parses_as_identity() {
		let bump = Bump::new();
		for source in ["DROP USER alice", "DROP USER IF EXISTS alice"] {
			let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
			let mut parser = Parser::new(&bump, source, tokens);
			let stmts = parser.parse().unwrap();
			let node = stmts[0].first_unchecked();
			let drop = match node {
				Ast::Drop(d) => d,
				_ => panic!("expected Drop"),
			};
			assert!(matches!(drop, AstDrop::Identity(_)), "source {source:?} must drop an identity");
		}
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
