// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Maybe-qualified identifier types for AST
//! These types allow optional qualification as they come directly from user
//! input

use reifydb_type::fragment::Fragment;

use crate::ast::tokenize::token::Token;

/// Represents a source identifier that hasn't been resolved to a specific type yet
/// Used in AST parsing before we know whether it's a table, view, or ring buffer
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnresolvedPrimitiveIdentifier {
	pub namespace: Option<Fragment>,
	pub name: Fragment,
	pub alias: Option<Fragment>,
}

impl UnresolvedPrimitiveIdentifier {
	pub fn new(namespace: Option<Fragment>, name: Fragment) -> Self {
		Self {
			namespace,
			name,
			alias: None,
		}
	}

	pub fn with_alias(mut self, alias: Fragment) -> Self {
		self.alias = Some(alias);
		self
	}

	pub fn effective_name(&self) -> &str {
		self.alias.as_ref().map(|a| a.text()).unwrap_or_else(|| self.name.text())
	}
}

/// An unqualified identifier that hasn't been parsed for qualification yet.
/// This is used in the AST for simple identifiers before they're resolved
/// to specific types (column, table, namespace, etc.)
#[derive(Debug, Clone, PartialEq)]
pub struct UnqualifiedIdentifier {
	pub token: Token,
}

impl UnqualifiedIdentifier {
	pub fn new(token: Token) -> Self {
		Self {
			token,
		}
	}

	pub fn from_fragment(fragment: Fragment) -> Self {
		use crate::ast::tokenize::token::TokenKind;
		Self {
			token: Token {
				kind: TokenKind::Identifier,
				fragment,
			},
		}
	}

	pub fn text(&self) -> &str {
		self.token.fragment.text()
	}

	pub fn fragment(&self) -> &Fragment {
		&self.token.fragment
	}

	pub fn into_fragment(self) -> Fragment {
		self.token.fragment
	}
}

/// Maybe-qualified namespace identifier - just a name
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedNamespaceIdentifier {
	pub name: Fragment,
}

impl MaybeQualifiedNamespaceIdentifier {
	pub fn new(name: Fragment) -> Self {
		Self {
			name,
		}
	}
}

/// Maybe-qualified table identifier for tables - namespace is optional
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedTableIdentifier {
	/// Namespace containing this table (optional in user input)
	pub namespace: Option<Fragment>,
	/// Table name
	pub name: Fragment,
	/// Alias for this table in query context
	pub alias: Option<Fragment>,
}

impl MaybeQualifiedTableIdentifier {
	pub fn new(name: Fragment) -> Self {
		Self {
			namespace: None,
			name,
			alias: None,
		}
	}

	pub fn with_namespace(mut self, namespace: Fragment) -> Self {
		self.namespace = Some(namespace);
		self
	}

	pub fn with_alias(mut self, alias: Fragment) -> Self {
		self.alias = Some(alias);
		self
	}
}

/// Maybe-qualified deferred view identifier - namespace is optional
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedDeferredViewIdentifier {
	/// Namespace containing this view (optional in user input)
	pub namespace: Option<Fragment>,
	/// View name
	pub name: Fragment,
	/// Alias for this view in query context
	pub alias: Option<Fragment>,
}

impl MaybeQualifiedDeferredViewIdentifier {
	pub fn new(name: Fragment) -> Self {
		Self {
			namespace: None,
			name,
			alias: None,
		}
	}

	pub fn with_namespace(mut self, namespace: Fragment) -> Self {
		self.namespace = Some(namespace);
		self
	}

	pub fn with_alias(mut self, alias: Fragment) -> Self {
		self.alias = Some(alias);
		self
	}
}

/// Maybe-qualified transactional view identifier - namespace is optional
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedTransactionalViewIdentifier {
	/// Namespace containing this view (optional in user input)
	pub namespace: Option<Fragment>,
	/// View name
	pub name: Fragment,
	/// Alias for this view in query context
	pub alias: Option<Fragment>,
}

impl MaybeQualifiedTransactionalViewIdentifier {
	pub fn new(name: Fragment) -> Self {
		Self {
			namespace: None,
			name,
			alias: None,
		}
	}

	pub fn with_namespace(mut self, namespace: Fragment) -> Self {
		self.namespace = Some(namespace);
		self
	}

	pub fn with_alias(mut self, alias: Fragment) -> Self {
		self.alias = Some(alias);
		self
	}
}

/// Maybe-qualified view identifier (generic) - namespace is optional
/// Used when we don't know the specific view type yet (e.g., ALTER VIEW)
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedViewIdentifier {
	/// Namespace containing this view (optional in user input)
	pub namespace: Option<Fragment>,
	/// View name
	pub name: Fragment,
	/// Alias for this view in query context
	pub alias: Option<Fragment>,
}

impl MaybeQualifiedViewIdentifier {
	pub fn new(name: Fragment) -> Self {
		Self {
			namespace: None,
			name,
			alias: None,
		}
	}

	pub fn with_namespace(mut self, namespace: Fragment) -> Self {
		self.namespace = Some(namespace);
		self
	}

	pub fn with_alias(mut self, alias: Fragment) -> Self {
		self.alias = Some(alias);
		self
	}
}

/// Maybe-qualified flow identifier - namespace is optional
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedFlowIdentifier {
	/// Namespace containing this flow (optional in user input)
	pub namespace: Option<Fragment>,
	/// Flow name
	pub name: Fragment,
	/// Alias for this flow in query context
	pub alias: Option<Fragment>,
}

impl MaybeQualifiedFlowIdentifier {
	pub fn new(name: Fragment) -> Self {
		Self {
			namespace: None,
			name,
			alias: None,
		}
	}

	pub fn with_namespace(mut self, namespace: Fragment) -> Self {
		self.namespace = Some(namespace);
		self
	}

	pub fn with_alias(mut self, alias: Fragment) -> Self {
		self.alias = Some(alias);
		self
	}
}

/// Maybe-qualified ring buffer identifier - namespace is optional
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedRingBufferIdentifier {
	/// Namespace containing this ring buffer (optional in user input)
	pub namespace: Option<Fragment>,
	/// Ring buffer name
	pub name: Fragment,
	/// Alias for this ring buffer in query context
	pub alias: Option<Fragment>,
}

impl MaybeQualifiedRingBufferIdentifier {
	pub fn new(name: Fragment) -> Self {
		Self {
			namespace: None,
			name,
			alias: None,
		}
	}

	pub fn with_namespace(mut self, namespace: Fragment) -> Self {
		self.namespace = Some(namespace);
		self
	}

	pub fn with_alias(mut self, alias: Fragment) -> Self {
		self.alias = Some(alias);
		self
	}
}

/// Maybe-qualified dictionary identifier - namespace is optional
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedDictionaryIdentifier {
	/// Namespace containing this dictionary (optional in user input)
	pub namespace: Option<Fragment>,
	/// Dictionary name
	pub name: Fragment,
}

impl MaybeQualifiedDictionaryIdentifier {
	pub fn new(name: Fragment) -> Self {
		Self {
			namespace: None,
			name,
		}
	}

	pub fn with_namespace(mut self, namespace: Fragment) -> Self {
		self.namespace = Some(namespace);
		self
	}
}

/// Maybe-qualified sequence identifier - namespace is optional
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedSequenceIdentifier {
	pub namespace: Option<Fragment>,
	pub name: Fragment,
}

impl MaybeQualifiedSequenceIdentifier {
	pub fn new(name: Fragment) -> Self {
		Self {
			namespace: None,
			name,
		}
	}

	pub fn with_namespace(mut self, namespace: Fragment) -> Self {
		self.namespace = Some(namespace);
		self
	}
}

/// Maybe-qualified index identifier - namespace is optional
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedIndexIdentifier {
	pub namespace: Option<Fragment>,
	pub table: Fragment,
	pub name: Fragment,
}

impl MaybeQualifiedIndexIdentifier {
	pub fn new(table: Fragment, name: Fragment) -> Self {
		Self {
			namespace: None,
			table,
			name,
		}
	}

	pub fn with_schema(mut self, namespace: Fragment) -> Self {
		self.namespace = Some(namespace);
		self
	}
}

/// How a maybe-qualified column is referenced
#[derive(Debug, Clone, PartialEq)]
pub enum MaybeQualifiedColumnPrimitive {
	/// Qualified by primitive name (table/view) - namespace still optional
	Primitive {
		namespace: Option<Fragment>,
		primitive: Fragment,
	},
	/// Qualified by alias
	Alias(Fragment),
	/// Not qualified (needs resolution based on context)
	Unqualified,
}

/// Maybe-qualified column identifier - primitive qualification is optional
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedColumnIdentifier {
	pub primitive: MaybeQualifiedColumnPrimitive,
	pub name: Fragment,
}

impl MaybeQualifiedColumnIdentifier {
	pub fn unqualified(name: Fragment) -> Self {
		Self {
			primitive: MaybeQualifiedColumnPrimitive::Unqualified,
			name,
		}
	}

	pub fn with_primitive(namespace: Option<Fragment>, primitive: Fragment, name: Fragment) -> Self {
		Self {
			primitive: MaybeQualifiedColumnPrimitive::Primitive {
				namespace,
				primitive,
			},
			name,
		}
	}

	pub fn with_alias(alias: Fragment, name: Fragment) -> Self {
		Self {
			primitive: MaybeQualifiedColumnPrimitive::Alias(alias),
			name,
		}
	}
}

/// Maybe-qualified function identifier - namespaces can be partial
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedFunctionIdentifier {
	/// Namespace chain (may be empty or partial)
	pub namespaces: Vec<Fragment>,
	/// Function name
	pub name: Fragment,
}

impl MaybeQualifiedFunctionIdentifier {
	pub fn new(name: Fragment) -> Self {
		Self {
			namespaces: Vec::new(),
			name,
		}
	}

	pub fn with_namespaces(mut self, namespaces: Vec<Fragment>) -> Self {
		self.namespaces = namespaces;
		self
	}
}
