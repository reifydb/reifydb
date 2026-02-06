// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Maybe-qualified identifier types for AST
//! These types allow optional qualification as they come directly from user
//! input

use crate::{bump::BumpFragment, token::token::Token};

/// Represents a source identifier that hasn't been resolved to a specific type yet
/// Used in AST parsing before we know whether it's a table, view, or ring buffer
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnresolvedPrimitiveIdentifier<'bump> {
	pub namespace: Option<BumpFragment<'bump>>,
	pub name: BumpFragment<'bump>,
	pub alias: Option<BumpFragment<'bump>>,
}

impl<'bump> UnresolvedPrimitiveIdentifier<'bump> {
	pub fn new(namespace: Option<BumpFragment<'bump>>, name: BumpFragment<'bump>) -> Self {
		Self {
			namespace,
			name,
			alias: None,
		}
	}

	pub fn with_alias(mut self, alias: BumpFragment<'bump>) -> Self {
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
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UnqualifiedIdentifier<'bump> {
	pub token: Token<'bump>,
}

impl<'bump> UnqualifiedIdentifier<'bump> {
	pub fn new(token: Token<'bump>) -> Self {
		Self {
			token,
		}
	}

	pub fn from_fragment(fragment: BumpFragment<'bump>) -> Self {
		use crate::token::token::TokenKind;
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

	pub fn fragment(&self) -> &BumpFragment<'bump> {
		&self.token.fragment
	}

	pub fn into_fragment(self) -> BumpFragment<'bump> {
		self.token.fragment
	}
}

/// Maybe-qualified namespace identifier - just a name
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MaybeQualifiedNamespaceIdentifier<'bump> {
	pub name: BumpFragment<'bump>,
}

impl<'bump> MaybeQualifiedNamespaceIdentifier<'bump> {
	pub fn new(name: BumpFragment<'bump>) -> Self {
		Self {
			name,
		}
	}
}

/// Maybe-qualified table identifier for tables - namespace is optional
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MaybeQualifiedTableIdentifier<'bump> {
	/// Namespace containing this table (optional in user input)
	pub namespace: Option<BumpFragment<'bump>>,
	/// Table name
	pub name: BumpFragment<'bump>,
	/// Alias for this table in query context
	pub alias: Option<BumpFragment<'bump>>,
}

impl<'bump> MaybeQualifiedTableIdentifier<'bump> {
	pub fn new(name: BumpFragment<'bump>) -> Self {
		Self {
			namespace: None,
			name,
			alias: None,
		}
	}

	pub fn with_namespace(mut self, namespace: BumpFragment<'bump>) -> Self {
		self.namespace = Some(namespace);
		self
	}

	pub fn with_alias(mut self, alias: BumpFragment<'bump>) -> Self {
		self.alias = Some(alias);
		self
	}
}

/// Maybe-qualified deferred view identifier - namespace is optional
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MaybeQualifiedDeferredViewIdentifier<'bump> {
	/// Namespace containing this view (optional in user input)
	pub namespace: Option<BumpFragment<'bump>>,
	/// View name
	pub name: BumpFragment<'bump>,
	/// Alias for this view in query context
	pub alias: Option<BumpFragment<'bump>>,
}

impl<'bump> MaybeQualifiedDeferredViewIdentifier<'bump> {
	pub fn new(name: BumpFragment<'bump>) -> Self {
		Self {
			namespace: None,
			name,
			alias: None,
		}
	}

	pub fn with_namespace(mut self, namespace: BumpFragment<'bump>) -> Self {
		self.namespace = Some(namespace);
		self
	}

	pub fn with_alias(mut self, alias: BumpFragment<'bump>) -> Self {
		self.alias = Some(alias);
		self
	}
}

/// Maybe-qualified transactional view identifier - namespace is optional
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MaybeQualifiedTransactionalViewIdentifier<'bump> {
	/// Namespace containing this view (optional in user input)
	pub namespace: Option<BumpFragment<'bump>>,
	/// View name
	pub name: BumpFragment<'bump>,
	/// Alias for this view in query context
	pub alias: Option<BumpFragment<'bump>>,
}

impl<'bump> MaybeQualifiedTransactionalViewIdentifier<'bump> {
	pub fn new(name: BumpFragment<'bump>) -> Self {
		Self {
			namespace: None,
			name,
			alias: None,
		}
	}

	pub fn with_namespace(mut self, namespace: BumpFragment<'bump>) -> Self {
		self.namespace = Some(namespace);
		self
	}

	pub fn with_alias(mut self, alias: BumpFragment<'bump>) -> Self {
		self.alias = Some(alias);
		self
	}
}

/// Maybe-qualified view identifier (generic) - namespace is optional
/// Used when we don't know the specific view type yet (e.g., ALTER VIEW)
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MaybeQualifiedViewIdentifier<'bump> {
	/// Namespace containing this view (optional in user input)
	pub namespace: Option<BumpFragment<'bump>>,
	/// View name
	pub name: BumpFragment<'bump>,
	/// Alias for this view in query context
	pub alias: Option<BumpFragment<'bump>>,
}

impl<'bump> MaybeQualifiedViewIdentifier<'bump> {
	pub fn new(name: BumpFragment<'bump>) -> Self {
		Self {
			namespace: None,
			name,
			alias: None,
		}
	}

	pub fn with_namespace(mut self, namespace: BumpFragment<'bump>) -> Self {
		self.namespace = Some(namespace);
		self
	}

	pub fn with_alias(mut self, alias: BumpFragment<'bump>) -> Self {
		self.alias = Some(alias);
		self
	}
}

/// Maybe-qualified flow identifier - namespace is optional
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MaybeQualifiedFlowIdentifier<'bump> {
	/// Namespace containing this flow (optional in user input)
	pub namespace: Option<BumpFragment<'bump>>,
	/// Flow name
	pub name: BumpFragment<'bump>,
	/// Alias for this flow in query context
	pub alias: Option<BumpFragment<'bump>>,
}

impl<'bump> MaybeQualifiedFlowIdentifier<'bump> {
	pub fn new(name: BumpFragment<'bump>) -> Self {
		Self {
			namespace: None,
			name,
			alias: None,
		}
	}

	pub fn with_namespace(mut self, namespace: BumpFragment<'bump>) -> Self {
		self.namespace = Some(namespace);
		self
	}

	pub fn with_alias(mut self, alias: BumpFragment<'bump>) -> Self {
		self.alias = Some(alias);
		self
	}
}

/// Maybe-qualified ring buffer identifier - namespace is optional
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MaybeQualifiedRingBufferIdentifier<'bump> {
	/// Namespace containing this ring buffer (optional in user input)
	pub namespace: Option<BumpFragment<'bump>>,
	/// Ring buffer name
	pub name: BumpFragment<'bump>,
	/// Alias for this ring buffer in query context
	pub alias: Option<BumpFragment<'bump>>,
}

impl<'bump> MaybeQualifiedRingBufferIdentifier<'bump> {
	pub fn new(name: BumpFragment<'bump>) -> Self {
		Self {
			namespace: None,
			name,
			alias: None,
		}
	}

	pub fn with_namespace(mut self, namespace: BumpFragment<'bump>) -> Self {
		self.namespace = Some(namespace);
		self
	}

	pub fn with_alias(mut self, alias: BumpFragment<'bump>) -> Self {
		self.alias = Some(alias);
		self
	}
}

/// Maybe-qualified dictionary identifier - namespace is optional
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MaybeQualifiedDictionaryIdentifier<'bump> {
	/// Namespace containing this dictionary (optional in user input)
	pub namespace: Option<BumpFragment<'bump>>,
	/// Dictionary name
	pub name: BumpFragment<'bump>,
}

impl<'bump> MaybeQualifiedDictionaryIdentifier<'bump> {
	pub fn new(name: BumpFragment<'bump>) -> Self {
		Self {
			namespace: None,
			name,
		}
	}

	pub fn with_namespace(mut self, namespace: BumpFragment<'bump>) -> Self {
		self.namespace = Some(namespace);
		self
	}
}

/// Maybe-qualified sequence identifier - namespace is optional
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MaybeQualifiedSequenceIdentifier<'bump> {
	pub namespace: Option<BumpFragment<'bump>>,
	pub name: BumpFragment<'bump>,
}

impl<'bump> MaybeQualifiedSequenceIdentifier<'bump> {
	pub fn new(name: BumpFragment<'bump>) -> Self {
		Self {
			namespace: None,
			name,
		}
	}

	pub fn with_namespace(mut self, namespace: BumpFragment<'bump>) -> Self {
		self.namespace = Some(namespace);
		self
	}
}

/// Maybe-qualified index identifier - namespace is optional
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MaybeQualifiedIndexIdentifier<'bump> {
	pub namespace: Option<BumpFragment<'bump>>,
	pub table: BumpFragment<'bump>,
	pub name: BumpFragment<'bump>,
}

impl<'bump> MaybeQualifiedIndexIdentifier<'bump> {
	pub fn new(table: BumpFragment<'bump>, name: BumpFragment<'bump>) -> Self {
		Self {
			namespace: None,
			table,
			name,
		}
	}

	pub fn with_schema(mut self, namespace: BumpFragment<'bump>) -> Self {
		self.namespace = Some(namespace);
		self
	}
}

/// How a maybe-qualified column is referenced
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MaybeQualifiedColumnPrimitive<'bump> {
	/// Qualified by primitive name (table/view) - namespace still optional
	Primitive {
		namespace: Option<BumpFragment<'bump>>,
		primitive: BumpFragment<'bump>,
	},
	/// Qualified by alias
	Alias(BumpFragment<'bump>),
	/// Not qualified (needs resolution based on context)
	Unqualified,
}

/// Maybe-qualified column identifier - primitive qualification is optional
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MaybeQualifiedColumnIdentifier<'bump> {
	pub primitive: MaybeQualifiedColumnPrimitive<'bump>,
	pub name: BumpFragment<'bump>,
}

impl<'bump> MaybeQualifiedColumnIdentifier<'bump> {
	pub fn unqualified(name: BumpFragment<'bump>) -> Self {
		Self {
			primitive: MaybeQualifiedColumnPrimitive::Unqualified,
			name,
		}
	}

	pub fn with_primitive(
		namespace: Option<BumpFragment<'bump>>,
		primitive: BumpFragment<'bump>,
		name: BumpFragment<'bump>,
	) -> Self {
		Self {
			primitive: MaybeQualifiedColumnPrimitive::Primitive {
				namespace,
				primitive,
			},
			name,
		}
	}

	pub fn with_alias(alias: BumpFragment<'bump>, name: BumpFragment<'bump>) -> Self {
		Self {
			primitive: MaybeQualifiedColumnPrimitive::Alias(alias),
			name,
		}
	}
}

/// Maybe-qualified function identifier - namespaces can be partial
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedFunctionIdentifier<'bump> {
	/// Namespace chain (may be empty or partial)
	pub namespaces: Vec<BumpFragment<'bump>>,
	/// Function name
	pub name: BumpFragment<'bump>,
}

impl<'bump> MaybeQualifiedFunctionIdentifier<'bump> {
	pub fn new(name: BumpFragment<'bump>) -> Self {
		Self {
			namespaces: Vec::new(),
			name,
		}
	}

	pub fn with_namespaces(mut self, namespaces: Vec<BumpFragment<'bump>>) -> Self {
		self.namespaces = namespaces;
		self
	}
}
