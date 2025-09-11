// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Maybe-qualified identifier types for AST
//! These types allow optional qualification as they come directly from user
//! input

use reifydb_core::interface::identifier::SourceKind;
use reifydb_type::Fragment;

use crate::ast::tokenize::Token;

/// An unqualified identifier that hasn't been parsed for qualification yet.
/// This is used in the AST for simple identifiers before they're resolved
/// to specific types (column, table, schema, etc.)
#[derive(Debug, Clone, PartialEq)]
pub struct UnqualifiedIdentifier<'a> {
	pub token: Token<'a>,
}

impl<'a> UnqualifiedIdentifier<'a> {
	pub fn new(token: Token<'a>) -> Self {
		Self {
			token,
		}
	}

	pub fn from_fragment(fragment: Fragment<'a>) -> Self {
		use crate::ast::tokenize::TokenKind;
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

	pub fn fragment(&self) -> &Fragment<'a> {
		&self.token.fragment
	}

	pub fn into_fragment(self) -> Fragment<'a> {
		self.token.fragment
	}
}

impl<'a> reifydb_type::IntoFragment<'a> for UnqualifiedIdentifier<'a> {
	fn into_fragment(self) -> Fragment<'a> {
		self.token.fragment
	}
}

/// Maybe-qualified schema identifier - just a name
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedSchemaIdentifier<'a> {
	pub name: Fragment<'a>,
}

impl<'a> MaybeQualifiedSchemaIdentifier<'a> {
	pub fn new(name: Fragment<'a>) -> Self {
		Self {
			name,
		}
	}
}

/// Maybe-qualified source identifier for tables/views - schema is optional
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedSourceIdentifier<'a> {
	/// Schema containing this source (optional in user input)
	pub schema: Option<Fragment<'a>>,
	/// Source name
	pub name: Fragment<'a>,
	/// Alias for this source in query context
	pub alias: Option<Fragment<'a>>,
	/// Type of source (may be Unknown before resolution)
	pub kind: SourceKind,
}

impl<'a> MaybeQualifiedSourceIdentifier<'a> {
	pub fn new(name: Fragment<'a>) -> Self {
		Self {
			schema: None,
			name,
			alias: None,
			kind: SourceKind::Unknown,
		}
	}

	pub fn with_schema(mut self, schema: Fragment<'a>) -> Self {
		self.schema = Some(schema);
		self
	}

	pub fn with_alias(mut self, alias: Fragment<'a>) -> Self {
		self.alias = Some(alias);
		self
	}

	pub fn with_kind(mut self, kind: SourceKind) -> Self {
		self.kind = kind;
		self
	}
}

/// Maybe-qualified sequence identifier - schema is optional
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedSequenceIdentifier<'a> {
	pub schema: Option<Fragment<'a>>,
	pub name: Fragment<'a>,
}

impl<'a> MaybeQualifiedSequenceIdentifier<'a> {
	pub fn new(name: Fragment<'a>) -> Self {
		Self {
			schema: None,
			name,
		}
	}

	pub fn with_schema(mut self, schema: Fragment<'a>) -> Self {
		self.schema = Some(schema);
		self
	}
}

/// Maybe-qualified index identifier - schema is optional
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedIndexIdentifier<'a> {
	pub schema: Option<Fragment<'a>>,
	pub table: Fragment<'a>,
	pub name: Fragment<'a>,
}

impl<'a> MaybeQualifiedIndexIdentifier<'a> {
	pub fn new(table: Fragment<'a>, name: Fragment<'a>) -> Self {
		Self {
			schema: None,
			table,
			name,
		}
	}

	pub fn with_schema(mut self, schema: Fragment<'a>) -> Self {
		self.schema = Some(schema);
		self
	}
}

/// How a maybe-qualified column is referenced
#[derive(Debug, Clone, PartialEq)]
pub enum MaybeQualifiedColumnSource<'a> {
	/// Qualified by source name (table/view) - schema still optional
	Source {
		schema: Option<Fragment<'a>>,
		source: Fragment<'a>,
	},
	/// Qualified by alias
	Alias(Fragment<'a>),
	/// Not qualified (needs resolution based on context)
	Unqualified,
}

/// Maybe-qualified column identifier - source qualification is optional
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedColumnIdentifier<'a> {
	pub source: MaybeQualifiedColumnSource<'a>,
	pub name: Fragment<'a>,
}

impl<'a> MaybeQualifiedColumnIdentifier<'a> {
	pub fn unqualified(name: Fragment<'a>) -> Self {
		Self {
			source: MaybeQualifiedColumnSource::Unqualified,
			name,
		}
	}

	pub fn with_source(
		schema: Option<Fragment<'a>>,
		source: Fragment<'a>,
		name: Fragment<'a>,
	) -> Self {
		Self {
			source: MaybeQualifiedColumnSource::Source {
				schema,
				source,
			},
			name,
		}
	}

	pub fn with_alias(alias: Fragment<'a>, name: Fragment<'a>) -> Self {
		Self {
			source: MaybeQualifiedColumnSource::Alias(alias),
			name,
		}
	}
}

/// Maybe-qualified function identifier - namespaces can be partial
#[derive(Debug, Clone, PartialEq)]
pub struct MaybeQualifiedFunctionIdentifier<'a> {
	/// Namespace chain (may be empty or partial)
	pub namespaces: Vec<Fragment<'a>>,
	/// Function name
	pub name: Fragment<'a>,
}

impl<'a> MaybeQualifiedFunctionIdentifier<'a> {
	pub fn new(name: Fragment<'a>) -> Self {
		Self {
			namespaces: Vec::new(),
			name,
		}
	}

	pub fn with_namespaces(
		mut self,
		namespaces: Vec<Fragment<'a>>,
	) -> Self {
		self.namespaces = namespaces;
		self
	}
}
