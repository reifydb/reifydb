// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Fragment;
use serde::{Deserialize, Serialize};

/// Namespace identifier - always unqualified
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamespaceIdentifier<'a> {
	pub name: Fragment<'a>,
}

impl<'a> NamespaceIdentifier<'a> {
	pub fn new(name: Fragment<'a>) -> Self {
		Self {
			name,
		}
	}

	pub fn into_owned(self) -> NamespaceIdentifier<'static> {
		NamespaceIdentifier {
			name: Fragment::Owned(self.name.into_owned()),
		}
	}
}

/// Fully qualified source identifier for tables, views, and future source types
/// Used in logical and physical plans where everything must be fully qualified
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceIdentifier<'a> {
	/// Namespace containing this source
	pub namespace: Fragment<'a>,
	/// Source name
	pub name: Fragment<'a>,
	/// Alias for this source in query context
	pub alias: Option<Fragment<'a>>,
	/// Type of source - must be known
	pub kind: SourceKind,
}

impl<'a> SourceIdentifier<'a> {
	pub fn new(
		namespace: Fragment<'a>,
		name: Fragment<'a>,
		kind: SourceKind,
	) -> Self {
		Self {
			namespace,
			name,
			alias: None,
			kind,
		}
	}

	pub fn with_alias(mut self, alias: Fragment<'a>) -> Self {
		self.alias = Some(alias);
		self
	}

	pub fn with_kind(mut self, kind: SourceKind) -> Self {
		self.kind = kind;
		self
	}

	pub fn into_owned(self) -> SourceIdentifier<'static> {
		SourceIdentifier {
			namespace: Fragment::Owned(self.namespace.into_owned()),
			name: Fragment::Owned(self.name.into_owned()),
			alias: self
				.alias
				.map(|a| Fragment::Owned(a.into_owned())),
			kind: self.kind,
		}
	}

	/// Get the effective name for this source (alias if present, otherwise
	/// name)
	pub fn effective_name(&self) -> &str {
		self.alias
			.as_ref()
			.map(|a| a.text())
			.unwrap_or_else(|| self.name.text())
	}
}

/// Types of sources that can be referenced
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceKind {
	Table,
	TableVirtual,
	View,
	DeferredView,
	TransactionalView,
	Unknown,
}

impl SourceKind {
	pub fn is_view(&self) -> bool {
		matches!(
			self,
			SourceKind::View
				| SourceKind::DeferredView
				| SourceKind::TransactionalView
		)
	}
}

/// Column identifier with source qualification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnIdentifier<'a> {
	pub source: ColumnSource<'a>,
	pub name: Fragment<'a>,
}

impl<'a> ColumnIdentifier<'a> {
	pub fn with_source(
		namespace: Fragment<'a>,
		source: Fragment<'a>,
		name: Fragment<'a>,
	) -> Self {
		Self {
			source: ColumnSource::Source {
				namespace,
				source,
			},
			name,
		}
	}

	pub fn with_alias(alias: Fragment<'a>, name: Fragment<'a>) -> Self {
		Self {
			source: ColumnSource::Alias(alias),
			name,
		}
	}

	pub fn into_owned(self) -> ColumnIdentifier<'static> {
		ColumnIdentifier {
			source: self.source.into_owned(),
			name: Fragment::Owned(self.name.into_owned()),
		}
	}
}

/// How a column is qualified in plans (always fully qualified)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnSource<'a> {
	/// Fully qualified by namespace.source
	Source {
		namespace: Fragment<'a>,
		source: Fragment<'a>,
	},
	/// Qualified by alias (which maps to a fully qualified source)
	Alias(Fragment<'a>),
}

impl<'a> ColumnSource<'a> {
	pub fn into_owned(self) -> ColumnSource<'static> {
		match self {
			ColumnSource::Source {
				namespace,
				source,
			} => ColumnSource::Source {
				namespace: Fragment::Owned(
					namespace.into_owned(),
				),
				source: Fragment::Owned(source.into_owned()),
			},
			ColumnSource::Alias(alias) => ColumnSource::Alias(
				Fragment::Owned(alias.into_owned()),
			),
		}
	}

	pub fn as_fragment(&self) -> &Fragment<'a> {
		match self {
			ColumnSource::Source {
				source,
				..
			} => source,
			ColumnSource::Alias(alias) => alias,
		}
	}
}

/// Function identifier with namespace support
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionIdentifier<'a> {
	/// Namespace chain (e.g., ["pg_catalog", "string"] for
	/// pg_catalog::string::substr)
	pub namespaces: Vec<Fragment<'a>>,
	/// Function name
	pub name: Fragment<'a>,
}

impl<'a> FunctionIdentifier<'a> {
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

	pub fn into_owned(self) -> FunctionIdentifier<'static> {
		FunctionIdentifier {
			namespaces: self
				.namespaces
				.into_iter()
				.map(|n| Fragment::Owned(n.into_owned()))
				.collect(),
			name: Fragment::Owned(self.name.into_owned()),
		}
	}

	/// Get the fully qualified function name as a string
	pub fn qualified_name(&self) -> String {
		if self.namespaces.is_empty() {
			self.name.text().to_string()
		} else {
			let mut parts: Vec<&str> = self
				.namespaces
				.iter()
				.map(|n| n.text())
				.collect();
			parts.push(self.name.text());
			parts.join("::")
		}
	}
}

/// Fully qualified sequence identifier
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SequenceIdentifier<'a> {
	pub namespace: Fragment<'a>,
	pub name: Fragment<'a>,
}

impl<'a> SequenceIdentifier<'a> {
	pub fn new(namespace: Fragment<'a>, name: Fragment<'a>) -> Self {
		Self {
			namespace,
			name,
		}
	}

	pub fn into_owned(self) -> SequenceIdentifier<'static> {
		SequenceIdentifier {
			namespace: Fragment::Owned(self.namespace.into_owned()),
			name: Fragment::Owned(self.name.into_owned()),
		}
	}
}

/// Fully qualified index identifier
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexIdentifier<'a> {
	pub namespace: Fragment<'a>,
	pub table: Fragment<'a>,
	pub name: Fragment<'a>,
}

impl<'a> IndexIdentifier<'a> {
	pub fn new(
		namespace: Fragment<'a>,
		table: Fragment<'a>,
		name: Fragment<'a>,
	) -> Self {
		Self {
			namespace,
			table,
			name,
		}
	}

	pub fn into_owned(self) -> IndexIdentifier<'static> {
		IndexIdentifier {
			namespace: Fragment::Owned(self.namespace.into_owned()),
			table: Fragment::Owned(self.table.into_owned()),
			name: Fragment::Owned(self.name.into_owned()),
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::OwnedFragment;

	use super::*;

	#[test]
	fn test_source_identifier_creation() {
		let namespace =
			Fragment::Owned(OwnedFragment::testing("public"));
		let name = Fragment::Owned(OwnedFragment::testing("users"));
		let source = SourceIdentifier::new(
			namespace,
			name,
			SourceKind::Table,
		);

		assert_eq!(source.namespace.text(), "public");
		assert_eq!(source.name.text(), "users");
		assert!(source.alias.is_none());
		assert_eq!(source.kind, SourceKind::Table);
	}

	#[test]
	fn test_source_identifier_with_different_namespace() {
		let namespace =
			Fragment::Owned(OwnedFragment::testing("mynamespace"));
		let name = Fragment::Owned(OwnedFragment::testing("users"));
		let source = SourceIdentifier::new(
			namespace,
			name,
			SourceKind::Table,
		);

		assert_eq!(source.namespace.text(), "mynamespace");
		assert_eq!(source.name.text(), "users");
	}

	#[test]
	fn test_source_identifier_with_alias() {
		let namespace =
			Fragment::Owned(OwnedFragment::testing("public"));
		let name = Fragment::Owned(OwnedFragment::testing("users"));
		let alias = Fragment::Owned(OwnedFragment::testing("u"));
		let source = SourceIdentifier::new(
			namespace,
			name,
			SourceKind::Table,
		)
		.with_alias(alias);

		assert_eq!(source.effective_name(), "u");
		assert_eq!(source.alias.as_ref().unwrap().text(), "u");
	}

	#[test]
	fn test_column_identifier_with_alias() {
		let name = Fragment::Owned(OwnedFragment::testing("id"));
		let alias = Fragment::Owned(OwnedFragment::testing("u"));
		let column = ColumnIdentifier::with_alias(alias, name);

		assert!(matches!(column.source, ColumnSource::Alias(_)));
		if let ColumnSource::Alias(a) = &column.source {
			assert_eq!(a.text(), "u");
		}
		assert_eq!(column.name.text(), "id");
	}

	#[test]
	fn test_column_identifier_with_source() {
		let name = Fragment::Owned(OwnedFragment::testing("id"));
		let source = Fragment::Owned(OwnedFragment::testing("users"));
		let namespace =
			Fragment::Owned(OwnedFragment::testing("public"));
		let column =
			ColumnIdentifier::with_source(namespace, source, name);

		match &column.source {
			ColumnSource::Source {
				namespace,
				source,
			} => {
				assert_eq!(namespace.text(), "public");
				assert_eq!(source.text(), "users");
			}
			_ => panic!("Expected Source variant"),
		}
		assert_eq!(column.name.text(), "id");
	}

	#[test]
	fn test_function_identifier() {
		let name = Fragment::Owned(OwnedFragment::testing("substr"));
		let ns1 = Fragment::Owned(OwnedFragment::testing("pg_catalog"));
		let ns2 = Fragment::Owned(OwnedFragment::testing("string"));
		let func = FunctionIdentifier::new(name)
			.with_namespaces(vec![ns1, ns2]);

		assert_eq!(func.qualified_name(), "pg_catalog::string::substr");
	}

	#[test]
	fn test_source_kind_predicates() {
		assert!(SourceKind::View.is_view());
		assert!(SourceKind::DeferredView.is_view());
		assert!(SourceKind::TransactionalView.is_view());
		assert!(!SourceKind::Table.is_view());
	}

	#[test]
	fn test_into_owned() {
		let namespace =
			Fragment::Owned(OwnedFragment::testing("public"));
		let name = Fragment::Owned(OwnedFragment::testing("users"));
		let source = SourceIdentifier::new(
			namespace,
			name,
			SourceKind::Table,
		);

		let owned = source.into_owned();
		assert_eq!(owned.namespace.text(), "public");
		assert_eq!(owned.name.text(), "users");
		assert_eq!(owned.kind, SourceKind::Table);
	}
}
