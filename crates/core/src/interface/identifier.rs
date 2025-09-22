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

	pub fn to_static(&self) -> NamespaceIdentifier<'static> {
		NamespaceIdentifier {
			name: Fragment::owned_internal(self.name.text()),
		}
	}

	pub fn into_owned(self) -> NamespaceIdentifier<'static> {
		NamespaceIdentifier {
			name: Fragment::Owned(self.name.into_owned()),
		}
	}
}

/// Fully qualified table identifier
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableIdentifier<'a> {
	pub namespace: Fragment<'a>,
	pub name: Fragment<'a>,
	pub alias: Option<Fragment<'a>>,
}

/// Fully qualified virtual table identifier (system tables)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableVirtualIdentifier<'a> {
	pub namespace: Fragment<'a>,
	pub name: Fragment<'a>,
	pub alias: Option<Fragment<'a>>,
}

/// Fully qualified view identifier
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ViewIdentifier<'a> {
	pub namespace: Fragment<'a>,
	pub name: Fragment<'a>,
	pub alias: Option<Fragment<'a>>,
}

/// Fully qualified source identifier for tables, views, and future source types
/// Used in logical and physical plans where everything must be fully qualified
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SourceIdentifier<'a> {
	Table(TableIdentifier<'a>),
	TableVirtual(TableVirtualIdentifier<'a>),
	View(ViewIdentifier<'a>),
	RingBuffer(RingBufferIdentifier<'a>),
}

impl<'a> TableIdentifier<'a> {
	pub fn new(namespace: Fragment<'a>, name: Fragment<'a>) -> Self {
		Self {
			namespace,
			name,
			alias: None,
		}
	}

	pub fn to_static(&self) -> TableIdentifier<'static> {
		TableIdentifier {
			namespace: Fragment::owned_internal(self.namespace.text()),
			name: Fragment::owned_internal(self.name.text()),
			alias: self.alias.as_ref().map(|a| Fragment::owned_internal(a.text())),
		}
	}

	pub fn with_alias(mut self, alias: Fragment<'a>) -> Self {
		self.alias = Some(alias);
		self
	}

	pub fn into_owned(self) -> TableIdentifier<'static> {
		TableIdentifier {
			namespace: Fragment::Owned(self.namespace.into_owned()),
			name: Fragment::Owned(self.name.into_owned()),
			alias: self.alias.map(|a| Fragment::Owned(a.into_owned())),
		}
	}

	pub fn effective_name(&self) -> &str {
		self.alias.as_ref().map(|a| a.text()).unwrap_or_else(|| self.name.text())
	}
}

impl<'a> TableVirtualIdentifier<'a> {
	pub fn new(namespace: Fragment<'a>, name: Fragment<'a>) -> Self {
		Self {
			namespace,
			name,
			alias: None,
		}
	}

	pub fn to_static(&self) -> TableVirtualIdentifier<'static> {
		TableVirtualIdentifier {
			namespace: Fragment::owned_internal(self.namespace.text()),
			name: Fragment::owned_internal(self.name.text()),
			alias: self.alias.as_ref().map(|a| Fragment::owned_internal(a.text())),
		}
	}

	pub fn with_alias(mut self, alias: Fragment<'a>) -> Self {
		self.alias = Some(alias);
		self
	}

	pub fn into_owned(self) -> TableVirtualIdentifier<'static> {
		TableVirtualIdentifier {
			namespace: Fragment::Owned(self.namespace.into_owned()),
			name: Fragment::Owned(self.name.into_owned()),
			alias: self.alias.map(|a| Fragment::Owned(a.into_owned())),
		}
	}

	pub fn effective_name(&self) -> &str {
		self.alias.as_ref().map(|a| a.text()).unwrap_or_else(|| self.name.text())
	}
}

/// Fully qualified ring buffer identifier
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RingBufferIdentifier<'a> {
	pub namespace: Fragment<'a>,
	pub name: Fragment<'a>,
	pub alias: Option<Fragment<'a>>,
}

impl<'a> RingBufferIdentifier<'a> {
	pub fn new(namespace: Fragment<'a>, name: Fragment<'a>) -> Self {
		Self {
			namespace,
			name,
			alias: None,
		}
	}

	pub fn to_static(&self) -> RingBufferIdentifier<'static> {
		RingBufferIdentifier {
			namespace: Fragment::owned_internal(self.namespace.text()),
			name: Fragment::owned_internal(self.name.text()),
			alias: self.alias.as_ref().map(|a| Fragment::owned_internal(a.text())),
		}
	}

	pub fn with_alias(mut self, alias: Fragment<'a>) -> Self {
		self.alias = Some(alias);
		self
	}

	pub fn into_owned(self) -> RingBufferIdentifier<'static> {
		RingBufferIdentifier {
			namespace: Fragment::Owned(self.namespace.into_owned()),
			name: Fragment::Owned(self.name.into_owned()),
			alias: self.alias.map(|a| Fragment::Owned(a.into_owned())),
		}
	}

	pub fn effective_name(&self) -> &str {
		self.alias.as_ref().map(|a| a.text()).unwrap_or_else(|| self.name.text())
	}
}

impl<'a> ViewIdentifier<'a> {
	pub fn new(namespace: Fragment<'a>, name: Fragment<'a>) -> Self {
		Self {
			namespace,
			name,
			alias: None,
		}
	}

	pub fn to_static(&self) -> ViewIdentifier<'static> {
		ViewIdentifier {
			namespace: Fragment::owned_internal(self.namespace.text()),
			name: Fragment::owned_internal(self.name.text()),
			alias: self.alias.as_ref().map(|a| Fragment::owned_internal(a.text())),
		}
	}

	pub fn with_alias(mut self, alias: Fragment<'a>) -> Self {
		self.alias = Some(alias);
		self
	}

	pub fn into_owned(self) -> ViewIdentifier<'static> {
		ViewIdentifier {
			namespace: Fragment::Owned(self.namespace.into_owned()),
			name: Fragment::Owned(self.name.into_owned()),
			alias: self.alias.map(|a| Fragment::Owned(a.into_owned())),
		}
	}

	pub fn effective_name(&self) -> &str {
		self.alias.as_ref().map(|a| a.text()).unwrap_or_else(|| self.name.text())
	}
}

/// Represents a source identifier that hasn't been resolved to a specific type
/// yet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnresolvedSourceIdentifier<'a> {
	pub namespace: Option<Fragment<'a>>,
	pub name: Fragment<'a>,
	pub alias: Option<Fragment<'a>>,
}

impl<'a> UnresolvedSourceIdentifier<'a> {
	pub fn new(namespace: Option<Fragment<'a>>, name: Fragment<'a>) -> Self {
		Self {
			namespace,
			name,
			alias: None,
		}
	}

	pub fn with_alias(mut self, alias: Fragment<'a>) -> Self {
		self.alias = Some(alias);
		self
	}

	pub fn into_owned(self) -> UnresolvedSourceIdentifier<'static> {
		UnresolvedSourceIdentifier {
			namespace: self.namespace.map(|ns| Fragment::Owned(ns.into_owned())),
			name: Fragment::Owned(self.name.into_owned()),
			alias: self.alias.map(|a| Fragment::Owned(a.into_owned())),
		}
	}

	pub fn effective_name(&self) -> &str {
		self.alias.as_ref().map(|a| a.text()).unwrap_or_else(|| self.name.text())
	}
}

impl<'a> SourceIdentifier<'a> {
	pub fn into_owned(self) -> SourceIdentifier<'static> {
		match self {
			Self::Table(t) => SourceIdentifier::Table(t.into_owned()),
			Self::TableVirtual(t) => SourceIdentifier::TableVirtual(t.into_owned()),
			Self::View(v) => SourceIdentifier::View(v.into_owned()),
			Self::RingBuffer(r) => SourceIdentifier::RingBuffer(r.into_owned()),
		}
	}

	pub fn to_static(&self) -> SourceIdentifier<'static> {
		match self {
			Self::Table(t) => SourceIdentifier::Table(t.to_static()),
			Self::TableVirtual(t) => SourceIdentifier::TableVirtual(t.to_static()),
			Self::View(v) => SourceIdentifier::View(v.to_static()),
			Self::RingBuffer(r) => SourceIdentifier::RingBuffer(r.to_static()),
		}
	}

	/// Get the effective name for this source (alias if present, otherwise
	/// name)
	pub fn effective_name(&self) -> &str {
		match self {
			Self::Table(t) => t.effective_name(),
			Self::TableVirtual(t) => t.effective_name(),
			Self::View(v) => v.effective_name(),
			Self::RingBuffer(r) => r.effective_name(),
		}
	}

	/// Get the namespace fragment
	pub fn namespace(&self) -> &Fragment<'a> {
		match self {
			Self::Table(t) => &t.namespace,
			Self::TableVirtual(t) => &t.namespace,
			Self::View(v) => &v.namespace,
			Self::RingBuffer(r) => &r.namespace,
		}
	}

	/// Get the name fragment
	pub fn name(&self) -> &Fragment<'a> {
		match self {
			Self::Table(t) => &t.name,
			Self::TableVirtual(t) => &t.name,
			Self::View(v) => &v.name,
			Self::RingBuffer(r) => &r.name,
		}
	}

	/// Get the alias if present
	pub fn alias(&self) -> Option<&Fragment<'a>> {
		match self {
			Self::Table(t) => t.alias.as_ref(),
			Self::TableVirtual(t) => t.alias.as_ref(),
			Self::View(v) => v.alias.as_ref(),
			Self::RingBuffer(r) => r.alias.as_ref(),
		}
	}
}

/// Column identifier with source qualification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnIdentifier<'a> {
	pub source: ColumnSource<'a>,
	pub name: Fragment<'a>,
}

impl<'a> ColumnIdentifier<'a> {
	pub fn with_source(namespace: Fragment<'a>, source: Fragment<'a>, name: Fragment<'a>) -> Self {
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

	pub fn to_static(&self) -> ColumnIdentifier<'static> {
		ColumnIdentifier {
			source: self.source.to_static(),
			name: Fragment::owned_internal(self.name.text()),
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
				namespace: Fragment::Owned(namespace.into_owned()),
				source: Fragment::Owned(source.into_owned()),
			},
			ColumnSource::Alias(alias) => ColumnSource::Alias(Fragment::Owned(alias.into_owned())),
		}
	}

	pub fn to_static(&self) -> ColumnSource<'static> {
		match self {
			ColumnSource::Source {
				namespace,
				source,
			} => ColumnSource::Source {
				namespace: Fragment::owned_internal(namespace.text()),
				source: Fragment::owned_internal(source.text()),
			},
			ColumnSource::Alias(alias) => ColumnSource::Alias(Fragment::owned_internal(alias.text())),
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

	pub fn with_namespaces(mut self, namespaces: Vec<Fragment<'a>>) -> Self {
		self.namespaces = namespaces;
		self
	}

	pub fn into_owned(self) -> FunctionIdentifier<'static> {
		FunctionIdentifier {
			namespaces: self.namespaces.into_iter().map(|n| Fragment::Owned(n.into_owned())).collect(),
			name: Fragment::Owned(self.name.into_owned()),
		}
	}

	/// Get the fully qualified function name as a string
	pub fn qualified_name(&self) -> String {
		if self.namespaces.is_empty() {
			self.name.text().to_string()
		} else {
			let mut parts: Vec<&str> = self.namespaces.iter().map(|n| n.text()).collect();
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
	pub fn new(namespace: Fragment<'a>, table: Fragment<'a>, name: Fragment<'a>) -> Self {
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
		let namespace = Fragment::Owned(OwnedFragment::testing("public"));
		let name = Fragment::Owned(OwnedFragment::testing("users"));
		let source = SourceIdentifier::Table(TableIdentifier::new(namespace, name));

		if let SourceIdentifier::Table(t) = &source {
			assert_eq!(t.namespace.text(), "public");
			assert_eq!(t.name.text(), "users");
			assert!(t.alias.is_none());
		} else {
			panic!("Expected Table variant");
		}
	}

	#[test]
	fn test_source_identifier_with_different_namespace() {
		let namespace = Fragment::Owned(OwnedFragment::testing("mynamespace"));
		let name = Fragment::Owned(OwnedFragment::testing("users"));
		let source = SourceIdentifier::Table(TableIdentifier::new(namespace, name));

		if let SourceIdentifier::Table(t) = &source {
			assert_eq!(t.namespace.text(), "mynamespace");
			assert_eq!(t.name.text(), "users");
		} else {
			panic!("Expected Table variant");
		}
	}

	#[test]
	fn test_source_identifier_with_alias() {
		let namespace = Fragment::Owned(OwnedFragment::testing("public"));
		let name = Fragment::Owned(OwnedFragment::testing("users"));
		let alias = Fragment::Owned(OwnedFragment::testing("u"));
		let source = SourceIdentifier::Table(TableIdentifier::new(namespace, name).with_alias(alias));

		assert_eq!(source.effective_name(), "u");
		if let SourceIdentifier::Table(t) = &source {
			assert_eq!(t.alias.as_ref().unwrap().text(), "u");
		} else {
			panic!("Expected Table variant");
		}
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
		let namespace = Fragment::Owned(OwnedFragment::testing("public"));
		let column = ColumnIdentifier::with_source(namespace, source, name);

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
		let func = FunctionIdentifier::new(name).with_namespaces(vec![ns1, ns2]);

		assert_eq!(func.qualified_name(), "pg_catalog::string::substr");
	}

	#[test]
	fn test_into_owned() {
		let namespace = Fragment::Owned(OwnedFragment::testing("public"));
		let name = Fragment::Owned(OwnedFragment::testing("users"));
		let source = SourceIdentifier::Table(TableIdentifier::new(namespace, name));

		let owned = source.into_owned();
		if let SourceIdentifier::Table(t) = &owned {
			assert_eq!(t.namespace.text(), "public");
			assert_eq!(t.name.text(), "users");
		} else {
			panic!("Expected Table variant");
		}
	}
}
