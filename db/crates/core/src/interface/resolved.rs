// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::rc::Rc;

use reifydb_type::{Fragment, TypeConstraint};
use serde::{Deserialize, Serialize};

use super::{
	ColumnDef, NamespaceDef, TableDef, TableVirtualDef, ViewDef,
	identifier::{
		ColumnIdentifier, DeferredViewIdentifier, FunctionIdentifier, IndexIdentifier, NamespaceIdentifier,
		SequenceIdentifier, SourceIdentifier, TableIdentifier, TableVirtualIdentifier,
		TransactionalViewIdentifier,
	},
};

/// Resolved namespace with both identifier and definition
#[derive(Debug, Clone)]
pub struct ResolvedNamespace<'a> {
	pub identifier: NamespaceIdentifier<'a>,
	pub def: NamespaceDef,
}

impl<'a> ResolvedNamespace<'a> {
	pub fn new(identifier: NamespaceIdentifier<'a>, def: NamespaceDef) -> Self {
		Self {
			identifier,
			def,
		}
	}

	/// Get the namespace name
	pub fn name(&self) -> &str {
		&self.def.name
	}

	/// Get the fragment for error reporting
	pub fn fragment(&self) -> &Fragment<'a> {
		&self.identifier.name
	}
}

/// Resolved physical table
#[derive(Debug, Clone)]
pub struct ResolvedTable<'a> {
	pub identifier: TableIdentifier<'a>,
	pub namespace: Rc<ResolvedNamespace<'a>>,
	pub def: TableDef,
}

impl<'a> ResolvedTable<'a> {
	pub fn new(identifier: TableIdentifier<'a>, namespace: Rc<ResolvedNamespace<'a>>, def: TableDef) -> Self {
		Self {
			identifier,
			namespace,
			def,
		}
	}

	/// Get the table name
	pub fn name(&self) -> &str {
		&self.def.name
	}

	/// Get the effective name (considering aliases)
	pub fn effective_name(&self) -> &str {
		self.identifier.effective_name()
	}

	/// Get fully qualified name
	pub fn fully_qualified_name(&self) -> String {
		format!("{}.{}", self.namespace.name(), self.name())
	}

	/// Get columns
	pub fn columns(&self) -> &[ColumnDef] {
		&self.def.columns
	}

	/// Find a column by name
	pub fn find_column(&self, name: &str) -> Option<&ColumnDef> {
		self.def.columns.iter().find(|c| c.name == name)
	}
}

/// Resolved virtual table (system tables, information_schema)
#[derive(Debug, Clone)]
pub struct ResolvedTableVirtual<'a> {
	pub identifier: TableVirtualIdentifier<'a>,
	pub namespace: Rc<ResolvedNamespace<'a>>,
	pub def: TableVirtualDef,
}

impl<'a> ResolvedTableVirtual<'a> {
	pub fn new(
		identifier: TableVirtualIdentifier<'a>,
		namespace: Rc<ResolvedNamespace<'a>>,
		def: TableVirtualDef,
	) -> Self {
		Self {
			identifier,
			namespace,
			def,
		}
	}

	pub fn name(&self) -> &str {
		&self.def.name
	}

	pub fn effective_name(&self) -> &str {
		self.identifier.effective_name()
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.def.columns
	}
}

/// Resolved standard view
#[derive(Debug, Clone)]
pub struct ResolvedView<'a> {
	pub identifier: SourceIdentifier<'a>,
	pub namespace: Rc<ResolvedNamespace<'a>>,
	pub def: ViewDef,
}

impl<'a> ResolvedView<'a> {
	pub fn new(identifier: SourceIdentifier<'a>, namespace: Rc<ResolvedNamespace<'a>>, def: ViewDef) -> Self {
		Self {
			identifier,
			namespace,
			def,
		}
	}

	pub fn name(&self) -> &str {
		&self.def.name
	}

	pub fn effective_name(&self) -> &str {
		self.identifier.effective_name()
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.def.columns
	}

	pub fn fully_qualified_name(&self) -> String {
		format!("{}.{}", self.namespace.name(), self.name())
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedDeferredView<'a> {
	pub identifier: DeferredViewIdentifier<'a>,
	pub namespace: Rc<ResolvedNamespace<'a>>,
	pub def: ViewDef,
}

impl<'a> ResolvedDeferredView<'a> {
	pub fn new(identifier: DeferredViewIdentifier<'a>, namespace: Rc<ResolvedNamespace<'a>>, def: ViewDef) -> Self {
		Self {
			identifier,
			namespace,
			def,
		}
	}

	pub fn name(&self) -> &str {
		&self.def.name
	}

	pub fn effective_name(&self) -> &str {
		self.identifier.effective_name()
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.def.columns
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedTransactionalView<'a> {
	pub identifier: TransactionalViewIdentifier<'a>,
	pub namespace: Rc<ResolvedNamespace<'a>>,
	pub def: ViewDef,
}

impl<'a> ResolvedTransactionalView<'a> {
	pub fn new(
		identifier: TransactionalViewIdentifier<'a>,
		namespace: Rc<ResolvedNamespace<'a>>,
		def: ViewDef,
	) -> Self {
		Self {
			identifier,
			namespace,
			def,
		}
	}

	pub fn name(&self) -> &str {
		&self.def.name
	}

	pub fn effective_name(&self) -> &str {
		self.identifier.effective_name()
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.def.columns
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedSequence<'a> {
	pub identifier: SequenceIdentifier<'a>,
	pub namespace: Rc<ResolvedNamespace<'a>>,
	pub def: SequenceDef,
}

#[derive(Debug, Clone)]
pub struct ResolvedIndex<'a> {
	pub identifier: IndexIdentifier<'a>,
	pub table: Rc<ResolvedTable<'a>>,
	pub def: IndexDef,
}

#[derive(Debug, Clone)]
pub struct ResolvedFunction<'a> {
	pub identifier: FunctionIdentifier<'a>,
	pub namespace: Vec<Rc<ResolvedNamespace<'a>>>,
	pub def: FunctionDef,
}
/// Unified enum for any resolved source type
#[derive(Debug, Clone)]
pub enum ResolvedSource<'a> {
	Table(ResolvedTable<'a>),
	TableVirtual(ResolvedTableVirtual<'a>),
	View(ResolvedView<'a>),
	DeferredView(ResolvedDeferredView<'a>),
	TransactionalView(ResolvedTransactionalView<'a>),
}

impl<'a> ResolvedSource<'a> {
	/// Get the identifier for any source type as a SourceIdentifier enum
	pub fn identifier(&self) -> SourceIdentifier<'a> {
		match self {
			Self::Table(t) => SourceIdentifier::Table(t.identifier.clone()),
			Self::TableVirtual(t) => SourceIdentifier::TableVirtual(t.identifier.clone()),
			Self::View(v) => v.identifier.clone(), // Keep as is
			// for now since
			// ResolvedView
			// will be removed
			Self::DeferredView(v) => SourceIdentifier::DeferredView(v.identifier.clone()),
			Self::TransactionalView(v) => SourceIdentifier::TransactionalView(v.identifier.clone()),
		}
	}

	/// Get the namespace if this source has one
	pub fn namespace(&self) -> Option<&Rc<ResolvedNamespace<'a>>> {
		match self {
			Self::Table(t) => Some(&t.namespace),
			Self::TableVirtual(t) => Some(&t.namespace),
			Self::View(v) => Some(&v.namespace),
			Self::DeferredView(v) => Some(&v.namespace),
			Self::TransactionalView(v) => Some(&v.namespace),
		}
	}

	/// Get the effective name (considering aliases)
	pub fn effective_name(&self) -> &str {
		match self {
			Self::Table(t) => t.effective_name(),
			Self::TableVirtual(t) => t.effective_name(),
			Self::View(v) => v.effective_name(),
			Self::DeferredView(v) => v.effective_name(),
			Self::TransactionalView(v) => v.effective_name(),
		}
	}

	/// Check if this source supports indexes
	pub fn supports_indexes(&self) -> bool {
		matches!(self, Self::Table(_))
	}

	/// Check if this source supports mutations
	pub fn supports_mutations(&self) -> bool {
		matches!(self, Self::Table(_))
	}

	/// Get columns for this source
	pub fn columns(&self) -> &[ColumnDef] {
		match self {
			Self::Table(t) => t.columns(),
			Self::TableVirtual(t) => t.columns(),
			Self::View(v) => v.columns(),
			Self::DeferredView(v) => v.columns(),
			Self::TransactionalView(v) => v.columns(),
		}
	}

	/// Find a column by name
	pub fn find_column(&self, name: &str) -> Option<&ColumnDef> {
		self.columns().iter().find(|c| c.name == name)
	}

	/// Get the source kind name for error messages
	pub fn kind_name(&self) -> &'static str {
		match self {
			Self::Table(_) => "table",
			Self::TableVirtual(_) => "virtual table",
			Self::View(_) => "view",
			Self::DeferredView(_) => "deferred view",
			Self::TransactionalView(_) => "transactional view",
		}
	}

	/// Get fully qualified name if available
	pub fn fully_qualified_name(&self) -> Option<String> {
		match self {
			Self::Table(t) => Some(t.fully_qualified_name()),
			Self::View(v) => Some(v.fully_qualified_name()),
			Self::DeferredView(v) => Some(format!("{}.{}", v.namespace.name(), v.name())),
			Self::TransactionalView(v) => Some(format!("{}.{}", v.namespace.name(), v.name())),
			Self::TableVirtual(t) => Some(format!("{}.{}", t.namespace.name(), t.name())),
		}
	}

	/// Convert to a table if this is a table source
	pub fn as_table(&self) -> Option<&ResolvedTable<'a>> {
		match self {
			Self::Table(t) => Some(t),
			_ => None,
		}
	}

	/// Convert to a view if this is a view source
	pub fn as_view(&self) -> Option<&ResolvedView<'a>> {
		match self {
			Self::View(v) => Some(v),
			_ => None,
		}
	}
}

/// Column with its resolved source
#[derive(Debug, Clone)]
pub struct ResolvedColumn<'a> {
	/// Original identifier with fragments
	pub identifier: ColumnIdentifier<'a>,
	/// The resolved source this column belongs to
	pub source: Rc<ResolvedSource<'a>>,
	/// The column definition
	pub def: ColumnDef,
}

impl<'a> ResolvedColumn<'a> {
	pub fn new(identifier: ColumnIdentifier<'a>, source: Rc<ResolvedSource<'a>>, def: ColumnDef) -> Self {
		Self {
			identifier,
			source,
			def,
		}
	}

	/// Get the column name
	pub fn name(&self) -> &str {
		&self.def.name
	}

	/// Get the type constraint of this column
	pub fn type_constraint(&self) -> &TypeConstraint {
		&self.def.constraint
	}

	/// Check if column has auto increment
	pub fn is_auto_increment(&self) -> bool {
		self.def.auto_increment
	}

	/// Get the namespace this column belongs to
	pub fn namespace(&self) -> Option<&Rc<ResolvedNamespace<'a>>> {
		self.source.namespace()
	}

	/// Get fully qualified name
	pub fn fully_qualified_name(&self) -> String {
		match self.source.fully_qualified_name() {
			Some(source_name) => {
				format!("{}.{}", source_name, self.name())
			}
			None => format!("{}.{}", self.source.effective_name(), self.name()),
		}
	}

	/// Get the fragment for error reporting
	pub fn fragment(&self) -> &Fragment<'a> {
		&self.identifier.name
	}
}

// Placeholder types - these will be defined properly in catalog
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceDef {
	pub name: String,
	pub current_value: i64,
	pub increment: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
	pub name: String,
	pub columns: Vec<String>,
	pub unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionDef {
	pub name: String,
	pub parameters: Vec<String>,
	pub return_type: String,
}

#[cfg(test)]
mod tests {
	use reifydb_type::{OwnedFragment, Type};

	use super::*;
	use crate::interface::{ColumnId, NamespaceId, TableId, catalog::ColumnIndex};

	fn test_namespace_def() -> NamespaceDef {
		NamespaceDef {
			id: NamespaceId(1),
			name: "public".to_string(),
		}
	}

	fn test_table_def() -> TableDef {
		TableDef {
			id: TableId(1),
			namespace: NamespaceId(1),
			name: "users".to_string(),
			columns: vec![
				ColumnDef {
					id: ColumnId(1),
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Int8),
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
				},
				ColumnDef {
					id: ColumnId(2),
					name: "name".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
				},
			],
			primary_key: None,
		}
	}

	#[test]
	fn test_resolved_namespace() {
		let identifier = NamespaceIdentifier {
			name: Fragment::Owned(OwnedFragment::testing("public")),
		};
		let def = test_namespace_def();
		let resolved = ResolvedNamespace::new(identifier, def);

		assert_eq!(resolved.name(), "public");
		assert_eq!(resolved.fragment().text(), "public");
	}

	#[test]
	fn test_resolved_table() {
		let namespace_ident = NamespaceIdentifier {
			name: Fragment::Owned(OwnedFragment::testing("public")),
		};
		let namespace = Rc::new(ResolvedNamespace::new(namespace_ident, test_namespace_def()));

		let table_ident = TableIdentifier::new(
			Fragment::Owned(OwnedFragment::testing("public")),
			Fragment::Owned(OwnedFragment::testing("users")),
		);
		let table = ResolvedTable::new(table_ident, namespace.clone(), test_table_def());

		assert_eq!(table.name(), "users");
		assert_eq!(table.fully_qualified_name(), "public.users");
		assert_eq!(table.columns().len(), 2);
		assert!(table.find_column("id").is_some());
		assert!(table.find_column("nonexistent").is_none());
	}

	#[test]
	fn test_resolved_source_enum() {
		let namespace = Rc::new(ResolvedNamespace::new(
			NamespaceIdentifier {
				name: Fragment::Owned(OwnedFragment::testing("public")),
			},
			test_namespace_def(),
		));

		let table = ResolvedTable::new(
			TableIdentifier::new(
				Fragment::Owned(OwnedFragment::testing("public")),
				Fragment::Owned(OwnedFragment::testing("users")),
			),
			namespace,
			test_table_def(),
		);

		let source = ResolvedSource::Table(table);

		assert!(source.supports_indexes());
		assert!(source.supports_mutations());
		assert_eq!(source.kind_name(), "table");
		assert_eq!(source.effective_name(), "users");
		assert_eq!(source.fully_qualified_name(), Some("public.users".to_string()));
		assert!(source.as_table().is_some());
		assert!(source.as_view().is_none());
	}

	#[test]
	fn test_resolved_column() {
		let namespace = Rc::new(ResolvedNamespace::new(
			NamespaceIdentifier {
				name: Fragment::Owned(OwnedFragment::testing("public")),
			},
			test_namespace_def(),
		));

		let table = ResolvedTable::new(
			TableIdentifier::new(
				Fragment::Owned(OwnedFragment::testing("public")),
				Fragment::Owned(OwnedFragment::testing("users")),
			),
			namespace,
			test_table_def(),
		);

		let source = Rc::new(ResolvedSource::Table(table));

		let column_ident = ColumnIdentifier::with_source(
			Fragment::Owned(OwnedFragment::testing("public")),
			Fragment::Owned(OwnedFragment::testing("users")),
			Fragment::Owned(OwnedFragment::testing("id")),
		);

		let column_def = ColumnDef {
			id: ColumnId(1),
			name: "id".to_string(),
			constraint: TypeConstraint::unconstrained(Type::Int8),
			policies: vec![],
			index: ColumnIndex(0),
			auto_increment: false,
		};

		let column = ResolvedColumn::new(column_ident, source, column_def);

		assert_eq!(column.name(), "id");
		assert_eq!(column.type_constraint(), &TypeConstraint::unconstrained(Type::Int8));
		assert!(!column.is_auto_increment());
		assert_eq!(column.fully_qualified_name(), "public.users.id");
	}
}
