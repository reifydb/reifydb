// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_type::{Fragment, TypeConstraint};
use serde::{Deserialize, Serialize};

use super::{
	ColumnDef, NamespaceDef, RingBufferDef, TableDef, TableVirtualDef, ViewDef,
	identifier::{
		ColumnIdentifier, DeferredViewIdentifier, FunctionIdentifier, IndexIdentifier, NamespaceIdentifier,
		RingBufferIdentifier, SequenceIdentifier, SourceIdentifier, TableIdentifier, TableVirtualIdentifier,
		TransactionalViewIdentifier,
	},
};

/// Resolved namespace with both identifier and definition
#[derive(Debug, Clone)]
pub struct ResolvedNamespace<'a>(Arc<ResolvedNamespaceInner<'a>>);

#[derive(Debug)]
struct ResolvedNamespaceInner<'a> {
	pub identifier: NamespaceIdentifier<'a>,
	pub def: NamespaceDef,
}

impl<'a> ResolvedNamespace<'a> {
	pub fn new(identifier: NamespaceIdentifier<'a>, def: NamespaceDef) -> Self {
		Self(Arc::new(ResolvedNamespaceInner {
			identifier,
			def,
		}))
	}

	/// Get the namespace name
	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	/// Get the namespace def
	pub fn def(&self) -> &NamespaceDef {
		&self.0.def
	}

	/// Get the fragment for error reporting
	pub fn fragment(&self) -> &Fragment<'a> {
		&self.0.identifier.name
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_owned_resolved_namespace(&self) -> ResolvedNamespace<'static> {
		ResolvedNamespace(Arc::new(ResolvedNamespaceInner {
			identifier: self.0.identifier.to_owned_identifier(),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved physical table
#[derive(Debug, Clone)]
pub struct ResolvedTable<'a>(Arc<ResolvedTableInner<'a>>);

#[derive(Debug)]
struct ResolvedTableInner<'a> {
	pub identifier: TableIdentifier<'a>,
	pub namespace: ResolvedNamespace<'a>,
	pub def: TableDef,
}

impl<'a> ResolvedTable<'a> {
	pub fn new(identifier: TableIdentifier<'a>, namespace: ResolvedNamespace<'a>, def: TableDef) -> Self {
		Self(Arc::new(ResolvedTableInner {
			identifier,
			namespace,
			def,
		}))
	}

	/// Get the table name
	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	/// Get the table def
	pub fn def(&self) -> &TableDef {
		&self.0.def
	}

	/// Get the namespace
	pub fn namespace(&self) -> &ResolvedNamespace<'a> {
		&self.0.namespace
	}

	/// Get the identifier
	pub fn identifier(&self) -> &TableIdentifier<'a> {
		&self.0.identifier
	}

	/// Get the effective name (considering aliases)
	pub fn effective_name(&self) -> &str {
		self.0.identifier.effective_name()
	}

	/// Get fully qualified name
	pub fn fully_qualified_name(&self) -> String {
		format!("{}.{}", self.0.namespace.name(), self.name())
	}

	/// Get columns
	pub fn columns(&self) -> &[ColumnDef] {
		&self.0.def.columns
	}

	/// Find a column by name
	pub fn find_column(&self, name: &str) -> Option<&ColumnDef> {
		self.0.def.columns.iter().find(|c| c.name == name)
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_owned_resolved_table(&self) -> ResolvedTable<'static> {
		ResolvedTable(Arc::new(ResolvedTableInner {
			identifier: self.0.identifier.to_owned_identifier(),
			namespace: self.0.namespace.to_owned_resolved_namespace(),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved virtual table (system tables, information_schema)
#[derive(Debug, Clone)]
pub struct ResolvedTableVirtual<'a>(Arc<ResolvedTableVirtualInner<'a>>);

#[derive(Debug)]
struct ResolvedTableVirtualInner<'a> {
	pub identifier: TableVirtualIdentifier<'a>,
	pub namespace: ResolvedNamespace<'a>,
	pub def: TableVirtualDef,
}

impl<'a> ResolvedTableVirtual<'a> {
	pub fn new(
		identifier: TableVirtualIdentifier<'a>,
		namespace: ResolvedNamespace<'a>,
		def: TableVirtualDef,
	) -> Self {
		Self(Arc::new(ResolvedTableVirtualInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	pub fn def(&self) -> &TableVirtualDef {
		&self.0.def
	}

	pub fn namespace(&self) -> &ResolvedNamespace<'a> {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &TableVirtualIdentifier<'a> {
		&self.0.identifier
	}

	pub fn effective_name(&self) -> &str {
		self.0.identifier.effective_name()
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.0.def.columns
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_owned_resolved_table_virtual(&self) -> ResolvedTableVirtual<'static> {
		ResolvedTableVirtual(Arc::new(ResolvedTableVirtualInner {
			identifier: self.0.identifier.to_owned_identifier(),
			namespace: self.0.namespace.to_owned_resolved_namespace(),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved standard view
#[derive(Debug, Clone)]
pub struct ResolvedView<'a>(Arc<ResolvedViewInner<'a>>);

#[derive(Debug)]
struct ResolvedViewInner<'a> {
	pub identifier: SourceIdentifier<'a>,
	pub namespace: ResolvedNamespace<'a>,
	pub def: ViewDef,
}

impl<'a> ResolvedView<'a> {
	pub fn new(identifier: SourceIdentifier<'a>, namespace: ResolvedNamespace<'a>, def: ViewDef) -> Self {
		Self(Arc::new(ResolvedViewInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	pub fn def(&self) -> &ViewDef {
		&self.0.def
	}

	pub fn namespace(&self) -> &ResolvedNamespace<'a> {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &SourceIdentifier<'a> {
		&self.0.identifier
	}

	pub fn effective_name(&self) -> &str {
		self.0.identifier.effective_name()
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.0.def.columns
	}

	pub fn fully_qualified_name(&self) -> String {
		format!("{}.{}", self.0.namespace.name(), self.name())
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_owned_resolved_view(&self) -> ResolvedView<'static> {
		ResolvedView(Arc::new(ResolvedViewInner {
			identifier: self.0.identifier.to_owned_identifier(),
			namespace: self.0.namespace.to_owned_resolved_namespace(),
			def: self.0.def.clone(),
		}))
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedDeferredView<'a>(Arc<ResolvedDeferredViewInner<'a>>);

#[derive(Debug)]
struct ResolvedDeferredViewInner<'a> {
	pub identifier: DeferredViewIdentifier<'a>,
	pub namespace: ResolvedNamespace<'a>,
	pub def: ViewDef,
}

impl<'a> ResolvedDeferredView<'a> {
	pub fn new(identifier: DeferredViewIdentifier<'a>, namespace: ResolvedNamespace<'a>, def: ViewDef) -> Self {
		Self(Arc::new(ResolvedDeferredViewInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	pub fn def(&self) -> &ViewDef {
		&self.0.def
	}

	pub fn namespace(&self) -> &ResolvedNamespace<'a> {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &DeferredViewIdentifier<'a> {
		&self.0.identifier
	}

	pub fn effective_name(&self) -> &str {
		self.0.identifier.effective_name()
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.0.def.columns
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedTransactionalView<'a>(Arc<ResolvedTransactionalViewInner<'a>>);

#[derive(Debug)]
struct ResolvedTransactionalViewInner<'a> {
	pub identifier: TransactionalViewIdentifier<'a>,
	pub namespace: ResolvedNamespace<'a>,
	pub def: ViewDef,
}

impl<'a> ResolvedTransactionalView<'a> {
	pub fn new(
		identifier: TransactionalViewIdentifier<'a>,
		namespace: ResolvedNamespace<'a>,
		def: ViewDef,
	) -> Self {
		Self(Arc::new(ResolvedTransactionalViewInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	pub fn def(&self) -> &ViewDef {
		&self.0.def
	}

	pub fn namespace(&self) -> &ResolvedNamespace<'a> {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &TransactionalViewIdentifier<'a> {
		&self.0.identifier
	}

	pub fn effective_name(&self) -> &str {
		self.0.identifier.effective_name()
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.0.def.columns
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedRingBuffer<'a>(Arc<ResolvedRingBufferInner<'a>>);

#[derive(Debug)]
struct ResolvedRingBufferInner<'a> {
	pub identifier: RingBufferIdentifier<'a>,
	pub namespace: ResolvedNamespace<'a>,
	pub def: RingBufferDef,
}

impl<'a> ResolvedRingBuffer<'a> {
	pub fn new(identifier: RingBufferIdentifier<'a>, namespace: ResolvedNamespace<'a>, def: RingBufferDef) -> Self {
		Self(Arc::new(ResolvedRingBufferInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	pub fn def(&self) -> &RingBufferDef {
		&self.0.def
	}

	pub fn namespace(&self) -> &ResolvedNamespace<'a> {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &RingBufferIdentifier<'a> {
		&self.0.identifier
	}

	pub fn effective_name(&self) -> &str {
		self.0.identifier.effective_name()
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.0.def.columns
	}

	pub fn fully_qualified_name(&self) -> String {
		format!("{}.{}", self.0.namespace.name(), self.name())
	}

	pub fn capacity(&self) -> u64 {
		self.0.def.capacity
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_owned_resolved_ring_buffer(&self) -> ResolvedRingBuffer<'static> {
		ResolvedRingBuffer(Arc::new(ResolvedRingBufferInner {
			identifier: self.0.identifier.to_owned_identifier(),
			namespace: self.0.namespace.to_owned_resolved_namespace(),
			def: self.0.def.clone(),
		}))
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedSequence<'a>(Arc<ResolvedSequenceInner<'a>>);

#[derive(Debug)]
struct ResolvedSequenceInner<'a> {
	pub identifier: SequenceIdentifier<'a>,
	pub namespace: ResolvedNamespace<'a>,
	pub def: SequenceDef,
}

impl<'a> ResolvedSequence<'a> {
	pub fn new(identifier: SequenceIdentifier<'a>, namespace: ResolvedNamespace<'a>, def: SequenceDef) -> Self {
		Self(Arc::new(ResolvedSequenceInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn identifier(&self) -> &SequenceIdentifier<'a> {
		&self.0.identifier
	}

	pub fn namespace(&self) -> &ResolvedNamespace<'a> {
		&self.0.namespace
	}

	pub fn def(&self) -> &SequenceDef {
		&self.0.def
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedIndex<'a>(Arc<ResolvedIndexInner<'a>>);

#[derive(Debug)]
struct ResolvedIndexInner<'a> {
	pub identifier: IndexIdentifier<'a>,
	pub table: ResolvedTable<'a>,
	pub def: IndexDef,
}

impl<'a> ResolvedIndex<'a> {
	pub fn new(identifier: IndexIdentifier<'a>, table: ResolvedTable<'a>, def: IndexDef) -> Self {
		Self(Arc::new(ResolvedIndexInner {
			identifier,
			table,
			def,
		}))
	}

	pub fn identifier(&self) -> &IndexIdentifier<'a> {
		&self.0.identifier
	}

	pub fn table(&self) -> &ResolvedTable<'a> {
		&self.0.table
	}

	pub fn def(&self) -> &IndexDef {
		&self.0.def
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedFunction<'a>(Arc<ResolvedFunctionInner<'a>>);

#[derive(Debug)]
struct ResolvedFunctionInner<'a> {
	pub identifier: FunctionIdentifier<'a>,
	pub namespace: Vec<ResolvedNamespace<'a>>,
	pub def: FunctionDef,
}

impl<'a> ResolvedFunction<'a> {
	pub fn new(
		identifier: FunctionIdentifier<'a>,
		namespace: Vec<ResolvedNamespace<'a>>,
		def: FunctionDef,
	) -> Self {
		Self(Arc::new(ResolvedFunctionInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn identifier(&self) -> &FunctionIdentifier<'a> {
		&self.0.identifier
	}

	pub fn namespace(&self) -> &[ResolvedNamespace<'a>] {
		&self.0.namespace
	}

	pub fn def(&self) -> &FunctionDef {
		&self.0.def
	}
}
/// Unified enum for any resolved source type
#[derive(Debug, Clone)]
pub enum ResolvedSource<'a> {
	Table(ResolvedTable<'a>),
	TableVirtual(ResolvedTableVirtual<'a>),
	View(ResolvedView<'a>),
	DeferredView(ResolvedDeferredView<'a>),
	TransactionalView(ResolvedTransactionalView<'a>),
	RingBuffer(ResolvedRingBuffer<'a>),
}

impl<'a> ResolvedSource<'a> {
	/// Get the identifier for any source type as a SourceIdentifier enum
	pub fn identifier(&self) -> SourceIdentifier<'a> {
		match self {
			Self::Table(t) => SourceIdentifier::Table(t.identifier().clone()),
			Self::TableVirtual(t) => SourceIdentifier::TableVirtual(t.identifier().clone()),
			Self::View(v) => v.identifier().clone(),
			Self::DeferredView(v) => SourceIdentifier::DeferredView(v.identifier().clone()),
			Self::TransactionalView(v) => SourceIdentifier::TransactionalView(v.identifier().clone()),
			Self::RingBuffer(r) => SourceIdentifier::RingBuffer(r.identifier().clone()),
		}
	}

	/// Get the namespace if this source has one
	pub fn namespace(&self) -> Option<&ResolvedNamespace<'a>> {
		match self {
			Self::Table(t) => Some(t.namespace()),
			Self::TableVirtual(t) => Some(t.namespace()),
			Self::View(v) => Some(v.namespace()),
			Self::DeferredView(v) => Some(v.namespace()),
			Self::TransactionalView(v) => Some(v.namespace()),
			Self::RingBuffer(r) => Some(r.namespace()),
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
			Self::RingBuffer(r) => r.effective_name(),
		}
	}

	/// Check if this source supports indexes
	pub fn supports_indexes(&self) -> bool {
		matches!(self, Self::Table(_))
	}

	/// Check if this source supports mutations
	pub fn supports_mutations(&self) -> bool {
		matches!(self, Self::Table(_) | Self::RingBuffer(_))
	}

	/// Get columns for this source
	pub fn columns(&self) -> &[ColumnDef] {
		match self {
			Self::Table(t) => t.columns(),
			Self::TableVirtual(t) => t.columns(),
			Self::View(v) => v.columns(),
			Self::DeferredView(v) => v.columns(),
			Self::TransactionalView(v) => v.columns(),
			Self::RingBuffer(r) => r.columns(),
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
			Self::RingBuffer(_) => "ring buffer",
		}
	}

	/// Get fully qualified name if available
	pub fn fully_qualified_name(&self) -> Option<String> {
		match self {
			Self::Table(t) => Some(t.fully_qualified_name()),
			Self::View(v) => Some(v.fully_qualified_name()),
			Self::DeferredView(v) => Some(format!("{}.{}", v.namespace().name(), v.name())),
			Self::TransactionalView(v) => Some(format!("{}.{}", v.namespace().name(), v.name())),
			Self::TableVirtual(t) => Some(format!("{}.{}", t.namespace().name(), t.name())),
			Self::RingBuffer(r) => Some(r.fully_qualified_name()),
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

	/// Convert to a ring buffer if this is a ring buffer source
	pub fn as_ring_buffer(&self) -> Option<&ResolvedRingBuffer<'a>> {
		match self {
			Self::RingBuffer(r) => Some(r),
			_ => None,
		}
	}
}

/// Column with its resolved source
#[derive(Debug, Clone)]
pub struct ResolvedColumn<'a>(Arc<ResolvedColumnInner<'a>>);

#[derive(Debug)]
struct ResolvedColumnInner<'a> {
	/// Original identifier with fragments
	pub identifier: ColumnIdentifier<'a>,
	/// The resolved source this column belongs to
	pub source: ResolvedSource<'a>,
	/// The column definition
	pub def: ColumnDef,
}

impl<'a> ResolvedColumn<'a> {
	pub fn new(identifier: ColumnIdentifier<'a>, source: ResolvedSource<'a>, def: ColumnDef) -> Self {
		Self(Arc::new(ResolvedColumnInner {
			identifier,
			source,
			def,
		}))
	}

	/// Get the column name
	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	/// Get the column def
	pub fn def(&self) -> &ColumnDef {
		&self.0.def
	}

	/// Get the identifier
	pub fn identifier(&self) -> &ColumnIdentifier<'a> {
		&self.0.identifier
	}

	/// Get the source
	pub fn source(&self) -> &ResolvedSource<'a> {
		&self.0.source
	}

	/// Get the type constraint of this column
	pub fn type_constraint(&self) -> &TypeConstraint {
		&self.0.def.constraint
	}

	/// Check if column has auto increment
	pub fn is_auto_increment(&self) -> bool {
		self.0.def.auto_increment
	}

	/// Get the namespace this column belongs to
	pub fn namespace(&self) -> Option<&ResolvedNamespace<'a>> {
		self.0.source.namespace()
	}

	/// Get fully qualified name
	pub fn fully_qualified_name(&self) -> String {
		match self.0.source.fully_qualified_name() {
			Some(source_name) => {
				format!("{}.{}", source_name, self.name())
			}
			None => format!("{}.{}", self.0.source.effective_name(), self.name()),
		}
	}

	/// Get the fragment for error reporting
	pub fn fragment(&self) -> &Fragment<'a> {
		&self.0.identifier.name
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
		let namespace = ResolvedNamespace::new(namespace_ident, test_namespace_def());

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
		let namespace = ResolvedNamespace::new(
			NamespaceIdentifier {
				name: Fragment::Owned(OwnedFragment::testing("public")),
			},
			test_namespace_def(),
		);

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
		let namespace = ResolvedNamespace::new(
			NamespaceIdentifier {
				name: Fragment::Owned(OwnedFragment::testing("public")),
			},
			test_namespace_def(),
		);

		let table = ResolvedTable::new(
			TableIdentifier::new(
				Fragment::Owned(OwnedFragment::testing("public")),
				Fragment::Owned(OwnedFragment::testing("users")),
			),
			namespace,
			test_table_def(),
		);

		let source = ResolvedSource::Table(table);

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
