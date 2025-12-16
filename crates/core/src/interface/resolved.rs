// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_type::{Fragment, Type, TypeConstraint, diagnostic::number::NumberOfRangeColumnDescriptor};
use serde::{Deserialize, Serialize};

use super::{
	ColumnDef, ColumnPolicyKind, DictionaryDef, FlowDef, NamespaceDef, RingBufferDef, TableDef, TableVirtualDef,
	ViewDef,
};

/// Resolved namespace with both identifier and definition
#[derive(Debug, Clone)]
pub struct ResolvedNamespace<'a>(Arc<ResolvedNamespaceInner<'a>>);

#[derive(Debug)]
struct ResolvedNamespaceInner<'a> {
	pub identifier: Fragment<'a>,
	pub def: NamespaceDef,
}

impl<'a> ResolvedNamespace<'a> {
	pub fn new(identifier: Fragment<'a>, def: NamespaceDef) -> Self {
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
		&self.0.identifier
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedNamespace<'static> {
		ResolvedNamespace(Arc::new(ResolvedNamespaceInner {
			identifier: Fragment::owned_internal(self.0.identifier.text()),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved physical table
#[derive(Debug, Clone)]
pub struct ResolvedTable<'a>(Arc<ResolvedTableInner<'a>>);

#[derive(Debug)]
struct ResolvedTableInner<'a> {
	pub identifier: Fragment<'a>,
	pub namespace: ResolvedNamespace<'a>,
	pub def: TableDef,
}

impl<'a> ResolvedTable<'a> {
	pub fn new(identifier: Fragment<'a>, namespace: ResolvedNamespace<'a>, def: TableDef) -> Self {
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
	pub fn identifier(&self) -> &Fragment<'a> {
		&self.0.identifier
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
	pub fn to_static(&self) -> ResolvedTable<'static> {
		ResolvedTable(Arc::new(ResolvedTableInner {
			identifier: Fragment::owned_internal(self.0.identifier.text()),
			namespace: self.0.namespace.to_static(),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved virtual table (system tables, information_schema)
#[derive(Debug, Clone)]
pub struct ResolvedTableVirtual<'a>(Arc<ResolvedTableVirtualInner<'a>>);

#[derive(Debug)]
struct ResolvedTableVirtualInner<'a> {
	pub identifier: Fragment<'a>,
	pub namespace: ResolvedNamespace<'a>,
	pub def: TableVirtualDef,
}

impl<'a> ResolvedTableVirtual<'a> {
	pub fn new(identifier: Fragment<'a>, namespace: ResolvedNamespace<'a>, def: TableVirtualDef) -> Self {
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

	pub fn identifier(&self) -> &Fragment<'a> {
		&self.0.identifier
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.0.def.columns
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedTableVirtual<'static> {
		ResolvedTableVirtual(Arc::new(ResolvedTableVirtualInner {
			identifier: Fragment::owned_internal(self.0.identifier.text()),
			namespace: self.0.namespace.to_static(),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved ring buffer
#[derive(Debug, Clone)]
pub struct ResolvedRingBuffer<'a>(Arc<ResolvedRingBufferInner<'a>>);

#[derive(Debug)]
struct ResolvedRingBufferInner<'a> {
	pub identifier: Fragment<'a>,
	pub namespace: ResolvedNamespace<'a>,
	pub def: RingBufferDef,
}

impl<'a> ResolvedRingBuffer<'a> {
	pub fn new(identifier: Fragment<'a>, namespace: ResolvedNamespace<'a>, def: RingBufferDef) -> Self {
		Self(Arc::new(ResolvedRingBufferInner {
			identifier,
			namespace,
			def,
		}))
	}

	/// Get the ring buffer name
	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	/// Get the ring buffer def
	pub fn def(&self) -> &RingBufferDef {
		&self.0.def
	}

	/// Get the namespace
	pub fn namespace(&self) -> &ResolvedNamespace<'a> {
		&self.0.namespace
	}

	/// Get the identifier
	pub fn identifier(&self) -> &Fragment<'a> {
		&self.0.identifier
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
	pub fn to_static(&self) -> ResolvedRingBuffer<'static> {
		ResolvedRingBuffer(Arc::new(ResolvedRingBufferInner {
			identifier: Fragment::owned_internal(self.0.identifier.text()),
			namespace: self.0.namespace.to_static(),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved flow
#[derive(Debug, Clone)]
pub struct ResolvedFlow<'a>(Arc<ResolvedFlowInner<'a>>);

#[derive(Debug)]
struct ResolvedFlowInner<'a> {
	pub identifier: Fragment<'a>,
	pub namespace: ResolvedNamespace<'a>,
	pub def: FlowDef,
}

impl<'a> ResolvedFlow<'a> {
	pub fn new(identifier: Fragment<'a>, namespace: ResolvedNamespace<'a>, def: FlowDef) -> Self {
		Self(Arc::new(ResolvedFlowInner {
			identifier,
			namespace,
			def,
		}))
	}

	/// Get the flow name
	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	/// Get the flow def
	pub fn def(&self) -> &FlowDef {
		&self.0.def
	}

	/// Get the namespace
	pub fn namespace(&self) -> &ResolvedNamespace<'a> {
		&self.0.namespace
	}

	/// Get the identifier
	pub fn identifier(&self) -> &Fragment<'a> {
		&self.0.identifier
	}

	/// Get fully qualified name
	pub fn fully_qualified_name(&self) -> String {
		format!("{}.{}", self.0.namespace.name(), self.name())
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedFlow<'static> {
		ResolvedFlow(Arc::new(ResolvedFlowInner {
			identifier: Fragment::owned_internal(self.0.identifier.text()),
			namespace: self.0.namespace.to_static(),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved dictionary
#[derive(Debug, Clone)]
pub struct ResolvedDictionary<'a>(Arc<ResolvedDictionaryInner<'a>>);

#[derive(Debug)]
struct ResolvedDictionaryInner<'a> {
	pub identifier: Fragment<'a>,
	pub namespace: ResolvedNamespace<'a>,
	pub def: DictionaryDef,
}

impl<'a> ResolvedDictionary<'a> {
	pub fn new(identifier: Fragment<'a>, namespace: ResolvedNamespace<'a>, def: DictionaryDef) -> Self {
		Self(Arc::new(ResolvedDictionaryInner {
			identifier,
			namespace,
			def,
		}))
	}

	/// Get the dictionary name
	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	/// Get the dictionary def
	pub fn def(&self) -> &DictionaryDef {
		&self.0.def
	}

	/// Get the namespace
	pub fn namespace(&self) -> &ResolvedNamespace<'a> {
		&self.0.namespace
	}

	/// Get the identifier
	pub fn identifier(&self) -> &Fragment<'a> {
		&self.0.identifier
	}

	/// Get fully qualified name
	pub fn fully_qualified_name(&self) -> String {
		format!("{}.{}", self.0.namespace.name(), self.name())
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedDictionary<'static> {
		ResolvedDictionary(Arc::new(ResolvedDictionaryInner {
			identifier: Fragment::owned_internal(self.0.identifier.text()),
			namespace: self.0.namespace.to_static(),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved standard view
#[derive(Debug, Clone)]
pub struct ResolvedView<'a>(Arc<ResolvedViewInner<'a>>);

#[derive(Debug)]
struct ResolvedViewInner<'a> {
	pub identifier: Fragment<'a>,
	pub namespace: ResolvedNamespace<'a>,
	pub def: ViewDef,
}

impl<'a> ResolvedView<'a> {
	pub fn new(identifier: Fragment<'a>, namespace: ResolvedNamespace<'a>, def: ViewDef) -> Self {
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

	pub fn identifier(&self) -> &Fragment<'a> {
		&self.0.identifier
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.0.def.columns
	}

	pub fn fully_qualified_name(&self) -> String {
		format!("{}.{}", self.0.namespace.name(), self.name())
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedView<'static> {
		ResolvedView(Arc::new(ResolvedViewInner {
			identifier: Fragment::owned_internal(self.0.identifier.text()),
			namespace: self.0.namespace.to_static(),
			def: self.0.def.clone(),
		}))
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedDeferredView<'a>(Arc<ResolvedDeferredViewInner<'a>>);

#[derive(Debug)]
struct ResolvedDeferredViewInner<'a> {
	pub identifier: Fragment<'a>,
	pub namespace: ResolvedNamespace<'a>,
	pub def: ViewDef,
}

impl<'a> ResolvedDeferredView<'a> {
	pub fn new(identifier: Fragment<'a>, namespace: ResolvedNamespace<'a>, def: ViewDef) -> Self {
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

	pub fn identifier(&self) -> &Fragment<'a> {
		&self.0.identifier
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.0.def.columns
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedDeferredView<'static> {
		ResolvedDeferredView(Arc::new(ResolvedDeferredViewInner {
			identifier: Fragment::owned_internal(self.0.identifier.text()),
			namespace: self.0.namespace.to_static(),
			def: self.0.def.clone(),
		}))
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedTransactionalView<'a>(Arc<ResolvedTransactionalViewInner<'a>>);

#[derive(Debug)]
struct ResolvedTransactionalViewInner<'a> {
	pub identifier: Fragment<'a>,
	pub namespace: ResolvedNamespace<'a>,
	pub def: ViewDef,
}

impl<'a> ResolvedTransactionalView<'a> {
	pub fn new(identifier: Fragment<'a>, namespace: ResolvedNamespace<'a>, def: ViewDef) -> Self {
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

	pub fn identifier(&self) -> &Fragment<'a> {
		&self.0.identifier
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.0.def.columns
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedTransactionalView<'static> {
		ResolvedTransactionalView(Arc::new(ResolvedTransactionalViewInner {
			identifier: Fragment::owned_internal(self.0.identifier.text()),
			namespace: self.0.namespace.to_static(),
			def: self.0.def.clone(),
		}))
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedSequence<'a>(Arc<ResolvedSequenceInner<'a>>);

#[derive(Debug)]
struct ResolvedSequenceInner<'a> {
	pub identifier: Fragment<'a>,
	pub namespace: ResolvedNamespace<'a>,
	pub def: SequenceDef,
}

impl<'a> ResolvedSequence<'a> {
	pub fn new(identifier: Fragment<'a>, namespace: ResolvedNamespace<'a>, def: SequenceDef) -> Self {
		Self(Arc::new(ResolvedSequenceInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn identifier(&self) -> &Fragment<'a> {
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
	pub identifier: Fragment<'a>,
	pub table: ResolvedTable<'a>,
	pub def: IndexDef,
}

impl<'a> ResolvedIndex<'a> {
	pub fn new(identifier: Fragment<'a>, table: ResolvedTable<'a>, def: IndexDef) -> Self {
		Self(Arc::new(ResolvedIndexInner {
			identifier,
			table,
			def,
		}))
	}

	pub fn identifier(&self) -> &Fragment<'a> {
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
	pub identifier: Fragment<'a>,
	pub namespace: Vec<ResolvedNamespace<'a>>,
	pub def: FunctionDef,
}

impl<'a> ResolvedFunction<'a> {
	pub fn new(identifier: Fragment<'a>, namespace: Vec<ResolvedNamespace<'a>>, def: FunctionDef) -> Self {
		Self(Arc::new(ResolvedFunctionInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn identifier(&self) -> &Fragment<'a> {
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
	Flow(ResolvedFlow<'a>),
	Dictionary(ResolvedDictionary<'a>),
}

impl<'a> ResolvedSource<'a> {
	/// Get the identifier fragment
	pub fn identifier(&self) -> &Fragment<'a> {
		match self {
			Self::Table(t) => t.identifier(),
			Self::TableVirtual(t) => t.identifier(),
			Self::View(v) => v.identifier(),
			Self::DeferredView(v) => v.identifier(),
			Self::TransactionalView(v) => v.identifier(),
			Self::RingBuffer(r) => r.identifier(),
			Self::Flow(f) => f.identifier(),
			Self::Dictionary(d) => d.identifier(),
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
			Self::Flow(f) => Some(f.namespace()),
			Self::Dictionary(d) => Some(d.namespace()),
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
			Self::Flow(_f) => unreachable!(),
			Self::Dictionary(_d) => unreachable!(), // Dictionary columns are dynamic (id, value)
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
			Self::Flow(_) => "flow",
			Self::Dictionary(_) => "dictionary",
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
			Self::Flow(f) => Some(f.fully_qualified_name()),
			Self::Dictionary(d) => Some(d.fully_qualified_name()),
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
	pub fn as_ringbuffer(&self) -> Option<&ResolvedRingBuffer<'a>> {
		match self {
			Self::RingBuffer(r) => Some(r),
			_ => None,
		}
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedSource<'static> {
		match self {
			Self::Table(t) => ResolvedSource::Table(t.to_static()),
			Self::TableVirtual(t) => ResolvedSource::TableVirtual(t.to_static()),
			Self::View(v) => ResolvedSource::View(v.to_static()),
			Self::DeferredView(v) => ResolvedSource::DeferredView(v.to_static()),
			Self::TransactionalView(v) => ResolvedSource::TransactionalView(v.to_static()),
			Self::RingBuffer(r) => ResolvedSource::RingBuffer(r.to_static()),
			Self::Flow(f) => ResolvedSource::Flow(f.to_static()),
			Self::Dictionary(d) => ResolvedSource::Dictionary(d.to_static()),
		}
	}

	/// Convert to a dictionary if this is a dictionary source
	pub fn as_dictionary(&self) -> Option<&ResolvedDictionary<'a>> {
		match self {
			Self::Dictionary(d) => Some(d),
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
	pub identifier: Fragment<'a>,
	/// The resolved source this column belongs to
	pub source: ResolvedSource<'a>,
	/// The column definition
	pub def: ColumnDef,
}

impl<'a> ResolvedColumn<'a> {
	pub fn new(identifier: Fragment<'a>, source: ResolvedSource<'a>, def: ColumnDef) -> Self {
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
	pub fn identifier(&self) -> &Fragment<'a> {
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

	/// Get the column type
	pub fn column_type(&self) -> Type {
		self.0.def.constraint.get_type()
	}

	/// Get the column policies
	pub fn policies(&self) -> Vec<ColumnPolicyKind> {
		self.0.def.policies.iter().map(|p| p.policy.clone()).collect()
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
	pub fn qualified_name(&self) -> String {
		match self.0.source.fully_qualified_name() {
			Some(source_name) => {
				format!("{}.{}", source_name, self.name())
			}
			None => format!("{}.{}", self.0.source.identifier().text(), self.name()),
		}
	}

	/// Get the fragment for error reporting
	pub fn fragment(&self) -> &Fragment<'a> {
		&self.0.identifier
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedColumn<'static> {
		ResolvedColumn(Arc::new(ResolvedColumnInner {
			identifier: Fragment::owned_internal(self.0.identifier.text()),
			source: self.0.source.to_static(),
			def: self.0.def.clone(),
		}))
	}
}

// Helper function to convert ResolvedColumn to NumberOfRangeColumnDescriptor
// This is used in evaluation context for error reporting
pub fn resolved_column_to_number_descriptor<'a>(column: &'a ResolvedColumn<'a>) -> NumberOfRangeColumnDescriptor<'a> {
	let (namespace, table) = match column.source() {
		ResolvedSource::Table(table) => (Some(table.namespace().name().as_ref()), Some(table.name().as_ref())),
		ResolvedSource::TableVirtual(table) => {
			(Some(table.namespace().name().as_ref()), Some(table.name().as_ref()))
		}
		ResolvedSource::RingBuffer(rb) => (Some(rb.namespace().name().as_ref()), Some(rb.name().as_ref())),
		ResolvedSource::View(view) => (Some(view.namespace().name().as_ref()), Some(view.name().as_ref())),
		ResolvedSource::DeferredView(view) => {
			(Some(view.namespace().name().as_ref()), Some(view.name().as_ref()))
		}
		ResolvedSource::TransactionalView(view) => {
			(Some(view.namespace().name().as_ref()), Some(view.name().as_ref()))
		}
		ResolvedSource::Flow(flow) => (Some(flow.namespace().name().as_ref()), Some(flow.name().as_ref())),
		ResolvedSource::Dictionary(dict) => {
			(Some(dict.namespace().name().as_ref()), Some(dict.name().as_ref()))
		}
	};

	let mut descriptor = NumberOfRangeColumnDescriptor::new();
	if let Some(ns) = namespace {
		descriptor = descriptor.with_namespace(ns);
	}
	if let Some(tbl) = table {
		descriptor = descriptor.with_table(tbl);
	}
	descriptor.with_column(column.name().as_ref()).with_column_type(column.column_type())
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
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(2),
					name: "name".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
			],
			primary_key: None,
		}
	}

	#[test]
	fn test_resolved_namespace() {
		let identifier = Fragment::Owned(OwnedFragment::testing("public"));
		let def = test_namespace_def();
		let resolved = ResolvedNamespace::new(identifier, def);

		assert_eq!(resolved.name(), "public");
		assert_eq!(resolved.fragment().text(), "public");
	}

	#[test]
	fn test_resolved_table() {
		let namespace_ident = Fragment::Owned(OwnedFragment::testing("public"));
		let namespace = ResolvedNamespace::new(namespace_ident, test_namespace_def());

		let table_ident = Fragment::Owned(OwnedFragment::testing("users"));
		let table = ResolvedTable::new(table_ident, namespace.clone(), test_table_def());

		assert_eq!(table.name(), "users");
		assert_eq!(table.fully_qualified_name(), "public.users");
		assert_eq!(table.columns().len(), 2);
		assert!(table.find_column("id").is_some());
		assert!(table.find_column("nonexistent").is_none());
	}

	#[test]
	fn test_resolved_source_enum() {
		let namespace =
			ResolvedNamespace::new(Fragment::Owned(OwnedFragment::testing("public")), test_namespace_def());

		let table = ResolvedTable::new(
			Fragment::Owned(OwnedFragment::testing("users")),
			namespace,
			test_table_def(),
		);

		let source = ResolvedSource::Table(table);

		assert!(source.supports_indexes());
		assert!(source.supports_mutations());
		assert_eq!(source.kind_name(), "table");
		// effective_name removed - use identifier().text() instead
		assert_eq!(source.fully_qualified_name(), Some("public.users".to_string()));
		assert!(source.as_table().is_some());
		assert!(source.as_view().is_none());
	}

	#[test]
	fn test_resolved_column() {
		let namespace =
			ResolvedNamespace::new(Fragment::Owned(OwnedFragment::testing("public")), test_namespace_def());

		let table = ResolvedTable::new(
			Fragment::Owned(OwnedFragment::testing("users")),
			namespace,
			test_table_def(),
		);

		let source = ResolvedSource::Table(table);

		let column_ident = Fragment::Owned(OwnedFragment::testing("id"));

		let column_def = ColumnDef {
			id: ColumnId(1),
			name: "id".to_string(),
			constraint: TypeConstraint::unconstrained(Type::Int8),
			policies: vec![],
			index: ColumnIndex(0),
			auto_increment: false,
			dictionary_id: None,
		};

		let column = ResolvedColumn::new(column_ident, source, column_def);

		assert_eq!(column.name(), "id");
		assert_eq!(column.type_constraint(), &TypeConstraint::unconstrained(Type::Int8));
		assert!(!column.is_auto_increment());
		assert_eq!(column.qualified_name(), "public.users.id");
	}
}
