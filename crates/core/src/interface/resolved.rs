// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_type::{
	error::NumberOutOfRangeDescriptor,
	fragment::Fragment,
	value::{constraint::TypeConstraint, r#type::Type},
};
use serde::{Deserialize, Serialize};

use super::catalog::{
	column::ColumnDef,
	dictionary::DictionaryDef,
	flow::FlowDef,
	namespace::NamespaceDef,
	property::ColumnPropertyKind,
	ringbuffer::RingBufferDef,
	series::SeriesDef,
	subscription::{SubscriptionColumnDef, SubscriptionDef},
	table::TableDef,
	view::ViewDef,
	vtable::VTableDef,
};

/// Resolved namespace with both identifier and definition
#[derive(Debug, Clone)]
pub struct ResolvedNamespace(Arc<ResolvedNamespaceInner>);

#[derive(Debug)]
struct ResolvedNamespaceInner {
	pub identifier: Fragment,
	pub def: NamespaceDef,
}

impl ResolvedNamespace {
	pub fn new(identifier: Fragment, def: NamespaceDef) -> Self {
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
	pub fn fragment(&self) -> &Fragment {
		&self.0.identifier
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedNamespace {
		ResolvedNamespace(Arc::new(ResolvedNamespaceInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved physical table
#[derive(Debug, Clone)]
pub struct ResolvedTable(Arc<ResolvedTableInner>);

#[derive(Debug)]
struct ResolvedTableInner {
	pub identifier: Fragment,
	pub namespace: ResolvedNamespace,
	pub def: TableDef,
}

impl ResolvedTable {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: TableDef) -> Self {
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
	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	/// Get the identifier
	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	/// Get fully qualified name
	pub fn fully_qualified_name(&self) -> String {
		format!("{}::{}", self.0.namespace.name(), self.name())
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
	pub fn to_static(&self) -> ResolvedTable {
		ResolvedTable(Arc::new(ResolvedTableInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			namespace: self.0.namespace.clone(),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved virtual table (system tables, information_schema)
#[derive(Debug, Clone)]
pub struct ResolvedTableVirtual(Arc<ResolvedTableVirtualInner>);

#[derive(Debug)]
struct ResolvedTableVirtualInner {
	pub identifier: Fragment,
	pub namespace: ResolvedNamespace,
	pub def: VTableDef,
}

impl ResolvedTableVirtual {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: VTableDef) -> Self {
		Self(Arc::new(ResolvedTableVirtualInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	pub fn def(&self) -> &VTableDef {
		&self.0.def
	}

	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.0.def.columns
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedTableVirtual {
		ResolvedTableVirtual(Arc::new(ResolvedTableVirtualInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			namespace: self.0.namespace.clone(),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved ring buffer
#[derive(Debug, Clone)]
pub struct ResolvedRingBuffer(Arc<ResolvedRingBufferInner>);

#[derive(Debug)]
struct ResolvedRingBufferInner {
	pub identifier: Fragment,
	pub namespace: ResolvedNamespace,
	pub def: RingBufferDef,
}

impl ResolvedRingBuffer {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: RingBufferDef) -> Self {
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
	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	/// Get the identifier
	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	/// Get fully qualified name
	pub fn fully_qualified_name(&self) -> String {
		format!("{}::{}", self.0.namespace.name(), self.name())
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
	pub fn to_static(&self) -> ResolvedRingBuffer {
		ResolvedRingBuffer(Arc::new(ResolvedRingBufferInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			namespace: self.0.namespace.clone(),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved flow
#[derive(Debug, Clone)]
pub struct ResolvedFlow(Arc<ResolvedFlowInner>);

#[derive(Debug)]
struct ResolvedFlowInner {
	pub identifier: Fragment,
	pub namespace: ResolvedNamespace,
	pub def: FlowDef,
}

impl ResolvedFlow {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: FlowDef) -> Self {
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
	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	/// Get the identifier
	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	/// Get fully qualified name
	pub fn fully_qualified_name(&self) -> String {
		format!("{}::{}", self.0.namespace.name(), self.name())
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedFlow {
		ResolvedFlow(Arc::new(ResolvedFlowInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			namespace: self.0.namespace.clone(),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved dictionary
#[derive(Debug, Clone)]
pub struct ResolvedDictionary(Arc<ResolvedDictionaryInner>);

#[derive(Debug)]
struct ResolvedDictionaryInner {
	pub identifier: Fragment,
	pub namespace: ResolvedNamespace,
	pub def: DictionaryDef,
}

impl ResolvedDictionary {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: DictionaryDef) -> Self {
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
	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	/// Get the identifier
	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	/// Get fully qualified name
	pub fn fully_qualified_name(&self) -> String {
		format!("{}::{}", self.0.namespace.name(), self.name())
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedDictionary {
		ResolvedDictionary(Arc::new(ResolvedDictionaryInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			namespace: self.0.namespace.clone(),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved series
#[derive(Debug, Clone)]
pub struct ResolvedSeries(Arc<ResolvedSeriesInner>);

#[derive(Debug)]
struct ResolvedSeriesInner {
	pub identifier: Fragment,
	pub namespace: ResolvedNamespace,
	pub def: SeriesDef,
}

impl ResolvedSeries {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: SeriesDef) -> Self {
		Self(Arc::new(ResolvedSeriesInner {
			identifier,
			namespace,
			def,
		}))
	}

	/// Get the series name
	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	/// Get the series def
	pub fn def(&self) -> &SeriesDef {
		&self.0.def
	}

	/// Get the namespace
	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	/// Get the identifier
	pub fn identifier(&self) -> &Fragment {
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
	pub fn to_static(&self) -> ResolvedSeries {
		ResolvedSeries(Arc::new(ResolvedSeriesInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			namespace: self.0.namespace.clone(),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved subscription (global entity, no namespace)
#[derive(Debug, Clone)]
pub struct ResolvedSubscription(Arc<ResolvedSubscriptionInner>);

#[derive(Debug)]
struct ResolvedSubscriptionInner {
	pub identifier: Fragment,
	pub def: SubscriptionDef,
}

impl ResolvedSubscription {
	pub fn new(identifier: Fragment, def: SubscriptionDef) -> Self {
		Self(Arc::new(ResolvedSubscriptionInner {
			identifier,
			def,
		}))
	}

	/// Get the subscription ID as a string identifier
	pub fn id_str(&self) -> String {
		format!("subscription_{}", self.0.def.id.0)
	}

	/// Get the subscription def
	pub fn def(&self) -> &SubscriptionDef {
		&self.0.def
	}

	/// Get the identifier
	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	/// Get columns
	pub fn columns(&self) -> &[SubscriptionColumnDef] {
		&self.0.def.columns
	}

	/// Find a column by name
	pub fn find_column(&self, name: &str) -> Option<&SubscriptionColumnDef> {
		self.0.def.columns.iter().find(|c| c.name == name)
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedSubscription {
		ResolvedSubscription(Arc::new(ResolvedSubscriptionInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			def: self.0.def.clone(),
		}))
	}
}

/// Resolved transaction view
#[derive(Debug, Clone)]
pub struct ResolvedView(Arc<ResolvedViewInner>);

#[derive(Debug)]
struct ResolvedViewInner {
	pub identifier: Fragment,
	pub namespace: ResolvedNamespace,
	pub def: ViewDef,
}

impl ResolvedView {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: ViewDef) -> Self {
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

	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.0.def.columns
	}

	pub fn fully_qualified_name(&self) -> String {
		format!("{}::{}", self.0.namespace.name(), self.name())
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedView {
		ResolvedView(Arc::new(ResolvedViewInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			namespace: self.0.namespace.clone(),
			def: self.0.def.clone(),
		}))
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedDeferredView(Arc<ResolvedDeferredViewInner>);

#[derive(Debug)]
struct ResolvedDeferredViewInner {
	pub identifier: Fragment,
	pub namespace: ResolvedNamespace,
	pub def: ViewDef,
}

impl ResolvedDeferredView {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: ViewDef) -> Self {
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

	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.0.def.columns
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedDeferredView {
		ResolvedDeferredView(Arc::new(ResolvedDeferredViewInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			namespace: self.0.namespace.clone(),
			def: self.0.def.clone(),
		}))
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedTransactionalView(Arc<ResolvedTransactionalViewInner>);

#[derive(Debug)]
struct ResolvedTransactionalViewInner {
	pub identifier: Fragment,
	pub namespace: ResolvedNamespace,
	pub def: ViewDef,
}

impl ResolvedTransactionalView {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: ViewDef) -> Self {
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

	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.0.def.columns
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedTransactionalView {
		ResolvedTransactionalView(Arc::new(ResolvedTransactionalViewInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			namespace: self.0.namespace.clone(),
			def: self.0.def.clone(),
		}))
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedSequence(Arc<ResolvedSequenceInner>);

#[derive(Debug)]
struct ResolvedSequenceInner {
	pub identifier: Fragment,
	pub namespace: ResolvedNamespace,
	pub def: SequenceDef,
}

impl ResolvedSequence {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: SequenceDef) -> Self {
		Self(Arc::new(ResolvedSequenceInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	pub fn def(&self) -> &SequenceDef {
		&self.0.def
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedIndex(Arc<ResolvedIndexInner>);

#[derive(Debug)]
struct ResolvedIndexInner {
	pub identifier: Fragment,
	pub table: ResolvedTable,
	pub def: IndexDef,
}

impl ResolvedIndex {
	pub fn new(identifier: Fragment, table: ResolvedTable, def: IndexDef) -> Self {
		Self(Arc::new(ResolvedIndexInner {
			identifier,
			table,
			def,
		}))
	}

	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn table(&self) -> &ResolvedTable {
		&self.0.table
	}

	pub fn def(&self) -> &IndexDef {
		&self.0.def
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedFunction(Arc<ResolvedFunctionInner>);

#[derive(Debug)]
struct ResolvedFunctionInner {
	pub identifier: Fragment,
	pub namespace: Vec<ResolvedNamespace>,
	pub def: FunctionDef,
}

impl ResolvedFunction {
	pub fn new(identifier: Fragment, namespace: Vec<ResolvedNamespace>, def: FunctionDef) -> Self {
		Self(Arc::new(ResolvedFunctionInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn namespace(&self) -> &[ResolvedNamespace] {
		&self.0.namespace
	}

	pub fn def(&self) -> &FunctionDef {
		&self.0.def
	}
}
/// Unified enum for any resolved primitive type
#[derive(Debug, Clone)]
pub enum ResolvedPrimitive {
	Table(ResolvedTable),
	TableVirtual(ResolvedTableVirtual),
	View(ResolvedView),
	DeferredView(ResolvedDeferredView),
	TransactionalView(ResolvedTransactionalView),
	RingBuffer(ResolvedRingBuffer),
	Flow(ResolvedFlow),
	Dictionary(ResolvedDictionary),
	Series(ResolvedSeries),
}

impl ResolvedPrimitive {
	/// Get the identifier fragment
	pub fn identifier(&self) -> &Fragment {
		match self {
			Self::Table(t) => t.identifier(),
			Self::TableVirtual(t) => t.identifier(),
			Self::View(v) => v.identifier(),
			Self::DeferredView(v) => v.identifier(),
			Self::TransactionalView(v) => v.identifier(),
			Self::RingBuffer(r) => r.identifier(),
			Self::Flow(f) => f.identifier(),
			Self::Dictionary(d) => d.identifier(),
			Self::Series(s) => s.identifier(),
		}
	}

	/// Get the namespace if this primitive has one
	pub fn namespace(&self) -> Option<&ResolvedNamespace> {
		match self {
			Self::Table(t) => Some(t.namespace()),
			Self::TableVirtual(t) => Some(t.namespace()),
			Self::View(v) => Some(v.namespace()),
			Self::DeferredView(v) => Some(v.namespace()),
			Self::TransactionalView(v) => Some(v.namespace()),
			Self::RingBuffer(r) => Some(r.namespace()),
			Self::Flow(f) => Some(f.namespace()),
			Self::Dictionary(d) => Some(d.namespace()),
			Self::Series(s) => Some(s.namespace()),
		}
	}

	/// Check if this primitive supports indexes
	pub fn supports_indexes(&self) -> bool {
		matches!(self, Self::Table(_))
	}

	/// Check if this primitive supports mutations
	pub fn supports_mutations(&self) -> bool {
		matches!(self, Self::Table(_) | Self::RingBuffer(_) | Self::Series(_))
	}

	/// Get columns for this primitive
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
			Self::Series(s) => s.columns(),
		}
	}

	/// Find a column by name
	pub fn find_column(&self, name: &str) -> Option<&ColumnDef> {
		self.columns().iter().find(|c| c.name == name)
	}

	/// Get the primitive kind name for error messages
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
			Self::Series(_) => "series",
		}
	}

	/// Get fully qualified name if available
	pub fn fully_qualified_name(&self) -> Option<String> {
		match self {
			Self::Table(t) => Some(t.fully_qualified_name()),
			Self::View(v) => Some(v.fully_qualified_name()),
			Self::DeferredView(v) => Some(format!("{}::{}", v.namespace().name(), v.name())),
			Self::TransactionalView(v) => Some(format!("{}::{}", v.namespace().name(), v.name())),
			Self::TableVirtual(t) => Some(format!("{}::{}", t.namespace().name(), t.name())),
			Self::RingBuffer(r) => Some(r.fully_qualified_name()),
			Self::Flow(f) => Some(f.fully_qualified_name()),
			Self::Dictionary(d) => Some(d.fully_qualified_name()),
			Self::Series(s) => Some(s.fully_qualified_name()),
		}
	}

	/// Convert to a table if this is a table primitive
	pub fn as_table(&self) -> Option<&ResolvedTable> {
		match self {
			Self::Table(t) => Some(t),
			_ => None,
		}
	}

	/// Convert to a view if this is a view primitive
	pub fn as_view(&self) -> Option<&ResolvedView> {
		match self {
			Self::View(v) => Some(v),
			_ => None,
		}
	}

	/// Convert to a ring buffer if this is a ring buffer primitive
	pub fn as_ringbuffer(&self) -> Option<&ResolvedRingBuffer> {
		match self {
			Self::RingBuffer(r) => Some(r),
			_ => None,
		}
	}

	/// Convert to a dictionary if this is a dictionary primitive
	pub fn as_dictionary(&self) -> Option<&ResolvedDictionary> {
		match self {
			Self::Dictionary(d) => Some(d),
			_ => None,
		}
	}

	/// Convert to a series if this is a series primitive
	pub fn as_series(&self) -> Option<&ResolvedSeries> {
		match self {
			Self::Series(s) => Some(s),
			_ => None,
		}
	}
}

/// Column with its resolved primitive
#[derive(Debug, Clone)]
pub struct ResolvedColumn(Arc<ResolvedColumnInner>);

#[derive(Debug)]
struct ResolvedColumnInner {
	/// Original identifier with fragments
	pub identifier: Fragment,
	/// The resolved primitive this column belongs to
	pub primitive: ResolvedPrimitive,
	/// The column definition
	pub def: ColumnDef,
}

impl ResolvedColumn {
	pub fn new(identifier: Fragment, primitive: ResolvedPrimitive, def: ColumnDef) -> Self {
		Self(Arc::new(ResolvedColumnInner {
			identifier,
			primitive,
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
	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	/// Get the primitive
	pub fn primitive(&self) -> &ResolvedPrimitive {
		&self.0.primitive
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
	pub fn properties(&self) -> Vec<ColumnPropertyKind> {
		self.0.def.properties.iter().map(|p| p.property.clone()).collect()
	}

	/// Check if column has auto increment
	pub fn is_auto_increment(&self) -> bool {
		self.0.def.auto_increment
	}

	/// Get the namespace this column belongs to
	pub fn namespace(&self) -> Option<&ResolvedNamespace> {
		self.0.primitive.namespace()
	}

	/// Get fully qualified name
	pub fn qualified_name(&self) -> String {
		match self.0.primitive.fully_qualified_name() {
			Some(primitive_name) => {
				format!("{}.{}", primitive_name, self.name())
			}
			None => format!("{}.{}", self.0.primitive.identifier().text(), self.name()),
		}
	}

	/// Get the fragment for error reporting
	pub fn fragment(&self) -> &Fragment {
		&self.0.identifier
	}

	/// Convert to owned version with 'static lifetime
	pub fn to_static(&self) -> ResolvedColumn {
		ResolvedColumn(Arc::new(ResolvedColumnInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			primitive: self.0.primitive.clone(),
			def: self.0.def.clone(),
		}))
	}
}

// Helper function to convert ResolvedColumn to NumberOutOfRangeDescriptor
// This is used in evaluation context for error reporting
pub fn resolved_column_to_number_descriptor(column: &ResolvedColumn) -> NumberOutOfRangeDescriptor {
	let (namespace, table) = match column.primitive() {
		ResolvedPrimitive::Table(table) => {
			(Some(table.namespace().name().to_string()), Some(table.name().to_string()))
		}
		ResolvedPrimitive::TableVirtual(table) => {
			(Some(table.namespace().name().to_string()), Some(table.name().to_string()))
		}
		ResolvedPrimitive::RingBuffer(rb) => {
			(Some(rb.namespace().name().to_string()), Some(rb.name().to_string()))
		}
		ResolvedPrimitive::View(view) => {
			(Some(view.namespace().name().to_string()), Some(view.name().to_string()))
		}
		ResolvedPrimitive::DeferredView(view) => {
			(Some(view.namespace().name().to_string()), Some(view.name().to_string()))
		}
		ResolvedPrimitive::TransactionalView(view) => {
			(Some(view.namespace().name().to_string()), Some(view.name().to_string()))
		}
		ResolvedPrimitive::Flow(flow) => {
			(Some(flow.namespace().name().to_string()), Some(flow.name().to_string()))
		}
		ResolvedPrimitive::Dictionary(dict) => {
			(Some(dict.namespace().name().to_string()), Some(dict.name().to_string()))
		}
		ResolvedPrimitive::Series(series) => {
			(Some(series.namespace().name().to_string()), Some(series.name().to_string()))
		}
	};

	NumberOutOfRangeDescriptor {
		namespace,
		table,
		column: Some(column.name().to_string()),
		column_type: Some(column.column_type()),
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
pub mod tests {
	use reifydb_type::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, r#type::Type},
	};

	use super::*;
	use crate::interface::catalog::{
		column::ColumnIndex,
		id::{ColumnId, NamespaceId, TableId},
	};

	fn test_namespace_def() -> NamespaceDef {
		NamespaceDef {
			id: NamespaceId(1),
			name: "public".to_string(),
			parent_id: NamespaceId::ROOT,
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
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(2),
					name: "name".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
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
		let identifier = Fragment::testing("public");
		let def = test_namespace_def();
		let resolved = ResolvedNamespace::new(identifier, def);

		assert_eq!(resolved.name(), "public");
		assert_eq!(resolved.fragment().text(), "public");
	}

	#[test]
	fn test_resolved_table() {
		let namespace_ident = Fragment::testing("public");
		let namespace = ResolvedNamespace::new(namespace_ident, test_namespace_def());

		let table_ident = Fragment::testing("users");
		let table = ResolvedTable::new(table_ident, namespace.clone(), test_table_def());

		assert_eq!(table.name(), "users");
		assert_eq!(table.fully_qualified_name(), "public::users");
		assert_eq!(table.columns().len(), 2);
		assert!(table.find_column("id").is_some());
		assert!(table.find_column("nonexistent").is_none());
	}

	#[test]
	fn test_resolved_primitive_enum() {
		let namespace = ResolvedNamespace::new(Fragment::testing("public"), test_namespace_def());

		let table = ResolvedTable::new(Fragment::testing("users"), namespace, test_table_def());

		let primitive = ResolvedPrimitive::Table(table);

		assert!(primitive.supports_indexes());
		assert!(primitive.supports_mutations());
		assert_eq!(primitive.kind_name(), "table");
		// effective_name removed - use identifier().text() instead
		assert_eq!(primitive.fully_qualified_name(), Some("public::users".to_string()));
		assert!(primitive.as_table().is_some());
		assert!(primitive.as_view().is_none());
	}

	#[test]
	fn test_resolved_column() {
		let namespace = ResolvedNamespace::new(Fragment::testing("public"), test_namespace_def());

		let table = ResolvedTable::new(Fragment::testing("users"), namespace, test_table_def());

		let primitive = ResolvedPrimitive::Table(table);

		let column_ident = Fragment::testing("id");

		let column_def = ColumnDef {
			id: ColumnId(1),
			name: "id".to_string(),
			constraint: TypeConstraint::unconstrained(Type::Int8),
			properties: vec![],
			index: ColumnIndex(0),
			auto_increment: false,
			dictionary_id: None,
		};

		let column = ResolvedColumn::new(column_ident, primitive, column_def);

		assert_eq!(column.name(), "id");
		assert_eq!(column.type_constraint(), &TypeConstraint::unconstrained(Type::Int8));
		assert!(!column.is_auto_increment());
		assert_eq!(column.qualified_name(), "public::users.id");
	}
}
