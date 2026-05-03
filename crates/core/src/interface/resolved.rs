// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_type::{
	error::NumberOutOfRangeDescriptor,
	fragment::Fragment,
	value::{constraint::TypeConstraint, r#type::Type},
};
use serde::{Deserialize, Serialize};

use super::catalog::{
	column::Column, dictionary::Dictionary, namespace::Namespace, property::ColumnPropertyKind,
	ringbuffer::RingBuffer, series::Series, table::Table, view::View, vtable::VTable,
};

#[derive(Debug, Clone)]
pub enum ResolvedNamespace {
	Local(Arc<ResolvedNamespaceInner>),
	Remote(Arc<ResolvedNamespaceInner>),
}

#[derive(Debug)]
pub struct ResolvedNamespaceInner {
	identifier: Fragment,
	def: Namespace,
}

impl ResolvedNamespace {
	pub fn new(identifier: Fragment, def: Namespace) -> Self {
		let is_remote = def.is_remote();
		let inner = Arc::new(ResolvedNamespaceInner {
			identifier,
			def,
		});
		if is_remote {
			Self::Remote(inner)
		} else {
			Self::Local(inner)
		}
	}

	fn inner(&self) -> &ResolvedNamespaceInner {
		match self {
			Self::Local(inner) | Self::Remote(inner) => inner,
		}
	}

	pub fn name(&self) -> &str {
		self.inner().def.name()
	}

	pub fn def(&self) -> &Namespace {
		&self.inner().def
	}

	pub fn fragment(&self) -> &Fragment {
		&self.inner().identifier
	}

	pub fn is_remote(&self) -> bool {
		matches!(self, Self::Remote(_))
	}

	pub fn address(&self) -> Option<&str> {
		self.inner().def.address()
	}

	pub fn to_static(&self) -> ResolvedNamespace {
		let inner = self.inner();
		ResolvedNamespace::new(Fragment::internal(inner.identifier.text()), inner.def.clone())
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedTable(Arc<ResolvedTableInner>);

#[derive(Debug)]
struct ResolvedTableInner {
	pub identifier: Fragment,
	pub namespace: ResolvedNamespace,
	pub def: Table,
}

impl ResolvedTable {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: Table) -> Self {
		Self(Arc::new(ResolvedTableInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	pub fn def(&self) -> &Table {
		&self.0.def
	}

	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn fully_qualified_name(&self) -> String {
		format!("{}::{}", self.0.namespace.name(), self.name())
	}

	pub fn columns(&self) -> &[Column] {
		&self.0.def.columns
	}

	pub fn find_column(&self, name: &str) -> Option<&Column> {
		self.0.def.columns.iter().find(|c| c.name == name)
	}

	pub fn to_static(&self) -> ResolvedTable {
		ResolvedTable(Arc::new(ResolvedTableInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			namespace: self.0.namespace.clone(),
			def: self.0.def.clone(),
		}))
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedTableVirtual(Arc<ResolvedTableVirtualInner>);

#[derive(Debug)]
struct ResolvedTableVirtualInner {
	pub identifier: Fragment,
	pub namespace: ResolvedNamespace,
	pub def: VTable,
}

impl ResolvedTableVirtual {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: VTable) -> Self {
		Self(Arc::new(ResolvedTableVirtualInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	pub fn def(&self) -> &VTable {
		&self.0.def
	}

	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn columns(&self) -> &[Column] {
		&self.0.def.columns
	}

	pub fn to_static(&self) -> ResolvedTableVirtual {
		ResolvedTableVirtual(Arc::new(ResolvedTableVirtualInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			namespace: self.0.namespace.clone(),
			def: self.0.def.clone(),
		}))
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedRingBuffer(Arc<ResolvedRingBufferInner>);

#[derive(Debug)]
struct ResolvedRingBufferInner {
	pub identifier: Fragment,
	pub namespace: ResolvedNamespace,
	pub def: RingBuffer,
}

impl ResolvedRingBuffer {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: RingBuffer) -> Self {
		Self(Arc::new(ResolvedRingBufferInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	pub fn def(&self) -> &RingBuffer {
		&self.0.def
	}

	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn fully_qualified_name(&self) -> String {
		format!("{}::{}", self.0.namespace.name(), self.name())
	}

	pub fn columns(&self) -> &[Column] {
		&self.0.def.columns
	}

	pub fn find_column(&self, name: &str) -> Option<&Column> {
		self.0.def.columns.iter().find(|c| c.name == name)
	}

	pub fn to_static(&self) -> ResolvedRingBuffer {
		ResolvedRingBuffer(Arc::new(ResolvedRingBufferInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			namespace: self.0.namespace.clone(),
			def: self.0.def.clone(),
		}))
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedDictionary(Arc<ResolvedDictionaryInner>);

#[derive(Debug)]
struct ResolvedDictionaryInner {
	pub identifier: Fragment,
	pub namespace: ResolvedNamespace,
	pub def: Dictionary,
}

impl ResolvedDictionary {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: Dictionary) -> Self {
		Self(Arc::new(ResolvedDictionaryInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	pub fn def(&self) -> &Dictionary {
		&self.0.def
	}

	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn fully_qualified_name(&self) -> String {
		format!("{}::{}", self.0.namespace.name(), self.name())
	}

	pub fn to_static(&self) -> ResolvedDictionary {
		ResolvedDictionary(Arc::new(ResolvedDictionaryInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			namespace: self.0.namespace.clone(),
			def: self.0.def.clone(),
		}))
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedSeries(Arc<ResolvedSeriesInner>);

#[derive(Debug)]
struct ResolvedSeriesInner {
	pub identifier: Fragment,
	pub namespace: ResolvedNamespace,
	pub def: Series,
}

impl ResolvedSeries {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: Series) -> Self {
		Self(Arc::new(ResolvedSeriesInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	pub fn def(&self) -> &Series {
		&self.0.def
	}

	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn fully_qualified_name(&self) -> String {
		format!("{}.{}", self.0.namespace.name(), self.name())
	}

	pub fn columns(&self) -> &[Column] {
		&self.0.def.columns
	}

	pub fn find_column(&self, name: &str) -> Option<&Column> {
		self.0.def.columns.iter().find(|c| c.name == name)
	}

	pub fn to_static(&self) -> ResolvedSeries {
		ResolvedSeries(Arc::new(ResolvedSeriesInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			namespace: self.0.namespace.clone(),
			def: self.0.def.clone(),
		}))
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedView(Arc<ResolvedViewInner>);

#[derive(Debug)]
struct ResolvedViewInner {
	pub identifier: Fragment,
	pub namespace: ResolvedNamespace,
	pub def: View,
}

impl ResolvedView {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: View) -> Self {
		Self(Arc::new(ResolvedViewInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn name(&self) -> &str {
		self.0.def.name()
	}

	pub fn def(&self) -> &View {
		&self.0.def
	}

	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn columns(&self) -> &[Column] {
		self.0.def.columns()
	}

	pub fn fully_qualified_name(&self) -> String {
		format!("{}::{}", self.0.namespace.name(), self.name())
	}

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
	pub def: View,
}

impl ResolvedDeferredView {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: View) -> Self {
		Self(Arc::new(ResolvedDeferredViewInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn name(&self) -> &str {
		self.0.def.name()
	}

	pub fn def(&self) -> &View {
		&self.0.def
	}

	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn columns(&self) -> &[Column] {
		self.0.def.columns()
	}

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
	pub def: View,
}

impl ResolvedTransactionalView {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: View) -> Self {
		Self(Arc::new(ResolvedTransactionalViewInner {
			identifier,
			namespace,
			def,
		}))
	}

	pub fn name(&self) -> &str {
		self.0.def.name()
	}

	pub fn def(&self) -> &View {
		&self.0.def
	}

	pub fn namespace(&self) -> &ResolvedNamespace {
		&self.0.namespace
	}

	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn columns(&self) -> &[Column] {
		self.0.def.columns()
	}

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
	pub def: Sequence,
}

impl ResolvedSequence {
	pub fn new(identifier: Fragment, namespace: ResolvedNamespace, def: Sequence) -> Self {
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

	pub fn def(&self) -> &Sequence {
		&self.0.def
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedIndex(Arc<ResolvedIndexInner>);

#[derive(Debug)]
struct ResolvedIndexInner {
	pub identifier: Fragment,
	pub table: ResolvedTable,
	pub def: Index,
}

impl ResolvedIndex {
	pub fn new(identifier: Fragment, table: ResolvedTable, def: Index) -> Self {
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

	pub fn def(&self) -> &Index {
		&self.0.def
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedFunction(Arc<ResolvedFunctionInner>);

#[derive(Debug)]
struct ResolvedFunctionInner {
	pub identifier: Fragment,
	pub namespace: Vec<ResolvedNamespace>,
	pub def: Function,
}

impl ResolvedFunction {
	pub fn new(identifier: Fragment, namespace: Vec<ResolvedNamespace>, def: Function) -> Self {
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

	pub fn def(&self) -> &Function {
		&self.0.def
	}
}

#[derive(Debug, Clone)]
pub enum ResolvedShape {
	Table(ResolvedTable),
	TableVirtual(ResolvedTableVirtual),
	View(ResolvedView),
	DeferredView(ResolvedDeferredView),
	TransactionalView(ResolvedTransactionalView),
	RingBuffer(ResolvedRingBuffer),
	Dictionary(ResolvedDictionary),
	Series(ResolvedSeries),
}

impl ResolvedShape {
	pub fn identifier(&self) -> &Fragment {
		match self {
			Self::Table(t) => t.identifier(),
			Self::TableVirtual(t) => t.identifier(),
			Self::View(v) => v.identifier(),
			Self::DeferredView(v) => v.identifier(),
			Self::TransactionalView(v) => v.identifier(),
			Self::RingBuffer(r) => r.identifier(),
			Self::Dictionary(d) => d.identifier(),
			Self::Series(s) => s.identifier(),
		}
	}

	pub fn name(&self) -> &str {
		match self {
			Self::Table(t) => t.name(),
			Self::TableVirtual(t) => t.name(),
			Self::View(v) => v.name(),
			Self::DeferredView(v) => v.name(),
			Self::TransactionalView(v) => v.name(),
			Self::RingBuffer(r) => r.name(),
			Self::Dictionary(d) => d.name(),
			Self::Series(s) => s.name(),
		}
	}

	pub fn namespace(&self) -> Option<&ResolvedNamespace> {
		match self {
			Self::Table(t) => Some(t.namespace()),
			Self::TableVirtual(t) => Some(t.namespace()),
			Self::View(v) => Some(v.namespace()),
			Self::DeferredView(v) => Some(v.namespace()),
			Self::TransactionalView(v) => Some(v.namespace()),
			Self::RingBuffer(r) => Some(r.namespace()),
			Self::Dictionary(d) => Some(d.namespace()),
			Self::Series(s) => Some(s.namespace()),
		}
	}

	pub fn supports_indexes(&self) -> bool {
		matches!(self, Self::Table(_))
	}

	pub fn supports_mutations(&self) -> bool {
		matches!(self, Self::Table(_) | Self::RingBuffer(_) | Self::Series(_))
	}

	pub fn columns(&self) -> &[Column] {
		match self {
			Self::Table(t) => t.columns(),
			Self::TableVirtual(t) => t.columns(),
			Self::View(v) => v.columns(),
			Self::DeferredView(v) => v.columns(),
			Self::TransactionalView(v) => v.columns(),
			Self::RingBuffer(r) => r.columns(),
			Self::Dictionary(_d) => unreachable!(),
			Self::Series(s) => s.columns(),
		}
	}

	pub fn find_column(&self, name: &str) -> Option<&Column> {
		self.columns().iter().find(|c| c.name == name)
	}

	pub fn kind_name(&self) -> &'static str {
		match self {
			Self::Table(_) => "table",
			Self::TableVirtual(_) => "virtual table",
			Self::View(_) => "view",
			Self::DeferredView(_) => "deferred view",
			Self::TransactionalView(_) => "transactional view",
			Self::RingBuffer(_) => "ring buffer",
			Self::Dictionary(_) => "dictionary",
			Self::Series(_) => "series",
		}
	}

	pub fn fully_qualified_name(&self) -> Option<String> {
		match self {
			Self::Table(t) => Some(t.fully_qualified_name()),
			Self::View(v) => Some(v.fully_qualified_name()),
			Self::DeferredView(v) => Some(format!("{}::{}", v.namespace().name(), v.name())),
			Self::TransactionalView(v) => Some(format!("{}::{}", v.namespace().name(), v.name())),
			Self::TableVirtual(t) => Some(format!("{}::{}", t.namespace().name(), t.name())),
			Self::RingBuffer(r) => Some(r.fully_qualified_name()),
			Self::Dictionary(d) => Some(d.fully_qualified_name()),
			Self::Series(s) => Some(s.fully_qualified_name()),
		}
	}

	pub fn as_table(&self) -> Option<&ResolvedTable> {
		match self {
			Self::Table(t) => Some(t),
			_ => None,
		}
	}

	pub fn as_view(&self) -> Option<&ResolvedView> {
		match self {
			Self::View(v) => Some(v),
			_ => None,
		}
	}

	pub fn as_ringbuffer(&self) -> Option<&ResolvedRingBuffer> {
		match self {
			Self::RingBuffer(r) => Some(r),
			_ => None,
		}
	}

	pub fn as_dictionary(&self) -> Option<&ResolvedDictionary> {
		match self {
			Self::Dictionary(d) => Some(d),
			_ => None,
		}
	}

	pub fn as_series(&self) -> Option<&ResolvedSeries> {
		match self {
			Self::Series(s) => Some(s),
			_ => None,
		}
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedColumn(Arc<ResolvedColumnInner>);

#[derive(Debug)]
struct ResolvedColumnInner {
	pub identifier: Fragment,

	pub shape: ResolvedShape,

	pub def: Column,
}

impl ResolvedColumn {
	pub fn new(identifier: Fragment, shape: ResolvedShape, def: Column) -> Self {
		Self(Arc::new(ResolvedColumnInner {
			identifier,
			shape,
			def,
		}))
	}

	pub fn name(&self) -> &str {
		&self.0.def.name
	}

	pub fn def(&self) -> &Column {
		&self.0.def
	}

	pub fn identifier(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn shape(&self) -> &ResolvedShape {
		&self.0.shape
	}

	pub fn type_constraint(&self) -> &TypeConstraint {
		&self.0.def.constraint
	}

	pub fn column_type(&self) -> Type {
		self.0.def.constraint.get_type()
	}

	pub fn properties(&self) -> Vec<ColumnPropertyKind> {
		self.0.def.properties.iter().map(|p| p.property.clone()).collect()
	}

	pub fn is_auto_increment(&self) -> bool {
		self.0.def.auto_increment
	}

	pub fn namespace(&self) -> Option<&ResolvedNamespace> {
		self.0.shape.namespace()
	}

	pub fn qualified_name(&self) -> String {
		match self.0.shape.fully_qualified_name() {
			Some(shape_name) => {
				format!("{}.{}", shape_name, self.name())
			}
			None => format!("{}.{}", self.0.shape.identifier().text(), self.name()),
		}
	}

	pub fn fragment(&self) -> &Fragment {
		&self.0.identifier
	}

	pub fn to_static(&self) -> ResolvedColumn {
		ResolvedColumn(Arc::new(ResolvedColumnInner {
			identifier: Fragment::internal(self.0.identifier.text()),
			shape: self.0.shape.clone(),
			def: self.0.def.clone(),
		}))
	}
}

pub fn resolved_column_to_number_descriptor(column: &ResolvedColumn) -> NumberOutOfRangeDescriptor {
	let (namespace, table) = match column.shape() {
		ResolvedShape::Table(table) => {
			(Some(table.namespace().name().to_string()), Some(table.name().to_string()))
		}
		ResolvedShape::TableVirtual(table) => {
			(Some(table.namespace().name().to_string()), Some(table.name().to_string()))
		}
		ResolvedShape::RingBuffer(rb) => (Some(rb.namespace().name().to_string()), Some(rb.name().to_string())),
		ResolvedShape::View(view) => (Some(view.namespace().name().to_string()), Some(view.name().to_string())),
		ResolvedShape::DeferredView(view) => {
			(Some(view.namespace().name().to_string()), Some(view.name().to_string()))
		}
		ResolvedShape::TransactionalView(view) => {
			(Some(view.namespace().name().to_string()), Some(view.name().to_string()))
		}
		ResolvedShape::Dictionary(dict) => {
			(Some(dict.namespace().name().to_string()), Some(dict.name().to_string()))
		}
		ResolvedShape::Series(series) => {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sequence {
	pub name: String,
	pub current_value: i64,
	pub increment: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
	pub name: String,
	pub columns: Vec<String>,
	pub unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Function {
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

	fn test_namespace_def() -> Namespace {
		Namespace::Local {
			id: NamespaceId::SYSTEM,
			name: "public".to_string(),
			local_name: "public".to_string(),
			parent_id: NamespaceId::ROOT,
		}
	}

	fn test_table() -> Table {
		Table {
			id: TableId(1),
			namespace: NamespaceId::SYSTEM,
			name: "users".to_string(),
			columns: vec![
				Column {
					id: ColumnId(1),
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Int8),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
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
			underlying: false,
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
		let table = ResolvedTable::new(table_ident, namespace.clone(), test_table());

		assert_eq!(table.name(), "users");
		assert_eq!(table.fully_qualified_name(), "public::users");
		assert_eq!(table.columns().len(), 2);
		assert!(table.find_column("id").is_some());
		assert!(table.find_column("nonexistent").is_none());
	}

	#[test]
	fn test_resolved_shape_enum() {
		let namespace = ResolvedNamespace::new(Fragment::testing("public"), test_namespace_def());

		let table = ResolvedTable::new(Fragment::testing("users"), namespace, test_table());

		let shape = ResolvedShape::Table(table);

		assert!(shape.supports_indexes());
		assert!(shape.supports_mutations());
		assert_eq!(shape.kind_name(), "table");
		// effective_name removed - use identifier().text() instead
		assert_eq!(shape.fully_qualified_name(), Some("public::users".to_string()));
		assert!(shape.as_table().is_some());
		assert!(shape.as_view().is_none());
	}

	#[test]
	fn test_resolved_column() {
		let namespace = ResolvedNamespace::new(Fragment::testing("public"), test_namespace_def());

		let table = ResolvedTable::new(Fragment::testing("users"), namespace, test_table());

		let shape = ResolvedShape::Table(table);

		let column_ident = Fragment::testing("id");

		let column = Column {
			id: ColumnId(1),
			name: "id".to_string(),
			constraint: TypeConstraint::unconstrained(Type::Int8),
			properties: vec![],
			index: ColumnIndex(0),
			auto_increment: false,
			dictionary_id: None,
		};

		let column = ResolvedColumn::new(column_ident, shape, column);

		assert_eq!(column.name(), "id");
		assert_eq!(column.type_constraint(), &TypeConstraint::unconstrained(Type::Int8));
		assert!(!column.is_auto_increment());
		assert_eq!(column.qualified_name(), "public::users.id");
	}
}
