// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Plan types - resolved catalog entities and computed expressions.
//!
//! This module provides:
//! - Catalog entity types (Namespace, Table, CatalogColumn, View, etc.)
//! - The `Column` enum distinguishing catalog vs computed columns
//! - Output schema tracking for pipeline stages

use bumpalo::{Bump, collections::Vec as BumpVec};
use reifydb_core::interface::catalog::id::{
	ColumnId, DictionaryId, IndexId, NamespaceId, RingBufferId, SequenceId, TableId, ViewId,
};
use reifydb_type::value::r#type::Type;

use crate::{
	plan::compile::core::{PlanError, PlanErrorKind},
	token::span::Span,
};
// ============================================================================
// Catalog Entity Types
// ============================================================================

/// Namespace - bump allocated.
#[derive(Debug, Clone, Copy)]
pub struct Namespace<'bump> {
	pub id: NamespaceId,
	pub name: &'bump str,
	pub span: Span,
}

/// Table - bump allocated.
#[derive(Debug, Clone, Copy)]
pub struct Table<'bump> {
	pub id: TableId,
	pub namespace: &'bump Namespace<'bump>,
	pub name: &'bump str,
	pub columns: &'bump [CatalogColumn<'bump>],
	pub span: Span,
}

/// Catalog column - a column that exists in the catalog (table, view, etc.)
#[derive(Debug, Clone, Copy)]
pub struct CatalogColumn<'bump> {
	pub id: ColumnId,
	pub name: &'bump str,
	pub column_type: Type,
	pub column_index: u32,
	pub span: Span,
}

/// View - bump allocated.
#[derive(Debug, Clone, Copy)]
pub struct View<'bump> {
	pub id: ViewId,
	pub namespace: &'bump Namespace<'bump>,
	pub name: &'bump str,
	pub columns: &'bump [CatalogColumn<'bump>],
	pub span: Span,
}

/// Index - bump allocated.
#[derive(Debug, Clone, Copy)]
pub struct Index<'bump> {
	pub id: IndexId,
	pub table: &'bump Table<'bump>,
	pub name: &'bump str,
	pub columns: &'bump [&'bump CatalogColumn<'bump>],
	pub unique: bool,
	pub span: Span,
}

/// Sequence - bump allocated.
#[derive(Debug, Clone, Copy)]
pub struct Sequence<'bump> {
	pub id: SequenceId,
	pub namespace: &'bump Namespace<'bump>,
	pub name: &'bump str,
	pub span: Span,
}

/// Ring buffer - bump allocated.
#[derive(Debug, Clone, Copy)]
pub struct RingBuffer<'bump> {
	pub id: RingBufferId,
	pub namespace: &'bump Namespace<'bump>,
	pub name: &'bump str,
	pub columns: &'bump [CatalogColumn<'bump>],
	pub capacity: u64,
	pub span: Span,
}

/// Dictionary - bump allocated.
#[derive(Debug, Clone, Copy)]
pub struct Dictionary<'bump> {
	pub id: DictionaryId,
	pub namespace: &'bump Namespace<'bump>,
	pub name: &'bump str,
	pub key_type: Type,
	pub value_type: Type,
	pub span: Span,
}

/// Unified primitive - any scannable data source.
#[derive(Debug, Clone, Copy)]
pub enum Primitive<'bump> {
	Table(&'bump Table<'bump>),
	View(&'bump View<'bump>),
	RingBuffer(&'bump RingBuffer<'bump>),
	Dictionary(&'bump Dictionary<'bump>),
}

impl<'bump> Primitive<'bump> {
	/// Get the span of this primitive.
	pub fn span(&self) -> Span {
		match self {
			Primitive::Table(t) => t.span,
			Primitive::View(v) => v.span,
			Primitive::RingBuffer(r) => r.span,
			Primitive::Dictionary(d) => d.span,
		}
	}

	/// Get the columns of this primitive, if it has any.
	pub fn columns(&self) -> Option<&'bump [CatalogColumn<'bump>]> {
		match self {
			Primitive::Table(t) => Some(t.columns),
			Primitive::View(v) => Some(v.columns),
			Primitive::RingBuffer(r) => Some(r.columns),
			Primitive::Dictionary(_) => None,
		}
	}

	/// Get the name of this primitive.
	pub fn name(&self) -> &'bump str {
		match self {
			Primitive::Table(t) => t.name,
			Primitive::View(v) => v.name,
			Primitive::RingBuffer(r) => r.name,
			Primitive::Dictionary(d) => d.name,
		}
	}

	/// Get the namespace of this primitive.
	pub fn namespace(&self) -> &'bump Namespace<'bump> {
		match self {
			Primitive::Table(t) => t.namespace,
			Primitive::View(v) => v.namespace,
			Primitive::RingBuffer(r) => r.namespace,
			Primitive::Dictionary(d) => d.namespace,
		}
	}
}

/// Function reference.
#[derive(Debug, Clone, Copy)]
pub struct Function<'bump> {
	pub name: &'bump str,
	pub is_aggregate: bool,
	pub span: Span,
}

/// Variable reference.
#[derive(Debug, Clone, Copy)]
pub struct Variable<'bump> {
	pub name: &'bump str,
	pub variable_id: u32,
	pub span: Span,
}

// ============================================================================
// Column Types (Catalog vs Computed)
// ============================================================================

/// A column - either from catalog or computed during pipeline.
#[derive(Debug, Clone, Copy)]
pub enum Column<'bump> {
	/// Column that exists in the catalog (table, view, etc.)
	Catalog(&'bump CatalogColumn<'bump>),
	/// Column computed during pipeline execution (map, extend, aggregate)
	Computed(&'bump ComputedColumn<'bump>),
}

/// A column computed during pipeline execution.
#[derive(Debug, Clone, Copy)]
pub struct ComputedColumn<'bump> {
	/// Name of the computed column
	pub name: &'bump str,
	/// Inferred type
	pub column_type: Type,
	/// Source span for error reporting
	pub span: Span,
}

impl<'bump> Column<'bump> {
	/// Get the column name regardless of variant.
	pub fn name(&self) -> &'bump str {
		match self {
			Column::Catalog(col) => col.name,
			Column::Computed(col) => col.name,
		}
	}

	/// Get the column type regardless of variant.
	pub fn column_type(&self) -> Type {
		match self {
			Column::Catalog(col) => col.column_type.clone(),
			Column::Computed(col) => col.column_type.clone(),
		}
	}

	/// Get the span regardless of variant.
	pub fn span(&self) -> Span {
		match self {
			Column::Catalog(col) => col.span,
			Column::Computed(col) => col.span,
		}
	}
}

// ============================================================================
// Output Schema Tracking
// ============================================================================

/// Output schema of a pipeline stage - tracks available columns.
///
/// This structure tracks which columns are available at each point in a pipeline,
/// supporting both qualified references (like `u.name`) and unqualified references (like `name`).
pub struct OutputSchema<'bump> {
	bump: &'bump Bump,
	/// Named sources (for qualified refs like u.name)
	sources: BumpVec<'bump, SchemaSource<'bump>>,
	/// Flat list of all available columns (for unqualified refs)
	all_columns: BumpVec<'bump, Column<'bump>>,
}

/// A named source of columns (table, alias, or subquery).
pub struct SchemaSource<'bump> {
	/// The name used to qualify columns (alias or table name)
	name: &'bump str,
	/// Columns available from this source
	columns: BumpVec<'bump, Column<'bump>>,
}

impl<'bump> OutputSchema<'bump> {
	/// Create a new empty output schema.
	pub fn new_in(bump: &'bump Bump) -> Self {
		Self {
			bump,
			sources: BumpVec::new_in(bump),
			all_columns: BumpVec::new_in(bump),
		}
	}

	/// Add a source (table/alias) with its catalog columns.
	pub fn add_source(&mut self, name: &'bump str, columns: &'bump [CatalogColumn<'bump>]) {
		let mut source_cols = BumpVec::with_capacity_in(columns.len(), self.bump);
		for col in columns.iter() {
			let schema_col = Column::Catalog(col);
			source_cols.push(schema_col);
			self.all_columns.push(schema_col);
		}
		self.sources.push(SchemaSource {
			name,
			columns: source_cols,
		});
	}

	/// Add a computed column (from MAP, EXTEND, AGGREGATE).
	/// Uses Type::Any as a stub
	/// FIXME actual type inference to be implemented later.
	pub fn add_computed(&mut self, name: &'bump str, span: Span) -> Column<'bump> {
		let col = self.bump.alloc(ComputedColumn {
			name,
			column_type: Type::Any, // Stub: actual type inference to be implemented
			span,
		});
		let schema_col = Column::Computed(col);
		self.all_columns.push(schema_col);
		schema_col
	}

	/// Resolve a qualified column reference (e.g., u.name).
	pub fn resolve_qualified(&self, source: &str, column: &str) -> Option<Column<'bump>> {
		for src in self.sources.iter() {
			if src.name == source {
				return src.columns.iter().find(|c| c.name() == column).copied();
			}
		}
		None
	}

	/// Resolve an unqualified column reference (e.g., name).
	/// Returns error if the column is ambiguous (exists in multiple sources).
	pub fn resolve_unqualified(&self, column: &str, span: Span) -> Result<Column<'bump>, PlanError> {
		let matches: Vec<_> = self.all_columns.iter().filter(|c| c.name() == column).collect();
		match matches.len() {
			0 => Err(PlanError {
				kind: PlanErrorKind::ColumnNotFound(column.to_string()),
				span,
			}),
			1 => Ok(*matches[0]),
			_ => Err(PlanError {
				kind: PlanErrorKind::ColumnNotFound(format!("ambiguous column: {}", column)),
				span,
			}),
		}
	}

	/// Create a new schema with only the specified columns (for MAP projection).
	pub fn project(&self, columns: &[Column<'bump>]) -> OutputSchema<'bump> {
		let mut new_schema = OutputSchema::new_in(self.bump);
		for col in columns.iter() {
			new_schema.all_columns.push(*col);
		}
		new_schema
	}

	/// Clone the schema (for branching pipelines).
	pub fn clone_schema(&self) -> OutputSchema<'bump> {
		let mut new_schema = OutputSchema::new_in(self.bump);
		for src in self.sources.iter() {
			let mut source_cols = BumpVec::with_capacity_in(src.columns.len(), self.bump);
			for col in src.columns.iter() {
				source_cols.push(*col);
			}
			new_schema.sources.push(SchemaSource {
				name: src.name,
				columns: source_cols,
			});
		}
		for col in self.all_columns.iter() {
			new_schema.all_columns.push(*col);
		}
		new_schema
	}

	/// Get all columns in this schema.
	pub fn columns(&self) -> &[Column<'bump>] {
		&self.all_columns
	}

	/// Merge another schema into this one (for joins).
	pub fn merge(&mut self, other: &OutputSchema<'bump>) {
		// Add all sources from the other schema
		for src in other.sources.iter() {
			let mut source_cols = BumpVec::with_capacity_in(src.columns.len(), self.bump);
			for col in src.columns.iter() {
				source_cols.push(*col);
			}
			self.sources.push(SchemaSource {
				name: src.name,
				columns: source_cols,
			});
		}
		// Add all columns from the other schema
		for col in other.all_columns.iter() {
			self.all_columns.push(*col);
		}
	}
}
