// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Compiled program structure for bytecode execution.

use std::{ops::Deref, sync::Arc};

use super::opcode::ObjectType;
use crate::{
	expression::types::{CompiledExpr, CompiledFilter},
	token::span::Span,
};

/// Source map for mapping bytecode offsets to source spans.
/// Used for error reporting at runtime.
#[derive(Debug, Clone, Default)]
pub struct SourceMap {
	/// Entries sorted by bytecode_offset for binary search.
	entries: Vec<SourceMapEntry>,
}

/// A single entry in the source map.
#[derive(Debug, Clone, Copy)]
pub struct SourceMapEntry {
	/// Offset into the bytecode where this span starts.
	pub bytecode_offset: u32,
	/// The source span corresponding to this bytecode location.
	pub span: Span,
}

impl SourceMap {
	/// Create a new empty source map.
	pub fn new() -> Self {
		Self {
			entries: Vec::new(),
		}
	}

	/// Add an entry to the source map.
	/// Entries should be added in order of increasing bytecode_offset.
	pub fn add(&mut self, bytecode_offset: u32, span: Span) {
		self.entries.push(SourceMapEntry {
			bytecode_offset,
			span,
		});
	}

	/// Build a source map from a vector of entries.
	/// The entries will be sorted by bytecode_offset.
	pub fn from_entries(mut entries: Vec<SourceMapEntry>) -> Self {
		entries.sort_by_key(|e| e.bytecode_offset);
		Self {
			entries,
		}
	}

	/// Lookup the span for a given bytecode offset.
	/// Returns the span of the most recent instruction at or before the offset.
	pub fn lookup(&self, offset: usize) -> Option<Span> {
		if self.entries.is_empty() {
			return None;
		}

		let offset = offset as u32;

		// Binary search for the largest entry <= offset
		match self.entries.binary_search_by_key(&offset, |e| e.bytecode_offset) {
			Ok(idx) => Some(self.entries[idx].span),
			Err(idx) => {
				if idx > 0 {
					Some(self.entries[idx - 1].span)
				} else {
					None
				}
			}
		}
	}

	/// Get the number of entries in the source map.
	pub fn len(&self) -> usize {
		self.entries.len()
	}

	/// Check if the source map is empty.
	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}
}

/// Inner data structure for a compiled program.
///
/// This contains all the actual program data and is wrapped in an Arc
/// by CompiledProgram for cheap cloning.
#[derive(Debug, Clone)]
pub struct Inner {
	/// Raw bytecode.
	pub bytecode: Vec<u8>,

	/// Constant pool (literals, strings).
	pub constants: Vec<Constant>,

	/// Source definitions (table/view names for scan).
	pub sources: Vec<SourceDef>,

	/// Source map for error reporting.
	pub source_map: SourceMap,

	/// Column lists (for select).
	pub column_lists: Vec<Vec<String>>,

	/// Sort specifications.
	pub sort_specs: Vec<SortSpec>,

	/// Extension specifications (name, expr_index pairs).
	pub extension_specs: Vec<Vec<(String, u16)>>,

	/// Subquery definitions for IN/EXISTS expressions.
	pub subqueries: Vec<SubqueryDef>,

	/// DDL definitions for CREATE/ALTER/DROP operations.
	pub ddl_defs: Vec<DdlDef>,

	/// DML target definitions for INSERT/UPDATE/DELETE.
	pub dml_targets: Vec<DmlTarget>,

	/// Compiled expressions for evaluation.
	pub compiled_exprs: Vec<CompiledExpr>,

	/// Compiled filters for predicate evaluation.
	pub compiled_filters: Vec<CompiledFilter>,

	/// Entry point offset.
	pub entry_point: usize,

	/// Script function definitions (user-defined functions).
	pub script_functions: Vec<ScriptFunctionDef>,
}

/// A compiled program ready for VM execution.
///
/// This struct is designed to be:
/// - Cheap to clone (uses Arc internally)
/// - Long-lived (suitable for prepared statement caching)
/// - Reusable (can be executed multiple times with different parameters)
/// - Immutable after construction (built via CompiledProgramBuilder)
#[derive(Debug, Clone)]
pub struct CompiledProgram {
	inner: Arc<Inner>,
}

/// A constant value in the program.
#[derive(Debug, Clone, PartialEq)]
pub enum Constant {
	Undefined,
	Bool(bool),
	Int(i64),
	Float(f64),
	String(String),
	Bytes(Vec<u8>),
}

impl Deref for CompiledProgram {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

/// Builder for constructing a CompiledProgram.
///
/// Use this to build up a program incrementally during compilation,
/// then call `build()` to create the final immutable CompiledProgram.
#[derive(Debug)]
pub struct CompiledProgramBuilder {
	pub bytecode: Vec<u8>,
	pub constants: Vec<Constant>,
	pub sources: Vec<SourceDef>,
	pub source_map: SourceMap,
	pub column_lists: Vec<Vec<String>>,
	pub sort_specs: Vec<SortSpec>,
	pub extension_specs: Vec<Vec<(String, u16)>>,
	pub subqueries: Vec<SubqueryDef>,
	pub ddl_defs: Vec<DdlDef>,
	pub dml_targets: Vec<DmlTarget>,
	pub compiled_exprs: Vec<CompiledExpr>,
	pub compiled_filters: Vec<CompiledFilter>,
	pub entry_point: usize,
	pub script_functions: Vec<ScriptFunctionDef>,
}

impl CompiledProgramBuilder {
	/// Create a new empty program builder.
	pub fn new() -> Self {
		Self {
			bytecode: Vec::new(),
			constants: Vec::new(),
			sources: Vec::new(),
			source_map: SourceMap::new(),
			column_lists: Vec::new(),
			sort_specs: Vec::new(),
			extension_specs: Vec::new(),
			subqueries: Vec::new(),
			ddl_defs: Vec::new(),
			dml_targets: Vec::new(),
			compiled_exprs: Vec::new(),
			compiled_filters: Vec::new(),
			entry_point: 0,
			script_functions: Vec::new(),
		}
	}

	/// Build the final CompiledProgram.
	///
	/// This consumes the builder and wraps the data in an Arc for cheap cloning.
	pub fn build(self) -> CompiledProgram {
		CompiledProgram {
			inner: Arc::new(Inner {
				bytecode: self.bytecode,
				constants: self.constants,
				sources: self.sources,
				source_map: self.source_map,
				column_lists: self.column_lists,
				sort_specs: self.sort_specs,
				extension_specs: self.extension_specs,
				subqueries: self.subqueries,
				ddl_defs: self.ddl_defs,
				dml_targets: self.dml_targets,
				compiled_exprs: self.compiled_exprs,
				compiled_filters: self.compiled_filters,
				entry_point: self.entry_point,
				script_functions: self.script_functions,
			}),
		}
	}

	/// Get mutable access to the bytecode vector.
	pub fn bytecode_mut(&mut self) -> &mut Vec<u8> {
		&mut self.bytecode
	}

	/// Set the entry point offset.
	pub fn set_entry_point(&mut self, entry_point: usize) {
		self.entry_point = entry_point;
	}

	/// Get mutable access to the source map.
	pub fn source_map_mut(&mut self) -> &mut SourceMap {
		&mut self.source_map
	}

	/// Add a constant and return its index.
	pub fn add_constant(&mut self, value: Constant) -> u16 {
		// Check if constant already exists
		for (i, existing) in self.constants.iter().enumerate() {
			if *existing == value {
				return i as u16;
			}
		}
		let index = self.constants.len();
		self.constants.push(value);
		index as u16
	}

	/// Add a source and return its index.
	pub fn add_source(&mut self, source: SourceDef) -> u16 {
		// Check if source already exists
		for (i, existing) in self.sources.iter().enumerate() {
			if existing.name == source.name {
				return i as u16;
			}
		}
		let index = self.sources.len();
		self.sources.push(source);
		index as u16
	}

	/// Add a column list and return its index.
	pub fn add_column_list(&mut self, columns: Vec<String>) -> u16 {
		let index = self.column_lists.len();
		self.column_lists.push(columns);
		index as u16
	}

	/// Add a sort specification and return its index.
	pub fn add_sort_spec(&mut self, spec: SortSpec) -> u16 {
		let index = self.sort_specs.len();
		self.sort_specs.push(spec);
		index as u16
	}

	/// Add an extension specification and return its index.
	pub fn add_extension_spec(&mut self, spec: Vec<(String, u16)>) -> u16 {
		let index = self.extension_specs.len();
		self.extension_specs.push(spec);
		index as u16
	}

	/// Add a subquery and return its index.
	pub fn add_subquery(&mut self, subquery: SubqueryDef) -> u16 {
		let index = self.subqueries.len();
		self.subqueries.push(subquery);
		index as u16
	}

	/// Add a DDL definition and return its index.
	pub fn add_ddl_def(&mut self, def: DdlDef) -> u16 {
		let index = self.ddl_defs.len();
		self.ddl_defs.push(def);
		index as u16
	}

	/// Add a DML target and return its index.
	pub fn add_dml_target(&mut self, target: DmlTarget) -> u16 {
		let index = self.dml_targets.len();
		self.dml_targets.push(target);
		index as u16
	}

	/// Add a compiled expression and return its index.
	pub fn add_compiled_expr(&mut self, expr: CompiledExpr) -> u16 {
		let index = self.compiled_exprs.len();
		self.compiled_exprs.push(expr);
		index as u16
	}

	/// Add a compiled filter and return its index.
	pub fn add_compiled_filter(&mut self, filter: CompiledFilter) -> u16 {
		let index = self.compiled_filters.len();
		self.compiled_filters.push(filter);
		index as u16
	}

	/// Add a script function and return its index.
	pub fn add_script_function(&mut self, func: ScriptFunctionDef) -> u16 {
		let index = self.script_functions.len();
		self.script_functions.push(func);
		index as u16
	}
}

impl Default for CompiledProgramBuilder {
	fn default() -> Self {
		Self::new()
	}
}

/// Definition of a data source (table, view, etc.).
#[derive(Debug, Clone)]
pub struct SourceDef {
	/// Source name (fully qualified).
	pub name: String,
	/// Optional alias.
	pub alias: Option<String>,
}

/// Sort specification for ORDER BY.
#[derive(Debug, Clone)]
pub struct SortSpec {
	/// Sort keys.
	pub keys: Vec<SortKey>,
}

/// A single sort key.
#[derive(Debug, Clone)]
pub struct SortKey {
	/// Column name or expression index.
	pub column: String,
	/// Sort direction.
	pub direction: SortDirection,
	/// Nulls ordering.
	pub nulls: NullsOrder,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SortDirection {
	#[default]
	Asc,
	Desc,
}

/// Nulls ordering.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NullsOrder {
	#[default]
	First,
	Last,
}

/// Definition of a subquery for IN/EXISTS expressions.
#[derive(Debug, Clone)]
pub struct SubqueryDef {
	/// Compiled bytecode for the subquery.
	/// This is a complete program that can be executed independently.
	pub bytecode: Vec<u8>,

	/// Constants used by the subquery.
	pub constants: Vec<Constant>,

	/// Sources used by the subquery.
	pub sources: Vec<SourceDef>,

	/// Column references from outer query (for correlated subqueries).
	pub outer_refs: Vec<String>,

	/// Source map for error reporting.
	pub source_map: SourceMap,
}

/// DDL definition for CREATE/ALTER/DROP operations.
#[derive(Debug, Clone)]
pub enum DdlDef {
	CreateNamespace(CreateNamespaceDef),
	CreateTable(CreateTableDef),
	CreateView(CreateViewDef),
	CreateIndex(CreateIndexDef),
	CreateSequence(CreateSequenceDef),
	CreateRingBuffer(CreateRingBufferDef),
	CreateDictionary(CreateDictionaryDef),
	AlterTable(AlterTableDef),
	AlterSequence(AlterSequenceDef),
	Drop(DropDef),
}

/// Create namespace definition.
#[derive(Debug, Clone)]
pub struct CreateNamespaceDef {
	pub name: String,
	pub if_not_exists: bool,
}

/// Create table definition.
#[derive(Debug, Clone)]
pub struct CreateTableDef {
	pub namespace: Option<String>,
	pub name: String,
	pub columns: Vec<ColumnDef>,
	pub primary_key: Option<Vec<String>>,
	pub if_not_exists: bool,
}

/// Column definition.
#[derive(Debug, Clone)]
pub struct ColumnDef {
	pub name: String,
	pub data_type: String,
	pub nullable: bool,
	pub default: Option<u16>, // Constant index
}

/// Create view definition.
#[derive(Debug, Clone)]
pub struct CreateViewDef {
	pub namespace: Option<String>,
	pub name: String,
	pub query_bytecode_offset: usize,
	pub if_not_exists: bool,
}

/// Create index definition.
#[derive(Debug, Clone)]
pub struct CreateIndexDef {
	pub table: String,
	pub name: String,
	pub columns: Vec<String>,
	pub unique: bool,
}

/// Create sequence definition.
#[derive(Debug, Clone)]
pub struct CreateSequenceDef {
	pub namespace: Option<String>,
	pub name: String,
	pub start: i64,
	pub increment: i64,
	pub if_not_exists: bool,
}

/// Create ring buffer definition.
#[derive(Debug, Clone)]
pub struct CreateRingBufferDef {
	pub namespace: Option<String>,
	pub name: String,
	pub columns: Vec<ColumnDef>,
	pub capacity: u64,
	pub if_not_exists: bool,
}

/// Create dictionary definition.
#[derive(Debug, Clone)]
pub struct CreateDictionaryDef {
	pub namespace: Option<String>,
	pub name: String,
	pub key_type: String,
	pub value_type: String,
	pub if_not_exists: bool,
}

/// Alter table definition.
#[derive(Debug, Clone)]
pub struct AlterTableDef {
	pub table: String,
	pub action: AlterTableAction,
}

/// Alter table action.
#[derive(Debug, Clone)]
pub enum AlterTableAction {
	AddColumn(ColumnDef),
	DropColumn(String),
	RenameColumn {
		old: String,
		new: String,
	},
}

/// Alter sequence definition.
#[derive(Debug, Clone)]
pub struct AlterSequenceDef {
	pub sequence: String,
	pub restart: Option<i64>,
}

/// Drop definition.
#[derive(Debug, Clone)]
pub struct DropDef {
	pub object_type: ObjectType,
	pub name: String,
	pub if_exists: bool,
}

/// DML target for INSERT/UPDATE/DELETE.
#[derive(Debug, Clone)]
pub struct DmlTarget {
	/// Target type.
	pub target_type: DmlTargetType,
	/// Target name (table, ring buffer, or dictionary).
	pub name: String,
	/// Column names.
	pub columns: Option<Vec<String>>,
}

/// DML target type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlTargetType {
	Table,
	RingBuffer,
	Dictionary,
}

/// Script function definition (user-defined function in RQL scripts).
#[derive(Debug, Clone)]
pub struct ScriptFunctionDef {
	/// Function name.
	pub name: String,
	/// Offset into bytecode where function body starts.
	pub bytecode_offset: usize,
	/// Length of function body in bytes.
	pub bytecode_len: usize,
	/// Number of parameters (0 for parameterless functions).
	pub parameter_count: u8,
}
