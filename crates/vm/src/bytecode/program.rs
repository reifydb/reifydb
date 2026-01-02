// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Program structure for compiled bytecode.
//!
//! DEPRECATED: This module is deprecated. Use `reifydb_rqlv2::bytecode::CompiledProgram` instead.

#![allow(dead_code)]

use reifydb_type::Value;

use crate::operator::sort::SortSpec;

/// A compiled VM program.
#[derive(Debug, Clone)]
pub struct Program {
	/// Raw bytecode.
	pub bytecode: Vec<u8>,

	/// Constant pool (literals, strings).
	pub constants: Vec<Value>,

	/// Expression pool (AST expressions, used during compilation).
	/// DEPRECATED: Use RQLv2's expression system instead.
	pub expressions: Vec<String>, // Placeholder - was Vec<Expr>

	/// Compiled filter expressions (closure-compiled for fast evaluation).
	/// DEPRECATED: Use RQLv2's CompiledFilter instead.
	pub compiled_filters: Vec<String>, // Placeholder - was Vec<CompiledFilter>

	/// Compiled expressions (closure-compiled for fast evaluation).
	/// DEPRECATED: Use RQLv2's CompiledExpr instead.
	pub compiled_exprs: Vec<String>, // Placeholder - was Vec<CompiledExpr>

	/// Source definitions (table names for scan).
	pub sources: Vec<SourceDef>,

	/// User-defined functions.
	pub functions: Vec<FunctionDef>,

	/// Column lists (for select).
	pub column_lists: Vec<Vec<String>>,

	/// Sort specifications.
	pub sort_specs: Vec<Vec<SortSpec>>,

	/// Extension specifications (name, expr_index pairs).
	pub extension_specs: Vec<Vec<(String, u16)>>,

	/// Subquery definitions for IN/EXISTS expressions.
	pub subqueries: Vec<SubqueryDef>,

	/// Entry point offset.
	pub entry_point: usize,
}

impl Program {
	/// Create a new empty program.
	pub fn new() -> Self {
		Self {
			bytecode: Vec::new(),
			constants: Vec::new(),
			expressions: Vec::new(),
			compiled_filters: Vec::new(),
			compiled_exprs: Vec::new(),
			sources: Vec::new(),
			functions: Vec::new(),
			column_lists: Vec::new(),
			sort_specs: Vec::new(),
			extension_specs: Vec::new(),
			subqueries: Vec::new(),
			entry_point: 0,
		}
	}

	/// Add a constant and return its index.
	pub fn add_constant(&mut self, value: Value) -> u16 {
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

	/// Add an expression and return its index.
	/// DEPRECATED: Use RQLv2's expression system instead.
	#[deprecated(note = "Use RQLv2's expression system")]
	pub fn add_expression(&mut self, _expr: String) -> u16 {
		let index = self.expressions.len();
		self.expressions.push(String::new());
		index as u16
	}

	/// Add a compiled filter and return its index.
	/// DEPRECATED: Use RQLv2's CompiledFilter instead.
	#[deprecated(note = "Use RQLv2's CompiledFilter")]
	pub fn add_compiled_filter(&mut self, _filter: String) -> u16 {
		let index = self.compiled_filters.len();
		self.compiled_filters.push(String::new());
		index as u16
	}

	/// Add a compiled expression and return its index.
	/// DEPRECATED: Use RQLv2's CompiledExpr instead.
	#[deprecated(note = "Use RQLv2's CompiledExpr")]
	pub fn add_compiled_expr(&mut self, _expr: String) -> u16 {
		let index = self.compiled_exprs.len();
		self.compiled_exprs.push(String::new());
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

	/// Add a function and return its index.
	pub fn add_function(&mut self, func: FunctionDef) -> u16 {
		let index = self.functions.len();
		self.functions.push(func);
		index as u16
	}

	/// Add a column list and return its index.
	pub fn add_column_list(&mut self, columns: Vec<String>) -> u16 {
		let index = self.column_lists.len();
		self.column_lists.push(columns);
		index as u16
	}

	/// Add a sort specification and return its index.
	pub fn add_sort_spec(&mut self, spec: Vec<SortSpec>) -> u16 {
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

	/// Find a function by name.
	pub fn find_function(&self, name: &str) -> Option<(u16, &FunctionDef)> {
		self.functions.iter().enumerate().find(|(_, f)| f.name == name).map(|(i, f)| (i as u16, f))
	}

	/// Add a subquery and return its index.
	pub fn add_subquery(&mut self, subquery: SubqueryDef) -> u16 {
		let index = self.subqueries.len();
		self.subqueries.push(subquery);
		index as u16
	}
}

impl Default for Program {
	fn default() -> Self {
		Self::new()
	}
}

/// Definition of a data source (table).
#[derive(Debug, Clone)]
pub struct SourceDef {
	/// Table name.
	pub name: String,
}

/// Definition of a user-defined function.
#[derive(Debug, Clone)]
pub struct FunctionDef {
	/// Function name.
	pub name: String,

	/// Function parameters.
	pub parameters: Vec<ParameterDef>,

	/// Offset into bytecode where function body starts.
	pub bytecode_offset: usize,

	/// Length of function bytecode.
	pub bytecode_len: usize,
}

/// Definition of a function parameter.
#[derive(Debug, Clone)]
pub struct ParameterDef {
	/// Parameter name.
	pub name: String,

	/// Optional type annotation.
	pub param_type: Option<String>,
}

/// Definition of a subquery for IN/EXISTS expressions.
#[derive(Debug, Clone)]
pub struct SubqueryDef {
	/// Source table name (for the scan stage).
	pub source_name: String,

	/// Filter expression index (if any).
	pub filter_expr_index: Option<u16>,

	/// Select column list index (if any).
	pub select_list_index: Option<u16>,

	/// Take limit (if any).
	pub take_limit: Option<u64>,

	/// Column references from outer query (for future correlated subquery support).
	pub outer_refs: Vec<String>,
}
