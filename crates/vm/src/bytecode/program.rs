// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Program structure for compiled bytecode.

use reifydb_type::Value;

use crate::{
	expr::{CompiledExpr, CompiledFilter, Expr},
	operator::sort::SortSpec,
};

/// A compiled VM program.
#[derive(Debug, Clone)]
pub struct Program {
	/// Raw bytecode.
	pub bytecode: Vec<u8>,

	/// Constant pool (literals, strings).
	pub constants: Vec<Value>,

	/// Expression pool (AST expressions, used during compilation).
	pub expressions: Vec<Expr>,

	/// Compiled filter expressions (closure-compiled for fast evaluation).
	pub compiled_filters: Vec<CompiledFilter>,

	/// Compiled expressions (closure-compiled for fast evaluation).
	pub compiled_exprs: Vec<CompiledExpr>,

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
	pub fn add_expression(&mut self, expr: Expr) -> u16 {
		let index = self.expressions.len();
		self.expressions.push(expr);
		index as u16
	}

	/// Add a compiled filter and return its index.
	pub fn add_compiled_filter(&mut self, filter: CompiledFilter) -> u16 {
		let index = self.compiled_filters.len();
		self.compiled_filters.push(filter);
		index as u16
	}

	/// Add a compiled expression and return its index.
	pub fn add_compiled_expr(&mut self, expr: CompiledExpr) -> u16 {
		let index = self.compiled_exprs.len();
		self.compiled_exprs.push(expr);
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
