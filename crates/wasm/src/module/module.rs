// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crate::module::{
	FunctionIndex,
	function::{Export, Function},
	global::Global,
	memory::Memory,
	table::Table,
	types::FunctionType,
};

pub type ModuleId = u16;

pub struct Module {
	pub id: ModuleId,
	pub exports: Box<[Export]>,
	pub functions: Box<[Arc<Function>]>,
	pub function_types: Box<[FunctionType]>,
	pub globals: Box<[Global]>,
	pub memories: Box<[Memory]>,
	pub tables: Box<[Table]>,
	pub data_segments: Box<[DataSegment]>,
	pub element_segments: Box<[ElementSegment]>,
	pub start_function: Option<FunctionIndex>,
	pub function_imports: Vec<(String, String)>,
	pub table_imports: Vec<(String, String)>,
	pub memory_imports: Vec<(String, String)>,
	pub global_imports: Vec<(String, String)>,
	pub active_elements: Vec<ActiveElementInfo>,
	pub active_data: Vec<ActiveDataInfo>,
}

/// Represents an initializer for an active element segment entry.
#[derive(Clone, Debug)]
pub enum ActiveElementInit {
	/// A direct function reference.
	FuncRef(usize),
	/// Resolved from a global.get expression (global index).
	GlobalGet(usize),
	/// A null reference.
	RefNull,
}

/// Active element segment info for application during instantiation.
#[derive(Clone)]
pub struct ActiveElementInfo {
	pub table_idx: usize,
	pub offset: usize,
	pub inits: Vec<ActiveElementInit>,
}

/// Active data segment info for application during instantiation.
#[derive(Clone)]
pub struct ActiveDataInfo {
	pub mem_idx: usize,
	pub offset: usize,
	pub data: Box<[u8]>,
}

/// A passive data segment that can be used by memory.init and dropped by data.drop.
#[derive(Clone)]
pub struct DataSegment {
	pub data: Option<Box<[u8]>>,
}

/// A passive element segment that can be used by table.init and dropped by elem.drop.
#[derive(Clone)]
pub struct ElementSegment {
	pub elements: Option<Box<[Option<usize>]>>,
}

impl Module {
	pub fn new(
		id: ModuleId,
		exports: Box<[Export]>,
		functions: Box<[Arc<Function>]>,
		function_types: Box<[FunctionType]>,
		globals: Box<[Global]>,
		memories: Box<[Memory]>,
		tables: Box<[Table]>,
	) -> Self {
		Self {
			id,
			exports,
			functions,
			function_types,
			globals,
			memories,
			tables,
			data_segments: Box::new([]),
			element_segments: Box::new([]),
			start_function: None,
			function_imports: Vec::new(),
			table_imports: Vec::new(),
			memory_imports: Vec::new(),
			global_imports: Vec::new(),
			active_elements: Vec::new(),
			active_data: Vec::new(),
		}
	}

	pub fn with_segments(
		id: ModuleId,
		exports: Box<[Export]>,
		functions: Box<[Arc<Function>]>,
		function_types: Box<[FunctionType]>,
		globals: Box<[Global]>,
		memories: Box<[Memory]>,
		tables: Box<[Table]>,
		data_segments: Box<[DataSegment]>,
		element_segments: Box<[ElementSegment]>,
		start_function: Option<FunctionIndex>,
		function_imports: Vec<(String, String)>,
		table_imports: Vec<(String, String)>,
		memory_imports: Vec<(String, String)>,
		global_imports: Vec<(String, String)>,
		active_elements: Vec<ActiveElementInfo>,
		active_data: Vec<ActiveDataInfo>,
	) -> Self {
		Self {
			id,
			exports,
			functions,
			function_types,
			globals,
			memories,
			tables,
			data_segments,
			element_segments,
			start_function,
			function_imports,
			table_imports,
			memory_imports,
			global_imports,
			active_elements,
			active_data,
		}
	}
}
