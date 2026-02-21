// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crate::module::{Export, Function, FunctionIndex, FunctionType, Global, Memory, Table};

pub type ModuleId = u16;

// ---------------------------------------------------------------------------
// Module
// ---------------------------------------------------------------------------

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
		}
	}
}
