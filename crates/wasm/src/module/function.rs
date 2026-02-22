// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::module::{
	FunctionIndex, GlobalIndex, MemoryIndex, TableIndex,
	types::{FunctionType, Instruction, ValueType, ValueTypes},
};

pub enum Function {
	Local(FunctionLocal),
	External(FunctionExternal),
}

impl Function {
	pub fn local(ty: FunctionType, locals: ValueTypes, instructions: Box<[Instruction]>) -> Self {
		Function::Local(FunctionLocal {
			function_type: ty,
			locals,
			instructions,
		})
	}
}

pub struct FunctionExternal {
	pub module: String,
	pub function_name: String,
	pub function_type: FunctionType,
}

pub struct FunctionLocal {
	pub function_type: FunctionType,
	pub locals: ValueTypes,
	pub instructions: Box<[Instruction]>,
}

impl FunctionLocal {
	pub fn result_count(&self) -> usize {
		self.function_type.results.len()
	}

	pub fn parameter_count(&self) -> usize {
		self.function_type.params.len()
	}

	pub fn parameters(&self) -> &ValueTypes {
		&self.function_type.params
	}

	pub fn locals(&self) -> &[ValueType] {
		self.locals.as_ref()
	}

	pub fn instructions(&self) -> &Box<[Instruction]> {
		&self.instructions
	}
}

#[derive(Clone)]
pub struct Export {
	pub name: String,
	pub data: ExportData,
}

impl Export {
	pub fn new_function(name: String, addr: FunctionIndex) -> Self {
		Self {
			name,
			data: ExportData::Function(addr),
		}
	}

	pub fn new_global(name: String, idx: GlobalIndex) -> Self {
		Self {
			name,
			data: ExportData::Global(idx),
		}
	}

	pub fn new_memory(name: String, idx: MemoryIndex) -> Self {
		Self {
			name,
			data: ExportData::Memory(idx),
		}
	}

	pub fn name(&self) -> &str {
		self.name.as_ref()
	}

	pub fn data(&self) -> &ExportData {
		&self.data
	}
}

#[derive(Clone)]
pub enum ExportData {
	Function(FunctionIndex),
	Global(GlobalIndex),
	Memory(MemoryIndex),
	Table(TableIndex),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExternalIndex(pub u32);
