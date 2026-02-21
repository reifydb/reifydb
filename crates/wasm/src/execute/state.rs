// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crate::{
	execute::Result,
	module::{
		DataSegment, ElementSegment, Export, Function, FunctionIndex, FunctionType, FunctionTypeIndex, Global,
		GlobalIndex, Memory, MemoryIndex, Module, Table, TableElementIndex, TableIndex, Trap, TrapNotFound,
		TrapOutOfRange, Value, ValueType,
	},
};

// ---------------------------------------------------------------------------
// StateError
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum StateError {
	NotFoundFunction(String),
	NotFoundMemory(MemoryIndex),
	NotFoundModule(String),
	NotFoundTypes,
}

impl std::fmt::Display for StateError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			StateError::NotFoundFunction(name) => write!(f, "Function not found: {}", name),
			StateError::NotFoundModule(name) => write!(f, "Module not found: {}", name),
			StateError::NotFoundMemory(addr) => write!(f, "Memory not found: {}", addr),
			StateError::NotFoundTypes => write!(f, "Types not found"),
		}
	}
}

// ---------------------------------------------------------------------------
// StateGlobal
// ---------------------------------------------------------------------------

pub struct StateGlobal {
	data: Vec<Global>,
}

impl From<&Module> for StateGlobal {
	fn from(value: &Module) -> Self {
		Self {
			data: value.globals.iter().map(|g| g.clone()).collect(),
		}
	}
}

impl StateGlobal {
	pub fn set(&mut self, idx: GlobalIndex, value: Value) -> Result<()> {
		self.data[idx as usize] = Global {
			mutable: false,
			value,
		};
		Ok(())
	}

	pub fn get(&mut self, idx: GlobalIndex) -> Result<Value> {
		Ok(self.data[idx as usize].value.clone())
	}
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

pub struct State {
	pub(crate) exports: Box<[Export]>,
	pub(crate) functions: Box<[Arc<Function>]>,
	pub(crate) function_types: Box<[FunctionType]>,
	pub(crate) global: StateGlobal,
	pub(crate) memories: Box<[Memory]>,
	pub(crate) tables: Box<[Table]>,
	pub(crate) data_segments: Vec<DataSegment>,
	pub(crate) element_segments: Vec<ElementSegment>,
}

impl State {
	pub fn new(module: &Module) -> std::result::Result<Self, StateError> {
		Ok(Self {
			exports: module.exports.to_vec().into_boxed_slice(),
			functions: module.functions.clone(),
			function_types: module.function_types.clone(),
			global: StateGlobal::from(module),
			memories: module.memories.to_vec().into_boxed_slice(),
			tables: module.tables.to_vec().into_boxed_slice(),
			data_segments: module.data_segments.to_vec(),
			element_segments: module.element_segments.to_vec(),
		})
	}

	pub fn function(&self, idx: FunctionIndex) -> std::result::Result<Arc<Function>, Trap> {
		self.functions
			.get(idx as usize)
			.ok_or(Trap::NotFound(TrapNotFound::FunctionLocal(idx)))
			.map(|arc| arc.clone())
	}

	pub fn function_type(&self, idx: FunctionTypeIndex) -> std::result::Result<FunctionType, Trap> {
		self.function_types
			.get(idx as usize)
			.ok_or(Trap::NotFound(TrapNotFound::FunctionType(idx)))
			.map(|ft| ft.clone())
	}

	pub fn export(&self, name: impl Into<String>) -> std::result::Result<Export, Trap> {
		let name = name.into();
		self.exports
			.iter()
			.find(|export| export.name().eq(&name))
			.map(|e| e.clone())
			.ok_or(Trap::NotFound(TrapNotFound::ExportedFunction(name)))
	}

	pub fn memory(&self, idx: MemoryIndex) -> std::result::Result<&Memory, Trap> {
		self.memories.get(idx as usize).ok_or(Trap::NotFound(TrapNotFound::Memory(idx)))
	}

	pub fn memory_mut(&mut self, idx: MemoryIndex) -> std::result::Result<&mut Memory, Trap> {
		self.memories.get_mut(idx as usize).ok_or(Trap::NotFound(TrapNotFound::Memory(idx)))
	}

	pub fn table(&self, idx: TableIndex) -> std::result::Result<&Table, Trap> {
		self.tables.get(idx as usize).ok_or(Trap::NotFound(TrapNotFound::Table(idx)))
	}

	pub fn table_at(
		&self,
		table_idx: TableIndex,
		element_idx: TableElementIndex,
	) -> std::result::Result<Value, Trap> {
		let table = self.table(table_idx)?;

		let element = table
			.elements
			.get(element_idx as usize)
			.ok_or(Trap::OutOfRange(TrapOutOfRange::Table(table_idx)))?;

		match element {
			Some(value) => Ok(value.clone()),
			None => Ok(Value::RefNull(ValueType::RefFunc)),
		}
	}

	pub fn table_mut(&mut self, idx: TableIndex) -> std::result::Result<&mut Table, Trap> {
		self.tables.get_mut(idx as usize).ok_or(Trap::NotFound(TrapNotFound::Table(idx)))
	}

	pub fn memory_copy(&mut self, dst: usize, src: usize, len: usize) -> Result<()> {
		let memory = self.memory(0)?;
		let mem_len = memory.len();
		if src.checked_add(len).map_or(true, |end| end > mem_len)
			|| dst.checked_add(len).map_or(true, |end| end > mem_len)
		{
			return Err(Trap::OutOfRange(TrapOutOfRange::Memory(0)));
		}
		let memory = self.memory_mut(0)?;
		memory.data.copy_within(src..src + len, dst);
		Ok(())
	}

	pub fn memory_fill(&mut self, dst: usize, val: u8, len: usize) -> Result<()> {
		let memory = self.memory(0)?;
		let mem_len = memory.len();
		if dst.checked_add(len).map_or(true, |end| end > mem_len) {
			return Err(Trap::OutOfRange(TrapOutOfRange::Memory(0)));
		}
		let memory = self.memory_mut(0)?;
		memory.data[dst..dst + len].fill(val);
		Ok(())
	}

	pub fn memory_init(&mut self, data_idx: u32, dst: usize, src: usize, len: usize) -> Result<()> {
		let seg =
			self.data_segments.get(data_idx as usize).ok_or(Trap::OutOfRange(TrapOutOfRange::Memory(0)))?;
		match seg.data.as_ref() {
			Some(data) => {
				if src.checked_add(len).map_or(true, |end| end > data.len()) {
					return Err(Trap::OutOfRange(TrapOutOfRange::Memory(0)));
				}
				let memory = self.memory(0)?;
				let mem_len = memory.len();
				if dst.checked_add(len).map_or(true, |end| end > mem_len) {
					return Err(Trap::OutOfRange(TrapOutOfRange::Memory(0)));
				}
				let src_data = data[src..src + len].to_vec();
				let memory = self.memory_mut(0)?;
				memory.data[dst..dst + len].copy_from_slice(&src_data);
				Ok(())
			}
			None => {
				// Dropped segment: len==0 is a no-op, len>0 traps with out-of-bounds
				if len == 0 && src == 0 {
					let memory = self.memory(0)?;
					if dst > memory.len() {
						return Err(Trap::OutOfRange(TrapOutOfRange::Memory(0)));
					}
					Ok(())
				} else {
					Err(Trap::OutOfRange(TrapOutOfRange::Memory(0)))
				}
			}
		}
	}

	pub fn data_drop(&mut self, data_idx: u32) -> Result<()> {
		if let Some(seg) = self.data_segments.get_mut(data_idx as usize) {
			seg.data = None;
		}
		Ok(())
	}

	pub fn table_grow(&mut self, table_idx: TableIndex, n: u32, init: Value) -> Result<i32> {
		let table = self.table_mut(table_idx)?;
		let old_size = table.elements.len() as u32;
		let new_size = old_size.checked_add(n);

		match new_size {
			Some(new_size) => {
				if let Some(max) = table.limit.max {
					if new_size > max {
						return Ok(-1);
					}
				}
				table.elements.resize(new_size as usize, Some(init));
				Ok(old_size as i32)
			}
			None => Ok(-1),
		}
	}

	pub fn table_size(&self, table_idx: TableIndex) -> Result<u32> {
		let table = self.table(table_idx)?;
		Ok(table.elements.len() as u32)
	}

	pub fn table_fill(&mut self, table_idx: TableIndex, dst: u32, val: Value, len: u32) -> Result<()> {
		let table = self.table(table_idx)?;
		let table_len = table.elements.len() as u32;
		if dst.checked_add(len).map_or(true, |end| end > table_len) {
			return Err(Trap::OutOfRange(TrapOutOfRange::Table(table_idx)));
		}
		let table = self.table_mut(table_idx)?;
		for i in dst..dst + len {
			table.elements[i as usize] = Some(val.clone());
		}
		Ok(())
	}

	pub fn table_copy(
		&mut self,
		dst_idx: TableIndex,
		src_idx: TableIndex,
		dst: u32,
		src: u32,
		len: u32,
	) -> Result<()> {
		// Validate bounds first
		let src_len = self.table(src_idx)?.elements.len() as u32;
		let dst_len = self.table(dst_idx)?.elements.len() as u32;
		if src.checked_add(len).map_or(true, |end| end > src_len)
			|| dst.checked_add(len).map_or(true, |end| end > dst_len)
		{
			return Err(Trap::OutOfRange(TrapOutOfRange::Table(dst_idx)));
		}

		if dst_idx == src_idx {
			let table = self.table_mut(dst_idx)?;
			if dst <= src {
				for i in 0..len {
					table.elements[(dst + i) as usize] = table.elements[(src + i) as usize].clone();
				}
			} else {
				for i in (0..len).rev() {
					table.elements[(dst + i) as usize] = table.elements[(src + i) as usize].clone();
				}
			}
		} else {
			// Different tables — need to collect from source first
			let src_elements: Vec<_> = {
				let src_table = self.table(src_idx)?;
				(src as usize..(src + len) as usize).map(|i| src_table.elements[i].clone()).collect()
			};
			let dst_table = self.table_mut(dst_idx)?;
			for (i, elem) in src_elements.into_iter().enumerate() {
				dst_table.elements[dst as usize + i] = elem;
			}
		}
		Ok(())
	}

	pub fn table_init(&mut self, table_idx: TableIndex, elem_idx: u32, dst: u32, src: u32, len: u32) -> Result<()> {
		let seg = self.element_segments.get(elem_idx as usize).ok_or(Trap::UndefinedElement)?;
		match seg.elements.as_ref() {
			Some(elements) => {
				if src.checked_add(len).map_or(true, |end| end > elements.len() as u32) {
					return Err(Trap::OutOfRange(TrapOutOfRange::Table(table_idx)));
				}
				let table_len = self.table(table_idx)?.elements.len() as u32;
				if dst.checked_add(len).map_or(true, |end| end > table_len) {
					return Err(Trap::OutOfRange(TrapOutOfRange::Table(table_idx)));
				}
				let src_elems: Vec<_> = elements[src as usize..(src + len) as usize].to_vec();
				let table = self.table_mut(table_idx)?;
				for (i, func_idx_opt) in src_elems.into_iter().enumerate() {
					table.elements[dst as usize + i] = func_idx_opt.map(|idx| Value::RefFunc(idx));
				}
				Ok(())
			}
			None => {
				// Dropped segment: len==0 is a no-op, len>0 traps with out-of-bounds
				if len == 0 && src == 0 {
					// Also check dst is in bounds
					let table_len = self.table(table_idx)?.elements.len() as u32;
					if dst > table_len {
						return Err(Trap::OutOfRange(TrapOutOfRange::Table(table_idx)));
					}
					Ok(())
				} else {
					Err(Trap::OutOfRange(TrapOutOfRange::Table(table_idx)))
				}
			}
		}
	}

	pub fn elem_drop(&mut self, elem_idx: u32) -> Result<()> {
		if let Some(seg) = self.element_segments.get_mut(elem_idx as usize) {
			seg.elements = None;
		}
		Ok(())
	}
}
