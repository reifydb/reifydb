// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

//! WebAssembly interpreter for sandboxed execution in ReifyDB.
//!
//! This crate provides a safe WASM interpreter that can execute untrusted
//! WebAssembly modules within the database engine. All execution is sandboxed
//! — WASM modules cannot access host memory, make system calls, or escape
//! their linear memory sandbox.

#![forbid(unsafe_code)]

pub mod compile;
pub mod config;
pub mod execute;
pub mod module;
pub mod parse;
pub mod util;

use std::{cell::RefCell, collections::HashMap, fmt, rc::Rc, sync::Arc};

use compile::compiler::{CompilationError, Compiler};
use config::WasmConfig;
use execute::{
	Result as ExecResult,
	exec::{Exec, HostFunctionRegistry, ImportedModuleContext},
	state::State,
};
use module::{
	PAGE_SIZE, Trap, TrapNotFound, TrapOutOfRange,
	function::{ExportData, Function},
	global::Global,
	memory::Memory,
	module::{ActiveElementInit, Module, ModuleId},
	table::Table,
	value::Value,
};
use parse::{WasmParseError, binary::WasmParser};

/// Errors that can occur in the WASM environment.
#[derive(Debug, PartialEq)]
pub enum EnvironmentError {
	LoadError(LoadError),
	Trapped(Trap),
}

impl fmt::Display for EnvironmentError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			EnvironmentError::LoadError(e) => write!(f, "{}", e),
			EnvironmentError::Trapped(e) => write!(f, "{}", e),
		}
	}
}

impl From<LoadError> for EnvironmentError {
	fn from(value: LoadError) -> Self {
		EnvironmentError::LoadError(value)
	}
}

impl From<Trap> for EnvironmentError {
	fn from(value: Trap) -> Self {
		EnvironmentError::Trapped(value)
	}
}

impl From<WasmParseError> for EnvironmentError {
	fn from(value: WasmParseError) -> Self {
		EnvironmentError::LoadError(value.into())
	}
}

/// Errors that can occur during module loading.
#[derive(Debug, PartialEq)]
pub enum LoadError {
	CompilationFailed(String),
	NotFound(String),
	WasmParsingFailed(String),
	Unlinkable(String),
}

impl fmt::Display for LoadError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			LoadError::CompilationFailed(e) => write!(f, "{}", e),
			LoadError::NotFound(e) => write!(f, "{}", e),
			LoadError::WasmParsingFailed(e) => write!(f, "{}", e),
			LoadError::Unlinkable(e) => write!(f, "{}", e),
		}
	}
}

impl From<CompilationError> for LoadError {
	fn from(value: CompilationError) -> Self {
		LoadError::CompilationFailed(value.to_string())
	}
}

impl From<WasmParseError> for LoadError {
	fn from(value: WasmParseError) -> Self {
		LoadError::WasmParsingFailed(value.to_string())
	}
}

impl LoadError {
	pub fn not_found(reason: impl Into<String>) -> Self {
		Self::NotFound(reason.into())
	}

	pub fn compilation_failed(reason: impl Into<String>) -> Self {
		Self::CompilationFailed(reason.into())
	}

	pub fn wasm_parsing_failed(reason: impl Into<String>) -> Self {
		Self::WasmParsingFailed(reason.into())
	}
}

/// Source type wrappers for loading WASM modules.
pub mod source {
	pub mod binary {
		pub struct Bytes<T>
		where
			T: AsRef<[u8]>,
		{
			data: T,
		}

		impl<T> Bytes<T>
		where
			T: AsRef<[u8]>,
		{
			pub fn as_ref(&self) -> &[u8] {
				self.data.as_ref()
			}
		}

		pub fn bytes<T: AsRef<[u8]>>(data: T) -> Bytes<T> {
			Bytes {
				data,
			}
		}
	}

	pub mod text {
		pub struct WasmString<T>
		where
			T: AsRef<str>,
		{
			data: T,
		}

		impl<T> WasmString<T>
		where
			T: AsRef<str>,
		{
			pub fn as_ref(&self) -> &str {
				self.data.as_ref()
			}
		}

		pub fn string<T: AsRef<str>>(data: T) -> WasmString<T> {
			WasmString {
				data,
			}
		}
	}
}

/// A WASM module instance ready for execution.
pub struct Instance {
	exec: Exec,
}

impl Instance {
	pub fn invoke(&mut self, name: impl Into<String>, args: impl AsRef<[Value]>) -> Result<Box<[Value]>, Trap> {
		self.exec.invoke(name, args)
	}

	pub fn register_host_function(
		&mut self,
		module: impl Into<String>,
		name: impl Into<String>,
		f: impl Fn(&mut Exec) -> ExecResult<()> + Send + Sync + 'static,
	) {
		self.exec.register_host_function(module, name, f);
	}

	/// Write bytes into WASM linear memory at the given offset.
	pub fn write_memory(&mut self, offset: usize, data: &[u8]) -> Result<(), Trap> {
		let mem_rc = self.exec.state.memory_rc(0)?;
		let mut memory = mem_rc.borrow_mut();
		let end = offset + data.len();
		if end > memory.len() {
			return Err(Trap::OutOfRange(TrapOutOfRange::Memory(end)));
		}
		memory.data[offset..end].copy_from_slice(data);
		Ok(())
	}

	/// Read bytes from WASM linear memory at the given offset.
	pub fn read_memory(&self, offset: usize, len: usize) -> Result<Vec<u8>, Trap> {
		let mem_rc = self.exec.state.memory_rc(0)?;
		let memory = mem_rc.borrow();
		let end = offset + len;
		if end > memory.len() {
			return Err(Trap::OutOfRange(TrapOutOfRange::Memory(end)));
		}
		Ok(memory.data[offset..end].to_vec())
	}
}

/// Host memory descriptor for import resolution.
pub struct HostMemory {
	pub min_pages: u32,
	pub max_pages: Option<u32>,
}

/// Host table descriptor for import resolution.
pub struct HostTable {
	pub min: u32,
	pub max: Option<u32>,
}

struct RegisteredModule {
	functions: Vec<(String, Arc<Function>)>,
	tables: Vec<(String, Rc<RefCell<Table>>)>,
	memories: Vec<(String, Rc<RefCell<Memory>>)>,
	globals: Vec<(String, Rc<RefCell<Global>>)>,
	context: Rc<ImportedModuleContext>,
}

/// The main WASM engine that manages modules and instances.
pub struct Engine {
	compiler: Compiler,
	config: WasmConfig,
	host_functions: HostFunctionRegistry,
	host_globals: Vec<(String, String, Value)>,
	host_memories: Vec<(String, String, HostMemory)>,
	host_tables: Vec<(String, String, HostTable)>,
	registered_modules: HashMap<String, RegisteredModule>,
	modules: Vec<Module>,
	instances: Vec<Instance>,
}

impl Default for Engine {
	fn default() -> Self {
		Self {
			compiler: Compiler::default(),
			config: WasmConfig::default(),
			host_functions: HostFunctionRegistry::default(),
			host_globals: vec![],
			host_memories: vec![],
			host_tables: vec![],
			registered_modules: HashMap::new(),
			modules: vec![],
			instances: vec![],
		}
	}
}

impl Engine {
	pub fn with_config(config: WasmConfig) -> Self {
		Self {
			compiler: Compiler::default(),
			config,
			host_functions: HostFunctionRegistry::default(),
			host_globals: vec![],
			host_memories: vec![],
			host_tables: vec![],
			registered_modules: HashMap::new(),
			modules: vec![],
			instances: vec![],
		}
	}

	pub fn register_host_global(&mut self, module: impl Into<String>, name: impl Into<String>, value: Value) {
		self.host_globals.push((module.into(), name.into(), value));
	}

	pub fn register_host_memory(
		&mut self,
		module: impl Into<String>,
		name: impl Into<String>,
		min_pages: u32,
		max_pages: Option<u32>,
	) {
		self.host_memories.push((
			module.into(),
			name.into(),
			HostMemory {
				min_pages,
				max_pages,
			},
		));
	}

	pub fn register_host_table(
		&mut self,
		module: impl Into<String>,
		name: impl Into<String>,
		min: u32,
		max: Option<u32>,
	) {
		self.host_tables.push((
			module.into(),
			name.into(),
			HostTable {
				min,
				max,
			},
		));
	}

	pub fn register_host_function(
		&mut self,
		module: impl Into<String>,
		name: impl Into<String>,
		f: impl Fn(&mut Exec) -> ExecResult<()> + Send + Sync + 'static,
	) {
		self.host_functions.register(module, name, f);
	}

	pub fn invoke(&mut self, name: impl Into<String>, args: impl AsRef<[Value]>) -> Result<Box<[Value]>, Trap> {
		let len = self.instances.len();
		let instance = self.instances.get_mut(len - 1).unwrap();
		instance.invoke(name, args)
	}

	pub fn invoke_on(
		&mut self,
		instance_idx: usize,
		name: impl Into<String>,
		args: impl AsRef<[Value]>,
	) -> Result<Box<[Value]>, Trap> {
		let name = name.into();
		let instance = self.instances.get_mut(instance_idx).unwrap();
		instance.invoke(name, args)
	}

	pub fn get_global_on(&mut self, instance_idx: usize, name: &str) -> Result<Value, Trap> {
		let instance = self.instances.get_mut(instance_idx).unwrap();
		let export = instance.exec.state.export(name)?;
		match export.data {
			ExportData::Global(idx) => instance.exec.state.global.get(idx),
			_ => Err(Trap::NotFound(TrapNotFound::ExportedFunction(name.to_string()))),
		}
	}

	pub fn instance_count(&self) -> usize {
		self.instances.len()
	}

	/// Write bytes into WASM linear memory of the last instance.
	pub fn write_memory(&mut self, offset: usize, data: &[u8]) -> Result<(), Trap> {
		let len = self.instances.len();
		let instance = self.instances.get_mut(len - 1).unwrap();
		instance.write_memory(offset, data)
	}

	/// Read bytes from WASM linear memory of the last instance.
	pub fn read_memory(&self, offset: usize, len: usize) -> Result<Vec<u8>, Trap> {
		let inst_len = self.instances.len();
		let instance = self.instances.get(inst_len - 1).unwrap();
		instance.read_memory(offset, len)
	}

	/// Get the value of an exported global by name from the last instance.
	pub fn get_global(&mut self, name: &str) -> Result<Value, Trap> {
		let len = self.instances.len();
		let instance = self.instances.get_mut(len - 1).unwrap();
		let export = instance.exec.state.export(name)?;
		match export.data {
			ExportData::Global(idx) => instance.exec.state.global.get(idx),
			_ => Err(Trap::NotFound(TrapNotFound::ExportedFunction(name.to_string()))),
		}
	}

	fn build_registered_module(instance: &Instance) -> RegisteredModule {
		let state = &instance.exec.state;
		let mut functions = Vec::new();
		let mut tables = Vec::new();
		let mut memories = Vec::new();
		let mut globals = Vec::new();

		for export in state.exports.iter() {
			match &export.data {
				ExportData::Function(func_idx) => {
					if let Ok(func) = state.function(*func_idx) {
						functions.push((export.name.clone(), func));
					}
				}
				ExportData::Table(idx) => {
					if let Some(table_rc) = state.tables.get(*idx) {
						tables.push((export.name.clone(), Rc::clone(table_rc)));
					}
				}
				ExportData::Memory(idx) => {
					if let Some(mem_rc) = state.memories.get(*idx) {
						memories.push((export.name.clone(), Rc::clone(mem_rc)));
					}
				}
				ExportData::Global(idx) => {
					if let Some(global_rc) = state.global.data.get(*idx) {
						globals.push((export.name.clone(), Rc::clone(global_rc)));
					}
				}
			}
		}

		let context = Rc::new(ImportedModuleContext {
			functions: state.functions.clone(),
			function_types: state.function_types.clone(),
			tables: state.tables.iter().map(|t| Rc::clone(t)).collect(),
			memories: state.memories.iter().map(|m| Rc::clone(m)).collect(),
			globals: state.global.data.iter().map(|g| Rc::clone(g)).collect(),
		});

		RegisteredModule {
			functions,
			tables,
			memories,
			globals,
			context,
		}
	}

	/// Register the last instance's exports under the given module name.
	pub fn register_module(&mut self, name: impl Into<String>) {
		let name = name.into();
		let len = self.instances.len();
		if len == 0 {
			return;
		}
		let instance = &self.instances[len - 1];
		let reg = Self::build_registered_module(instance);
		self.registered_modules.insert(name, reg);
	}

	/// Register a specific instance's exports under the given module name.
	pub fn register_module_at(&mut self, instance_idx: usize, name: impl Into<String>) {
		let name = name.into();
		let instance = &self.instances[instance_idx];
		let reg = Self::build_registered_module(instance);
		self.registered_modules.insert(name, reg);
	}

	pub fn instantiate(&mut self, id: ModuleId) -> Result<&mut Instance, EnvironmentError> {
		let module = self.modules.get(id as usize).unwrap();
		let start_function = module.start_function;
		let active_elements = module.active_elements.clone();
		let active_data = module.active_data.clone();

		// Check for unknown imports before any resource linking or segment application
		for (mod_name, field_name) in &module.function_imports {
			let has_host =
				self.host_functions.functions.iter().any(|(m, n, _)| m == mod_name && n == field_name);
			let has_registered = self
				.registered_modules
				.get(mod_name)
				.map_or(false, |reg| reg.functions.iter().any(|(n, _)| n == field_name));
			if !has_host && !has_registered {
				return Err(EnvironmentError::LoadError(LoadError::Unlinkable(format!(
					"unknown import: {}.{}",
					mod_name, field_name
				))));
			}
		}
		for (mod_name, field_name) in &module.table_imports {
			let has_host = self.host_tables.iter().any(|(m, n, _)| m == mod_name && n == field_name);
			let has_registered = self
				.registered_modules
				.get(mod_name)
				.map_or(false, |reg| reg.tables.iter().any(|(n, _)| n == field_name));
			if !has_host && !has_registered {
				return Err(EnvironmentError::LoadError(LoadError::Unlinkable(format!(
					"unknown import: {}.{}",
					mod_name, field_name
				))));
			}
		}
		for (mod_name, field_name) in &module.memory_imports {
			let has_host = self.host_memories.iter().any(|(m, n, _)| m == mod_name && n == field_name);
			let has_registered = self
				.registered_modules
				.get(mod_name)
				.map_or(false, |reg| reg.memories.iter().any(|(n, _)| n == field_name));
			if !has_host && !has_registered {
				return Err(EnvironmentError::LoadError(LoadError::Unlinkable(format!(
					"unknown import: {}.{}",
					mod_name, field_name
				))));
			}
		}
		for (mod_name, field_name) in &module.global_imports {
			let has_host = self.host_globals.iter().any(|(m, n, _)| m == mod_name && n == field_name);
			let has_registered = self
				.registered_modules
				.get(mod_name)
				.map_or(false, |reg| reg.globals.iter().any(|(n, _)| n == field_name));
			if !has_host && !has_registered {
				return Err(EnvironmentError::LoadError(LoadError::Unlinkable(format!(
					"unknown import: {}.{}",
					mod_name, field_name
				))));
			}
		}

		let mut store = State::new(module).unwrap();

		// Link shared resources from registered modules
		for (i, (mod_name, field_name)) in module.table_imports.iter().enumerate() {
			if let Some(reg) = self.registered_modules.get(mod_name) {
				if let Some((_, table_rc)) = reg.tables.iter().find(|(n, _)| n == field_name) {
					if i < store.tables.len() {
						store.tables[i] = Rc::clone(table_rc);
					}
				}
			}
		}

		for (i, (mod_name, field_name)) in module.memory_imports.iter().enumerate() {
			if let Some(reg) = self.registered_modules.get(mod_name) {
				if let Some((_, mem_rc)) = reg.memories.iter().find(|(n, _)| n == field_name) {
					if i < store.memories.len() {
						store.memories[i] = Rc::clone(mem_rc);
					}
				}
			}
		}

		for (i, (mod_name, field_name)) in module.global_imports.iter().enumerate() {
			if let Some(reg) = self.registered_modules.get(mod_name) {
				if let Some((_, global_rc)) = reg.globals.iter().find(|(n, _)| n == field_name) {
					if i < store.global.data.len() {
						store.global.data[i] = Rc::clone(global_rc);
					}
				}
			}
		}

		// Apply active element segments to (possibly shared) tables.
		// Process each segment in order; on OOB, trap but keep earlier changes.
		let mut segment_trap: Option<Trap> = None;
		for elem_info in &active_elements {
			if let Some(table_rc) = store.tables.get(elem_info.table_idx) {
				let table_len = table_rc.borrow().elements.len();
				// Bounds check: if offset + length > table size, trap
				if elem_info
					.offset
					.checked_add(elem_info.inits.len())
					.map_or(true, |end| end > table_len)
				{
					segment_trap =
						Some(Trap::OutOfRange(TrapOutOfRange::Table(elem_info.table_idx)));
					break;
				}
				let mut table = table_rc.borrow_mut();
				if table.func_refs.len() < table_len {
					table.func_refs.resize(table_len, None);
				}
				for (i, init) in elem_info.inits.iter().enumerate() {
					let pos = elem_info.offset + i;
					match init {
						ActiveElementInit::FuncRef(func_idx) => {
							table.elements[pos] = Some(Value::RefFunc(*func_idx));
							if let Some(func) = store.functions.get(*func_idx) {
								let resolved = match func.as_ref() {
									Function::External(ext) => {
										self.registered_modules
											.get(&ext.module)
											.and_then(|reg| {
												reg.functions
													.iter()
													.find(|(n, _)| n == &ext.function_name)
													.map(|(_, f)| f.clone())
											})
									}
									_ => None,
								};
								table.func_refs[pos] =
									Some(resolved.unwrap_or(func.clone()));
							}
						}
						ActiveElementInit::GlobalGet(global_idx) => {
							let global_val = store.global.get(*global_idx).ok();
							match global_val {
								Some(Value::RefFunc(func_idx)) => {
									table.elements[pos] =
										Some(Value::RefFunc(func_idx));
									let mut resolved = false;
									if let Some((mod_name, _)) =
										module.global_imports.get(*global_idx)
									{
										if let Some(reg) = self
											.registered_modules
											.get(mod_name)
										{
											if let Some(func) = reg
												.context
												.functions
												.get(func_idx)
											{
												table.func_refs[pos] =
													Some(func
														.clone(
														));
												resolved = true;
											}
										}
									}
									if !resolved {
										if let Some(func) =
											store.functions.get(func_idx)
										{
											table.func_refs[pos] =
												Some(func.clone());
										}
									}
								}
								Some(Value::RefNull(_)) | None => {
									table.elements[pos] = None;
									table.func_refs[pos] = None;
								}
								_ => {}
							}
						}
						ActiveElementInit::RefNull => {
							table.elements[pos] = None;
							table.func_refs[pos] = None;
						}
					}
				}
			}
		}

		// Apply active data segments to (possibly shared) memories.
		// Process each segment in order; on OOB, trap but keep earlier changes.
		for data_info in &active_data {
			if let Some(mem_rc) = store.memories.get(data_info.mem_idx) {
				let mem_len = mem_rc.borrow().data.len();
				if data_info.offset.checked_add(data_info.data.len()).map_or(true, |end| end > mem_len)
				{
					if segment_trap.is_none() {
						segment_trap = Some(Trap::OutOfRange(TrapOutOfRange::Memory(
							data_info.mem_idx,
						)));
					}
					break;
				}
				let mut mem = mem_rc.borrow_mut();
				mem.data[data_info.offset..data_info.offset + data_info.data.len()]
					.copy_from_slice(&data_info.data);
			}
		}

		let mut exec = Exec::with_config(store, self.config.clone());

		// Copy registered host functions to the instance
		for (module, name, f) in &self.host_functions.functions {
			exec.host_functions.functions.push((module.clone(), name.clone(), f.clone()));
		}

		// Copy registered module functions to the instance (with context for function resolution)
		for (mod_name, reg) in &self.registered_modules {
			for (export_name, func) in &reg.functions {
				exec.imported_functions.push((
					mod_name.clone(),
					export_name.clone(),
					func.clone(),
					Some(reg.context.clone()),
				));
			}
		}

		let mut instance = Instance {
			exec,
		};

		// If any segment had an OOB trap, return it (but keep all changes applied before the trap)
		if let Some(trap) = segment_trap {
			self.instances.push(instance);
			return Err(EnvironmentError::Trapped(trap));
		}

		// Call the start function if one is defined
		if let Some(start_idx) = start_function {
			let result = instance.exec.call(&start_idx);
			// Push instance BEFORE checking the result, so changes from partial
			// start function execution persist (data/elem segments already applied)
			self.instances.push(instance);
			result?;
		} else {
			self.instances.push(instance);
		}

		let len = self.instances.len();
		Ok(&mut self.instances[len - 1])
	}
}

/// Trait for loading binary (.wasm) sources.
pub trait LoadBinary<SOURCE> {
	fn load(&mut self, source: SOURCE) -> Result<ModuleId, LoadError>;
}

impl<T: AsRef<[u8]>> LoadBinary<source::binary::Bytes<T>> for Engine {
	fn load(&mut self, source: source::binary::Bytes<T>) -> Result<ModuleId, LoadError> {
		let wasm = WasmParser::parse(source.as_ref())?;
		let module_id = self.modules.len() as ModuleId;

		// Augment host resources with registered module exports so the compiler
		// creates correctly-sized placeholder resources for imported tables/memories/globals.
		let mut all_globals: Vec<(String, String, Value)> = self.host_globals.clone();
		let mut all_memories: Vec<(String, String, HostMemory)> = Vec::new();
		let mut all_tables: Vec<(String, String, HostTable)> = Vec::new();

		for (m, n, hm) in &self.host_memories {
			all_memories.push((
				m.clone(),
				n.clone(),
				HostMemory {
					min_pages: hm.min_pages,
					max_pages: hm.max_pages,
				},
			));
		}
		for (m, n, ht) in &self.host_tables {
			all_tables.push((
				m.clone(),
				n.clone(),
				HostTable {
					min: ht.min,
					max: ht.max,
				},
			));
		}

		for (mod_name, reg) in &self.registered_modules {
			for (export_name, table_rc) in &reg.tables {
				let table = table_rc.borrow();
				all_tables.push((
					mod_name.clone(),
					export_name.clone(),
					HostTable {
						min: table.elements.len() as u32,
						max: table.limit.max,
					},
				));
			}
			for (export_name, mem_rc) in &reg.memories {
				let mem = mem_rc.borrow();
				let pages = (mem.data.len() as u32) / PAGE_SIZE;
				all_memories.push((
					mod_name.clone(),
					export_name.clone(),
					HostMemory {
						min_pages: pages,
						max_pages: mem.max,
					},
				));
			}
			for (export_name, global_rc) in &reg.globals {
				let global = global_rc.borrow();
				all_globals.push((mod_name.clone(), export_name.clone(), global.value.clone()));
			}
		}

		let module = self.compiler.compile_with_imports(
			module_id,
			wasm,
			&all_globals,
			&all_memories,
			&all_tables,
		)?;

		self.modules.push(module);

		Ok(module_id)
	}
}

/// Trait for spawning (load + instantiate) from binary sources.
pub trait SpawnBinary<SOURCE> {
	fn spawn(&mut self, source: SOURCE) -> Result<&mut Instance, EnvironmentError>;
}

impl<T: AsRef<[u8]>> SpawnBinary<source::binary::Bytes<T>> for Engine {
	fn spawn(&mut self, source: source::binary::Bytes<T>) -> Result<&mut Instance, EnvironmentError> {
		let module_id = self.load(source)?;
		self.instantiate(module_id)
	}
}
