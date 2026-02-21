// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

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

use compile::Compiler;
use config::WasmConfig;
use execute::{Exec, State, exec::HostFunctionRegistry};
use module::{Module, ModuleId, Trap, Value};

/// Errors that can occur in the WASM environment.
#[derive(Debug, PartialEq)]
pub enum EnvironmentError {
	LoadError(LoadError),
	Trapped(module::Trap),
}

impl std::fmt::Display for EnvironmentError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl From<module::Trap> for EnvironmentError {
	fn from(value: Trap) -> Self {
		EnvironmentError::Trapped(value)
	}
}

impl From<parse::WasmParseError> for EnvironmentError {
	fn from(value: parse::WasmParseError) -> Self {
		EnvironmentError::LoadError(value.into())
	}
}

/// Errors that can occur during module loading.
#[derive(Debug, PartialEq)]
pub enum LoadError {
	CompilationFailed(String),
	NotFound(String),
	WasmParsingFailed(String),
}

impl std::fmt::Display for LoadError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			LoadError::CompilationFailed(e) => write!(f, "{}", e),
			LoadError::NotFound(e) => write!(f, "{}", e),
			LoadError::WasmParsingFailed(e) => write!(f, "{}", e),
		}
	}
}

impl From<compile::CompilationError> for LoadError {
	fn from(value: compile::CompilationError) -> Self {
		LoadError::CompilationFailed(value.to_string())
	}
}

impl From<parse::WasmParseError> for LoadError {
	fn from(value: parse::WasmParseError) -> Self {
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
		f: impl Fn(&mut Exec) -> execute::Result<()> + Send + Sync + 'static,
	) {
		self.exec.register_host_function(module, name, f);
	}

	/// Write bytes into WASM linear memory at the given offset.
	pub fn write_memory(&mut self, offset: usize, data: &[u8]) -> Result<(), Trap> {
		let memory = self.exec.state.memory_mut(0)?;
		let end = offset + data.len();
		if end > memory.len() {
			return Err(Trap::OutOfRange(module::TrapOutOfRange::Memory(end)));
		}
		memory.data[offset..end].copy_from_slice(data);
		Ok(())
	}

	/// Read bytes from WASM linear memory at the given offset.
	pub fn read_memory(&self, offset: usize, len: usize) -> Result<Vec<u8>, Trap> {
		let memory = self.exec.state.memory(0)?;
		let end = offset + len;
		if end > memory.len() {
			return Err(Trap::OutOfRange(module::TrapOutOfRange::Memory(end)));
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

/// The main WASM engine that manages modules and instances.
pub struct Engine {
	compiler: Compiler,
	config: WasmConfig,
	host_functions: HostFunctionRegistry,
	host_globals: Vec<(String, String, Value)>,
	host_memories: Vec<(String, String, HostMemory)>,
	host_tables: Vec<(String, String, HostTable)>,
	registered_functions: Vec<(String, String, std::sync::Arc<module::Function>)>,
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
			registered_functions: vec![],
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
			registered_functions: vec![],
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
		f: impl Fn(&mut Exec) -> execute::Result<()> + Send + Sync + 'static,
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
		let instance = self.instances.get_mut(instance_idx).unwrap();
		instance.invoke(name, args)
	}

	pub fn get_global_on(&mut self, instance_idx: usize, name: &str) -> Result<Value, Trap> {
		let instance = self.instances.get_mut(instance_idx).unwrap();
		let export = instance.exec.state.export(name)?;
		match export.data {
			module::ExportData::Global(idx) => instance.exec.state.global.get(idx),
			_ => Err(Trap::NotFound(module::TrapNotFound::ExportedFunction(name.to_string()))),
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
			module::ExportData::Global(idx) => instance.exec.state.global.get(idx),
			_ => Err(Trap::NotFound(module::TrapNotFound::ExportedFunction(name.to_string()))),
		}
	}

	/// Register the last instance's exports under the given module name.
	/// This makes the exported functions available for import by subsequent modules.
	pub fn register_module(&mut self, name: impl Into<String>) {
		let name = name.into();
		let len = self.instances.len();
		if len == 0 {
			return;
		}
		let instance = &self.instances[len - 1];
		let state = &instance.exec.state;

		for export in state.exports.iter() {
			match &export.data {
				module::ExportData::Function(func_idx) => {
					if let Ok(func) = state.function(*func_idx) {
						self.registered_functions.push((
							name.clone(),
							export.name.clone(),
							func,
						));
					}
				}
				_ => {} // TODO: support global/memory/table exports
			}
		}
	}

	/// Register a specific instance's exports under the given module name.
	pub fn register_module_at(&mut self, instance_idx: usize, name: impl Into<String>) {
		let name = name.into();
		let instance = &self.instances[instance_idx];
		let state = &instance.exec.state;

		for export in state.exports.iter() {
			match &export.data {
				module::ExportData::Function(func_idx) => {
					if let Ok(func) = state.function(*func_idx) {
						self.registered_functions.push((
							name.clone(),
							export.name.clone(),
							func,
						));
					}
				}
				_ => {} // TODO: support global/memory/table exports
			}
		}
	}

	pub fn instantiate(&mut self, id: ModuleId) -> Result<&mut Instance, EnvironmentError> {
		let module = self.modules.get(id as usize).unwrap();
		let start_function = module.start_function;

		let store = State::new(module).unwrap();
		let mut exec = Exec::with_config(store, self.config.clone());

		// Copy registered host functions to the instance
		for (module, name, f) in &self.host_functions.functions {
			exec.host_functions.functions.push((module.clone(), name.clone(), f.clone()));
		}

		// Copy registered module functions to the instance
		for (module, name, f) in &self.registered_functions {
			exec.imported_functions.push((module.clone(), name.clone(), f.clone()));
		}

		let mut instance = Instance {
			exec,
		};

		// Call the start function if one is defined
		if let Some(start_idx) = start_function {
			instance.exec.call(&start_idx)?;
		}

		self.instances.push(instance);

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
		let wasm = parse::WasmParser::parse(source.as_ref())?;
		let module_id = self.modules.len() as ModuleId;
		let module = self.compiler.compile_with_imports(
			module_id,
			wasm,
			&self.host_globals,
			&self.host_memories,
			&self.host_tables,
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
