// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

//! WebAssembly runtime for sandboxed execution in ReifyDB.
//!
//! This crate provides a WASM runtime (backed by wasmtime on native targets)
//! that can execute untrusted WebAssembly modules within the database engine.

pub mod config;
pub mod module;

use std::fmt;

use module::Trap;
#[cfg(not(target_arch = "wasm32"))]
pub use wasmtime;
#[cfg(not(target_arch = "wasm32"))]
pub use wasmtime_wasi;

pub mod source {
	pub mod binary {
		pub struct Bytes<T>
		where
			T: AsRef<[u8]>,
		{
			pub data: T,
		}

		pub fn bytes<T: AsRef<[u8]>>(data: T) -> Bytes<T> {
			Bytes {
				data,
			}
		}
	}
}

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

#[derive(Debug, PartialEq)]
pub enum LoadError {
	CompilationFailed(String),
	Unlinkable(String),
}

impl fmt::Display for LoadError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			LoadError::CompilationFailed(msg) => write!(f, "compilation failed: {}", msg),
			LoadError::Unlinkable(msg) => write!(f, "unlinkable: {}", msg),
		}
	}
}

pub trait SpawnBinary<SOURCE> {
	fn spawn(&mut self, source: SOURCE) -> Result<(), EnvironmentError>;
}

#[cfg(not(target_arch = "wasm32"))]
mod native {
	use wasmtime::{Config, Engine as WtEngine, Instance, Linker, Module, ResourceLimiter, Result, Store, Val};

	use crate::{EnvironmentError, LoadError, SpawnBinary, Trap, config::WasmConfig, module::value::Value, source};

	struct StoreLimits {
		max_memory_bytes: usize,
	}

	impl ResourceLimiter for StoreLimits {
		fn memory_growing(&mut self, _current: usize, desired: usize, _maximum: Option<usize>) -> Result<bool> {
			Ok(desired <= self.max_memory_bytes)
		}

		fn table_growing(&mut self, _current: usize, _desired: usize, _maximum: Option<usize>) -> Result<bool> {
			Ok(true)
		}
	}

	pub struct Engine {
		wt_engine: WtEngine,
		store: Store<StoreLimits>,
		linker: Linker<StoreLimits>,
		instance: Option<Instance>,
		config: WasmConfig,
	}

	impl Default for Engine {
		fn default() -> Self {
			Self::with_config(WasmConfig::default())
		}
	}

	impl Engine {
		pub fn with_config(config: WasmConfig) -> Self {
			let mut wt_config = Config::new();
			wt_config.consume_fuel(true);
			let stack_bytes = (config.max_call_depth as usize) * 4096;
			wt_config.max_wasm_stack(stack_bytes);

			let wt_engine = WtEngine::new(&wt_config).expect("failed to create wasmtime engine");

			let limits = StoreLimits {
				max_memory_bytes: (config.max_memory_pages as usize) * 65536,
			};
			let mut store = Store::new(&wt_engine, limits);
			store.limiter(|s| s);
			store.set_fuel(config.max_instructions).expect("failed to set fuel");

			let linker = Linker::new(&wt_engine);

			Engine {
				wt_engine,
				store,
				linker,
				instance: None,
				config,
			}
		}

		pub fn invoke(
			&mut self,
			name: impl Into<String>,
			args: impl AsRef<[Value]>,
		) -> Result<Box<[Value]>, Trap> {
			let name = name.into();
			let instance = self.instance.ok_or_else(|| Trap::Error("no instance loaded".into()))?;

			let func = instance.get_func(&mut self.store, &name).ok_or_else(|| {
				Trap::Error(format!("unknown function: exported function '{}' not found", name))
			})?;

			let wt_args: Vec<Val> = args.as_ref().iter().map(|v| v.clone().into()).collect();
			let result_count = func.ty(&self.store).results().len();
			let mut results = vec![Val::I32(0); result_count];

			func.call(&mut self.store, &wt_args, &mut results)
				.map_err(|e| Trap::Error(format!("{}", e)))?;

			let values: Vec<Value> = results.into_iter().map(Value::from_wasmtime).collect();
			Ok(values.into_boxed_slice())
		}

		pub fn write_memory(&mut self, offset: usize, data: &[u8]) -> Result<(), EnvironmentError> {
			let instance = self.instance.ok_or_else(|| Trap::Error("no instance loaded".into()))?;

			let memory = instance
				.get_memory(&mut self.store, "memory")
				.ok_or_else(|| Trap::Error("memory export not found".into()))?;

			let mem_data = memory.data_mut(&mut self.store);
			let end = offset + data.len();
			if end > mem_data.len() {
				return Err(Trap::Error("out of bounds memory access".into()).into());
			}
			mem_data[offset..end].copy_from_slice(data);
			Ok(())
		}

		pub fn read_memory(&mut self, offset: usize, len: usize) -> Result<Vec<u8>, EnvironmentError> {
			let instance = self.instance.ok_or_else(|| Trap::Error("no instance loaded".into()))?;

			let memory = instance
				.get_memory(&mut self.store, "memory")
				.ok_or_else(|| Trap::Error("memory export not found".into()))?;

			let mem_data = memory.data(&self.store);
			let end = offset + len;
			if end > mem_data.len() {
				return Err(Trap::Error("out of bounds memory access".into()).into());
			}
			Ok(mem_data[offset..end].to_vec())
		}
	}

	impl<T: AsRef<[u8]>> SpawnBinary<source::binary::Bytes<T>> for Engine {
		fn spawn(&mut self, source: source::binary::Bytes<T>) -> Result<(), EnvironmentError> {
			let wt_module = Module::new(&self.wt_engine, source.data.as_ref())
				.map_err(|e| LoadError::CompilationFailed(format!("{}", e)))?;

			self.store
				.set_fuel(self.config.max_instructions)
				.map_err(|e| LoadError::Unlinkable(format!("failed to set fuel: {}", e)))?;

			let instance = self
				.linker
				.instantiate(&mut self.store, &wt_module)
				.map_err(|e| LoadError::Unlinkable(format!("{}", e)))?;

			self.instance = Some(instance);
			Ok(())
		}
	}
}

#[cfg(target_arch = "wasm32")]
mod stub {
	use crate::{EnvironmentError, SpawnBinary, Trap, config::WasmConfig, module::value::Value, source};

	pub struct Engine;

	impl Default for Engine {
		fn default() -> Self {
			Engine
		}
	}

	impl Engine {
		pub fn with_config(_: WasmConfig) -> Self {
			Engine
		}

		pub fn invoke(&mut self, _: impl Into<String>, _: impl AsRef<[Value]>) -> Result<Box<[Value]>, Trap> {
			unimplemented!("WASM UDF engine not available on wasm32")
		}

		pub fn write_memory(&mut self, _: usize, _: &[u8]) -> Result<(), EnvironmentError> {
			unimplemented!("WASM UDF engine not available on wasm32")
		}

		pub fn read_memory(&mut self, _: usize, _: usize) -> Result<Vec<u8>, EnvironmentError> {
			unimplemented!("WASM UDF engine not available on wasm32")
		}
	}

	impl<T: AsRef<[u8]>> SpawnBinary<source::binary::Bytes<T>> for Engine {
		fn spawn(&mut self, _: source::binary::Bytes<T>) -> Result<(), EnvironmentError> {
			unimplemented!("WASM UDF engine not available on wasm32")
		}
	}
}

#[cfg(not(target_arch = "wasm32"))]
pub use native::Engine;
#[cfg(target_arch = "wasm32")]
pub use stub::Engine;
