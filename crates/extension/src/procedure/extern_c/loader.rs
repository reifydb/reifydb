// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	fs,
	path::{Path, PathBuf},
	sync::{Arc, OnceLock},
};

use libloading::Symbol;
use reifydb_abi::procedure::{
	descriptor::ExternCProcedureDescriptor,
	types::{ExternCProcedureCreateFn, PROCEDURE_MAGIC},
};
use reifydb_routine_abi::registry::RoutinesConfigurator;
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_sdk::error::{Result as ExternCResult, SdkError};

use super::ExternCProcedure;
use crate::loader::{
	extern_c::{buffer_to_string, validate_api_version},
	extern_load::ExternLoad,
};

static GLOBAL_EXTERN_C_PROCEDURE_LOADER: OnceLock<RwLock<ProcedureLoader>> = OnceLock::new();

pub fn extern_c_procedure_loader() -> &'static RwLock<ProcedureLoader> {
	GLOBAL_EXTERN_C_PROCEDURE_LOADER.get_or_init(|| RwLock::new(ProcedureLoader::new()))
}

pub struct ProcedureLoader {
	cache: ExternLoad,
	procedure_paths: HashMap<String, PathBuf>,
}

impl ProcedureLoader {
	fn new() -> Self {
		Self {
			cache: ExternLoad::new(),
			procedure_paths: HashMap::new(),
		}
	}

	pub fn load_procedure_library(&mut self, path: &Path) -> ExternCResult<bool> {
		self.cache
			.check_magic(path, b"extern_c_procedure_magic\0", PROCEDURE_MAGIC)
			.map_err(|e| SdkError::Other(e.to_string()))
	}

	fn get_descriptor(&self, path: &Path) -> ExternCResult<ExternCProcedureDescriptor> {
		let library = self
			.cache
			.get(path)
			.ok_or_else(|| SdkError::Other(format!("Library not loaded: {}", path.display())))?;
		// SAFETY: the procedure ABI declares this symbol; the descriptor is module-static data.
		unsafe {
			let get_descriptor: Symbol<extern "C" fn() -> *const ExternCProcedureDescriptor> =
				library.get(b"extern_c_procedure_get_descriptor\0").map_err(|e| {
					SdkError::Other(format!(
						"Failed to find extern_c_procedure_get_descriptor: {}",
						e
					))
				})?;

			let descriptor_ptr = get_descriptor();
			if descriptor_ptr.is_null() {
				return Err(SdkError::Other("Descriptor is null".to_string()));
			}

			Ok(ExternCProcedureDescriptor {
				api: (*descriptor_ptr).api,
				name: (*descriptor_ptr).name,
				version: (*descriptor_ptr).version,
				description: (*descriptor_ptr).description,
				vtable: (*descriptor_ptr).vtable,
			})
		}
	}

	fn validate_and_register(
		&mut self,
		descriptor: &ExternCProcedureDescriptor,
		path: &Path,
	) -> ExternCResult<(String, u32)> {
		validate_api_version(descriptor.api).map_err(|e| SdkError::Other(e.to_string()))?;

		// SAFETY: the buffer points into the loaded image's static data, which outlives this read.
		let name = unsafe { buffer_to_string(&descriptor.name) };
		self.procedure_paths.insert(name.clone(), path.to_path_buf());

		Ok((name, descriptor.api))
	}

	pub fn register_procedure(&mut self, path: &Path) -> ExternCResult<Option<LoadedProcedureInfo>> {
		if !self.load_procedure_library(path)? {
			return Ok(None);
		}

		let descriptor = self.get_descriptor(path)?;
		let (name, api) = self.validate_and_register(&descriptor, path)?;

		// SAFETY: the descriptor's buffers are module-static data.
		let info = unsafe {
			LoadedProcedureInfo {
				name,
				library_path: path.to_path_buf(),
				api,
				version: buffer_to_string(&descriptor.version),
				description: buffer_to_string(&descriptor.description),
			}
		};

		Ok(Some(info))
	}

	pub fn load_procedure(&mut self, path: &Path, config: &[u8]) -> ExternCResult<Option<ExternCProcedure>> {
		if !self.load_procedure_library(path)? {
			return Ok(None);
		}

		let descriptor = self.get_descriptor(path)?;
		self.validate_and_register(&descriptor, path)?;

		let library = self.cache.library(path).map_err(|e| SdkError::Other(e.to_string()))?;
		// SAFETY: the ABI declares this symbol as ExternCProcedureCreateFn and the cache keeps it loaded.
		let create_fn: ExternCProcedureCreateFn = unsafe {
			let create_symbol: Symbol<ExternCProcedureCreateFn> =
				library.get(b"extern_c_procedure_create\0").map_err(|e| {
					SdkError::Other(format!("Failed to find extern_c_procedure_create: {}", e))
				})?;

			*create_symbol
		};

		let instance = create_fn(config.as_ptr(), config.len());
		if instance.is_null() {
			return Err(SdkError::Other("Failed to create procedure instance".to_string()));
		}

		// SAFETY: the buffer points into the loaded image's static data, which outlives this read.
		let name = unsafe { buffer_to_string(&descriptor.name) };
		Ok(Some(ExternCProcedure::new(name, descriptor, instance)))
	}

	pub fn create_procedure_by_name(&mut self, name: &str, config: &[u8]) -> ExternCResult<ExternCProcedure> {
		let path = self
			.procedure_paths
			.get(name)
			.ok_or_else(|| SdkError::Other(format!("Procedure not found: {}", name)))?
			.clone();

		self.load_procedure(&path, config)?
			.ok_or_else(|| SdkError::Other(format!("Procedure library no longer valid: {}", name)))
	}

	pub fn has_procedure(&self, name: &str) -> bool {
		self.procedure_paths.contains_key(name)
	}
}

#[derive(Debug, Clone)]
pub struct LoadedProcedureInfo {
	pub name: String,
	pub library_path: PathBuf,
	pub api: u32,
	pub version: String,
	pub description: String,
}

impl Default for ProcedureLoader {
	fn default() -> Self {
		Self::new()
	}
}

pub fn register_procedures_from_dir(
	dir: &Path,
	mut builder: RoutinesConfigurator,
) -> ExternCResult<RoutinesConfigurator> {
	let loader = extern_c_procedure_loader();
	let mut loader_guard = loader.write();

	let mut names = Vec::new();

	let entries = fs::read_dir(dir)
		.map_err(|e| SdkError::Other(format!("Failed to read directory {}: {}", dir.display(), e)))?;

	for entry in entries {
		let entry = entry.map_err(|e| SdkError::Other(format!("Failed to read directory entry: {}", e)))?;
		let path = entry.path();
		let ext = path.extension().and_then(|s| s.to_str());

		if ext == Some("so") || ext == Some("dylib") {
			match loader_guard.register_procedure(&path) {
				Ok(Some(info)) => {
					names.push(info.name);
				}
				Ok(None) => {}
				Err(e) => {
					eprintln!(
						"Warning: Failed to register procedure from {}: {}",
						path.display(),
						e
					);
				}
			}
		}
	}

	for name in names {
		let proc = loader_guard
			.create_procedure_by_name(&name, &[])
			.map_err(|e| SdkError::Other(format!("Failed to instantiate procedure '{}': {}", name, e)))?;
		builder = builder.register_procedure(Arc::new(proc));
	}

	Ok(builder)
}
