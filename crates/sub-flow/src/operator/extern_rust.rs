// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	path::{Path, PathBuf},
	sync::OnceLock,
};

use libloading::Symbol;
use reifydb_core::{
	common::{OperatorClass, WindowRequirements},
	interface::catalog::flow::OperatorId,
	operator_with::ApplyWith,
};
use reifydb_extension::loader::extern_load::ExternLoad;
use reifydb_flow::operator::BoxedHostOperator;
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_value::{Result, config::ExtensionParams, error::Error, value::constraint::TypeConstraint};

use crate::error::ExternOperatorError;

pub const EXTERN_RUST_OPERATOR_MAGIC: u32 = 0x5244_424E;

pub const EXTERN_RUST_ABI_TAG: u32 = 0x0308;

pub type ExternRustOperatorCreateFn = fn(OperatorId, &ExtensionParams, &ApplyWith) -> Result<BoxedHostOperator>;

pub struct ExternRustOperatorColumn {
	pub name: String,
	pub field_type: TypeConstraint,
	pub description: String,
}

pub struct ExternRustOperatorDescriptor {
	pub abi_tag: u32,
	pub name: String,
	pub version: String,
	pub description: String,
	pub capabilities: u32,
	pub input_columns: Vec<ExternRustOperatorColumn>,
	pub output_columns: Vec<ExternRustOperatorColumn>,
	pub class: OperatorClass,
	pub window: WindowRequirements,
	pub unmanaged_because: Option<&'static str>,
}

pub fn extern_rust_operator_magic() -> u32 {
	EXTERN_RUST_OPERATOR_MAGIC
}

pub fn check_extern_rust_abi_tag(abi_tag: u32) -> Result<()> {
	if abi_tag != EXTERN_RUST_ABI_TAG {
		return Err(Error::from(ExternOperatorError::AbiTagMismatch {
			plugin: abi_tag,
			host: EXTERN_RUST_ABI_TAG,
		}));
	}
	Ok(())
}

pub struct LoadedExternRustOperatorInfo {
	pub operator: String,
	pub library_path: PathBuf,
	pub abi_tag: u32,
	pub version: String,
	pub description: String,
	pub input_columns: Vec<ExternRustOperatorColumn>,
	pub output_columns: Vec<ExternRustOperatorColumn>,
	pub capabilities: u32,
	pub class: OperatorClass,
	pub window: WindowRequirements,
	pub unmanaged_because: Option<&'static str>,
}

static GLOBAL_EXTERN_RUST_OPERATOR_LOADER: OnceLock<RwLock<ExternRustOperatorLoader>> = OnceLock::new();

pub fn extern_rust_operator_loader() -> &'static RwLock<ExternRustOperatorLoader> {
	GLOBAL_EXTERN_RUST_OPERATOR_LOADER.get_or_init(|| RwLock::new(ExternRustOperatorLoader::new()))
}

pub struct ExternRustOperatorLoader {
	cache: ExternLoad,
	operator_paths: HashMap<String, PathBuf>,
}

impl ExternRustOperatorLoader {
	fn new() -> Self {
		Self {
			cache: ExternLoad::new(),
			operator_paths: HashMap::new(),
		}
	}

	fn load_library(&mut self, path: &Path) -> Result<bool> {
		self.cache
			.check_magic(path, b"reifydb_extern_rust_operator_magic\0", EXTERN_RUST_OPERATOR_MAGIC)
			.map_err(|e| {
				Error::from(ExternOperatorError::LibraryLoadFailed {
					path: path.display().to_string(),
					cause: e.to_string(),
				})
			})
	}

	fn descriptor(&self, path: &Path) -> Result<ExternRustOperatorDescriptor> {
		let library = self.cache.get(path).ok_or_else(|| {
			Error::from(ExternOperatorError::LibraryNotLoaded {
				path: path.display().to_string(),
			})
		})?;

		// SAFETY: load_library accepted this path only after the extern-Rust operator magic symbol
		// matched, so the object was built against this crate and declares the descriptor symbol with
		// this signature; Symbol borrows library, which stays loaded for the call.
		let descriptor = unsafe {
			let get_descriptor: Symbol<fn() -> ExternRustOperatorDescriptor> =
				library.get(b"reifydb_extern_rust_operator_descriptor\0").map_err(|e| {
					Error::from(ExternOperatorError::SymbolNotFound {
						symbol: "reifydb_extern_rust_operator_descriptor",
						cause: e.to_string(),
					})
				})?;
			get_descriptor()
		};

		check_extern_rust_abi_tag(descriptor.abi_tag)?;

		Ok(descriptor)
	}

	pub fn register_operator(&mut self, path: &Path) -> Result<Option<LoadedExternRustOperatorInfo>> {
		if !self.load_library(path)? {
			return Ok(None);
		}

		let descriptor = self.descriptor(path)?;
		self.operator_paths.insert(descriptor.name.clone(), path.to_path_buf());

		Ok(Some(loaded_info(descriptor, path)))
	}

	pub fn has_operator(&self, operator: &str) -> bool {
		self.operator_paths.contains_key(operator)
	}

	pub fn create_operator_by_name(
		&mut self,
		operator: &str,
		operator_id: OperatorId,
		params: &ExtensionParams,
		with: &ApplyWith,
	) -> Result<BoxedHostOperator> {
		let path = self
			.operator_paths
			.get(operator)
			.ok_or_else(|| {
				Error::from(ExternOperatorError::OperatorNotFound {
					operator: operator.to_string(),
				})
			})?
			.clone();

		if !self.load_library(&path)? {
			return Err(Error::from(ExternOperatorError::LibraryNotLoaded {
				path: operator.to_string(),
			}));
		}

		self.descriptor(&path)?;

		let library = self.cache.library(&path).map_err(|_| {
			Error::from(ExternOperatorError::LibraryNotLoaded {
				path: operator.to_string(),
			})
		})?;
		// SAFETY: load_library and descriptor accepted this path, so the object was built against this
		// crate and declares the create symbol with this signature; the copied-out pointer is called
		// before this method returns, while &mut self still holds the cache entry that keeps it mapped.
		let create: ExternRustOperatorCreateFn = unsafe {
			let create_symbol: Symbol<ExternRustOperatorCreateFn> =
				library.get(b"reifydb_extern_rust_operator_create\0").map_err(|e| {
					Error::from(ExternOperatorError::SymbolNotFound {
						symbol: "reifydb_extern_rust_operator_create",
						cause: e.to_string(),
					})
				})?;
			*create_symbol
		};

		create(operator_id, params, with)
	}
}

fn loaded_info(descriptor: ExternRustOperatorDescriptor, path: &Path) -> LoadedExternRustOperatorInfo {
	LoadedExternRustOperatorInfo {
		operator: descriptor.name,
		library_path: path.to_path_buf(),
		abi_tag: descriptor.abi_tag,
		version: descriptor.version,
		description: descriptor.description,
		input_columns: descriptor.input_columns,
		output_columns: descriptor.output_columns,
		capabilities: descriptor.capabilities,
		class: descriptor.class,
		window: descriptor.window,
		unmanaged_because: descriptor.unmanaged_because,
	}
}

impl Default for ExternRustOperatorLoader {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use std::path::Path;

	use reifydb_core::common::{OperatorClass, WindowRequirements, WindowSizeDomain};
	use reifydb_extension::operator::extern_c::loader::check_operator_abi_tag;
	use reifydb_sdk::flow::operator::extern_c::wire::types::OPERATOR_ABI_TAG;

	use super::{EXTERN_RUST_ABI_TAG, ExternRustOperatorDescriptor, check_extern_rust_abi_tag, loaded_info};

	#[test]
	fn extern_rust_abi_tag_accepts_match_rejects_mismatch() {
		// A mismatched tag must be refused, or an operator built against a different toolchain gets loaded.
		assert!(check_extern_rust_abi_tag(EXTERN_RUST_ABI_TAG).is_ok());
		assert!(check_extern_rust_abi_tag(EXTERN_RUST_ABI_TAG ^ 0x1).is_err());
		assert!(check_extern_rust_abi_tag(0).is_err());
	}

	#[test]
	fn extern_c_abi_tag_accepts_match_rejects_mismatch() {
		assert!(check_operator_abi_tag(OPERATOR_ABI_TAG).is_ok());
		assert!(check_operator_abi_tag(OPERATOR_ABI_TAG ^ 0x1).is_err());
		assert!(check_operator_abi_tag(0).is_err());
	}

	#[test]
	fn extern_rust_and_extern_c_tags_do_not_accept_each_other() {
		// The two tags must reject each other, or an extern-Rust `.so` validates against the extern-C check.
		assert_ne!(EXTERN_RUST_ABI_TAG, OPERATOR_ABI_TAG);
		assert!(check_extern_rust_abi_tag(OPERATOR_ABI_TAG).is_err());
		assert!(check_operator_abi_tag(EXTERN_RUST_ABI_TAG).is_err());
	}

	#[test]
	fn the_loaded_info_carries_the_descriptor_class_window_and_reason() {
		// A field dropped here reaches CREATE as some other class or window and skips its checks.
		let window = WindowRequirements {
			takes_window: true,
			kinds: &["sliding"],
			domain: WindowSizeDomain::Slots,
			needs_pane: true,
			throttles: false,
		};
		let descriptor = ExternRustOperatorDescriptor {
			abi_tag: EXTERN_RUST_ABI_TAG,
			name: "probe".to_string(),
			version: "0.0.1".to_string(),
			description: String::new(),
			capabilities: 0,
			input_columns: Vec::new(),
			output_columns: Vec::new(),
			class: OperatorClass::Unmanaged,
			window,
			unmanaged_because: Some("frees its own rows"),
		};
		let info = loaded_info(descriptor, Path::new("/probe.so"));
		assert_eq!(info.class, OperatorClass::Unmanaged);
		assert_eq!(info.window, window);
		assert_eq!(info.unmanaged_because, Some("frees its own rows"));
	}
}
