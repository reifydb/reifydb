// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	path::{Path, PathBuf},
	sync::OnceLock,
};

use libloading::Symbol;
use reifydb_core::{interface::catalog::flow::FlowNodeId, internal};
use reifydb_extension::loader::ffi::LibraryCache;
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_type::{
	Result,
	error::Error,
	value::{Value, constraint::TypeConstraint},
};

use crate::operator::BoxedOperator;

pub const NATIVE_OPERATOR_MAGIC: u32 = 0x5244_424E;

pub const NATIVE_ABI_TAG: u32 = 0x0308;

pub type NativeOperatorCreateFn = fn(FlowNodeId, &BTreeMap<String, Value>) -> Result<BoxedOperator>;

pub struct NativeOperatorColumn {
	pub name: String,
	pub field_type: TypeConstraint,
	pub description: String,
}

pub struct NativeOperatorDescriptor {
	pub abi_tag: u32,
	pub name: String,
	pub version: String,
	pub description: String,
	pub capabilities: u32,
	pub input_columns: Vec<NativeOperatorColumn>,
	pub output_columns: Vec<NativeOperatorColumn>,
}

pub fn native_operator_magic() -> u32 {
	NATIVE_OPERATOR_MAGIC
}

pub fn check_native_abi_tag(abi_tag: u32) -> Result<()> {
	if abi_tag != NATIVE_ABI_TAG {
		return Err(Error(Box::new(internal!(
			"native operator ABI tag mismatch: plugin reports {:#06x}, host expects {:#06x}",
			abi_tag,
			NATIVE_ABI_TAG
		))));
	}
	Ok(())
}

pub struct LoadedNativeOperatorInfo {
	pub operator: String,
	pub library_path: PathBuf,
	pub version: String,
	pub description: String,
	pub input_columns: Vec<NativeOperatorColumn>,
	pub output_columns: Vec<NativeOperatorColumn>,
	pub capabilities: u32,
}

static GLOBAL_NATIVE_OPERATOR_LOADER: OnceLock<RwLock<NativeOperatorLoader>> = OnceLock::new();

pub fn native_operator_loader() -> &'static RwLock<NativeOperatorLoader> {
	GLOBAL_NATIVE_OPERATOR_LOADER.get_or_init(|| RwLock::new(NativeOperatorLoader::new()))
}

pub struct NativeOperatorLoader {
	cache: LibraryCache,
	operator_paths: HashMap<String, PathBuf>,
}

impl NativeOperatorLoader {
	fn new() -> Self {
		Self {
			cache: LibraryCache::new(),
			operator_paths: HashMap::new(),
		}
	}

	fn load_library(&mut self, path: &Path) -> Result<bool> {
		self.cache
			.check_magic(path, b"reifydb_native_operator_magic\0", NATIVE_OPERATOR_MAGIC)
			.map_err(|e| Error(Box::new(internal!("{}", e))))
	}

	fn descriptor(&self, path: &Path) -> Result<NativeOperatorDescriptor> {
		let library = self
			.cache
			.get(path)
			.ok_or_else(|| Error(Box::new(internal!("Library not loaded: {}", path.display()))))?;

		let descriptor = unsafe {
			let get_descriptor: Symbol<fn() -> NativeOperatorDescriptor> =
				library.get(b"reifydb_native_operator_descriptor\0").map_err(|e| {
					Error(Box::new(internal!(
						"Failed to find reifydb_native_operator_descriptor: {}",
						e
					)))
				})?;
			get_descriptor()
		};

		check_native_abi_tag(descriptor.abi_tag)?;

		Ok(descriptor)
	}

	pub fn register_operator(&mut self, path: &Path) -> Result<Option<LoadedNativeOperatorInfo>> {
		if !self.load_library(path)? {
			return Ok(None);
		}

		let descriptor = self.descriptor(path)?;
		self.operator_paths.insert(descriptor.name.clone(), path.to_path_buf());

		Ok(Some(LoadedNativeOperatorInfo {
			operator: descriptor.name,
			library_path: path.to_path_buf(),
			version: descriptor.version,
			description: descriptor.description,
			input_columns: descriptor.input_columns,
			output_columns: descriptor.output_columns,
			capabilities: descriptor.capabilities,
		}))
	}

	pub fn has_operator(&self, operator: &str) -> bool {
		self.operator_paths.contains_key(operator)
	}

	pub fn create_operator_by_name(
		&mut self,
		operator: &str,
		operator_id: FlowNodeId,
		config: &BTreeMap<String, Value>,
	) -> Result<BoxedOperator> {
		let path = self
			.operator_paths
			.get(operator)
			.ok_or_else(|| Error(Box::new(internal!("Native operator not found: {}", operator))))?
			.clone();

		if !self.load_library(&path)? {
			return Err(Error(Box::new(internal!(
				"Native operator library no longer valid: {}",
				operator
			))));
		}

		self.descriptor(&path)?;

		let library = self.cache.get(&path).unwrap();
		let create: NativeOperatorCreateFn = unsafe {
			let create_symbol: Symbol<NativeOperatorCreateFn> =
				library.get(b"reifydb_native_operator_create\0").map_err(|e| {
					Error(Box::new(internal!(
						"Failed to find reifydb_native_operator_create: {}",
						e
					)))
				})?;
			*create_symbol
		};

		create(operator_id, config)
	}
}

impl Default for NativeOperatorLoader {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_abi::constants::OPERATOR_ABI_TAG;
	use reifydb_extension::operator::ffi_loader::check_operator_abi_tag;

	use super::{NATIVE_ABI_TAG, check_native_abi_tag};

	// A plugin whose abi_tag does not match the host's must be refused, so an
	// operator built against a different reifydb/toolchain is never loaded.
	#[test]
	fn native_abi_tag_accepts_match_rejects_mismatch() {
		assert!(check_native_abi_tag(NATIVE_ABI_TAG).is_ok());
		assert!(check_native_abi_tag(NATIVE_ABI_TAG ^ 0x1).is_err());
		assert!(check_native_abi_tag(0).is_err());
	}

	#[test]
	fn ffi_abi_tag_accepts_match_rejects_mismatch() {
		assert!(check_operator_abi_tag(OPERATOR_ABI_TAG).is_ok());
		assert!(check_operator_abi_tag(OPERATOR_ABI_TAG ^ 0x1).is_err());
		assert!(check_operator_abi_tag(0).is_err());
	}

	// The two tags must be distinct and must reject each other, so a native
	// `.so` can never validate against the ffi check or vice versa.
	#[test]
	fn native_and_ffi_tags_do_not_accept_each_other() {
		assert_ne!(NATIVE_ABI_TAG, OPERATOR_ABI_TAG);
		assert!(check_native_abi_tag(OPERATOR_ABI_TAG).is_err());
		assert!(check_operator_abi_tag(NATIVE_ABI_TAG).is_err());
	}
}
