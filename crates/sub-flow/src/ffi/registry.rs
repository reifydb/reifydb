// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Registry for FFI operators

use std::collections::HashSet;

use crate::ffi::loader::FFIOperatorLoader;

/// Registry for FFI operators
pub struct FFIOperatorRegistry {
	loader: FFIOperatorLoader,
	registered_types: HashSet<String>,
}

impl FFIOperatorRegistry {
	/// Create a new FFI operator registry
	pub fn new(loader: FFIOperatorLoader) -> Self {
		Self {
			loader,
			registered_types: HashSet::new(),
		}
	}

	/// Register an operator type
	pub fn register_type(&mut self, operator_type: String) {
		self.registered_types.insert(operator_type);
	}

	/// Get the loader
	pub fn loader(&self) -> &FFIOperatorLoader {
		&self.loader
	}

	/// Get the registered types
	pub fn registered_types(&self) -> &HashSet<String> {
		&self.registered_types
	}
}
