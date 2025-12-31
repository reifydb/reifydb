// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::{
	catalog::{NamespaceFFI, TableFFI},
	context::ContextFFI,
};

/// Catalog access callbacks (read-only access to catalog system)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CatalogCallbacks {
	/// Find a namespace by ID at specific version
	///
	/// # Parameters
	/// - `ctx`: FFI context
	/// - `namespace_id`: Namespace ID
	/// - `version`: Commit version for time-travel queries
	/// - `output`: Pointer to receive namespace definition
	///
	/// # Returns
	/// - 0 if namespace exists, 1 if not found, negative on error
	pub find_namespace:
		extern "C" fn(ctx: *mut ContextFFI, namespace_id: u64, version: u64, output: *mut NamespaceFFI) -> i32,

	/// Find a namespace by name at specific version
	///
	/// # Parameters
	/// - `ctx`: FFI context
	/// - `name_ptr`: Namespace name bytes (UTF-8)
	/// - `name_len`: Length of name
	/// - `version`: Commit version for time-travel queries
	/// - `output`: Pointer to receive namespace definition
	///
	/// # Returns
	/// - 0 if namespace exists, 1 if not found, negative on error
	pub find_namespace_by_name: extern "C" fn(
		ctx: *mut ContextFFI,
		name_ptr: *const u8,
		name_len: usize,
		version: u64,
		output: *mut NamespaceFFI,
	) -> i32,

	/// Find a table by ID at specific version
	///
	/// # Parameters
	/// - `ctx`: FFI context
	/// - `table_id`: Table ID
	/// - `version`: Commit version for time-travel queries
	/// - `output`: Pointer to receive table definition
	///
	/// # Returns
	/// - 0 if table exists, 1 if not found, negative on error
	pub find_table: extern "C" fn(ctx: *mut ContextFFI, table_id: u64, version: u64, output: *mut TableFFI) -> i32,

	/// Find a table by name in a namespace at specific version
	///
	/// # Parameters
	/// - `ctx`: FFI context
	/// - `namespace_id`: Namespace ID
	/// - `name_ptr`: Table name bytes (UTF-8)
	/// - `name_len`: Length of name
	/// - `version`: Commit version for time-travel queries
	/// - `output`: Pointer to receive table definition
	///
	/// # Returns
	/// - 0 if table exists, 1 if not found, negative on error
	pub find_table_by_name: extern "C" fn(
		ctx: *mut ContextFFI,
		namespace_id: u64,
		name_ptr: *const u8,
		name_len: usize,
		version: u64,
		output: *mut TableFFI,
	) -> i32,

	/// Free a namespace definition allocated by the host
	///
	/// # Parameters
	/// - `namespace`: Pointer to namespace to free
	pub free_namespace: extern "C" fn(namespace: *mut NamespaceFFI),

	/// Free a table definition allocated by the host
	///
	/// # Parameters
	/// - `table`: Pointer to table to free
	pub free_table: extern "C" fn(table: *mut TableFFI),
}
