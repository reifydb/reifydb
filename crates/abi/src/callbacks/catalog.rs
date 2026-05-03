// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	catalog::{namespace::NamespaceFFI, table::TableFFI},
	context::context::ContextFFI,
};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CatalogCallbacks {
	pub find_namespace:
		extern "C" fn(ctx: *mut ContextFFI, namespace_id: u64, version: u64, output: *mut NamespaceFFI) -> i32,

	pub find_namespace_by_name: extern "C" fn(
		ctx: *mut ContextFFI,
		name_ptr: *const u8,
		name_len: usize,
		version: u64,
		output: *mut NamespaceFFI,
	) -> i32,

	pub find_table: extern "C" fn(ctx: *mut ContextFFI, table_id: u64, version: u64, output: *mut TableFFI) -> i32,

	pub find_table_by_name: extern "C" fn(
		ctx: *mut ContextFFI,
		namespace_id: u64,
		name_ptr: *const u8,
		name_len: usize,
		version: u64,
		output: *mut TableFFI,
	) -> i32,

	pub free_namespace: extern "C" fn(namespace: *mut NamespaceFFI),

	pub free_table: extern "C" fn(table: *mut TableFFI),
}
