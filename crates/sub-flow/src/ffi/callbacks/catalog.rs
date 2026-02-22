// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Catalog access callbacks for FFI operators
//!
//! Provides read-only access to the catalog system (namespaces, tables)
//! with version-based queries for time-travel support.

use std::{slice::from_raw_parts, str::from_utf8};

use reifydb_abi::{
	catalog::{column::ColumnDefFFI, namespace::NamespaceFFI, primary_key::PrimaryKeyFFI, table::TableFFI},
	constants::{FFI_ERROR_INVALID_UTF8, FFI_ERROR_MARSHAL, FFI_ERROR_NULL_PTR, FFI_NOT_FOUND, FFI_OK},
	context::context::ContextFFI,
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::id::{NamespaceId, TableId},
};
use reifydb_engine::ffi::callbacks::memory::{host_alloc, host_free};
use reifydb_type::value::constraint::TypeConstraint;

use crate::ffi::context::get_transaction_mut;

/// Find a namespace by ID at a specific version
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_catalog_find_namespace(
	ctx: *mut ContextFFI,
	namespace_id: u64,
	version: u64,
	output: *mut NamespaceFFI,
) -> i32 {
	if ctx.is_null() || output.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		// Access catalog through the catalog() helper method
		let catalog = flow_txn.catalog();

		// Query catalog
		match catalog.materialized.find_namespace_at(NamespaceId(namespace_id), CommitVersion(version)) {
			Some(namespace) => {
				// Marshal to FFI
				*output = marshal_namespace(&namespace);
				FFI_OK
			}
			None => FFI_NOT_FOUND,
		}
	}
}

/// Find a namespace by name at a specific version
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_catalog_find_namespace_by_name(
	ctx: *mut ContextFFI,
	name_ptr: *const u8,
	name_len: usize,
	version: u64,
	output: *mut NamespaceFFI,
) -> i32 {
	if ctx.is_null() || name_ptr.is_null() || output.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		// Convert name bytes to string
		let name_bytes = from_raw_parts(name_ptr, name_len);
		let name = match from_utf8(name_bytes) {
			Ok(s) => s,
			Err(_) => return FFI_ERROR_INVALID_UTF8,
		};

		// Access catalog
		let catalog = flow_txn.catalog();

		// Query catalog
		match catalog.materialized.find_namespace_by_name_at(name, CommitVersion(version)) {
			Some(namespace) => {
				// Marshal to FFI
				*output = marshal_namespace(&namespace);
				FFI_OK
			}
			None => FFI_NOT_FOUND,
		}
	}
}

/// Find a table by ID at a specific version
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_catalog_find_table(
	ctx: *mut ContextFFI,
	table_id: u64,
	version: u64,
	output: *mut TableFFI,
) -> i32 {
	if ctx.is_null() || output.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		// Access catalog
		let catalog = flow_txn.catalog();

		// Query catalog
		match catalog.materialized.find_table_at(TableId(table_id), CommitVersion(version)) {
			Some(table) => {
				// Marshal to FFI
				match marshal_table(&table) {
					Ok(table_ffi) => {
						*output = table_ffi;
						FFI_OK
					}
					Err(_) => FFI_ERROR_MARSHAL,
				}
			}
			None => FFI_NOT_FOUND,
		}
	}
}

/// Find a table by name in a namespace at a specific version
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_catalog_find_table_by_name(
	ctx: *mut ContextFFI,
	namespace_id: u64,
	name_ptr: *const u8,
	name_len: usize,
	version: u64,
	output: *mut TableFFI,
) -> i32 {
	if ctx.is_null() || name_ptr.is_null() || output.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		// Convert name bytes to string
		let name_bytes = from_raw_parts(name_ptr, name_len);
		let name = match from_utf8(name_bytes) {
			Ok(s) => s,
			Err(_) => return FFI_ERROR_INVALID_UTF8,
		};

		// Access catalog
		let catalog = flow_txn.catalog();

		// Query catalog
		match catalog.materialized.find_table_by_name_at(
			NamespaceId(namespace_id),
			name,
			CommitVersion(version),
		) {
			Some(table) => {
				// Marshal to FFI
				match marshal_table(&table) {
					Ok(table_ffi) => {
						*output = table_ffi;
						FFI_OK
					}
					Err(_) => FFI_ERROR_MARSHAL,
				}
			}
			None => FFI_NOT_FOUND,
		}
	}
}

/// Free a namespace definition allocated by the host
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_catalog_free_namespace(namespace: *mut NamespaceFFI) {
	if namespace.is_null() {
		return;
	}

	unsafe {
		let ns = &*namespace;

		// Free name buffer
		if !ns.name.ptr.is_null() && ns.name.len > 0 {
			host_free(ns.name.ptr as *mut u8, ns.name.len);
		}
	}
}

/// Free a table definition allocated by the host
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_catalog_free_table(table: *mut TableFFI) {
	if table.is_null() {
		return;
	}

	unsafe {
		let tbl = &*table;

		// Free table name
		if !tbl.name.ptr.is_null() && tbl.name.len > 0 {
			host_free(tbl.name.ptr as *mut u8, tbl.name.len);
		}

		// Free columns array
		if !tbl.columns.is_null() && tbl.column_count > 0 {
			let columns_slice = from_raw_parts(tbl.columns, tbl.column_count);
			for col in columns_slice {
				// Free column name
				if !col.name.ptr.is_null() && col.name.len > 0 {
					host_free(col.name.ptr as *mut u8, col.name.len);
				}
			}
			// Free columns array itself
			host_free(tbl.columns as *mut u8, tbl.column_count * std::mem::size_of::<ColumnDefFFI>());
		}

		// Free primary key
		if !tbl.primary_key.is_null() {
			let pk = &*tbl.primary_key;
			// Free column IDs array
			if !pk.column_ids.is_null() && pk.column_count > 0 {
				host_free(pk.column_ids as *mut u8, pk.column_count * std::mem::size_of::<u64>());
			}
			// Free primary key struct itself
			host_free(tbl.primary_key as *mut u8, std::mem::size_of::<PrimaryKeyFFI>());
		}
	}
}

/// Marshal a NamespaceDef to FFI
fn marshal_namespace(namespace: &reifydb_core::interface::catalog::namespace::NamespaceDef) -> NamespaceFFI {
	// Allocate and copy name
	let name_bytes = namespace.name.as_bytes();
	let name_ptr = host_alloc(name_bytes.len());
	if !name_ptr.is_null() {
		unsafe {
			std::ptr::copy_nonoverlapping(name_bytes.as_ptr(), name_ptr, name_bytes.len());
		}
	}

	NamespaceFFI {
		id: namespace.id.0,
		name: reifydb_abi::data::buffer::BufferFFI {
			ptr: name_ptr,
			len: name_bytes.len(),
			cap: name_bytes.len(),
		},
		parent_id: namespace.parent_id.0,
	}
}

/// Marshal a TableDef to FFI
fn marshal_table(table: &reifydb_core::interface::catalog::table::TableDef) -> Result<TableFFI, &'static str> {
	// Allocate and copy table name
	let name_bytes = table.name.as_bytes();
	let name_ptr = host_alloc(name_bytes.len());
	if name_ptr.is_null() {
		return Err("Failed to allocate table name");
	}
	unsafe {
		std::ptr::copy_nonoverlapping(name_bytes.as_ptr(), name_ptr, name_bytes.len());
	}

	// Allocate columns array
	let columns_count = table.columns.len();
	let columns_ptr = if columns_count > 0 {
		let size = columns_count * std::mem::size_of::<ColumnDefFFI>();
		let ptr = host_alloc(size) as *mut ColumnDefFFI;
		if ptr.is_null() {
			// Clean up name before returning error
			host_free(name_ptr, name_bytes.len());
			return Err("Failed to allocate columns array");
		}

		// Marshal each column
		for (i, col) in table.columns.iter().enumerate() {
			unsafe {
				*ptr.add(i) = marshal_column(col)?;
			}
		}

		ptr
	} else {
		std::ptr::null_mut()
	};

	// Marshal primary key if present
	let (has_pk, pk_ptr) = if let Some(pk) = &table.primary_key {
		let pk_ptr = host_alloc(std::mem::size_of::<PrimaryKeyFFI>()) as *mut PrimaryKeyFFI;
		if pk_ptr.is_null() {
			// Clean up before returning error
			host_free(name_ptr, name_bytes.len());
			if !columns_ptr.is_null() {
				host_free(columns_ptr as *mut u8, columns_count * std::mem::size_of::<ColumnDefFFI>());
			}
			return Err("Failed to allocate primary key");
		}

		unsafe {
			*pk_ptr = marshal_primary_key(pk)?;
		}

		(1, pk_ptr)
	} else {
		(0, std::ptr::null_mut())
	};

	Ok(TableFFI {
		id: table.id.0,
		namespace_id: table.namespace.0,
		name: reifydb_abi::data::buffer::BufferFFI {
			ptr: name_ptr,
			len: name_bytes.len(),
			cap: name_bytes.len(),
		},
		columns: columns_ptr,
		column_count: columns_count,
		has_primary_key: has_pk,
		primary_key: pk_ptr,
	})
}

/// Marshal a ColumnDef to FFI
fn marshal_column(column: &reifydb_core::interface::catalog::column::ColumnDef) -> Result<ColumnDefFFI, &'static str> {
	// Allocate and copy column name
	let name_bytes = column.name.as_bytes();
	let name_ptr = host_alloc(name_bytes.len());
	if name_ptr.is_null() {
		return Err("Failed to allocate column name");
	}
	unsafe {
		std::ptr::copy_nonoverlapping(name_bytes.as_ptr(), name_ptr, name_bytes.len());
	}

	// Encode type constraint
	let (base_type, constraint_type, param1, param2) = encode_type_constraint(&column.constraint);

	Ok(ColumnDefFFI {
		id: column.id.0,
		name: reifydb_abi::data::buffer::BufferFFI {
			ptr: name_ptr,
			len: name_bytes.len(),
			cap: name_bytes.len(),
		},
		base_type,
		constraint_type,
		constraint_param1: param1,
		constraint_param2: param2,
		column_index: column.index.0,
		auto_increment: if column.auto_increment {
			1
		} else {
			0
		},
	})
}

/// Marshal a PrimaryKeyDef to FFI
fn marshal_primary_key(
	pk: &reifydb_core::interface::catalog::key::PrimaryKeyDef,
) -> Result<PrimaryKeyFFI, &'static str> {
	// Allocate column IDs array
	let column_count = pk.columns.len();
	let column_ids_ptr = if column_count > 0 {
		let size = column_count * std::mem::size_of::<u64>();
		let ptr = host_alloc(size) as *mut u64;
		if ptr.is_null() {
			return Err("Failed to allocate primary key column IDs");
		}

		// Copy column IDs
		for (i, col) in pk.columns.iter().enumerate() {
			unsafe {
				*ptr.add(i) = col.id.0;
			}
		}

		ptr
	} else {
		std::ptr::null_mut()
	};

	Ok(PrimaryKeyFFI {
		id: pk.id.0,
		column_count,
		column_ids: column_ids_ptr,
	})
}

/// Encode a TypeConstraint to FFI format
///
/// Returns: (base_type, constraint_type, param1, param2)
/// - constraint_type: 0=None, 1=MaxBytes, 2=PrecisionScale
fn encode_type_constraint(constraint: &TypeConstraint) -> (u8, u8, u32, u32) {
	let base_type = constraint.get_type().to_u8();

	match constraint.constraint() {
		None => (base_type, 0, 0, 0),
		Some(reifydb_type::value::constraint::Constraint::MaxBytes(max)) => (base_type, 1, max.value(), 0),
		Some(reifydb_type::value::constraint::Constraint::PrecisionScale(precision, scale)) => {
			(base_type, 2, precision.value() as u32, scale.value() as u32)
		}
		Some(reifydb_type::value::constraint::Constraint::Dictionary(_, _)) => {
			// Dictionary constraint: encode as type 3
			(base_type, 3, 0, 0)
		}
		Some(reifydb_type::value::constraint::Constraint::SumType(id)) => (base_type, 4, id.to_u64() as u32, 0),
	}
}
