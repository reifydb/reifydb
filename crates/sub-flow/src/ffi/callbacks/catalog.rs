// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{mem, ptr, slice::from_raw_parts, str::from_utf8};

use reifydb_abi::{
	catalog::{column::ColumnFFI, namespace::NamespaceFFI, primary_key::PrimaryKeyFFI, table::TableFFI},
	constants::{FFI_ERROR_INVALID_UTF8, FFI_ERROR_MARSHAL, FFI_ERROR_NULL_PTR, FFI_NOT_FOUND, FFI_OK},
	context::context::ContextFFI,
	data::buffer::BufferFFI,
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		column::Column,
		id::{NamespaceId, TableId},
		key::PrimaryKey,
		namespace::Namespace,
		table::Table,
	},
};
use reifydb_extension::procedure::ffi_callbacks::memory::{host_alloc, host_free};
use reifydb_type::value::constraint::{Constraint, TypeConstraint};

use crate::ffi::context::get_transaction_mut;

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

		let catalog = flow_txn.catalog();

		match catalog.cache().find_namespace_at(NamespaceId(namespace_id), CommitVersion(version)) {
			Some(namespace) => {
				*output = marshal_namespace(&namespace);
				FFI_OK
			}
			None => FFI_NOT_FOUND,
		}
	}
}

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

		let name_bytes = from_raw_parts(name_ptr, name_len);
		let name = match from_utf8(name_bytes) {
			Ok(s) => s,
			Err(_) => return FFI_ERROR_INVALID_UTF8,
		};

		let catalog = flow_txn.catalog();

		match catalog.cache().find_namespace_by_name_at(name, CommitVersion(version)) {
			Some(namespace) => {
				*output = marshal_namespace(&namespace);
				FFI_OK
			}
			None => FFI_NOT_FOUND,
		}
	}
}

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

		let catalog = flow_txn.catalog();

		match catalog.cache().find_table_at(TableId(table_id), CommitVersion(version)) {
			Some(table) => match marshal_table(&table) {
				Ok(table_ffi) => {
					*output = table_ffi;
					FFI_OK
				}
				Err(_) => FFI_ERROR_MARSHAL,
			},
			None => FFI_NOT_FOUND,
		}
	}
}

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

		let name_bytes = from_raw_parts(name_ptr, name_len);
		let name = match from_utf8(name_bytes) {
			Ok(s) => s,
			Err(_) => return FFI_ERROR_INVALID_UTF8,
		};

		let catalog = flow_txn.catalog();

		match catalog.cache().find_table_by_name_at(NamespaceId(namespace_id), name, CommitVersion(version)) {
			Some(table) => match marshal_table(&table) {
				Ok(table_ffi) => {
					*output = table_ffi;
					FFI_OK
				}
				Err(_) => FFI_ERROR_MARSHAL,
			},
			None => FFI_NOT_FOUND,
		}
	}
}

#[unsafe(no_mangle)]
pub(super) extern "C" fn host_catalog_free_namespace(namespace: *mut NamespaceFFI) {
	if namespace.is_null() {
		return;
	}

	unsafe {
		let ns = &*namespace;

		if !ns.name.ptr.is_null() && ns.name.len > 0 {
			host_free(ns.name.ptr as *mut u8, ns.name.len);
		}
	}
}

#[unsafe(no_mangle)]
pub(super) extern "C" fn host_catalog_free_table(table: *mut TableFFI) {
	if table.is_null() {
		return;
	}

	unsafe {
		let tbl = &*table;

		if !tbl.name.ptr.is_null() && tbl.name.len > 0 {
			host_free(tbl.name.ptr as *mut u8, tbl.name.len);
		}

		if !tbl.columns.is_null() && tbl.column_count > 0 {
			let columns_slice = from_raw_parts(tbl.columns, tbl.column_count);
			for col in columns_slice {
				if !col.name.ptr.is_null() && col.name.len > 0 {
					host_free(col.name.ptr as *mut u8, col.name.len);
				}
			}

			host_free(tbl.columns as *mut u8, tbl.column_count * mem::size_of::<ColumnFFI>());
		}

		if !tbl.primary_key.is_null() {
			let pk = &*tbl.primary_key;

			if !pk.column_ids.is_null() && pk.column_count > 0 {
				host_free(pk.column_ids as *mut u8, pk.column_count * mem::size_of::<u64>());
			}

			host_free(tbl.primary_key as *mut u8, mem::size_of::<PrimaryKeyFFI>());
		}
	}
}

fn marshal_namespace(namespace: &Namespace) -> NamespaceFFI {
	let name_bytes = namespace.name().as_bytes();
	let name_ptr = host_alloc(name_bytes.len());
	if !name_ptr.is_null() {
		unsafe {
			ptr::copy_nonoverlapping(name_bytes.as_ptr(), name_ptr, name_bytes.len());
		}
	}

	NamespaceFFI {
		id: namespace.id().0,
		name: BufferFFI {
			ptr: name_ptr,
			len: name_bytes.len(),
			cap: name_bytes.len(),
		},
		parent_id: namespace.parent_id().0,
	}
}

fn marshal_table(table: &Table) -> Result<TableFFI, &'static str> {
	let name_bytes = table.name.as_bytes();
	let name_ptr = host_alloc(name_bytes.len());
	if name_ptr.is_null() {
		return Err("Failed to allocate table name");
	}
	unsafe {
		ptr::copy_nonoverlapping(name_bytes.as_ptr(), name_ptr, name_bytes.len());
	}

	let columns_count = table.columns.len();
	let columns_ptr = if columns_count > 0 {
		let size = columns_count * mem::size_of::<ColumnFFI>();
		let ptr = host_alloc(size) as *mut ColumnFFI;
		if ptr.is_null() {
			unsafe { host_free(name_ptr, name_bytes.len()) };
			return Err("Failed to allocate columns array");
		}

		for (i, col) in table.columns.iter().enumerate() {
			unsafe {
				*ptr.add(i) = marshal_column(col)?;
			}
		}

		ptr
	} else {
		ptr::null_mut()
	};

	let (has_pk, pk_ptr) = if let Some(pk) = &table.primary_key {
		let pk_ptr = host_alloc(mem::size_of::<PrimaryKeyFFI>()) as *mut PrimaryKeyFFI;
		if pk_ptr.is_null() {
			unsafe { host_free(name_ptr, name_bytes.len()) };
			if !columns_ptr.is_null() {
				unsafe {
					host_free(columns_ptr as *mut u8, columns_count * mem::size_of::<ColumnFFI>())
				};
			}
			return Err("Failed to allocate primary key");
		}

		unsafe {
			*pk_ptr = marshal_primary_key(pk)?;
		}

		(1, pk_ptr)
	} else {
		(0, ptr::null_mut())
	};

	Ok(TableFFI {
		id: table.id.0,
		namespace_id: table.namespace.0,
		name: BufferFFI {
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

fn marshal_column(column: &Column) -> Result<ColumnFFI, &'static str> {
	let name_bytes = column.name.as_bytes();
	let name_ptr = host_alloc(name_bytes.len());
	if name_ptr.is_null() {
		return Err("Failed to allocate column name");
	}
	unsafe {
		ptr::copy_nonoverlapping(name_bytes.as_ptr(), name_ptr, name_bytes.len());
	}

	let (base_type, constraint_type, param1, param2) = encode_type_constraint(&column.constraint);

	Ok(ColumnFFI {
		id: column.id.0,
		name: BufferFFI {
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

fn marshal_primary_key(pk: &PrimaryKey) -> Result<PrimaryKeyFFI, &'static str> {
	let column_count = pk.columns.len();
	let column_ids_ptr = if column_count > 0 {
		let size = column_count * mem::size_of::<u64>();
		let ptr = host_alloc(size) as *mut u64;
		if ptr.is_null() {
			return Err("Failed to allocate primary key column IDs");
		}

		for (i, col) in pk.columns.iter().enumerate() {
			unsafe {
				*ptr.add(i) = col.id.0;
			}
		}

		ptr
	} else {
		ptr::null_mut()
	};

	Ok(PrimaryKeyFFI {
		id: pk.id.0,
		column_count,
		column_ids: column_ids_ptr,
	})
}

fn encode_type_constraint(constraint: &TypeConstraint) -> (u8, u8, u32, u32) {
	let base_type = constraint.get_type().to_u8();

	match constraint.constraint() {
		None => (base_type, 0, 0, 0),
		Some(Constraint::MaxBytes(max)) => (base_type, 1, max.value(), 0),
		Some(Constraint::PrecisionScale(precision, scale)) => {
			(base_type, 2, precision.value() as u32, scale.value() as u32)
		}
		Some(Constraint::Dictionary(_, _)) => (base_type, 3, 0, 0),
		Some(Constraint::SumType(id)) => (base_type, 4, id.to_u64() as u32, 0),
	}
}
