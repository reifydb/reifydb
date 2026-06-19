// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, ptr, slice::from_raw_parts, str::from_utf8};

use reifydb_abi::{
	catalog::{
		column::ColumnFFI,
		namespace::NamespaceFFI,
		primary_key::PrimaryKeyFFI,
		row_shape::{RowShapeFFI, RowShapeFieldFFI},
		table::TableFFI,
	},
	constants::{FFI_ERROR_INVALID_UTF8, FFI_ERROR_MARSHAL, FFI_ERROR_NULL_PTR, FFI_NOT_FOUND, FFI_OK},
	context::context::ContextFFI,
	data::buffer::BufferFFI,
};
use reifydb_core::{
	common::CommitVersion,
	encoded::shape::{RowShape, RowShapeField, fingerprint::RowShapeFingerprint},
	interface::catalog::{
		column::Column,
		id::{NamespaceId, TableId},
		key::PrimaryKey,
		namespace::Namespace,
		table::Table,
	},
};
use reifydb_extension::procedure::ffi_callbacks::memory::{host_alloc, host_free};
use reifydb_value::value::constraint::{Constraint, TypeConstraint};

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
pub(super) extern "C" fn host_catalog_find_row_shape(
	ctx: *mut ContextFFI,
	fingerprint: u64,
	output: *mut RowShapeFFI,
) -> i32 {
	if ctx.is_null() || output.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		let catalog = flow_txn.catalog();
		let fp = RowShapeFingerprint::from_le_bytes(fingerprint.to_le_bytes());

		match catalog.cache().find_row_shape(fp) {
			Some(shape) => match marshal_row_shape(&shape) {
				Ok(shape_ffi) => {
					*output = shape_ffi;
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

#[unsafe(no_mangle)]
pub(super) extern "C" fn host_catalog_free_row_shape(row_shape: *mut RowShapeFFI) {
	if row_shape.is_null() {
		return;
	}

	unsafe {
		let shape = &*row_shape;

		if !shape.fields.is_null() && shape.field_count > 0 {
			let fields_slice = from_raw_parts(shape.fields, shape.field_count);
			for field in fields_slice {
				if !field.name.ptr.is_null() && field.name.len > 0 {
					host_free(field.name.ptr as *mut u8, field.name.len);
				}
			}

			host_free(shape.fields as *mut u8, shape.field_count * mem::size_of::<RowShapeFieldFFI>());
		}
	}
}

fn marshal_row_shape(shape: &RowShape) -> Result<RowShapeFFI, &'static str> {
	let field_count = shape.fields().len();
	let fields_ptr = if field_count > 0 {
		let size = field_count * mem::size_of::<RowShapeFieldFFI>();
		let ptr = host_alloc(size) as *mut RowShapeFieldFFI;
		if ptr.is_null() {
			return Err("Failed to allocate row shape fields array");
		}

		for (i, field) in shape.fields().iter().enumerate() {
			match marshal_row_shape_field(field) {
				Ok(field_ffi) => unsafe {
					*ptr.add(i) = field_ffi;
				},
				Err(e) => {
					for j in 0..i {
						let earlier = unsafe { *ptr.add(j) };
						if !earlier.name.ptr.is_null() && earlier.name.len > 0 {
							unsafe {
								host_free(earlier.name.ptr as *mut u8, earlier.name.len)
							};
						}
					}
					unsafe { host_free(ptr as *mut u8, size) };
					return Err(e);
				}
			}
		}

		ptr
	} else {
		ptr::null_mut()
	};

	Ok(RowShapeFFI {
		fingerprint: shape.fingerprint().as_u64(),
		fields: fields_ptr,
		field_count,
	})
}

fn marshal_row_shape_field(field: &RowShapeField) -> Result<RowShapeFieldFFI, &'static str> {
	let name_bytes = field.name.as_bytes();
	let name_ptr = host_alloc(name_bytes.len());
	if name_ptr.is_null() && !name_bytes.is_empty() {
		return Err("Failed to allocate row shape field name");
	}
	if !name_bytes.is_empty() {
		unsafe {
			ptr::copy_nonoverlapping(name_bytes.as_ptr(), name_ptr, name_bytes.len());
		}
	}

	let (base_type, constraint_type, param1, param2) = encode_type_constraint(&field.constraint);

	Ok(RowShapeFieldFFI {
		name: BufferFFI {
			ptr: name_ptr,
			len: name_bytes.len(),
			cap: name_bytes.len(),
		},
		base_type,
		constraint_type,
		constraint_param1: param1,
		constraint_param2: param2,
		offset: field.offset,
		size: field.size,
		align: field.align,
	})
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

#[cfg(test)]
mod tests {
	use std::{slice::from_raw_parts, str::from_utf8};

	use reifydb_value::value::value_type::ValueType;

	use super::*;

	#[test]
	fn marshal_row_shape_emits_fingerprint_field_count_and_per_field_layout() {
		// This is the wire format the SDK reads back. If marshal ever stops setting fingerprint, or
		// reorders the (offset, size, align) triple, every downstream FFI operator silently decodes
		// into the wrong slots - exactly the panic class this feature exists to prevent.
		let shape = RowShape::new(vec![
			RowShapeField::new("id", TypeConstraint::unconstrained(ValueType::Uint8)),
			RowShapeField::new("mint", TypeConstraint::unconstrained(ValueType::Utf8)),
			RowShapeField::new("decimals", TypeConstraint::unconstrained(ValueType::Uint1)),
		]);

		let ffi = marshal_row_shape(&shape).expect("marshal must not allocate-fail for a 3-field shape");

		assert_eq!(
			ffi.fingerprint,
			shape.fingerprint().as_u64(),
			"fingerprint must round-trip - SDK uses it to confirm the resolved shape matches the row"
		);
		assert_eq!(ffi.field_count, 3);

		let fields_slice = unsafe { from_raw_parts(ffi.fields, ffi.field_count) };
		let names: Vec<&str> = fields_slice
			.iter()
			.map(|f| {
				let bytes = unsafe { from_raw_parts(f.name.ptr, f.name.len) };
				from_utf8(bytes).expect("marshalled names must be valid UTF-8")
			})
			.collect();
		assert_eq!(names, vec!["id", "mint", "decimals"]);

		for (ffi_field, shape_field) in fields_slice.iter().zip(shape.fields().iter()) {
			assert_eq!(
				ffi_field.offset, shape_field.offset,
				"offset divergence is the root cause of the 240-vs-120 utf8 panic"
			);
			assert_eq!(ffi_field.size, shape_field.size);
			assert_eq!(ffi_field.align, shape_field.align);
			assert_eq!(ffi_field.base_type, shape_field.constraint.get_type().to_u8());
		}

		// Reclaim the host-allocated buffers; if free ever crashes on a well-formed marshal output we
		// want the test to surface it rather than leak silently into other tests' allocations.
		let mut ffi_mut = ffi;
		host_catalog_free_row_shape(&mut ffi_mut as *mut RowShapeFFI);
	}
}
