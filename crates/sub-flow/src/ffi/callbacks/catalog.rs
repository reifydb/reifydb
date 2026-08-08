// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, ptr, slice::from_raw_parts};

use reifydb_abi::{
	catalog::row_shape::{RowShapeFFI, RowShapeFieldFFI},
	constants::{FFI_ERROR_MARSHAL, FFI_ERROR_NULL_PTR, FFI_NOT_FOUND, FFI_OK},
	context::context::ContextFFI,
	data::buffer::BufferFFI,
};
use reifydb_codec::{
	row::shape::{RowShape, RowShapeField, fingerprint::RowShapeFingerprint},
	tag::type_tag_byte,
};
use reifydb_extension::procedure::ffi_callbacks::memory::{host_alloc, host_free};
use reifydb_value::value::constraint::{Constraint, TypeConstraint};

use crate::ffi::context::get_transaction_mut;

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_catalog_find_row_shape(
	ctx: *mut ContextFFI,
	fingerprint: u64,
	output: *mut RowShapeFFI,
) -> i32 {
	if ctx.is_null() || output.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` and `output` are null-checked above; the guest must pass back the ContextFFI the
	// host handed it for this call (discharging get_transaction_mut) and an `output` valid and
	// aligned for one RowShapeFFI write.
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

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_catalog_free_row_shape(row_shape: *mut RowShapeFFI) {
	if row_shape.is_null() {
		return;
	}

	// SAFETY: `row_shape` is null-checked above and must point to a readable, not-yet-freed
	// RowShapeFFI that marshal_row_shape produced and the guest left unmodified, so `fields` holds
	// `field_count` initialised RowShapeFieldFFI and every pointer/size pair below is exactly the
	// host_alloc block and its size (discharges host_free).
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
				// SAFETY: `ptr` is the non-null host_alloc block of `size` bytes above, so it holds
				// `field_count` slots at align 8 >= align_of::<RowShapeFieldFFI>(), and `i` is below
				// that count; RowShapeFieldFFI is Copy, so the write drops nothing uninitialised.
				Ok(field_ffi) => unsafe {
					*ptr.add(i) = field_ffi;
				},
				Err(e) => {
					for j in 0..i {
						// SAFETY: `j < i` means slot `j` was written by an earlier iteration,
						// so it is an initialised Copy value inside the same block.
						let earlier = unsafe { *ptr.add(j) };
						if !earlier.name.ptr.is_null() && earlier.name.len > 0 {
							// SAFETY: discharges host_free; the name is the host_alloc
							// block of exactly `earlier.name.len` bytes that
							// marshal_row_shape_field made and nothing has freed yet.
							unsafe {
								host_free(earlier.name.ptr as *mut u8, earlier.name.len)
							};
						}
					}
					// SAFETY: discharges host_free; `ptr`/`size` are exactly the host_alloc block
					// and size above, freed once on this path.
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
		// SAFETY: `name_bytes` is non-empty here, so the null return was rejected above and
		// `name_ptr` is a fresh host_alloc block of `name_bytes.len()` bytes that cannot overlap the
		// borrowed source.
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
	})
}

fn encode_type_constraint(constraint: &TypeConstraint) -> (u8, u8, u32, u32) {
	let base_type = type_tag_byte(&constraint.get_type());

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

	use reifydb_codec::row::shape::RowFamily;
	use reifydb_value::value::value_type::ValueType;

	use super::*;

	#[test]
	fn marshal_row_shape_emits_fingerprint_field_count_and_per_field_layout() {
		// Dropping the fingerprint or reordering the (offset, size) pair makes every downstream FFI operator
		// decode into the wrong slots, silently.
		let shape = RowShape::new(
			RowFamily::Deprecated,
			vec![
				RowShapeField::new("id", TypeConstraint::unconstrained(ValueType::Uint8)),
				RowShapeField::new("mint", TypeConstraint::unconstrained(ValueType::Utf8)),
				RowShapeField::new("decimals", TypeConstraint::unconstrained(ValueType::Uint1)),
			],
		);

		let ffi = marshal_row_shape(&shape).expect("marshal must not allocate-fail for a 3-field shape");

		assert_eq!(
			ffi.fingerprint,
			shape.fingerprint().as_u64(),
			"fingerprint must round-trip - SDK uses it to confirm the resolved shape matches the row"
		);
		assert_eq!(ffi.field_count, 3);

		// SAFETY: marshal_row_shape returned Ok, so `fields` is a host allocation of exactly
		// `field_count` initialised entries and stays alive until the free call below.
		let fields_slice = unsafe { from_raw_parts(ffi.fields, ffi.field_count) };
		let names: Vec<&str> = fields_slice
			.iter()
			.map(|f| {
				// SAFETY: each marshalled name is a host allocation of `len` bytes owned by
				// the same still-live RowShapeFFI.
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
			assert_eq!(ffi_field.base_type, type_tag_byte(&shape_field.constraint.get_type()));
		}

		// Freeing here surfaces a crash on well-formed marshal output rather than leaking into
		// other tests' allocations.
		let mut ffi_mut = ffi;
		host_catalog_free_row_shape(&mut ffi_mut as *mut RowShapeFFI);
	}
}
