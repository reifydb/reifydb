// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{mem::MaybeUninit, slice::from_raw_parts, str};

use reifydb_abi::{
	catalog::row_shape::{RowShapeFFI, RowShapeFieldFFI},
	constants::{FFI_NOT_FOUND, FFI_OK},
};
use reifydb_core::encoded::shape::{RowShape, RowShapeField, fingerprint::RowShapeFingerprint};

use super::decode_type_constraint;
use crate::{error::FFIError, operator::context::ffi::FFIOperatorContext};

pub(super) fn raw_catalog_find_row_shape(
	ctx: &FFIOperatorContext,
	fingerprint: RowShapeFingerprint,
) -> Result<Option<RowShape>, FFIError> {
	unsafe {
		let callback = (*ctx.ctx).callbacks.catalog.find_row_shape;

		let mut output = MaybeUninit::<RowShapeFFI>::uninit();

		let result = callback(ctx.ctx, fingerprint.as_u64(), output.as_mut_ptr());

		match result {
			FFI_OK => {
				let ffi_shape = output.assume_init();
				let shape = unmarshal_row_shape(&ffi_shape)?;

				let free_callback = (*ctx.ctx).callbacks.catalog.free_row_shape;
				free_callback(&mut output.as_mut_ptr().read());

				Ok(Some(shape))
			}
			FFI_NOT_FOUND => Ok(None),
			_ => Err(FFIError::Other("Failed to find row shape".to_string())),
		}
	}
}

unsafe fn unmarshal_row_shape(ffi_shape: &RowShapeFFI) -> Result<RowShape, FFIError> {
	let fields = if !ffi_shape.fields.is_null() && ffi_shape.field_count > 0 {
		let slice = unsafe { from_raw_parts(ffi_shape.fields, ffi_shape.field_count) };
		let mut out = Vec::with_capacity(slice.len());
		for ffi_field in slice {
			out.push(unsafe { unmarshal_row_shape_field(ffi_field)? });
		}
		out
	} else {
		Vec::new()
	};

	Ok(RowShape::from_parts(RowShapeFingerprint::new(ffi_shape.fingerprint), fields))
}

unsafe fn unmarshal_row_shape_field(ffi_field: &RowShapeFieldFFI) -> Result<RowShapeField, FFIError> {
	let name_bytes = if !ffi_field.name.ptr.is_null() && ffi_field.name.len > 0 {
		unsafe { from_raw_parts(ffi_field.name.ptr, ffi_field.name.len) }
	} else {
		&[]
	};

	let name = str::from_utf8(name_bytes)
		.map_err(|_| FFIError::Other("Invalid UTF-8 in row shape field name".to_string()))?
		.to_string();

	let constraint = decode_type_constraint(
		ffi_field.base_type,
		ffi_field.constraint_type,
		ffi_field.constraint_param1,
		ffi_field.constraint_param2,
	)?;

	Ok(RowShapeField {
		name,
		constraint,
		offset: ffi_field.offset,
		size: ffi_field.size,
		align: ffi_field.align,
	})
}

#[cfg(test)]
mod tests {
	use std::ptr;

	use reifydb_abi::data::buffer::BufferFFI;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

	use super::*;

	fn make_name_buffer(s: &str) -> (BufferFFI, Box<[u8]>) {
		// We verify the unmarshaller correctly copies name bytes out of the FFI struct rather than
		// retaining the host pointer. Owning the backing slice in the test prevents accidental reads
		// from freed memory if the impl ever changes.
		let bytes: Box<[u8]> = s.as_bytes().into();
		let buffer = BufferFFI {
			ptr: bytes.as_ptr(),
			len: bytes.len(),
			cap: bytes.len(),
		};
		(buffer, bytes)
	}

	#[test]
	fn unmarshal_round_trips_a_three_field_shape() {
		// Models exactly what the host marshal layer produces: a fingerprint plus N flattened
		// RowShapeFieldFFI entries. If unmarshal misinterprets type constraints or offsets, downstream
		// operators silently read the wrong bytes - the bug class this whole feature exists to prevent.
		let original = RowShape::new(vec![
			RowShapeField::new("id", TypeConstraint::unconstrained(Type::Uint8)),
			RowShapeField::new("mint", TypeConstraint::unconstrained(Type::Utf8)),
			RowShapeField::new("decimals", TypeConstraint::unconstrained(Type::Uint1)),
		]);

		let (id_name, _id_keep) = make_name_buffer("id");
		let (mint_name, _mint_keep) = make_name_buffer("mint");
		let (dec_name, _dec_keep) = make_name_buffer("decimals");

		let fields: Vec<RowShapeFieldFFI> = original
			.fields()
			.iter()
			.zip([id_name, mint_name, dec_name])
			.map(|(f, name_buf)| RowShapeFieldFFI {
				name: name_buf,
				base_type: f.constraint.get_type().to_u8(),
				constraint_type: 0,
				constraint_param1: 0,
				constraint_param2: 0,
				offset: f.offset,
				size: f.size,
				align: f.align,
			})
			.collect();

		let ffi = RowShapeFFI {
			fingerprint: original.fingerprint().as_u64(),
			fields: fields.as_ptr(),
			field_count: fields.len(),
		};

		let decoded = unsafe { unmarshal_row_shape(&ffi).expect("unmarshal must succeed for valid FFI") };

		assert_eq!(
			decoded.fingerprint(),
			original.fingerprint(),
			"fingerprint must survive marshalling - otherwise SDK callers cannot recognise the shape"
		);
		assert_eq!(decoded.fields().len(), original.fields().len());
		for (a, b) in decoded.fields().iter().zip(original.fields().iter()) {
			assert_eq!(a.name, b.name, "field name must round-trip");
			assert_eq!(
				a.constraint.get_type().to_u8(),
				b.constraint.get_type().to_u8(),
				"field type must round-trip - this is what enables correct decoding"
			);
			assert_eq!(a.offset, b.offset, "offset drift breaks every subsequent get_utf8 read");
			assert_eq!(a.size, b.size);
			assert_eq!(a.align, b.align);
		}
	}

	#[test]
	fn unmarshal_empty_shape_returns_empty_fields() {
		// Defensive: a future caller might marshal a metadata-only shape. The current callsite never
		// does, but the unmarshaller must not deref a null fields pointer when field_count == 0.
		let ffi = RowShapeFFI {
			fingerprint: 0,
			fields: ptr::null(),
			field_count: 0,
		};

		let decoded = unsafe { unmarshal_row_shape(&ffi).expect("empty shape must unmarshal cleanly") };
		assert!(decoded.fields().is_empty());
		assert_eq!(decoded.fingerprint().as_u64(), 0);
	}
}
