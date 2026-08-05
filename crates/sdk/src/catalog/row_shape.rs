// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem::MaybeUninit, slice::from_raw_parts, str};

use reifydb_abi::{
	catalog::row_shape::{RowShapeFFI, RowShapeFieldFFI},
	constants::{FFI_NOT_FOUND, FFI_OK},
};
use reifydb_codec::encoded::shape::{RowShape, RowShapeField, fingerprint::RowShapeFingerprint};
#[cfg(test)]
use reifydb_codec::tag::type_tag_byte;

use super::decode_type_constraint;
use crate::{error::SdkError, operator::context::ffi::FFIOperatorContext};

pub(super) fn raw_catalog_find_row_shape(
	ctx: &FFIOperatorContext,
	fingerprint: RowShapeFingerprint,
) -> Result<Option<RowShape>, SdkError> {
	// SAFETY: `FFIOperatorContext::new` asserts `ctx.ctx` is non-null and the host keeps the ContextFFI
	// alive for the call; on FFI_OK the host has written a fully initialised RowShapeFFI into `output`
	// whose field array stays live until `free_row_shape`, discharging `unmarshal_row_shape`.
	unsafe {
		let callback = (*ctx.ctx).callbacks.row_shape.find_row_shape;

		let mut output = MaybeUninit::<RowShapeFFI>::uninit();

		let result = callback(ctx.ctx, fingerprint.as_u64(), output.as_mut_ptr());

		match result {
			FFI_OK => {
				let ffi_shape = output.assume_init();
				let shape = unmarshal_row_shape(&ffi_shape)?;

				let free_callback = (*ctx.ctx).callbacks.row_shape.free_row_shape;
				free_callback(&mut output.as_mut_ptr().read());

				Ok(Some(shape))
			}
			FFI_NOT_FOUND => Ok(None),
			_ => Err(SdkError::Other("Failed to find row shape".to_string())),
		}
	}
}

/// # Safety
///
/// `ffi_shape.fields` must be null or valid for reads of `ffi_shape.field_count`
/// initialised, aligned `RowShapeFieldFFI` for the duration of the call, each of
/// which must satisfy the contract of [`unmarshal_row_shape_field`].
unsafe fn unmarshal_row_shape(ffi_shape: &RowShapeFFI) -> Result<RowShape, SdkError> {
	let fields = if !ffi_shape.fields.is_null() && ffi_shape.field_count > 0 {
		// SAFETY: discharges this function's own contract; the branch above established that `fields`
		// is non-null and `field_count` is non-zero.
		let slice = unsafe { from_raw_parts(ffi_shape.fields, ffi_shape.field_count) };
		let mut out = Vec::with_capacity(slice.len());
		for ffi_field in slice {
			// SAFETY: this function's contract requires every element of `fields` to satisfy
			// `unmarshal_row_shape_field`.
			out.push(unsafe { unmarshal_row_shape_field(ffi_field)? });
		}
		out
	} else {
		Vec::new()
	};

	Ok(RowShape::from_parts(RowShapeFingerprint::new(ffi_shape.fingerprint), fields))
}

/// # Safety
///
/// `ffi_field.name.ptr` must be null or valid for reads of `ffi_field.name.len`
/// initialised bytes for the duration of the call.
unsafe fn unmarshal_row_shape_field(ffi_field: &RowShapeFieldFFI) -> Result<RowShapeField, SdkError> {
	let name_bytes = if !ffi_field.name.ptr.is_null() && ffi_field.name.len > 0 {
		// SAFETY: discharges this function's own contract; the branch above established that
		// `name.ptr` is non-null and `name.len` is non-zero.
		unsafe { from_raw_parts(ffi_field.name.ptr, ffi_field.name.len) }
	} else {
		&[]
	};

	let name = str::from_utf8(name_bytes)
		.map_err(|_| SdkError::Other("Invalid UTF-8 in row shape field name".to_string()))?
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
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

	use super::*;

	fn make_name_buffer(s: &str) -> (BufferFFI, Box<[u8]>) {
		// The caller keeps the returned slice alive, so if the unmarshaller ever retained the host pointer
		// instead of copying the name bytes the test would still read live memory rather than crash.
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
		// Misreading a type constraint or offset here has downstream operators silently reading the wrong
		// bytes, which is the failure mode the shape exists to prevent.
		let original = RowShape::new(vec![
			RowShapeField::new("id", TypeConstraint::unconstrained(ValueType::Uint8)),
			RowShapeField::new("mint", TypeConstraint::unconstrained(ValueType::Utf8)),
			RowShapeField::new("decimals", TypeConstraint::unconstrained(ValueType::Uint1)),
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
				base_type: type_tag_byte(&f.constraint.get_type()),
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

		// SAFETY: `fields` and every name buffer borrow locals that stay alive past this call.
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
				type_tag_byte(&a.constraint.get_type()),
				type_tag_byte(&b.constraint.get_type()),
				"field type must round-trip - this is what enables correct decoding"
			);
			assert_eq!(a.offset, b.offset, "offset drift breaks every subsequent get_utf8 read");
			assert_eq!(a.size, b.size);
			assert_eq!(a.align, b.align);
		}
	}

	#[test]
	fn unmarshal_empty_shape_returns_empty_fields() {
		// No callsite marshals a metadata-only shape today, but the unmarshaller must not deref a null
		// fields pointer when field_count is 0.
		let ffi = RowShapeFFI {
			fingerprint: 0,
			fields: ptr::null(),
			field_count: 0,
		};

		// SAFETY: `fields` is null with `field_count` 0, which the contract admits.
		let decoded = unsafe { unmarshal_row_shape(&ffi).expect("empty shape must unmarshal cleanly") };
		assert!(decoded.fields().is_empty());
		assert_eq!(decoded.fingerprint().as_u64(), 0);
	}
}
