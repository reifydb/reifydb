//! Layout marshalling between Rust and FFI types

use std::ops::Deref;

use reifydb_core::value::encoded::EncodedValuesNamedLayout;
use reifydb_flow_operator_abi::*;
use reifydb_type::Type;

use crate::marshal::Marshaller;

impl Marshaller {
	/// Marshal a layout to FFI representation
	pub(crate) fn marshal_layout(&mut self, layout: &EncodedValuesNamedLayout) -> *const LayoutFFI {
		// Get the inner layout
		let layout_inner = layout.layout().deref();

		// Allocate fields array
		let field_count = layout_inner.fields.len();
		let fields_ptr = if field_count > 0 {
			let fields_array = self.arena.alloc(field_count * size_of::<FieldFFI>()) as *mut FieldFFI;

			unsafe {
				let fields_slice = std::slice::from_raw_parts_mut(fields_array, field_count);

				for (i, field) in layout_inner.fields.iter().enumerate() {
					fields_slice[i] = FieldFFI {
						offset: field.offset,
						size: field.size,
						align: field.align,
						field_type: field.r#type.to_u8(),
					};
				}
			}

			fields_array as *const FieldFFI
		} else {
			std::ptr::null()
		};

		// Allocate field names array
		let field_names_ptr = if field_count > 0 {
			let names_array = self.arena.alloc(field_count * size_of::<BufferFFI>()) as *mut BufferFFI;

			unsafe {
				let names_slice = std::slice::from_raw_parts_mut(names_array, field_count);

				for (i, name) in layout.names().iter().enumerate() {
					// Copy name bytes to arena
					let name_bytes = name.as_bytes();
					let name_ptr = self.arena.copy_bytes(name_bytes);

					names_slice[i] = BufferFFI {
						ptr: name_ptr,
						len: name_bytes.len(),
						cap: name_bytes.len(),
					};
				}
			}

			names_array as *const BufferFFI
		} else {
			std::ptr::null()
		};

		// Allocate LayoutFFI
		let layout_ffi = self.arena.alloc_type::<LayoutFFI>();

		if layout_ffi.is_null() {
			return std::ptr::null();
		}

		unsafe {
			*layout_ffi = LayoutFFI {
				fields: fields_ptr,
				field_count,
				field_names: field_names_ptr,
				bitvec_size: layout_inner.bitvec_size,
				static_section_size: layout_inner.static_section_size,
				alignment: layout_inner.alignment,
			};
		}

		layout_ffi as *const LayoutFFI
	}

	/// Unmarshal a layout from FFI representation
	pub(crate) fn unmarshal_layout(layout_ffi: *const LayoutFFI) -> EncodedValuesNamedLayout {
		assert!(!layout_ffi.is_null(), "LayoutFFI must not be null");

		unsafe {
			let layout = &*layout_ffi;

			let fields_slice = std::slice::from_raw_parts(layout.fields, layout.field_count);
			let names_slice = std::slice::from_raw_parts(layout.field_names, layout.field_count);

			let fields_with_names = fields_slice.iter().zip(names_slice.iter()).map(|(field, name_buf)| {
				let name = if !name_buf.ptr.is_null() && name_buf.len > 0 {
					let bytes = std::slice::from_raw_parts(name_buf.ptr, name_buf.len);
					String::from_utf8_lossy(bytes).into_owned()
				} else {
					String::new()
				};
				let field_type = Type::from_u8(field.field_type);
				(name, field_type)
			});

			EncodedValuesNamedLayout::new(fields_with_names)
		}
	}
}
