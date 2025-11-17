//! Row marshalling between Rust and FFI types

use reifydb_core::{CowVec, Row, value::encoded::EncodedValues};
use reifydb_flow_operator_abi::*;
use reifydb_type::RowNumber;

use crate::marshal::Marshaller;

impl Marshaller {
	/// Marshal a row to FFI representation
	pub fn marshal_row(&mut self, row: &Row) -> *const RowFFI {
		// Allocate RowFFI in arena
		let row_ffi = self.arena.alloc_type::<RowFFI>();

		if row_ffi.is_null() {
			return std::ptr::null();
		}

		// Copy encoded data to arena
		let encoded_ptr = self.arena.copy_bytes(row.encoded.as_ref());
		let encoded_len = row.encoded.len();

		// Marshal layout to FFI
		let layout_ptr = self.marshal_layout(&row.layout);

		unsafe {
			*row_ffi = RowFFI {
				number: row.number.into(),
				encoded: BufferFFI {
					ptr: encoded_ptr,
					len: encoded_len,
					cap: encoded_len,
				},
				layout: layout_ptr,
			};
		}

		row_ffi as *const RowFFI
	}

	/// Unmarshal a row from FFI representation
	pub fn unmarshal_row(&self, ffi: &RowFFI) -> Row {
		// Extract encoded data
		let encoded = if !ffi.encoded.ptr.is_null() && ffi.encoded.len > 0 {
			unsafe {
				let slice = std::slice::from_raw_parts(ffi.encoded.ptr, ffi.encoded.len);
				EncodedValues(CowVec::new(slice.to_vec()))
			}
		} else {
			EncodedValues(CowVec::new(Vec::new()))
		};

		// Unmarshal layout from LayoutFFI
		let layout = Self::unmarshal_layout(ffi.layout);

		Row {
			number: RowNumber::from(ffi.number),
			encoded,
			layout,
		}
	}

	/// Marshal rows to FFI representation
	pub fn marshal_rows(&mut self, rows: &[Option<Row>]) -> RowsFFI {
		let count = rows.len();

		if count == 0 {
			return RowsFFI {
				count: 0,
				rows: std::ptr::null_mut(),
			};
		}

		// Allocate array of row pointers
		let rows_array = self.arena.alloc(count * size_of::<*const RowFFI>()) as *mut *const RowFFI;

		if rows_array.is_null() {
			return RowsFFI {
				count: 0,
				rows: std::ptr::null_mut(),
			};
		}

		unsafe {
			let rows_slice = std::slice::from_raw_parts_mut(rows_array, count);

			for (i, row_opt) in rows.iter().enumerate() {
				rows_slice[i] = match row_opt {
					Some(row) => self.marshal_row(row),
					None => std::ptr::null(),
				};
			}
		}

		RowsFFI {
			count,
			rows: rows_array,
		}
	}
}
