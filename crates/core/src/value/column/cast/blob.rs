// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_value::{
	Result,
	error::TypeError,
	fragment::{Fragment, LazyFragment},
	value::{
		blob::Blob,
		column_view::{ColumnView, ViewData},
		value_type::ValueType,
	},
};

use crate::value::column::builder::ColumnBuilder;

pub fn to_blob(data: &ColumnView, lazy_fragment: impl LazyFragment) -> Result<(FieldRef, ArrayRef)> {
	match &data.data {
		ViewData::Utf8 {
			container,
			..
		} => {
			let mut out = ColumnBuilder::with_capacity(ValueType::Blob, container.len());
			for idx in 0..container.len() {
				if container.is_valid(idx) {
					let temp_fragment = Fragment::internal(container.value(idx));
					out.push(Blob::from_utf8(temp_fragment));
				} else {
					out.push_none()
				}
			}
			Ok(out.finish(data.field.name()))
		}
		_ => {
			let from = data.get_type();
			Err(TypeError::UnsupportedCast {
				from,
				to: ValueType::Blob,
				fragment: lazy_fragment.fragment(),
			}
			.into())
		}
	}
}

#[cfg(test)]
pub mod tests {
	use arrow_buffer::BooleanBuffer;
	use reifydb_value::{fragment::Fragment, value::container::varlen_array::get};

	use super::*;
	use crate::value::column::factory::{int4_with_bitvec, utf8_with_bitvec};

	#[test]
	fn test_from_utf8() {
		let strings = vec!["Hello".to_string(), "World".to_string()];
		let bitvec = BooleanBuffer::new_set(2);
		let container = utf8_with_bitvec("x", strings, bitvec);

		let result = to_blob(&ColumnView::try_from(&container).unwrap(), Fragment::testing_empty).unwrap();

		match ColumnView::try_from(&result).unwrap().data {
			ViewData::Blob {
				container,
				..
			} => {
				assert_eq!(get(container, 0), Some(b"Hello".as_slice()));
				assert_eq!(get(container, 1), Some(b"World".as_slice()));
			}
			_ => panic!("Expected BLOB column data"),
		}
	}

	#[test]
	fn test_unsupported() {
		let ints = vec![42i32];
		let bitvec = BooleanBuffer::new_set(1);
		let container = int4_with_bitvec("x", ints, bitvec);

		let result = to_blob(&ColumnView::try_from(&container).unwrap(), Fragment::testing_empty);
		assert!(result.is_err());
	}
}
