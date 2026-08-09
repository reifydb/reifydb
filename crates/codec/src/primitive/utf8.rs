// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str;

use reifydb_value::{reifydb_assertions, value::value_type::ValueType};

use crate::row::{bytes::RowBuilder, shape::RowShape};

impl RowShape {
	pub fn set_utf8(&self, row: &mut impl RowBuilder, index: usize, value: impl AsRef<str>) {
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*self.fields()[index].constraint.get_type().inner_type(), ValueType::Utf8);
		}
		self.replace_dynamic_data(row, index, value.as_ref().as_bytes());
	}

	pub fn get_utf8<'a>(&'a self, row: &'a [u8], index: usize) -> &'a str {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*field.constraint.get_type().inner_type(), ValueType::Utf8);
		}

		let ref_slice = &row[field.offset as usize..field.offset as usize + 8];
		let offset = u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]]) as usize;
		let length = u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]]) as usize;

		let dynamic_start = self.dynamic_section_start();
		let string_start = dynamic_start + offset;
		let string_slice = &row[string_start..string_start + length];

		// SAFETY: set_utf8 is the only writer of a Utf8 field and stores `&str` bytes verbatim, so the slice
		// delimited by this field's dynamic offset and length is valid UTF-8.
		unsafe { str::from_utf8_unchecked(string_slice) }
	}

	pub fn try_get_utf8<'a>(&'a self, row: &'a [u8], index: usize) -> Option<&'a str> {
		if self.is_defined(row, index) && self.fields()[index].constraint.get_type() == ValueType::Utf8 {
			Some(self.get_utf8(row, index))
		} else {
			None
		}
	}
}
