// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(reifydb_assertions)]
use reifydb_value::value::value_type::ValueType;
use reifydb_value::{reifydb_assertions, value::Value};

use crate::{
	row::{bytes::EncodedRowBuilder, shape::RowShape},
	value::{decode_value, encode_value},
};

impl RowShape {
	pub fn set_any(&self, row: &mut EncodedRowBuilder, index: usize, value: &Value) {
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*self.fields()[index].constraint.get_type().inner_type(), ValueType::Any);
		}
		let encoded = encode_value(value).expect("unsupported value in any row field");
		self.replace_dynamic_data(row, index, &encoded);
	}

	pub fn get_any(&self, row: &[u8], index: usize) -> Value {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*field.constraint.get_type().inner_type(), ValueType::Any);
		}

		let ref_slice = &row[field.offset as usize..field.offset as usize + 8];
		let offset = u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]]) as usize;
		let length = u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]]) as usize;

		let dynamic_start = self.dynamic_section_start();
		let data_start = dynamic_start + offset;
		let data_slice = &row[data_start..data_start + length];

		decode_value(data_slice).expect("corrupt any row field bytes")
	}
}
