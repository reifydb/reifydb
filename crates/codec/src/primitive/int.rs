// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	reifydb_assertions,
	value::{int::Int, value_type::ValueType},
};

use crate::{
	row::{bytes::RowBuilder, shape::RowShape},
	unscaled::{int_unscaled, read_le, write_le},
};

impl RowShape {
	pub fn set_int(&self, row: &mut impl RowBuilder, index: usize, value: &Int) {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
		}
		let ValueType::Int {
			precision,
		} = *field.constraint.get_type().inner_type()
		else {
			panic!("set_int on field {:?} of type {}", field.name, field.constraint.get_type());
		};
		let unscaled = int_unscaled(value, precision).unwrap_or_else(|| {
			panic!(
				"int {value} does not fit field {:?} of type {}",
				field.name,
				field.constraint.get_type()
			)
		});
		let start = field.offset as usize;
		write_le(unscaled, &mut row.as_mut_slice()[start..start + field.size as usize]);
		self.set_valid(row, index, true);
	}

	pub fn get_int(&self, row: &[u8], index: usize) -> Int {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert!(matches!(field.constraint.get_type().inner_type(), ValueType::Int { .. }));
		}
		let start = field.offset as usize;
		let unscaled = read_le(&row[start..start + field.size as usize]);
		Int::from_i256(unscaled).expect("a stored int is within 76 digits")
	}

	pub fn try_get_int(&self, row: &[u8], index: usize) -> Option<Int> {
		if self.is_defined(row, index)
			&& matches!(self.fields()[index].constraint.get_type(), ValueType::Int { .. })
		{
			Some(self.get_int(row, index))
		} else {
			None
		}
	}
}
