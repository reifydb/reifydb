// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	reifydb_assertions,
	value::{uint::Uint, value_type::ValueType},
};

use crate::{
	row::{bytes::RowBuilder, shape::RowShape},
	unscaled::{read_le, uint_unscaled, write_le},
};

impl RowShape {
	pub fn set_uint(&self, row: &mut impl RowBuilder, index: usize, value: &Uint) {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
		}
		let ValueType::Uint {
			precision,
		} = *field.constraint.get_type().inner_type()
		else {
			panic!("set_uint on field {:?} of type {}", field.name, field.constraint.get_type());
		};
		let unscaled = uint_unscaled(value, precision).unwrap_or_else(|| {
			panic!(
				"uint {value} does not fit field {:?} of type {}",
				field.name,
				field.constraint.get_type()
			)
		});
		let start = field.offset as usize;
		write_le(unscaled, &mut row.as_mut_slice()[start..start + field.size as usize]);
		self.set_valid(row, index, true);
	}

	pub fn get_uint(&self, row: &[u8], index: usize) -> Uint {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert!(matches!(field.constraint.get_type().inner_type(), ValueType::Uint { .. }));
		}
		let start = field.offset as usize;
		let unscaled = read_le(&row[start..start + field.size as usize]);
		Uint::from_i256(unscaled).expect("a stored uint is non-negative and within 76 digits")
	}

	pub fn try_get_uint(&self, row: &[u8], index: usize) -> Option<Uint> {
		if self.is_defined(row, index)
			&& matches!(self.fields()[index].constraint.get_type(), ValueType::Uint { .. })
		{
			Some(self.get_uint(row, index))
		} else {
			None
		}
	}
}
