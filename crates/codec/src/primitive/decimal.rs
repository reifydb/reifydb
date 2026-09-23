// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	reifydb_assertions,
	value::{decimal::Decimal, value_type::ValueType},
};

use crate::{
	row::{bytes::RowBuilder, shape::RowShape},
	unscaled::{decimal_unscaled, read_le, write_le},
};

impl RowShape {
	pub fn set_decimal(&self, row: &mut impl RowBuilder, index: usize, value: &Decimal) {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
		}
		let ValueType::Decimal {
			precision,
			scale,
		} = *field.constraint.get_type().inner_type()
		else {
			panic!("set_decimal on field {:?} of type {}", field.name, field.constraint.get_type());
		};
		let unscaled = decimal_unscaled(value, precision, scale).unwrap_or_else(|| {
			panic!(
				"decimal {value} does not fit field {:?} of type {}",
				field.name,
				field.constraint.get_type()
			)
		});
		let start = field.offset as usize;
		write_le(unscaled, &mut row.as_mut_slice()[start..start + field.size as usize]);
		self.set_valid(row, index, true);
	}

	pub fn get_decimal(&self, row: &[u8], index: usize) -> Decimal {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
		}
		let ValueType::Decimal {
			scale,
			..
		} = *field.constraint.get_type().inner_type()
		else {
			panic!("get_decimal on field {:?} of type {}", field.name, field.constraint.get_type());
		};
		let start = field.offset as usize;
		let unscaled = read_le(&row[start..start + field.size as usize]);
		Decimal::from_parts(unscaled, scale.value()).expect("a stored decimal is within 76 digits")
	}

	pub fn try_get_decimal(&self, row: &[u8], index: usize) -> Option<Decimal> {
		if self.is_defined(row, index)
			&& matches!(self.fields()[index].constraint.get_type().inner_type(), ValueType::Decimal { .. })
		{
			Some(self.get_decimal(row, index))
		} else {
			None
		}
	}
}
