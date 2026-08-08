// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{encoding::RowField, reifydb_assertions};

use crate::row::{
	bytes::{EncodedRowBuilder, read_defined},
	shape::RowShape,
};

impl RowShape {
	#[inline]
	pub fn set<T: RowField>(&self, row: &mut EncodedRowBuilder, index: usize, value: T) {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*field.constraint.get_type().inner_type(), T::VALUE_TYPE);
		}
		let offset = field.offset as usize;
		row.set_valid(index, true);
		value.write_le(&mut row.as_mut_slice()[offset..offset + T::ENCODED_SIZE]);
	}

	#[inline]
	pub fn get<T: RowField>(&self, row: &[u8], index: usize) -> T {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*field.constraint.get_type().inner_type(), T::VALUE_TYPE);
		}
		let offset = field.offset as usize;
		T::read_le(&row[offset..offset + T::ENCODED_SIZE])
	}

	#[inline]
	pub fn try_get<T: RowField>(&self, row: &[u8], index: usize) -> Option<T> {
		if read_defined(row, index) && self.fields()[index].constraint.get_type() == T::VALUE_TYPE {
			Some(self.get(row, index))
		} else {
			None
		}
	}
}
