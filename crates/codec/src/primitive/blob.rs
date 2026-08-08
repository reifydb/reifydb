// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	reifydb_assertions,
	value::{blob::Blob, value_type::ValueType},
};

use crate::row::{
	bytes::{EncodedRowBuilder, read_defined},
	shape::RowShape,
};

impl RowShape {
	pub fn set_blob(&self, row: &mut EncodedRowBuilder, index: usize, value: &Blob) {
		self.set_blob_from_slice(row, index, value.as_bytes());
	}

	pub fn set_blob_from_slice(&self, row: &mut EncodedRowBuilder, index: usize, bytes: &[u8]) {
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*self.fields()[index].constraint.get_type().inner_type(), ValueType::Blob);
		}
		self.replace_dynamic_data(row, index, bytes);
	}

	pub fn get_blob(&self, row: &[u8], index: usize) -> Blob {
		Blob::from_slice(self.get_blob_slice(row, index))
	}

	pub fn get_blob_slice<'a>(&self, row: &'a [u8], index: usize) -> &'a [u8] {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*field.constraint.get_type().inner_type(), ValueType::Blob);
		}

		let ref_slice = &row[field.offset as usize..field.offset as usize + 8];
		let offset = u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]]) as usize;
		let length = u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]]) as usize;

		let dynamic_start = self.dynamic_section_start();
		let blob_start = dynamic_start + offset;
		&row[blob_start..blob_start + length]
	}

	pub fn get_blob_slice_builder<'a>(&self, row: &'a EncodedRowBuilder, index: usize) -> &'a [u8] {
		let field = &self.fields()[index];
		let ref_slice = &row[field.offset as usize..field.offset as usize + 8];
		let offset = u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]]) as usize;
		let length = u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]]) as usize;
		let blob_start = self.dynamic_section_start() + offset;
		&row[blob_start..blob_start + length]
	}

	pub fn get_blob_slice_mut<'a>(&self, row: &'a mut [u8], index: usize) -> &'a mut [u8] {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*field.constraint.get_type().inner_type(), ValueType::Blob);
		}

		let ref_slice = &row[field.offset as usize..field.offset as usize + 8];
		let offset = u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]]) as usize;
		let length = u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]]) as usize;

		let blob_start = self.dynamic_section_start() + offset;
		&mut row[blob_start..blob_start + length]
	}

	pub fn try_get_blob(&self, row: &[u8], index: usize) -> Option<Blob> {
		if read_defined(row, index) && self.fields()[index].constraint.get_type() == ValueType::Blob {
			Some(self.get_blob(row, index))
		} else {
			None
		}
	}
}
