// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use postcard::from_bytes;
use reifydb_abi::data::column::ColumnTypeCode;
use reifydb_type::value::decimal::Decimal;

use crate::operator::change::{BorrowedColumn, BorrowedColumns};

#[derive(Clone, Copy)]
pub struct ColumnView<'a> {
	inner: BorrowedColumn<'a>,
}

impl<'a> ColumnView<'a> {
	pub(crate) fn new(inner: BorrowedColumn<'a>) -> Self {
		Self {
			inner,
		}
	}

	pub fn name(&self) -> &'a str {
		self.inner.name()
	}

	pub fn type_code(&self) -> ColumnTypeCode {
		self.inner.type_code()
	}

	pub fn len(&self) -> usize {
		self.inner.row_count()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn raw(&self) -> BorrowedColumn<'a> {
		self.inner
	}

	pub fn defined_bitvec(&self) -> Option<&'a [u8]> {
		let bv = self.inner.defined_bitvec();
		if bv.is_empty() {
			None
		} else {
			Some(bv)
		}
	}

	pub fn is_defined(&self, index: usize) -> bool {
		match self.defined_bitvec() {
			None => true,
			Some(bv) => match bv.get(index / 8) {
				Some(byte) => (byte >> (index % 8)) & 1 == 1,
				None => false,
			},
		}
	}

	pub fn u8(&self) -> Option<&'a [u8]> {
		if self.type_code() != ColumnTypeCode::Uint1 {
			return None;
		}
		unsafe { self.inner.as_slice::<u8>() }
	}

	pub fn u16(&self) -> Option<&'a [u16]> {
		if self.type_code() != ColumnTypeCode::Uint2 {
			return None;
		}
		unsafe { self.inner.as_slice::<u16>() }
	}

	pub fn u32(&self) -> Option<&'a [u32]> {
		if self.type_code() != ColumnTypeCode::Uint4 {
			return None;
		}
		unsafe { self.inner.as_slice::<u32>() }
	}

	pub fn u64(&self) -> Option<&'a [u64]> {
		if self.type_code() != ColumnTypeCode::Uint8 {
			return None;
		}
		unsafe { self.inner.as_slice::<u64>() }
	}

	pub fn u128(&self) -> Option<&'a [u128]> {
		if self.type_code() != ColumnTypeCode::Uint16 {
			return None;
		}
		unsafe { self.inner.as_slice::<u128>() }
	}

	pub fn i8(&self) -> Option<&'a [i8]> {
		if self.type_code() != ColumnTypeCode::Int1 {
			return None;
		}
		unsafe { self.inner.as_slice::<i8>() }
	}

	pub fn i16(&self) -> Option<&'a [i16]> {
		if self.type_code() != ColumnTypeCode::Int2 {
			return None;
		}
		unsafe { self.inner.as_slice::<i16>() }
	}

	pub fn i32(&self) -> Option<&'a [i32]> {
		if self.type_code() != ColumnTypeCode::Int4 {
			return None;
		}
		unsafe { self.inner.as_slice::<i32>() }
	}

	pub fn i64(&self) -> Option<&'a [i64]> {
		if self.type_code() != ColumnTypeCode::Int8 {
			return None;
		}
		unsafe { self.inner.as_slice::<i64>() }
	}

	pub fn i128(&self) -> Option<&'a [i128]> {
		if self.type_code() != ColumnTypeCode::Int16 {
			return None;
		}
		unsafe { self.inner.as_slice::<i128>() }
	}

	pub fn f32(&self) -> Option<&'a [f32]> {
		if self.type_code() != ColumnTypeCode::Float4 {
			return None;
		}
		unsafe { self.inner.as_slice::<f32>() }
	}

	pub fn f64(&self) -> Option<&'a [f64]> {
		if self.type_code() != ColumnTypeCode::Float8 {
			return None;
		}
		unsafe { self.inner.as_slice::<f64>() }
	}

	pub fn bool_iter(&self) -> Option<BoolIter<'a>> {
		if self.type_code() != ColumnTypeCode::Bool {
			return None;
		}
		Some(BoolIter {
			data: self.inner.data_bytes(),
			row_count: self.inner.row_count(),
			index: 0,
		})
	}

	pub fn utf8_iter(&self) -> Option<impl Iterator<Item = &'a str> + 'a> {
		if self.type_code() != ColumnTypeCode::Utf8 {
			return None;
		}
		Some(self.inner.iter_str())
	}

	pub fn blob_iter(&self) -> Option<impl Iterator<Item = &'a [u8]> + 'a> {
		if self.type_code() != ColumnTypeCode::Blob {
			return None;
		}
		Some(self.inner.iter_bytes())
	}

	pub fn decimal_iter(&self) -> Option<impl Iterator<Item = Option<Decimal>> + 'a> {
		if self.type_code() != ColumnTypeCode::Decimal {
			return None;
		}
		let data = self.inner.data_bytes();
		let offsets = self.inner.offsets();
		let row_count = self.inner.row_count();
		Some(DecimalIter {
			data,
			offsets,
			row_count,
			index: 0,
		})
	}

	pub fn to_u64_vec(&self) -> Option<Vec<u64>> {
		match self.type_code() {
			ColumnTypeCode::Uint8 => self.u64().map(|s| s.to_vec()),
			ColumnTypeCode::Uint4 => self.u32().map(|s| s.iter().map(|v| u64::from(*v)).collect()),
			ColumnTypeCode::Uint2 => self.u16().map(|s| s.iter().map(|v| u64::from(*v)).collect()),
			ColumnTypeCode::Uint1 => self.u8().map(|s| s.iter().map(|v| u64::from(*v)).collect()),
			_ => None,
		}
	}

	pub fn to_i64_vec(&self) -> Option<Vec<i64>> {
		match self.type_code() {
			ColumnTypeCode::Int8 => self.i64().map(|s| s.to_vec()),
			ColumnTypeCode::Int4 => self.i32().map(|s| s.iter().map(|v| i64::from(*v)).collect()),
			ColumnTypeCode::Int2 => self.i16().map(|s| s.iter().map(|v| i64::from(*v)).collect()),
			ColumnTypeCode::Int1 => self.i8().map(|s| s.iter().map(|v| i64::from(*v)).collect()),
			_ => None,
		}
	}

	pub fn to_f64_vec(&self) -> Option<Vec<f64>> {
		match self.type_code() {
			ColumnTypeCode::Float8 => self.f64().map(|s| s.to_vec()),
			ColumnTypeCode::Float4 => self.f32().map(|s| s.iter().map(|v| f64::from(*v)).collect()),
			_ => None,
		}
	}
}

pub struct BoolIter<'a> {
	data: &'a [u8],
	row_count: usize,
	index: usize,
}

impl<'a> Iterator for BoolIter<'a> {
	type Item = bool;

	fn next(&mut self) -> Option<bool> {
		if self.index >= self.row_count {
			return None;
		}
		let byte = *self.data.get(self.index / 8)?;
		let bit = (byte >> (self.index % 8)) & 1 == 1;
		self.index += 1;
		Some(bit)
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let remaining = self.row_count - self.index;
		(remaining, Some(remaining))
	}
}

impl<'a> ExactSizeIterator for BoolIter<'a> {}

pub struct DecimalIter<'a> {
	data: &'a [u8],
	offsets: &'a [u64],
	row_count: usize,
	index: usize,
}

impl<'a> Iterator for DecimalIter<'a> {
	type Item = Option<Decimal>;

	fn next(&mut self) -> Option<Option<Decimal>> {
		if self.index >= self.row_count {
			return None;
		}
		let i = self.index;
		self.index += 1;
		if i + 1 >= self.offsets.len() {
			return Some(None);
		}
		let start = self.offsets[i] as usize;
		let end = self.offsets[i + 1] as usize;
		if end > self.data.len() || start > end {
			return Some(None);
		}
		Some(from_bytes::<Decimal>(&self.data[start..end]).ok())
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let remaining = self.row_count - self.index;
		(remaining, Some(remaining))
	}
}

impl<'a> ExactSizeIterator for DecimalIter<'a> {}

impl<'a> BorrowedColumns<'a> {
	pub fn column_view(&self, name: &str) -> Option<ColumnView<'a>> {
		self.column(name).map(ColumnView::new)
	}

	pub fn column_view_at(&self, index: usize) -> Option<ColumnView<'a>> {
		self.columns().nth(index).map(ColumnView::new)
	}

	pub fn column_views(&self) -> impl Iterator<Item = ColumnView<'a>> + 'a {
		self.columns().map(ColumnView::new)
	}
}
