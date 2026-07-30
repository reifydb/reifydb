// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::marker::PhantomData;

use reifydb_abi::data::column::ColumnTypeCode;
use reifydb_value::{
	reifydb_assertions,
	value::{date::Date, datetime::DateTime, duration::Duration, time::Time},
};

use crate::{
	error::SdkError,
	operator::builder::{ColumnBuilder, ColumnsBuilder, CommittedColumn},
};

pub struct ScalarWriter<'a, T: Copy> {
	inner: ColumnBuilder<'a>,
	cursor: usize,
	#[cfg_attr(not(reifydb_assertions), allow(dead_code))]
	capacity: usize,
	defined: Option<Vec<bool>>,
	_t: PhantomData<T>,
}

impl<'a, T: Copy> ScalarWriter<'a, T> {
	fn new(inner: ColumnBuilder<'a>, capacity: usize) -> Self {
		Self {
			inner,
			cursor: 0,
			capacity,
			defined: None,
			_t: PhantomData,
		}
	}

	#[inline]
	pub fn push(&mut self, v: T) {
		reifydb_assertions! {
			assert!(self.cursor < self.capacity, "ScalarWriter::push past capacity");
		}
		unsafe {
			let data = self.inner.data_ptr() as *mut T;
			core::ptr::write_unaligned(data.add(self.cursor), v);
		}
		if let Some(d) = self.defined.as_mut() {
			d.push(true);
		}
		self.cursor += 1;
	}

	#[inline]
	pub fn push_none(&mut self)
	where
		T: Default,
	{
		reifydb_assertions! {
			assert!(self.cursor < self.capacity, "ScalarWriter::push_none past capacity");
		}
		unsafe {
			let data = self.inner.data_ptr() as *mut T;
			core::ptr::write_unaligned(data.add(self.cursor), T::default());
		}
		let d = self.defined.get_or_insert_with(|| vec![true; self.cursor]);
		d.push(false);
		self.cursor += 1;
	}

	#[inline]
	pub fn len(&self) -> usize {
		self.cursor
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.cursor == 0
	}

	pub fn finish(self) -> Result<CommittedColumn, SdkError> {
		if let Some(d) = &self.defined {
			self.inner.set_defined(d);
		}
		self.inner.commit(self.cursor)
	}
}

pub struct BoolWriter<'a> {
	inner: ColumnBuilder<'a>,
	values: Vec<bool>,
	defined: Option<Vec<bool>>,
}

impl<'a> BoolWriter<'a> {
	fn new(inner: ColumnBuilder<'a>, capacity: usize) -> Self {
		Self {
			inner,
			values: Vec::with_capacity(capacity),
			defined: None,
		}
	}

	#[inline]
	pub fn push(&mut self, v: bool) {
		self.values.push(v);
		if let Some(d) = self.defined.as_mut() {
			d.push(true);
		}
	}

	#[inline]
	pub fn push_none(&mut self) {
		self.values.push(false);
		let d = self.defined.get_or_insert_with(|| vec![true; self.values.len() - 1]);
		d.push(false);
	}

	#[inline]
	pub fn len(&self) -> usize {
		self.values.len()
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.values.is_empty()
	}

	pub fn finish(self) -> Result<CommittedColumn, SdkError> {
		if let Some(d) = &self.defined {
			self.inner.set_defined(d);
		}
		self.inner.write_bool(&self.values)
	}
}

pub struct VarLenWriter<'a> {
	inner: ColumnBuilder<'a>,
	item_cursor: usize,
	byte_cursor: usize,
	data_capacity: usize,
	#[cfg_attr(not(reifydb_assertions), allow(dead_code))]
	capacity: usize,
	defined: Option<Vec<bool>>,
	#[cfg_attr(not(reifydb_assertions), allow(dead_code))]
	type_code: ColumnTypeCode,
}

impl<'a> VarLenWriter<'a> {
	fn new(inner: ColumnBuilder<'a>, capacity: usize, expected_bytes: usize) -> Result<Self, SdkError> {
		let type_code = inner.type_code();
		reifydb_assertions! {
			assert!(
				matches!(
					type_code,
					ColumnTypeCode::Utf8 | ColumnTypeCode::Blob | ColumnTypeCode::Decimal
				),
				"VarLenWriter requires Utf8, Blob, or Decimal",
			);
		}
		let initial = expected_bytes.max(capacity);
		if initial > 0 {
			inner.grow(initial)?;
		}
		unsafe {
			core::ptr::write(inner.offsets_ptr(), 0u64);
		}
		Ok(Self {
			inner,
			item_cursor: 0,
			byte_cursor: 0,
			data_capacity: initial,
			capacity,
			defined: None,
			type_code,
		})
	}

	#[inline]
	fn ensure_capacity(&mut self, need: usize) -> Result<(), SdkError> {
		if self.byte_cursor + need <= self.data_capacity {
			return Ok(());
		}
		let extra = (self.byte_cursor + need - self.data_capacity).max(self.data_capacity.max(64));
		self.inner.grow(extra)?;
		self.data_capacity += extra;
		Ok(())
	}

	#[inline]
	fn push_bytes_internal(&mut self, bytes: &[u8]) -> Result<(), SdkError> {
		reifydb_assertions! {
			assert!(self.item_cursor < self.capacity, "VarLenWriter::push past capacity");
		}
		self.ensure_capacity(bytes.len())?;
		unsafe {
			let data = self.inner.data_ptr();
			let offsets = self.inner.offsets_ptr();
			if !bytes.is_empty() {
				core::ptr::copy_nonoverlapping(bytes.as_ptr(), data.add(self.byte_cursor), bytes.len());
			}
			self.byte_cursor += bytes.len();
			core::ptr::write(offsets.add(self.item_cursor + 1), self.byte_cursor as u64);
		}
		if let Some(d) = self.defined.as_mut() {
			d.push(true);
		}
		self.item_cursor += 1;
		Ok(())
	}

	pub fn push_str(&mut self, s: &str) -> Result<(), SdkError> {
		reifydb_assertions! {
			assert_eq!(self.type_code, ColumnTypeCode::Utf8);
		}
		self.push_bytes_internal(s.as_bytes())
	}

	pub fn push_bytes(&mut self, b: &[u8]) -> Result<(), SdkError> {
		reifydb_assertions! {
			assert!(matches!(self.type_code, ColumnTypeCode::Blob | ColumnTypeCode::Decimal));
		}
		self.push_bytes_internal(b)
	}

	pub fn push_none(&mut self) -> Result<(), SdkError> {
		reifydb_assertions! {
			assert!(self.item_cursor < self.capacity, "VarLenWriter::push_none past capacity");
		}
		unsafe {
			let offsets = self.inner.offsets_ptr();
			core::ptr::write(offsets.add(self.item_cursor + 1), self.byte_cursor as u64);
		}
		let d = self.defined.get_or_insert_with(|| vec![true; self.item_cursor]);
		d.push(false);
		self.item_cursor += 1;
		Ok(())
	}

	#[inline]
	pub fn len(&self) -> usize {
		self.item_cursor
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.item_cursor == 0
	}

	pub fn finish(self) -> Result<CommittedColumn, SdkError> {
		if let Some(d) = &self.defined {
			self.inner.set_defined(d);
		}
		self.inner.commit(self.item_cursor)
	}
}

pub type U8Writer<'a> = ScalarWriter<'a, u8>;
pub type U16Writer<'a> = ScalarWriter<'a, u16>;
pub type U32Writer<'a> = ScalarWriter<'a, u32>;
pub type U64Writer<'a> = ScalarWriter<'a, u64>;
pub type U128Writer<'a> = ScalarWriter<'a, u128>;
pub type I8Writer<'a> = ScalarWriter<'a, i8>;
pub type I16Writer<'a> = ScalarWriter<'a, i16>;
pub type I32Writer<'a> = ScalarWriter<'a, i32>;
pub type I64Writer<'a> = ScalarWriter<'a, i64>;
pub type I128Writer<'a> = ScalarWriter<'a, i128>;
pub type F32Writer<'a> = ScalarWriter<'a, f32>;
pub type F64Writer<'a> = ScalarWriter<'a, f64>;
pub type DateWriter<'a> = ScalarWriter<'a, Date>;
pub type DateTimeWriter<'a> = ScalarWriter<'a, DateTime>;
pub type TimeWriter<'a> = ScalarWriter<'a, Time>;
pub type DurationWriter<'a> = ScalarWriter<'a, Duration>;
pub type Utf8Writer<'a> = VarLenWriter<'a>;
pub type BlobWriter<'a> = VarLenWriter<'a>;
pub type DecimalWriter<'a> = VarLenWriter<'a>;

impl<'a> ColumnsBuilder<'a> {
	pub fn u8_writer(&mut self, capacity: usize) -> Result<U8Writer<'_>, SdkError> {
		Ok(ScalarWriter::new(self.acquire(ColumnTypeCode::Uint1, capacity)?, capacity))
	}
	pub fn u16_writer(&mut self, capacity: usize) -> Result<U16Writer<'_>, SdkError> {
		Ok(ScalarWriter::new(self.acquire(ColumnTypeCode::Uint2, capacity)?, capacity))
	}
	pub fn u32_writer(&mut self, capacity: usize) -> Result<U32Writer<'_>, SdkError> {
		Ok(ScalarWriter::new(self.acquire(ColumnTypeCode::Uint4, capacity)?, capacity))
	}
	pub fn u64_writer(&mut self, capacity: usize) -> Result<U64Writer<'_>, SdkError> {
		Ok(ScalarWriter::new(self.acquire(ColumnTypeCode::Uint8, capacity)?, capacity))
	}
	pub fn u128_writer(&mut self, capacity: usize) -> Result<U128Writer<'_>, SdkError> {
		Ok(ScalarWriter::new(self.acquire(ColumnTypeCode::Uint16, capacity)?, capacity))
	}
	pub fn i8_writer(&mut self, capacity: usize) -> Result<I8Writer<'_>, SdkError> {
		Ok(ScalarWriter::new(self.acquire(ColumnTypeCode::Int1, capacity)?, capacity))
	}
	pub fn i16_writer(&mut self, capacity: usize) -> Result<I16Writer<'_>, SdkError> {
		Ok(ScalarWriter::new(self.acquire(ColumnTypeCode::Int2, capacity)?, capacity))
	}
	pub fn i32_writer(&mut self, capacity: usize) -> Result<I32Writer<'_>, SdkError> {
		Ok(ScalarWriter::new(self.acquire(ColumnTypeCode::Int4, capacity)?, capacity))
	}
	pub fn i64_writer(&mut self, capacity: usize) -> Result<I64Writer<'_>, SdkError> {
		Ok(ScalarWriter::new(self.acquire(ColumnTypeCode::Int8, capacity)?, capacity))
	}
	pub fn i128_writer(&mut self, capacity: usize) -> Result<I128Writer<'_>, SdkError> {
		Ok(ScalarWriter::new(self.acquire(ColumnTypeCode::Int16, capacity)?, capacity))
	}
	pub fn f32_writer(&mut self, capacity: usize) -> Result<F32Writer<'_>, SdkError> {
		Ok(ScalarWriter::new(self.acquire(ColumnTypeCode::Float4, capacity)?, capacity))
	}
	pub fn f64_writer(&mut self, capacity: usize) -> Result<F64Writer<'_>, SdkError> {
		Ok(ScalarWriter::new(self.acquire(ColumnTypeCode::Float8, capacity)?, capacity))
	}
	pub fn date_writer(&mut self, capacity: usize) -> Result<DateWriter<'_>, SdkError> {
		Ok(ScalarWriter::new(self.acquire(ColumnTypeCode::Date, capacity)?, capacity))
	}
	pub fn datetime_writer(&mut self, capacity: usize) -> Result<DateTimeWriter<'_>, SdkError> {
		Ok(ScalarWriter::new(self.acquire(ColumnTypeCode::DateTime, capacity)?, capacity))
	}
	pub fn time_writer(&mut self, capacity: usize) -> Result<TimeWriter<'_>, SdkError> {
		Ok(ScalarWriter::new(self.acquire(ColumnTypeCode::Time, capacity)?, capacity))
	}
	pub fn duration_writer(&mut self, capacity: usize) -> Result<DurationWriter<'_>, SdkError> {
		Ok(ScalarWriter::new(self.acquire(ColumnTypeCode::Duration, capacity)?, capacity))
	}
	pub fn bool_writer(&mut self, capacity: usize) -> Result<BoolWriter<'_>, SdkError> {
		Ok(BoolWriter::new(self.acquire(ColumnTypeCode::Bool, capacity)?, capacity))
	}
	pub fn utf8_writer(&mut self, capacity: usize, expected_bytes: usize) -> Result<Utf8Writer<'_>, SdkError> {
		VarLenWriter::new(self.acquire(ColumnTypeCode::Utf8, capacity)?, capacity, expected_bytes)
	}
	pub fn blob_writer(&mut self, capacity: usize, expected_bytes: usize) -> Result<BlobWriter<'_>, SdkError> {
		VarLenWriter::new(self.acquire(ColumnTypeCode::Blob, capacity)?, capacity, expected_bytes)
	}
	pub fn decimal_writer(
		&mut self,
		capacity: usize,
		expected_bytes: usize,
	) -> Result<DecimalWriter<'_>, SdkError> {
		VarLenWriter::new(self.acquire(ColumnTypeCode::Decimal, capacity)?, capacity, expected_bytes)
	}
}
