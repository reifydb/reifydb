// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use core::marker::PhantomData;

use reifydb_abi::data::column::ColumnTypeCode;
use reifydb_type::value::{date::Date, datetime::DateTime, duration::Duration, time::Time};

use crate::{
	error::SdkError,
	operator::builder::{ColumnBuilder, ColumnsBuilder, CommittedColumn},
};

pub struct ScalarWriter<'a, T: Copy> {
	inner: ColumnBuilder<'a>,
	cursor: usize,
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
		debug_assert!(self.cursor < self.capacity, "ScalarWriter::push past capacity");
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
		debug_assert!(self.cursor < self.capacity, "ScalarWriter::push_none past capacity");
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
	capacity: usize,
	defined: Option<Vec<bool>>,
	type_code: ColumnTypeCode,
}

impl<'a> VarLenWriter<'a> {
	fn new(inner: ColumnBuilder<'a>, capacity: usize, expected_bytes: usize) -> Result<Self, SdkError> {
		let type_code = inner.type_code();
		debug_assert!(
			matches!(type_code, ColumnTypeCode::Utf8 | ColumnTypeCode::Blob | ColumnTypeCode::Decimal),
			"VarLenWriter requires Utf8, Blob, or Decimal",
		);
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
		debug_assert!(self.item_cursor < self.capacity, "VarLenWriter::push past capacity");
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
		debug_assert_eq!(self.type_code, ColumnTypeCode::Utf8);
		self.push_bytes_internal(s.as_bytes())
	}

	pub fn push_bytes(&mut self, b: &[u8]) -> Result<(), SdkError> {
		debug_assert!(matches!(self.type_code, ColumnTypeCode::Blob | ColumnTypeCode::Decimal));
		self.push_bytes_internal(b)
	}

	pub fn push_none(&mut self) -> Result<(), SdkError> {
		debug_assert!(self.item_cursor < self.capacity, "VarLenWriter::push_none past capacity");
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

#[cfg(test)]
mod tests {
	use reifydb_abi::operator::capabilities::OperatorCapability;
	use reifydb_core::interface::catalog::flow::FlowNodeId;
	use reifydb_type::value::{
		date::Date, datetime::DateTime, decimal::Decimal, duration::Duration, row_number::RowNumber, time::Time,
	};

	use crate::{
		config::Config,
		error::Result,
		operator::{
			FFIOperator, OperatorMetadata,
			change::BorrowedChange,
			column::{batch::InsertBatch, operator::OperatorColumn},
			context::ffi::FFIOperatorContext,
		},
		row,
		testing::{builders::TestChangeBuilder, harness::FFIOperatorHarnessBuilder},
	};

	struct U8Row {
		v: u8,
	}
	row!(U8Row {
		v: u8
	});

	struct OpU8;
	impl OperatorMetadata for OpU8 {
		const NAME: &'static str = "writer_u8";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpU8 {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<U8Row, _>::new(ctx, 3)?;
			for (i, &v) in [0u8, 1, u8::MAX].iter().enumerate() {
				batch.push(
					RowNumber(i as u64 + 1),
					&U8Row {
						v,
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn scalar_u8_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpU8>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").u8("v"), Some(0));
		assert_eq!(post.row_ref(1).expect("r1").u8("v"), Some(1));
		assert_eq!(post.row_ref(2).expect("r2").u8("v"), Some(u8::MAX));
	}

	struct U16Row {
		v: u16,
	}
	row!(U16Row {
		v: u16
	});

	struct OpU16;
	impl OperatorMetadata for OpU16 {
		const NAME: &'static str = "writer_u16";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpU16 {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<U16Row, _>::new(ctx, 3)?;
			for (i, &v) in [0u16, 1, u16::MAX].iter().enumerate() {
				batch.push(
					RowNumber(i as u64 + 1),
					&U16Row {
						v,
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn scalar_u16_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpU16>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").u16("v"), Some(0));
		assert_eq!(post.row_ref(1).expect("r1").u16("v"), Some(1));
		assert_eq!(post.row_ref(2).expect("r2").u16("v"), Some(u16::MAX));
	}

	struct U32Row {
		v: u32,
	}
	row!(U32Row {
		v: u32
	});

	struct OpU32;
	impl OperatorMetadata for OpU32 {
		const NAME: &'static str = "writer_u32";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpU32 {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<U32Row, _>::new(ctx, 3)?;
			for (i, &v) in [0u32, 1, u32::MAX].iter().enumerate() {
				batch.push(
					RowNumber(i as u64 + 1),
					&U32Row {
						v,
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn scalar_u32_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpU32>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").u32("v"), Some(0));
		assert_eq!(post.row_ref(1).expect("r1").u32("v"), Some(1));
		assert_eq!(post.row_ref(2).expect("r2").u32("v"), Some(u32::MAX));
	}

	struct U64Row {
		v: u64,
	}
	row!(U64Row {
		v: u64
	});

	struct OpU64;
	impl OperatorMetadata for OpU64 {
		const NAME: &'static str = "writer_u64";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpU64 {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<U64Row, _>::new(ctx, 3)?;
			for (i, &v) in [0u64, 1, u64::MAX].iter().enumerate() {
				batch.push(
					RowNumber(i as u64 + 1),
					&U64Row {
						v,
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn scalar_u64_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpU64>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").u64("v"), Some(0));
		assert_eq!(post.row_ref(1).expect("r1").u64("v"), Some(1));
		assert_eq!(post.row_ref(2).expect("r2").u64("v"), Some(u64::MAX));
	}

	struct I8Row {
		v: i8,
	}
	row!(I8Row {
		v: i8
	});

	struct OpI8;
	impl OperatorMetadata for OpI8 {
		const NAME: &'static str = "writer_i8";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpI8 {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<I8Row, _>::new(ctx, 3)?;
			for (i, &v) in [i8::MIN, 0_i8, i8::MAX].iter().enumerate() {
				batch.push(
					RowNumber(i as u64 + 1),
					&I8Row {
						v,
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn scalar_i8_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpI8>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").i8("v"), Some(i8::MIN));
		assert_eq!(post.row_ref(1).expect("r1").i8("v"), Some(0));
		assert_eq!(post.row_ref(2).expect("r2").i8("v"), Some(i8::MAX));
	}

	struct I16Row {
		v: i16,
	}
	row!(I16Row {
		v: i16
	});

	struct OpI16;
	impl OperatorMetadata for OpI16 {
		const NAME: &'static str = "writer_i16";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpI16 {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<I16Row, _>::new(ctx, 3)?;
			for (i, &v) in [i16::MIN, 0_i16, i16::MAX].iter().enumerate() {
				batch.push(
					RowNumber(i as u64 + 1),
					&I16Row {
						v,
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn scalar_i16_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpI16>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").i16("v"), Some(i16::MIN));
		assert_eq!(post.row_ref(1).expect("r1").i16("v"), Some(0));
		assert_eq!(post.row_ref(2).expect("r2").i16("v"), Some(i16::MAX));
	}

	struct I32Row {
		v: i32,
	}
	row!(I32Row {
		v: i32
	});

	struct OpI32;
	impl OperatorMetadata for OpI32 {
		const NAME: &'static str = "writer_i32";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpI32 {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<I32Row, _>::new(ctx, 3)?;
			for (i, &v) in [i32::MIN, 0_i32, i32::MAX].iter().enumerate() {
				batch.push(
					RowNumber(i as u64 + 1),
					&I32Row {
						v,
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn scalar_i32_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpI32>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").i32("v"), Some(i32::MIN));
		assert_eq!(post.row_ref(1).expect("r1").i32("v"), Some(0));
		assert_eq!(post.row_ref(2).expect("r2").i32("v"), Some(i32::MAX));
	}

	struct I64Row {
		v: i64,
	}
	row!(I64Row {
		v: i64
	});

	struct OpI64;
	impl OperatorMetadata for OpI64 {
		const NAME: &'static str = "writer_i64";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpI64 {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<I64Row, _>::new(ctx, 3)?;
			for (i, &v) in [i64::MIN, 0_i64, i64::MAX].iter().enumerate() {
				batch.push(
					RowNumber(i as u64 + 1),
					&I64Row {
						v,
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn scalar_i64_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpI64>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").i64("v"), Some(i64::MIN));
		assert_eq!(post.row_ref(1).expect("r1").i64("v"), Some(0));
		assert_eq!(post.row_ref(2).expect("r2").i64("v"), Some(i64::MAX));
	}

	struct F32Row {
		v: f32,
	}
	row!(F32Row {
		v: f32
	});

	struct OpF32;
	impl OperatorMetadata for OpF32 {
		const NAME: &'static str = "writer_f32";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpF32 {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<F32Row, _>::new(ctx, 3)?;
			for (i, &v) in [0.0_f32, -1.5_f32, f32::MAX].iter().enumerate() {
				batch.push(
					RowNumber(i as u64 + 1),
					&F32Row {
						v,
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn scalar_f32_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpF32>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").f32("v"), Some(0.0_f32));
		assert_eq!(post.row_ref(1).expect("r1").f32("v"), Some(-1.5_f32));
		assert_eq!(post.row_ref(2).expect("r2").f32("v"), Some(f32::MAX));
	}

	struct F64Row {
		v: f64,
	}
	row!(F64Row {
		v: f64
	});

	struct OpF64;
	impl OperatorMetadata for OpF64 {
		const NAME: &'static str = "writer_f64";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpF64 {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<F64Row, _>::new(ctx, 3)?;
			for (i, &v) in [0.0_f64, -1.5_f64, f64::MAX].iter().enumerate() {
				batch.push(
					RowNumber(i as u64 + 1),
					&F64Row {
						v,
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn scalar_f64_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpF64>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").f64("v"), Some(0.0_f64));
		assert_eq!(post.row_ref(1).expect("r1").f64("v"), Some(-1.5_f64));
		assert_eq!(post.row_ref(2).expect("r2").f64("v"), Some(f64::MAX));
	}

	struct BoolRow {
		v: bool,
	}
	row!(BoolRow {
		v: bool
	});

	struct OpBool;
	impl OperatorMetadata for OpBool {
		const NAME: &'static str = "writer_bool";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpBool {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<BoolRow, _>::new(ctx, 3)?;
			for (i, &v) in [true, false, true].iter().enumerate() {
				batch.push(
					RowNumber(i as u64 + 1),
					&BoolRow {
						v,
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn bool_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpBool>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").bool("v"), Some(true));
		assert_eq!(post.row_ref(1).expect("r1").bool("v"), Some(false));
		assert_eq!(post.row_ref(2).expect("r2").bool("v"), Some(true));
	}

	struct Utf8Row {
		s: String,
	}
	row!(Utf8Row {
		s: String
	});

	struct OpUtf8;
	impl OperatorMetadata for OpUtf8 {
		const NAME: &'static str = "writer_utf8";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpUtf8 {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let values = ["", "hello", "こんにちは"];
			let mut batch = InsertBatch::<Utf8Row, _>::new(ctx, values.len())?;
			for (i, &s) in values.iter().enumerate() {
				batch.push(
					RowNumber(i as u64 + 1),
					&Utf8Row {
						s: s.to_string(),
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn utf8_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpUtf8>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").utf8("s").as_deref(), Some(""));
		assert_eq!(post.row_ref(1).expect("r1").utf8("s").as_deref(), Some("hello"));
		assert_eq!(post.row_ref(2).expect("r2").utf8("s").as_deref(), Some("こんにちは"));
	}

	struct OpUtf8Growth;
	impl OperatorMetadata for OpUtf8Growth {
		const NAME: &'static str = "writer_utf8_growth";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpUtf8Growth {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			// AVG_BYTES for String is 24; 20 rows * 24 = 480 bytes pre-allocated.
			// Each string is 100 bytes so total 2000 bytes forces VarLenWriter::ensure_capacity.
			let mut batch = InsertBatch::<Utf8Row, _>::new(ctx, 20)?;
			for i in 0..20u64 {
				batch.push(
					RowNumber(i + 1),
					&Utf8Row {
						s: "x".repeat(100),
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn utf8_capacity_growth() {
		let mut h = FFIOperatorHarnessBuilder::<OpUtf8Growth>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 20);
		let expected = "x".repeat(100);
		for i in 0..20usize {
			assert_eq!(
				post.row_ref(i).expect("row").utf8("s").as_deref(),
				Some(expected.as_str()),
				"row {i}"
			);
		}
	}

	struct BlobRow {
		b: Vec<u8>,
	}
	row!(BlobRow { b: Vec<u8> });

	struct OpBlob;
	impl OperatorMetadata for OpBlob {
		const NAME: &'static str = "writer_blob";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpBlob {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let rows = [
				BlobRow {
					b: vec![],
				},
				BlobRow {
					b: vec![0u8, 1, 127, 255],
				},
				BlobRow {
					b: vec![42u8; 1000],
				},
			];
			let mut batch = InsertBatch::<BlobRow, _>::new(ctx, rows.len())?;
			for (i, row) in rows.iter().enumerate() {
				batch.push(RowNumber(i as u64 + 1), row)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn blob_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpBlob>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").blob("b"), Some(vec![]));
		assert_eq!(post.row_ref(1).expect("r1").blob("b"), Some(vec![0u8, 1, 127, 255]));
		assert_eq!(post.row_ref(2).expect("r2").blob("b"), Some(vec![42u8; 1000]));
	}

	struct DecimalRow {
		d: Decimal,
	}
	row!(DecimalRow {
		d: Decimal
	});

	struct OpDecimal;
	impl OperatorMetadata for OpDecimal {
		const NAME: &'static str = "writer_decimal";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpDecimal {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<DecimalRow, _>::new(ctx, 3)?;
			batch.push(
				RowNumber(1),
				&DecimalRow {
					d: Decimal::zero(),
				},
			)?;
			batch.push(
				RowNumber(2),
				&DecimalRow {
					d: Decimal::from_i64(1234),
				},
			)?;
			batch.push(
				RowNumber(3),
				&DecimalRow {
					d: Decimal::from_i64(-5678),
				},
			)?;
			batch.finish()
		}
	}

	#[test]
	fn decimal_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpDecimal>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").decimal("d"), Some(Decimal::zero()));
		assert_eq!(post.row_ref(1).expect("r1").decimal("d"), Some(Decimal::from_i64(1234)));
		assert_eq!(post.row_ref(2).expect("r2").decimal("d"), Some(Decimal::from_i64(-5678)));
	}

	struct WideRow {
		a: u128,
		b: i128,
	}
	row!(WideRow {
		a: u128,
		b: i128
	});

	struct OpWide;
	impl OperatorMetadata for OpWide {
		const NAME: &'static str = "writer_wide";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpWide {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<WideRow, _>::new(ctx, 1)?;
			batch.push(
				RowNumber(1),
				&WideRow {
					a: u128::MAX,
					b: i128::MIN,
				},
			)?;
			batch.finish()
		}
	}

	#[test]
	fn wide_integers_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpWide>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 1);
		assert_eq!(post.row_ref(0).expect("r0").u128("a"), Some(u128::MAX));
		assert_eq!(post.row_ref(0).expect("r0").i128("b"), Some(i128::MIN));
	}

	struct DateRow {
		v: Date,
	}
	row!(DateRow {
		v: Date
	});

	struct OpDate;
	impl OperatorMetadata for OpDate {
		const NAME: &'static str = "writer_date";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpDate {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let values = [
				Date::default(),
				Date::new(2024, 3, 15).expect("date"),
				Date::new(2554, 1, 1).expect("date"),
			];
			let mut batch = InsertBatch::<DateRow, _>::new(ctx, values.len())?;
			for (i, &v) in values.iter().enumerate() {
				batch.push(
					RowNumber(i as u64 + 1),
					&DateRow {
						v,
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn scalar_date_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpDate>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").date("v"), Some(Date::default()));
		assert_eq!(post.row_ref(1).expect("r1").date("v"), Date::new(2024, 3, 15));
		assert_eq!(post.row_ref(2).expect("r2").date("v"), Date::new(2554, 1, 1));
	}

	struct DateTimeRow {
		v: DateTime,
	}
	row!(DateTimeRow {
		v: DateTime
	});

	struct OpDateTime;
	impl OperatorMetadata for OpDateTime {
		const NAME: &'static str = "writer_datetime";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpDateTime {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let values = [
				DateTime::from_nanos(0),
				DateTime::from_nanos(1_700_000_000_000_000_000),
				DateTime::from_nanos(u64::MAX),
			];
			let mut batch = InsertBatch::<DateTimeRow, _>::new(ctx, values.len())?;
			for (i, &v) in values.iter().enumerate() {
				batch.push(
					RowNumber(i as u64 + 1),
					&DateTimeRow {
						v,
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn scalar_datetime_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpDateTime>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").datetime("v"), Some(DateTime::from_nanos(0)));
		assert_eq!(
			post.row_ref(1).expect("r1").datetime("v"),
			Some(DateTime::from_nanos(1_700_000_000_000_000_000))
		);
		assert_eq!(post.row_ref(2).expect("r2").datetime("v"), Some(DateTime::from_nanos(u64::MAX)));
	}

	struct TimeRow {
		v: Time,
	}
	row!(TimeRow {
		v: Time
	});

	struct OpTime;
	impl OperatorMetadata for OpTime {
		const NAME: &'static str = "writer_time";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpTime {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let values = [
				Time::default(),
				Time::new(14, 30, 45, 123_456_789).expect("time"),
				Time::new(23, 59, 59, 999_999_999).expect("time"),
			];
			let mut batch = InsertBatch::<TimeRow, _>::new(ctx, values.len())?;
			for (i, &v) in values.iter().enumerate() {
				batch.push(
					RowNumber(i as u64 + 1),
					&TimeRow {
						v,
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn scalar_time_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpTime>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").time("v"), Some(Time::default()));
		assert_eq!(post.row_ref(1).expect("r1").time("v"), Time::new(14, 30, 45, 123_456_789));
		assert_eq!(post.row_ref(2).expect("r2").time("v"), Time::new(23, 59, 59, 999_999_999));
	}

	struct DurationRow {
		v: Duration,
	}
	row!(DurationRow {
		v: Duration
	});

	struct OpDuration;
	impl OperatorMetadata for OpDuration {
		const NAME: &'static str = "writer_duration";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}
	impl FFIOperator for OpDuration {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let values = [
				Duration::default(),
				Duration::new(13, 5, 3_600_000_000_000).expect("duration"),
				Duration::from_seconds(-30).expect("duration"),
			];
			let mut batch = InsertBatch::<DurationRow, _>::new(ctx, values.len())?;
			for (i, &v) in values.iter().enumerate() {
				batch.push(
					RowNumber(i as u64 + 1),
					&DurationRow {
						v,
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn scalar_duration_roundtrip() {
		let mut h = FFIOperatorHarnessBuilder::<OpDuration>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		assert_eq!(post.row_ref(0).expect("r0").duration("v"), Some(Duration::default()));
		assert_eq!(post.row_ref(1).expect("r1").duration("v"), Duration::new(13, 5, 3_600_000_000_000).ok());
		assert_eq!(post.row_ref(2).expect("r2").duration("v"), Duration::from_seconds(-30).ok());
	}
}
