// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::{catalog::flow::OperatorId, flow::OperatorCapability};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		OperatorMetadata,
		change::BorrowedChange,
		column::{batch::InsertBatch, operator::OperatorColumn},
		extern_c::binding::{context::ExternCContext, operator::ExternCOperator},
	},
	row,
};
use reifydb_testing_sdk::{builders::TestChangeBuilder, harness::ExternCOperatorHarnessBuilder};
use reifydb_value::{
	config::Config,
	value::{
		date::Date, datetime::DateTime, decimal::Decimal, duration::Duration, row_number::RowNumber, time::Time,
	},
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpU8 {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpU8>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpU16 {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpU16>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpU32 {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpU32>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpU64 {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpU64>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpI8 {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpI8>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpI16 {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpI16>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpI32 {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpI32>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpI64 {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpI64>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpF32 {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpF32>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpF64 {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpF64>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpBool {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpBool>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpUtf8 {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpUtf8>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpUtf8Growth {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpUtf8Growth>::new().build().expect("harness");
	let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
	let post = out.diffs[0].post().expect("post");
	assert_eq!(post.row_count(), 20);
	let expected = "x".repeat(100);
	for i in 0..20usize {
		assert_eq!(post.row_ref(i).expect("row").utf8("s").as_deref(), Some(expected.as_str()), "row {i}");
	}
}

struct BlobRow {
	b: Vec<u8>,
}
row!(BlobRow { b: Vec<u8> });

struct OpBlob;
impl OperatorMetadata for OpBlob {
	const NAME: &'static str = "writer_blob";
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpBlob {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpBlob>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpDecimal {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpDecimal>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpWide {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpWide>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpDate {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
		let values =
			[Date::default(), Date::new(2024, 3, 15).expect("date"), Date::new(2554, 1, 1).expect("date")];
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
	let mut h = ExternCOperatorHarnessBuilder::<OpDate>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpDateTime {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpDateTime>::new().build().expect("harness");
	let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
	let post = out.diffs[0].post().expect("post");
	assert_eq!(post.row_count(), 3);
	assert_eq!(post.row_ref(0).expect("r0").datetime("v"), Some(DateTime::from_nanos(0)));
	assert_eq!(post.row_ref(1).expect("r1").datetime("v"), Some(DateTime::from_nanos(1_700_000_000_000_000_000)));
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpTime {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpTime>::new().build().expect("harness");
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl ExternCOperator for OpDuration {
	fn new(_: OperatorId, _: &Config) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut ExternCContext, _: BorrowedChange<'_>) -> Result<()> {
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
	let mut h = ExternCOperatorHarnessBuilder::<OpDuration>::new().build().expect("harness");
	let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
	let post = out.diffs[0].post().expect("post");
	assert_eq!(post.row_count(), 3);
	assert_eq!(post.row_ref(0).expect("r0").duration("v"), Some(Duration::default()));
	assert_eq!(post.row_ref(1).expect("r1").duration("v"), Duration::new(13, 5, 3_600_000_000_000).ok());
	assert_eq!(post.row_ref(2).expect("r2").duration("v"), Duration::from_seconds(-30).ok());
}
