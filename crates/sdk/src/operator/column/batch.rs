// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::marker::PhantomData;

use reifydb_type::value::row_number::RowNumber;

use crate::{
	error::SdkError,
	operator::{
		column::row::Row,
		context::{OperatorContext, RowEmit, UpdateEmit},
	},
};

pub struct InsertBatch<'a, R: Row, O: OperatorContext + 'a> {
	emit: O::InsertEmit<'a>,
	row_numbers: Vec<RowNumber>,
	_row: PhantomData<R>,
}

impl<'a, R: Row, O: OperatorContext + 'a> InsertBatch<'a, R, O> {
	pub fn new(ctx: &'a mut O, row_capacity: usize) -> Result<Self, SdkError> {
		Ok(Self {
			emit: ctx.insert_emit::<R>(row_capacity)?,
			row_numbers: Vec::with_capacity(row_capacity),
			_row: PhantomData,
		})
	}

	pub fn push(&mut self, row_number: RowNumber, row: &R) -> Result<(), SdkError> {
		row.encode_into(self.emit.sink())?;
		self.row_numbers.push(row_number);
		Ok(())
	}

	#[inline]
	pub fn len(&self) -> usize {
		self.row_numbers.len()
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.row_numbers.is_empty()
	}

	pub fn finish(self) -> Result<(), SdkError> {
		if self.row_numbers.is_empty() {
			return Ok(());
		}
		self.emit.finish(&self.row_numbers)
	}
}

pub struct UpdateBatch<'a, R: Row, O: OperatorContext + 'a> {
	emit: O::UpdateEmit<'a>,
	row_numbers: Vec<RowNumber>,
	_row: PhantomData<R>,
}

impl<'a, R: Row, O: OperatorContext + 'a> UpdateBatch<'a, R, O> {
	pub fn new(ctx: &'a mut O, row_capacity: usize) -> Result<Self, SdkError> {
		Ok(Self {
			emit: ctx.update_emit::<R>(row_capacity)?,
			row_numbers: Vec::with_capacity(row_capacity),
			_row: PhantomData,
		})
	}

	pub fn push(&mut self, row_number: RowNumber, pre_row: &R, post_row: &R) -> Result<(), SdkError> {
		pre_row.encode_into(self.emit.pre())?;
		post_row.encode_into(self.emit.post())?;
		self.row_numbers.push(row_number);
		Ok(())
	}

	#[inline]
	pub fn len(&self) -> usize {
		self.row_numbers.len()
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.row_numbers.is_empty()
	}

	pub fn finish(self) -> Result<(), SdkError> {
		if self.row_numbers.is_empty() {
			return Ok(());
		}
		self.emit.finish(&self.row_numbers)
	}
}

pub struct RemoveBatch<'a, R: Row, O: OperatorContext + 'a> {
	emit: O::RemoveEmit<'a>,
	row_numbers: Vec<RowNumber>,
	_row: PhantomData<R>,
}

impl<'a, R: Row, O: OperatorContext + 'a> RemoveBatch<'a, R, O> {
	pub fn new(ctx: &'a mut O, row_capacity: usize) -> Result<Self, SdkError> {
		Ok(Self {
			emit: ctx.remove_emit::<R>(row_capacity)?,
			row_numbers: Vec::with_capacity(row_capacity),
			_row: PhantomData,
		})
	}

	pub fn push(&mut self, row_number: RowNumber, row: &R) -> Result<(), SdkError> {
		row.encode_into(self.emit.sink())?;
		self.row_numbers.push(row_number);
		Ok(())
	}

	#[inline]
	pub fn len(&self) -> usize {
		self.row_numbers.len()
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.row_numbers.is_empty()
	}

	pub fn finish(self) -> Result<(), SdkError> {
		if self.row_numbers.is_empty() {
			return Ok(());
		}
		self.emit.finish(&self.row_numbers)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_abi::{flow::diff::DiffType, operator::capabilities::CAPABILITY_ALL_STANDARD};
	use reifydb_core::interface::catalog::flow::FlowNodeId;
	use reifydb_type::value::row_number::RowNumber;

	use crate::{
		config::Config,
		error::Result,
		operator::{
			FFIOperator, OperatorMetadata,
			change::BorrowedChange,
			column::{
				batch::{InsertBatch, RemoveBatch, UpdateBatch},
				operator::OperatorColumn,
			},
			context::ffi::FFIOperatorContext,
		},
		row,
		testing::{builders::TestChangeBuilder, harness::FFIOperatorHarnessBuilder},
	};

	struct Bar {
		mint: String,
		timestamp: u64,
		price: f64,
		is_open: bool,
		count: u32,
	}

	row!(Bar {
		mint: String,
		timestamp: u64,
		price: f64,
		is_open: bool,
		count: u32
	});

	struct EmitOpInsert;
	impl OperatorMetadata for EmitOpInsert {
		const NAME: &'static str = "batch_op_insert";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;
	}
	impl FFIOperator for EmitOpInsert {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<Bar, _>::new(ctx, 3)?;
			batch.push(
				RowNumber(1),
				&Bar {
					mint: "SOL".to_string(),
					timestamp: 100,
					price: 1.5,
					is_open: true,
					count: 10,
				},
			)?;
			batch.push(
				RowNumber(2),
				&Bar {
					mint: "BTC".to_string(),
					timestamp: 200,
					price: 50000.0,
					is_open: false,
					count: 20,
				},
			)?;
			batch.push(
				RowNumber(3),
				&Bar {
					mint: "ETH".to_string(),
					timestamp: 300,
					price: 3000.0,
					is_open: true,
					count: 30,
				},
			)?;
			batch.finish()
		}
	}

	#[test]
	fn insert_batch_emits_typed_columns_in_one_diff() {
		let mut h = FFIOperatorHarnessBuilder::<EmitOpInsert>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Insert);
		let post = diff.post().expect("post");
		assert_eq!(post.row_count(), 3);
		let r0 = post.row_ref(0).expect("r0");
		assert_eq!(r0.utf8("mint").as_deref(), Some("SOL"));
		assert_eq!(r0.u64("timestamp"), Some(100));
		assert_eq!(r0.f64("price"), Some(1.5));
		assert_eq!(r0.bool("is_open"), Some(true));
		assert_eq!(r0.u32("count"), Some(10));
		let r1 = post.row_ref(1).expect("r1");
		assert_eq!(r1.utf8("mint").as_deref(), Some("BTC"));
		assert_eq!(r1.u64("timestamp"), Some(200));
		assert_eq!(r1.f64("price"), Some(50000.0));
		assert_eq!(r1.bool("is_open"), Some(false));
		assert_eq!(r1.u32("count"), Some(20));
		let r2 = post.row_ref(2).expect("r2");
		assert_eq!(r2.utf8("mint").as_deref(), Some("ETH"));
		assert_eq!(r2.u64("timestamp"), Some(300));
		assert_eq!(r2.f64("price"), Some(3000.0));
		assert_eq!(r2.bool("is_open"), Some(true));
		assert_eq!(r2.u32("count"), Some(30));
	}

	struct EmitOpEmpty;
	impl OperatorMetadata for EmitOpEmpty {
		const NAME: &'static str = "batch_op_empty";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;
	}
	impl FFIOperator for EmitOpEmpty {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			InsertBatch::<Bar, _>::new(ctx, 0)?.finish()
		}
	}

	#[test]
	fn empty_batch_emits_no_diff() {
		let mut h = FFIOperatorHarnessBuilder::<EmitOpEmpty>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		assert_eq!(out.diffs.len(), 0);
	}

	struct EmitOpUpdate;
	impl OperatorMetadata for EmitOpUpdate {
		const NAME: &'static str = "batch_op_update";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;
	}
	impl FFIOperator for EmitOpUpdate {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = UpdateBatch::<Bar, _>::new(ctx, 1)?;
			batch.push(
				RowNumber(1),
				&Bar {
					mint: "PRE".to_string(),
					timestamp: 10,
					price: 1.0,
					is_open: false,
					count: 5,
				},
				&Bar {
					mint: "POST".to_string(),
					timestamp: 20,
					price: 2.0,
					is_open: true,
					count: 6,
				},
			)?;
			batch.finish()
		}
	}

	#[test]
	fn update_batch_roundtrips_all_fields() {
		let mut h = FFIOperatorHarnessBuilder::<EmitOpUpdate>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Update);
		let pre = diff.pre().expect("pre");
		let post = diff.post().expect("post");
		let r_pre = pre.row_ref(0).expect("r_pre");
		let r_post = post.row_ref(0).expect("r_post");
		assert_eq!(r_pre.utf8("mint").as_deref(), Some("PRE"));
		assert_eq!(r_pre.u64("timestamp"), Some(10));
		assert_eq!(r_pre.f64("price"), Some(1.0));
		assert_eq!(r_pre.bool("is_open"), Some(false));
		assert_eq!(r_pre.u32("count"), Some(5));
		assert_eq!(r_post.utf8("mint").as_deref(), Some("POST"));
		assert_eq!(r_post.u64("timestamp"), Some(20));
		assert_eq!(r_post.f64("price"), Some(2.0));
		assert_eq!(r_post.bool("is_open"), Some(true));
		assert_eq!(r_post.u32("count"), Some(6));
	}

	struct EmitOpRemove;
	impl OperatorMetadata for EmitOpRemove {
		const NAME: &'static str = "batch_op_remove";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;
	}
	impl FFIOperator for EmitOpRemove {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = RemoveBatch::<Bar, _>::new(ctx, 2)?;
			batch.push(
				RowNumber(1),
				&Bar {
					mint: "X".to_string(),
					timestamp: 0,
					price: 0.0,
					is_open: false,
					count: 0,
				},
			)?;
			batch.push(
				RowNumber(2),
				&Bar {
					mint: "Y".to_string(),
					timestamp: 0,
					price: 0.0,
					is_open: false,
					count: 0,
				},
			)?;
			batch.finish()
		}
	}

	#[test]
	fn remove_batch_emits_one_diff_with_n_rows() {
		let mut h = FFIOperatorHarnessBuilder::<EmitOpRemove>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Remove);
		assert_eq!(diff.pre().expect("pre").row_count(), 2);
	}

	struct EmitOpBig;
	impl OperatorMetadata for EmitOpBig {
		const NAME: &'static str = "batch_op_big";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;
	}
	impl FFIOperator for EmitOpBig {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<Bar, _>::new(ctx, 100)?;
			for i in 0..100u64 {
				batch.push(
					RowNumber(i + 1),
					&Bar {
						mint: format!("MINT{}", i),
						timestamp: i * 10,
						price: i as f64 * 1.5,
						is_open: i % 2 == 0,
						count: i as u32,
					},
				)?;
			}
			batch.finish()
		}
	}

	#[test]
	fn round_trip_100_rows_decodes_correctly() {
		let mut h = FFIOperatorHarnessBuilder::<EmitOpBig>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 100);
		for i in 0..100usize {
			let r = post.row_ref(i).expect("r");
			assert_eq!(r.utf8("mint").as_deref(), Some(format!("MINT{i}").as_str()));
			assert_eq!(r.u64("timestamp"), Some((i as u64) * 10));
			assert_eq!(r.f64("price"), Some(i as f64 * 1.5));
			assert_eq!(r.bool("is_open"), Some(i % 2 == 0));
			assert_eq!(r.u32("count"), Some(i as u32));
		}
	}

	struct OptU64Row {
		v: Option<u64>,
	}
	row!(OptU64Row { v: Option<u64> });

	struct EmitOpOptU64;
	impl OperatorMetadata for EmitOpOptU64 {
		const NAME: &'static str = "batch_op_opt_u64";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;
	}
	impl FFIOperator for EmitOpOptU64 {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<OptU64Row, _>::new(ctx, 4)?;
			batch.push(
				RowNumber(1),
				&OptU64Row {
					v: None,
				},
			)?;
			batch.push(
				RowNumber(2),
				&OptU64Row {
					v: Some(42),
				},
			)?;
			batch.push(
				RowNumber(3),
				&OptU64Row {
					v: None,
				},
			)?;
			batch.push(
				RowNumber(4),
				&OptU64Row {
					v: Some(u64::MAX),
				},
			)?;
			batch.finish()
		}
	}

	#[test]
	fn optional_scalar_some_and_none() {
		let mut h = FFIOperatorHarnessBuilder::<EmitOpOptU64>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 4);
		let r0 = post.row_ref(0).expect("r0");
		let r1 = post.row_ref(1).expect("r1");
		let r2 = post.row_ref(2).expect("r2");
		let r3 = post.row_ref(3).expect("r3");
		assert!(!r0.is_defined("v"));
		assert_eq!(r0.u64("v"), None);
		assert!(r1.is_defined("v"));
		assert_eq!(r1.u64("v"), Some(42));
		assert!(!r2.is_defined("v"));
		assert_eq!(r2.u64("v"), None);
		assert!(r3.is_defined("v"));
		assert_eq!(r3.u64("v"), Some(u64::MAX));
	}

	struct OptStrRow {
		s: Option<String>,
	}
	row!(OptStrRow { s: Option<String> });

	struct EmitOpOptStr;
	impl OperatorMetadata for EmitOpOptStr {
		const NAME: &'static str = "batch_op_opt_str";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;
	}
	impl FFIOperator for EmitOpOptStr {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<OptStrRow, _>::new(ctx, 4)?;
			batch.push(
				RowNumber(1),
				&OptStrRow {
					s: None,
				},
			)?;
			batch.push(
				RowNumber(2),
				&OptStrRow {
					s: Some("hi".to_string()),
				},
			)?;
			batch.push(
				RowNumber(3),
				&OptStrRow {
					s: None,
				},
			)?;
			batch.push(
				RowNumber(4),
				&OptStrRow {
					s: Some("".to_string()),
				},
			)?;
			batch.finish()
		}
	}

	#[test]
	fn optional_string_some_and_none() {
		let mut h = FFIOperatorHarnessBuilder::<EmitOpOptStr>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 4);
		let r0 = post.row_ref(0).expect("r0");
		let r1 = post.row_ref(1).expect("r1");
		let r2 = post.row_ref(2).expect("r2");
		let r3 = post.row_ref(3).expect("r3");
		assert!(!r0.is_defined("s"));
		assert_eq!(r0.utf8("s"), None);
		assert!(r1.is_defined("s"));
		assert_eq!(r1.utf8("s").as_deref(), Some("hi"));
		assert!(!r2.is_defined("s"));
		assert_eq!(r2.utf8("s"), None);
		assert!(r3.is_defined("s"));
		assert_eq!(r3.utf8("s").as_deref(), Some(""));
	}

	struct OptBlobRow {
		b: Option<Vec<u8>>,
	}
	row!(OptBlobRow { b: Option<Vec<u8>> });

	struct EmitOpOptBlob;
	impl OperatorMetadata for EmitOpOptBlob {
		const NAME: &'static str = "batch_op_opt_blob";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;
	}
	impl FFIOperator for EmitOpOptBlob {
		fn new(_: FlowNodeId, _: &Config) -> Result<Self> {
			Ok(Self)
		}
		fn apply(&mut self, ctx: &mut FFIOperatorContext, _: BorrowedChange<'_>) -> Result<()> {
			let mut batch = InsertBatch::<OptBlobRow, _>::new(ctx, 3)?;
			batch.push(
				RowNumber(1),
				&OptBlobRow {
					b: None,
				},
			)?;
			batch.push(
				RowNumber(2),
				&OptBlobRow {
					b: Some(vec![1u8, 2, 3]),
				},
			)?;
			batch.push(
				RowNumber(3),
				&OptBlobRow {
					b: None,
				},
			)?;
			batch.finish()
		}
	}

	#[test]
	fn optional_blob_some_and_none() {
		let mut h = FFIOperatorHarnessBuilder::<EmitOpOptBlob>::new().build().expect("harness");
		let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		let r0 = post.row_ref(0).expect("r0");
		let r1 = post.row_ref(1).expect("r1");
		let r2 = post.row_ref(2).expect("r2");
		assert!(!r0.is_defined("b"));
		assert_eq!(r0.blob("b"), None);
		assert!(r1.is_defined("b"));
		assert_eq!(r1.blob("b"), Some(vec![1u8, 2, 3]));
		assert!(!r2.is_defined("b"));
		assert_eq!(r2.blob("b"), None);
	}
}
