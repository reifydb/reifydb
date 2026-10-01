// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{WindowRequirements, WindowSizeDomain},
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	operator_with::ApplyWith,
};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		MountedOperator, OperatorMetadata,
		column::{
			batch::{InsertBatch, RemoveBatch, UpdateBatch},
			operator::OperatorColumn,
		},
		context::{GuestContext, Windowed},
		view::ChangeView,
	},
	row,
};
use reifydb_testing_sdk::{builders::TestChangeBuilder, in_process::harness::InProcessOperatorHarnessBuilder};
use reifydb_value::{
	config::ExtensionParams,
	value::{Value, blob::Blob, diff_type::DiffType, row_number::RowNumber, value_type::ValueType},
};

use crate::{read, read_value};

const NO_WINDOW: WindowRequirements = WindowRequirements {
	takes_window: false,
	kinds: &[],
	domain: WindowSizeDomain::Time,
	needs_pane: false,
	throttles: false,
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
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl MountedOperator for EmitOpInsert {
	type Class = Windowed;
	const WINDOW: WindowRequirements = NO_WINDOW;
	const UNMANAGED_BECAUSE: Option<&'static str> = None;
	fn create(_: OperatorId, _: &ExtensionParams, _: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut impl GuestContext<Windowed>, _: impl ChangeView) -> Result<()> {
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
	let mut h = InProcessOperatorHarnessBuilder::<EmitOpInsert>::new().build().expect("harness");
	let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	let diff = &out.diffs[0];
	assert_eq!(diff.kind(), DiffType::Insert);
	let post = diff.post().expect("post");
	assert_eq!(post.num_rows(), 3);
	let r0 = (post, 0);
	assert_eq!(read::<String>(r0, "mint").as_deref(), Some("SOL"));
	assert_eq!(read::<u64>(r0, "timestamp"), Some(100));
	assert_eq!(read::<f64>(r0, "price"), Some(1.5));
	assert_eq!(read::<bool>(r0, "is_open"), Some(true));
	assert_eq!(read::<u32>(r0, "count"), Some(10));
	let r1 = (post, 1);
	assert_eq!(read::<String>(r1, "mint").as_deref(), Some("BTC"));
	assert_eq!(read::<u64>(r1, "timestamp"), Some(200));
	assert_eq!(read::<f64>(r1, "price"), Some(50000.0));
	assert_eq!(read::<bool>(r1, "is_open"), Some(false));
	assert_eq!(read::<u32>(r1, "count"), Some(20));
	let r2 = (post, 2);
	assert_eq!(read::<String>(r2, "mint").as_deref(), Some("ETH"));
	assert_eq!(read::<u64>(r2, "timestamp"), Some(300));
	assert_eq!(read::<f64>(r2, "price"), Some(3000.0));
	assert_eq!(read::<bool>(r2, "is_open"), Some(true));
	assert_eq!(read::<u32>(r2, "count"), Some(30));
}

struct EmitOpEmpty;
impl OperatorMetadata for EmitOpEmpty {
	const NAME: &'static str = "batch_op_empty";
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl MountedOperator for EmitOpEmpty {
	type Class = Windowed;
	const WINDOW: WindowRequirements = NO_WINDOW;
	const UNMANAGED_BECAUSE: Option<&'static str> = None;
	fn create(_: OperatorId, _: &ExtensionParams, _: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut impl GuestContext<Windowed>, _: impl ChangeView) -> Result<()> {
		InsertBatch::<Bar, _>::new(ctx, 0)?.finish()
	}
}

#[test]
fn empty_batch_emits_no_diff() {
	let mut h = InProcessOperatorHarnessBuilder::<EmitOpEmpty>::new().build().expect("harness");
	let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
	assert_eq!(out.diffs.len(), 0);
}

struct EmitOpUpdate;
impl OperatorMetadata for EmitOpUpdate {
	const NAME: &'static str = "batch_op_update";
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl MountedOperator for EmitOpUpdate {
	type Class = Windowed;
	const WINDOW: WindowRequirements = NO_WINDOW;
	const UNMANAGED_BECAUSE: Option<&'static str> = None;
	fn create(_: OperatorId, _: &ExtensionParams, _: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut impl GuestContext<Windowed>, _: impl ChangeView) -> Result<()> {
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
	let mut h = InProcessOperatorHarnessBuilder::<EmitOpUpdate>::new().build().expect("harness");
	let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	let diff = &out.diffs[0];
	assert_eq!(diff.kind(), DiffType::Update);
	let pre = diff.pre().expect("pre");
	let post = diff.post().expect("post");
	let r_pre = (pre, 0);
	let r_post = (post, 0);
	assert_eq!(read::<String>(r_pre, "mint").as_deref(), Some("PRE"));
	assert_eq!(read::<u64>(r_pre, "timestamp"), Some(10));
	assert_eq!(read::<f64>(r_pre, "price"), Some(1.0));
	assert_eq!(read::<bool>(r_pre, "is_open"), Some(false));
	assert_eq!(read::<u32>(r_pre, "count"), Some(5));
	assert_eq!(read::<String>(r_post, "mint").as_deref(), Some("POST"));
	assert_eq!(read::<u64>(r_post, "timestamp"), Some(20));
	assert_eq!(read::<f64>(r_post, "price"), Some(2.0));
	assert_eq!(read::<bool>(r_post, "is_open"), Some(true));
	assert_eq!(read::<u32>(r_post, "count"), Some(6));
}

struct EmitOpRemove;
impl OperatorMetadata for EmitOpRemove {
	const NAME: &'static str = "batch_op_remove";
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl MountedOperator for EmitOpRemove {
	type Class = Windowed;
	const WINDOW: WindowRequirements = NO_WINDOW;
	const UNMANAGED_BECAUSE: Option<&'static str> = None;
	fn create(_: OperatorId, _: &ExtensionParams, _: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut impl GuestContext<Windowed>, _: impl ChangeView) -> Result<()> {
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
	let mut h = InProcessOperatorHarnessBuilder::<EmitOpRemove>::new().build().expect("harness");
	let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	let diff = &out.diffs[0];
	assert_eq!(diff.kind(), DiffType::Remove);
	assert_eq!(diff.pre().expect("pre").num_rows(), 2);
}

struct EmitOpBig;
impl OperatorMetadata for EmitOpBig {
	const NAME: &'static str = "batch_op_big";
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl MountedOperator for EmitOpBig {
	type Class = Windowed;
	const WINDOW: WindowRequirements = NO_WINDOW;
	const UNMANAGED_BECAUSE: Option<&'static str> = None;
	fn create(_: OperatorId, _: &ExtensionParams, _: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut impl GuestContext<Windowed>, _: impl ChangeView) -> Result<()> {
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
	let mut h = InProcessOperatorHarnessBuilder::<EmitOpBig>::new().build().expect("harness");
	let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
	let post = out.diffs[0].post().expect("post");
	assert_eq!(post.num_rows(), 100);
	for i in 0..100usize {
		let r = (post, i);
		assert_eq!(read::<String>(r, "mint").as_deref(), Some(format!("MINT{i}").as_str()));
		assert_eq!(read::<u64>(r, "timestamp"), Some((i as u64) * 10));
		assert_eq!(read::<f64>(r, "price"), Some(i as f64 * 1.5));
		assert_eq!(read::<bool>(r, "is_open"), Some(i % 2 == 0));
		assert_eq!(read::<u32>(r, "count"), Some(i as u32));
	}
}

struct OptU64Row {
	v: Option<u64>,
}
row!(OptU64Row { v: Option<u64> });

struct EmitOpOptU64;
impl OperatorMetadata for EmitOpOptU64 {
	const NAME: &'static str = "batch_op_opt_u64";
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl MountedOperator for EmitOpOptU64 {
	type Class = Windowed;
	const WINDOW: WindowRequirements = NO_WINDOW;
	const UNMANAGED_BECAUSE: Option<&'static str> = None;
	fn create(_: OperatorId, _: &ExtensionParams, _: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut impl GuestContext<Windowed>, _: impl ChangeView) -> Result<()> {
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
	let mut h = InProcessOperatorHarnessBuilder::<EmitOpOptU64>::new().build().expect("harness");
	let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
	let post = out.diffs[0].post().expect("post");
	assert_eq!(post.num_rows(), 4);
	let r0 = (post, 0);
	let r1 = (post, 1);
	let r2 = (post, 2);
	let r3 = (post, 3);
	assert!(matches!(read_value(r0, "v"), Some(Value::None { .. })));
	assert_eq!(read::<u64>(r0, "v"), None);
	assert!(!matches!(read_value(r1, "v"), Some(Value::None { .. })));
	assert_eq!(read::<u64>(r1, "v"), Some(42));
	assert!(matches!(read_value(r2, "v"), Some(Value::None { .. })));
	assert_eq!(read::<u64>(r2, "v"), None);
	assert!(!matches!(read_value(r3, "v"), Some(Value::None { .. })));
	assert_eq!(read::<u64>(r3, "v"), Some(u64::MAX));
}

struct OptStrRow {
	s: Option<String>,
}
row!(OptStrRow { s: Option<String> });

struct EmitOpOptStr;
impl OperatorMetadata for EmitOpOptStr {
	const NAME: &'static str = "batch_op_opt_str";
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl MountedOperator for EmitOpOptStr {
	type Class = Windowed;
	const WINDOW: WindowRequirements = NO_WINDOW;
	const UNMANAGED_BECAUSE: Option<&'static str> = None;
	fn create(_: OperatorId, _: &ExtensionParams, _: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut impl GuestContext<Windowed>, _: impl ChangeView) -> Result<()> {
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
	let mut h = InProcessOperatorHarnessBuilder::<EmitOpOptStr>::new().build().expect("harness");
	let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
	let post = out.diffs[0].post().expect("post");
	assert_eq!(post.num_rows(), 4);
	let r0 = (post, 0);
	let r1 = (post, 1);
	let r2 = (post, 2);
	let r3 = (post, 3);
	assert!(matches!(read_value(r0, "s"), Some(Value::None { .. })));
	assert_eq!(read::<String>(r0, "s"), None);
	assert!(!matches!(read_value(r1, "s"), Some(Value::None { .. })));
	assert_eq!(read::<String>(r1, "s").as_deref(), Some("hi"));
	assert!(matches!(read_value(r2, "s"), Some(Value::None { .. })));
	assert_eq!(read::<String>(r2, "s"), None);
	assert!(!matches!(read_value(r3, "s"), Some(Value::None { .. })));
	assert_eq!(read::<String>(r3, "s").as_deref(), Some(""));
}

struct OptBlobRow {
	b: Option<Vec<u8>>,
}
row!(OptBlobRow { b: Option<Vec<u8>> });

struct EmitOpOptBlob;
impl OperatorMetadata for EmitOpOptBlob {
	const NAME: &'static str = "batch_op_opt_blob";
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}
impl MountedOperator for EmitOpOptBlob {
	type Class = Windowed;
	const WINDOW: WindowRequirements = NO_WINDOW;
	const UNMANAGED_BECAUSE: Option<&'static str> = None;
	fn create(_: OperatorId, _: &ExtensionParams, _: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}
	fn apply(&mut self, ctx: &mut impl GuestContext<Windowed>, _: impl ChangeView) -> Result<()> {
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
	let mut h = InProcessOperatorHarnessBuilder::<EmitOpOptBlob>::new().build().expect("harness");
	let out = h.apply(TestChangeBuilder::new().build()).expect("apply");
	let post = out.diffs[0].post().expect("post");
	assert_eq!(post.num_rows(), 3);
	let r0 = (post, 0);
	let r1 = (post, 1);
	let r2 = (post, 2);
	assert!(matches!(read_value(r0, "b"), Some(Value::None { .. })));
	assert_eq!(
		read_value(r0, "b"),
		Some(Value::None {
			inner: ValueType::Blob
		})
	);
	assert!(!matches!(read_value(r1, "b"), Some(Value::None { .. })));
	assert_eq!(read_value(r1, "b"), Some(Value::Blob(Blob::new(vec![1u8, 2, 3]))));
	assert!(matches!(read_value(r2, "b"), Some(Value::None { .. })));
	assert_eq!(
		read_value(r2, "b"),
		Some(Value::None {
			inner: ValueType::Blob
		})
	);
}
