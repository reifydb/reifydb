// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::RefCell, fmt::Debug};

use reifydb_codec::tag::ValueKind;
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff, Diffs},
		flow::OperatorCapability,
	},
	operator_with::ApplyWith,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_sdk::{
	common::extern_c::wire::{
		buffer::ExternCBuffer,
		columns::{ExternCColumn, ExternCColumnData, ExternCColumns},
	},
	error::{Result, SdkError},
	flow::operator::{
		OperatorMetadata,
		change::{BorrowedChange, BorrowedColumns},
		column::operator::OperatorColumn,
		extern_c::binding::{context::ExternCContext, operator::ExternCOperator},
		view::{ColumnsView, RowView, in_process::InProcessColumnsView},
	},
};
use reifydb_testing_sdk::harness::ExternCOperatorHarnessBuilder;
use reifydb_value::{
	config::ExtensionParams,
	fragment::Fragment,
	value::{
		blob::Blob,
		constraint::{precision::Precision, scale::Scale},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		duration::Duration,
		int::Int,
		row_number::RowNumber,
		system_columns::SystemColumns,
		time::Time,
		uint::Uint,
	},
};

thread_local! {
	static EXTERN_C_READS: RefCell<Vec<(String, Vec<String>)>> = const { RefCell::new(Vec::new()) };
}

fn outcome<T: Debug>(read: std::result::Result<Option<T>, SdkError>) -> String {
	match read {
		Ok(value) => format!("{value:?}"),
		Err(SdkError::ColumnRead {
			reason,
			..
		}) => format!("error: {reason}"),
		Err(other) => panic!("a valid column must only fail with a column read error, got {other}"),
	}
}

fn read_all(row: &impl RowView, name: &str) -> Vec<String> {
	vec![
		outcome(row.utf8(name)),
		outcome(row.blob(name)),
		outcome(row.bool(name)),
		outcome(row.u8(name)),
		outcome(row.u16(name)),
		outcome(row.u32(name)),
		outcome(row.u64(name)),
		outcome(row.u128(name)),
		outcome(row.i8(name)),
		outcome(row.i16(name)),
		outcome(row.i32(name)),
		outcome(row.i64(name)),
		outcome(row.i128(name)),
		outcome(row.f32(name)),
		outcome(row.f64(name)),
		outcome(row.int(name)),
		outcome(row.uint(name)),
		outcome(row.decimal(name)),
		outcome(row.date(name)),
		outcome(row.datetime(name)),
		outcome(row.time(name)),
		outcome(row.duration(name)),
	]
}

pub struct ReadEveryWayOperator;

impl OperatorMetadata for ReadEveryWayOperator {
	const NAME: &'static str = "extern_c_read_every_way";
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "reads every column through every typed row reader";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl ExternCOperator for ReadEveryWayOperator {
	fn new(_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn apply(&mut self, _ctx: &mut ExternCContext, input: BorrowedChange<'_>) -> Result<()> {
		for diff in input.diffs() {
			let post = diff.post();
			let row = post.row(0).expect("one row");
			for col in post.columns() {
				let reads = read_all(&row, col.name());
				EXTERN_C_READS.with(|all| all.borrow_mut().push((col.name().to_string(), reads)));
			}
		}
		Ok(())
	}
}

fn one_row_of_every_type() -> Columns {
	let columns = vec![
		("int1", ColumnBuffer::int1([i8::MIN])),
		("int2", ColumnBuffer::int2([i16::MIN])),
		("int4", ColumnBuffer::int4([i32::MIN])),
		("int8", ColumnBuffer::int8([i64::MIN])),
		("int16", ColumnBuffer::int16([i128::MIN])),
		("uint1", ColumnBuffer::uint1([u8::MAX])),
		("uint2", ColumnBuffer::uint2([u16::MAX])),
		("uint4", ColumnBuffer::uint4([u32::MAX])),
		("uint8", ColumnBuffer::uint8([u64::MAX])),
		("uint16", ColumnBuffer::uint16([u128::MAX])),
		("float4", ColumnBuffer::float4([1.5])),
		("float8", ColumnBuffer::float8([1.5])),
		("float8_nan", ColumnBuffer::float8([f64::NAN])),
		("utf8", ColumnBuffer::utf8(["a"])),
		("blob", ColumnBuffer::blob([Blob::new(vec![1, 2])])),
		("bool", ColumnBuffer::bool([true])),
		("int", ColumnBuffer::int(Precision::MAX, [Int::MIN])),
		("uint", ColumnBuffer::uint(Precision::MAX, [Uint::MAX])),
		(
			"decimal",
			ColumnBuffer::decimal(Precision::new(38), Scale::new(2), [Decimal::parse("-1.25").unwrap()]),
		),
		("date", ColumnBuffer::date([Date::from_ymd(2026, 9, 24).unwrap()])),
		("datetime", ColumnBuffer::datetime([DateTime::from_nanos(1)])),
		("time", ColumnBuffer::time([Time::from_hms(1, 2, 3).unwrap()])),
		("duration", ColumnBuffer::duration([Duration::new(1, 2, 3).unwrap()])),
	];
	let now = DateTime::default();
	Columns::with_system(
		columns.into_iter().map(|(name, data)| ColumnWithName::new(Fragment::internal(name), data)).collect(),
		SystemColumns::new(vec![RowNumber(1)], Vec::new(), vec![now], vec![now], vec![now], Vec::new()),
	)
}

fn read_through_extern_c(columns: Columns) -> Vec<(String, Vec<String>)> {
	let mut diffs = Diffs::new();
	diffs.push(Diff::insert(columns));
	let change =
		Change::from_flow(OperatorId(1), ChangeVersion::from(CommitVersion(1)), diffs, DateTime::default());
	let mut harness = ExternCOperatorHarnessBuilder::<ReadEveryWayOperator>::new()
		.with_node_id(OperatorId(1))
		.build()
		.expect("build harness");
	EXTERN_C_READS.with(|all| all.borrow_mut().clear());
	harness.apply(change).expect("apply");
	EXTERN_C_READS.with(|all| all.take())
}

#[test]
fn both_row_views_read_every_column_type_the_same_way() {
	// A guest must see the same values and the same errors whether it runs in process or behind the extern-c ABI.
	let columns = one_row_of_every_type();
	let in_process: Vec<(String, Vec<String>)> = {
		let view = InProcessColumnsView::new(&columns);
		let row = view.row(0).unwrap();
		columns.iter().map(|col| (col.name().text().to_string(), read_all(&row, col.name().text()))).collect()
	};
	let extern_c = read_through_extern_c(columns.clone());
	assert_eq!(extern_c.len(), in_process.len(), "the extern-c operator must read every column");
	for ((name, local), (_, remote)) in in_process.iter().zip(&extern_c) {
		assert_eq!(local, remote, "column {name} reads differently in process and through extern-c");
	}
}

#[test]
fn the_shared_read_table_widens_refuses_and_rejects_nan() {
	// Guards the parity test from passing on two views that are wrong the same way.
	let columns = one_row_of_every_type();
	let view = InProcessColumnsView::new(&columns);
	let row = view.row(0).unwrap();
	assert_eq!(row.i128("int1").unwrap(), Some(i8::MIN as i128));
	assert_eq!(row.u128("uint1").unwrap(), Some(u8::MAX as u128));
	assert_eq!(row.f64("float4").unwrap(), Some(1.5));
	assert_eq!(outcome(row.i32("int8")), "error: wrong type");
	assert_eq!(outcome(row.u64("int4")), "error: wrong type");
	assert_eq!(outcome(row.f64("int")), "error: wrong type");
	assert_eq!(outcome(row.decimal("float8_nan")), "error: does not fit");
	assert_eq!(row.decimal("float8").unwrap(), Some(Decimal::parse("1.5").unwrap()));
}

#[test]
fn a_family_cell_past_76_digits_is_an_error_not_none() {
	// A corrupt cell used to decode to nothing, so the guest silently skipped a row it should have failed on.
	let mut cell = [0xFFu8; 32];
	cell[31] = 0x7F;
	let name = b"c";
	let row_numbers = [1u64];
	let time = [0u64];
	let column = ExternCColumn {
		name: ExternCBuffer::from_slice(name),
		data: ExternCColumnData {
			type_code: ValueKind::Int,
			precision: 76,
			scale: 0,
			row_count: 1,
			data: ExternCBuffer::from_slice(&cell),
			defined_bitvec: ExternCBuffer::empty(),
			offsets: ExternCBuffer::empty(),
		},
	};
	let columns = ExternCColumns {
		row_count: 1,
		column_count: 1,
		row_numbers: row_numbers.as_ptr(),
		columns: &column,
		time: time.as_ptr(),
	};
	// SAFETY: every buffer the columns point at is a local that outlives the borrow.
	let borrowed = unsafe { BorrowedColumns::from_extern_c(&columns) };
	let row = borrowed.row(0).unwrap();
	assert!(row.is_defined("c"));
	assert!(matches!(row.int("c"), Err(SdkError::InvalidInput(_))), "got {:?}", row.int("c"));
}
