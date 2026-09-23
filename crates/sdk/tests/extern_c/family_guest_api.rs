// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_codec::tag::ValueKind;
use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	operator_with::ApplyWith,
	value::column::buffer::ColumnBuffer,
};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		OperatorMetadata,
		change::BorrowedChange,
		column::operator::OperatorColumn,
		extern_c::binding::{context::ExternCContext, operator::ExternCOperator},
		view::RowView,
		view_column::ColumnView,
	},
};
use reifydb_value::{
	config::ExtensionParams,
	value::{
		constraint::{precision::Precision, scale::Scale},
		decimal::Decimal,
		int::Int,
		row_number::RowNumber,
		uint::Uint,
	},
};

use super::common::{assert_column_eq, round_trip_column_through};

pub struct FamilyEchoOperator;

impl OperatorMetadata for FamilyEchoOperator {
	const NAME: &'static str = "extern_c_family_echo";
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str =
		"reads int, uint and decimal columns through the guest readers and writes them back";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

fn read_both_ways<T: Clone + PartialEq + Debug>(
	view: &ColumnView<'_>,
	by_column: impl Iterator<Item = Option<T>>,
	by_row: Vec<Option<T>>,
) -> Vec<Option<T>> {
	// The column reader and the row reader decode the same cells, so any disagreement is a decode defect.
	let by_column: Vec<Option<T>> =
		by_column.enumerate().map(|(row, value)| value.filter(|_| view.is_defined(row))).collect();
	assert_eq!(by_column, by_row, "column {} reads differently by column and by row", view.name());
	by_row
}

impl ExternCOperator for FamilyEchoOperator {
	fn new(_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn apply(&mut self, ctx: &mut ExternCContext, input: BorrowedChange<'_>) -> Result<()> {
		let mut builder = ctx.builder();
		for diff in input.diffs() {
			let post = diff.post();
			let rows = post.row_count();
			let mut committed = Vec::new();
			let mut names = Vec::new();
			for view in post.column_views() {
				let name = view.name();
				let precision = Precision::try_new(view.raw().precision()).expect("a valid precision");
				let scale = Scale::try_new_with_precision(view.raw().scale(), precision)
					.expect("a valid scale");
				let column = match view.type_code() {
					ValueKind::Int => {
						let values = read_both_ways(
							&view,
							view.int_iter().expect("an int column"),
							post.rows().map(|row| row.int(name)).collect(),
						);
						let mut writer = builder.int_writer(rows.max(1), precision)?;
						for value in &values {
							match value {
								Some(value) => writer.push(value)?,
								None => writer.push_none()?,
							}
						}
						writer.finish()?
					}
					ValueKind::Uint => {
						let values = read_both_ways(
							&view,
							view.uint_iter().expect("a uint column"),
							post.rows().map(|row| row.uint(name)).collect(),
						);
						let mut writer = builder.uint_writer(rows.max(1), precision)?;
						for value in &values {
							match value {
								Some(value) => writer.push(value)?,
								None => writer.push_none()?,
							}
						}
						writer.finish()?
					}
					ValueKind::Decimal => {
						let values = read_both_ways(
							&view,
							view.decimal_iter().expect("a decimal column"),
							post.rows().map(|row| row.decimal(name)).collect(),
						);
						let mut writer =
							builder.decimal_writer(rows.max(1), precision, scale)?;
						for value in &values {
							match value {
								Some(value) => writer.push(value)?,
								None => writer.push_none()?,
							}
						}
						writer.finish()?
					}
					other => panic!(
						"the family echo only takes int, uint and decimal columns, got {other:?}"
					),
				};
				committed.push(column);
				names.push(name.to_string());
			}
			let names_ref: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
			let row_numbers: Vec<RowNumber> = post.row_numbers().iter().copied().map(RowNumber).collect();
			builder.emit_insert(&committed, &names_ref, &row_numbers)?;
		}
		Ok(())
	}
}

const WIDTHS: [Precision; 2] = [Precision::new(38), Precision::MAX];

fn echo(label: &str, input: ColumnBuffer) {
	let output = round_trip_column_through::<FamilyEchoOperator>("f", input.clone());
	assert_column_eq(label, &input, &output);
}

fn decimals(texts: &[&str]) -> Vec<Decimal> {
	texts.iter().map(|t| Decimal::parse(t).unwrap()).collect()
}

#[test]
fn int_reads_and_writes_through_the_guest_api_at_both_widths() {
	// The guest must size cells from the column precision; a fixed 16 or 32 byte guess breaks the other width.
	let edge = Int::parse("99999999999999999999999999999999999999").unwrap();
	for precision in WIDTHS {
		echo(
			&format!("int at precision {}", precision.value()),
			ColumnBuffer::int_with_bitvec(
				precision,
				[edge.clone(), Int::default(), edge.negate(), Int::from_i64(-1)],
				vec![true, false, true, true],
			),
		);
	}
	echo("int past i128", ColumnBuffer::int(Precision::MAX, [Int::MAX, Int::MIN, Int::from_i128(i128::MIN)]));
}

#[test]
fn uint_reads_and_writes_through_the_guest_api_at_both_widths() {
	// The guest must size cells from the column precision; a fixed 16 or 32 byte guess breaks the other width.
	let edge = Uint::parse("99999999999999999999999999999999999999").unwrap();
	for precision in WIDTHS {
		echo(
			&format!("uint at precision {}", precision.value()),
			ColumnBuffer::uint_with_bitvec(
				precision,
				[edge.clone(), Uint::default(), Uint::from_u64(u64::MAX)],
				vec![true, false, true],
			),
		);
	}
	echo("uint past u128", ColumnBuffer::uint(Precision::MAX, [Uint::MAX, Uint::from_u128(u128::MAX)]));
}

#[test]
fn decimal_reads_and_writes_through_the_guest_api_at_both_widths() {
	// The guest must carry the column scale from reader to writer, otherwise every value shifts by a power of ten.
	let (narrow, narrow_scale) = (Precision::new(38), Scale::new(10));
	echo(
		"decimal at precision 38",
		ColumnBuffer::decimal_with_bitvec(
			narrow,
			narrow_scale,
			decimals(&["9999999999999999999999999999.9999999999", "0", "-0.0000000001"]),
			vec![true, false, true],
		),
	);
	let (wide, wide_scale) = (Precision::MAX, Scale::new(40));
	echo(
		"decimal at precision 76",
		ColumnBuffer::decimal(
			wide,
			wide_scale,
			decimals(&[
				"999999999999999999999999999999999999.9999999999999999999999999999999999999999",
				"-0.0000000000000000000000000000000000000001",
			]),
		),
	);
}
