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
		row_number::RowNumber,
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
					ValueKind::Decimal => {
						let values = read_both_ways(
							&view,
							view.decimal_iter().expect("a decimal column"),
							post.rows().map(|row| row.decimal(name).unwrap()).collect(),
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

fn echo(label: &str, input: ColumnBuffer) {
	let output = round_trip_column_through::<FamilyEchoOperator>("f", input.clone());
	assert_column_eq(label, &input, &output);
}

fn decimals(texts: &[&str]) -> Vec<Decimal> {
	texts.iter().map(|t| Decimal::parse(t).unwrap()).collect()
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
