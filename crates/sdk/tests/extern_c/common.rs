// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(dead_code)]

use std::{marker::PhantomData, sync::Arc};

use arrow_array::{ArrayRef, UInt64Array};
use arrow_schema::FieldRef;
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff, Diffs},
		flow::OperatorCapability,
	},
	operator_with::ApplyWith,
	value::{batch::batch, column::factory::rename},
};
use reifydb_sdk::{
	error::{Result, SdkError},
	flow::operator::{
		NostateMount, NostateOperator, OperatorMetadata,
		column::{cell::Cell, operator::OperatorColumn},
		context::{GuestContext, Nostate},
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
	row,
};
use reifydb_testing_sdk::in_process::harness::InProcessOperatorHarness;
use reifydb_value::{
	config::ExtensionParams,
	value::{
		Value,
		column_view::ColumnView,
		container::temporal_array::datetime_array,
		date::Date,
		datetime::DateTime,
		duration::Duration,
		system_columns::{SystemColumn, user_columns, with_system_column},
		time::Time,
		value_type::ValueType,
	},
};

const COLUMN: &str = "column";

pub struct Cells<T> {
	v: T,
}

row!(impl (<T: Cell>) for Cells<T> {
	v: T
});

pub struct EchoOperator<T> {
	column: String,
	_cell: PhantomData<T>,
}

impl<T> OperatorMetadata for EchoOperator<T> {
	const NAME: &'static str = "in_process_round_trip_echo";
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "echoes every row of one column through the typed reader and sink";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl<T: Cell + Send + Sync + 'static> NostateOperator for EchoOperator<T> {
	fn create(_id: OperatorId, params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		let column = match params.get(COLUMN) {
			Some(Value::Utf8(column)) => column.clone(),
			other => {
				return Err(SdkError::InvalidInput(format!(
					"the echo needs a column name, got {other:?}"
				)));
			}
		};
		Ok(Self {
			column,
			_cell: PhantomData,
		})
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Nostate>, change: impl ChangeView) -> Result<()> {
		for index in 0..change.diff_count() {
			let diff = change.diff(index).expect("every counted diff resolves");
			let post = diff.post().expect("a round trip only inserts");
			let mut rows = Vec::with_capacity(post.row_count());
			let mut numbers = Vec::with_capacity(post.row_count());
			for position in 0..post.row_count() {
				let row = post.row(position).expect("every counted row resolves");
				let v = T::decode(&row, &self.column)?.expect("an optional cell always decodes");
				rows.push(Cells {
					v,
				});
				numbers.push(row.row_number().expect("every round trip row is numbered"));
			}
			ctx.emit_insert(&rows, &numbers)?;
		}
		Ok(())
	}
}

pub fn round_trip_column(name: &str, input: (FieldRef, ArrayRef)) -> (FieldRef, ArrayRef) {
	let inner = match ColumnView::try_from(&input).unwrap().get_type() {
		ValueType::Option(inner) => *inner,
		other => other,
	};
	match inner {
		ValueType::Boolean => round_trip_as::<bool>(name, input),
		ValueType::Float4 => round_trip_as::<f32>(name, input),
		ValueType::Float8 => round_trip_as::<f64>(name, input),
		ValueType::Int1 => round_trip_as::<i8>(name, input),
		ValueType::Int2 => round_trip_as::<i16>(name, input),
		ValueType::Int4 => round_trip_as::<i32>(name, input),
		ValueType::Int8 => round_trip_as::<i64>(name, input),
		ValueType::Int16 => round_trip_as::<i128>(name, input),
		ValueType::Uint1 => round_trip_as::<u8>(name, input),
		ValueType::Uint2 => round_trip_as::<u16>(name, input),
		ValueType::Uint4 => round_trip_as::<u32>(name, input),
		ValueType::Uint8 => round_trip_as::<u64>(name, input),
		ValueType::Uint16 => round_trip_as::<u128>(name, input),
		ValueType::Utf8 => round_trip_as::<String>(name, input),
		ValueType::Blob => round_trip_as::<Vec<u8>>(name, input),
		ValueType::Date => round_trip_as::<Date>(name, input),
		ValueType::DateTime => round_trip_as::<DateTime>(name, input),
		ValueType::Time => round_trip_as::<Time>(name, input),
		ValueType::Duration => round_trip_as::<Duration>(name, input),
		other => panic!("the in-process round trip has no typed cell for {other:?}"),
	}
}

fn round_trip_as<T: Cell + Send + Sync + 'static>(name: &str, input: (FieldRef, ArrayRef)) -> (FieldRef, ArrayRef) {
	let n = input.1.len();
	let row_numbers: Vec<u64> = (1..=(n as u64).max(1)).take(n).collect();
	let now = DateTime::default();
	let mut columns = batch(vec![rename(input, name)]).unwrap();
	columns = with_system_column(columns, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(row_numbers)))
		.unwrap();
	for column in [SystemColumn::CreatedAt, SystemColumn::UpdatedAt, SystemColumn::Time] {
		columns = with_system_column(columns, column, Arc::new(datetime_array(vec![now; n]))).unwrap();
	}

	let mut diffs: Diffs = Diffs::new();
	diffs.push(Diff::insert(columns));
	let change = Change::from_flow(OperatorId(1), ChangeVersion::from(CommitVersion(1)), diffs, now);

	let mut harness = InProcessOperatorHarness::<NostateMount<EchoOperator<Option<T>>>>::builder()
		.with_node_id(OperatorId(1))
		.add_param(COLUMN, Value::Utf8(name.to_string()))
		.build()
		.expect("build harness");
	let output = harness.apply(change).expect("apply");

	assert_eq!(output.diffs.len(), 1, "expected exactly one output diff");
	let out_columns = match &output.diffs[0] {
		Diff::Insert {
			post,
			..
		} => post,
		Diff::Update {
			post,
			..
		} => post,
		Diff::Remove {
			pre,
			..
		} => pre,
	};
	assert_eq!(user_columns(out_columns).count(), 1, "expected exactly one output column");
	let (field, array) = user_columns(out_columns).next().unwrap();
	(field.clone(), array.clone())
}

pub fn slice(column: (FieldRef, ArrayRef), start: usize, end: usize) -> (FieldRef, ArrayRef) {
	let (field, array) = column;
	(field, array.slice(start, end - start))
}

pub fn assert_column_eq(label: &str, expected: &(FieldRef, ArrayRef), actual: &(FieldRef, ArrayRef)) {
	let expected = ColumnView::try_from(expected).unwrap();
	let actual = ColumnView::try_from(actual).unwrap();
	assert_eq!(
		expected.get_type(),
		actual.get_type(),
		"{}: type mismatch: expected {:?}, got {:?}",
		label,
		expected.get_type(),
		actual.get_type()
	);
	assert_eq!(
		expected.len(),
		actual.len(),
		"{}: row count mismatch: expected {}, got {}",
		label,
		expected.len(),
		actual.len()
	);
	let exp: Vec<Value> = expected.iter().collect();
	let act: Vec<Value> = actual.iter().collect();
	for (i, (e, a)) in exp.iter().zip(act.iter()).enumerate() {
		let matches = values_match(e, a);
		if !matches {
			panic!("{}: row {}: expected {:?}, got {:?}", label, i, e, a);
		}
	}
}

/// Bit equality rather than `==` for floats, so a round trip that turns -0.0 into +0.0 or flattens a sub-normal
/// is caught; NaN is the one case compared by classification instead.
fn values_match(a: &Value, b: &Value) -> bool {
	use Value::*;
	match (a, b) {
		(Float4(av), Float4(bv)) => {
			let af: f32 = (*av).into();
			let bf: f32 = (*bv).into();
			(af.is_nan() && bf.is_nan()) || af.to_bits() == bf.to_bits()
		}
		(Float8(av), Float8(bv)) => {
			let af: f64 = (*av).into();
			let bf: f64 = (*bv).into();
			(af.is_nan() && bf.is_nan()) || af.to_bits() == bf.to_bits()
		}
		_ => a == b,
	}
}
