// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::RowShapeField;
use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::HeapSize,
	operator_with::{ApplyWith, WithSpan},
	row::Row as CoreRow,
};
use reifydb_flow::window::{
	accumulator::{
		WindowAccumulator,
		invertible::{moments::Moments, ordf64::OrdF64},
	},
	settings::WindowSettings,
	span::WindowSpan,
};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		OperatorMetadata,
		column::operator::OperatorColumn,
		context::{GuestContext, Windowed},
		extern_c::binding::operator::ExternCOperatorAdapter,
		view::RowView,
		windowed::{
			operator::{Emit, NoRolling, WindowedOperator},
			plain::PlainDriver,
		},
	},
	row,
};
use reifydb_testing_sdk::{
	builders::{TestChangeBuilder, TestOperatorRowBuilder},
	harness::ExternCOperatorHarnessBuilder,
};
use reifydb_value::{
	config::ExtensionParams,
	factory::time::millis,
	value::{Value, datetime::DateTime, value_type::ValueType},
};

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, Default, HeapSize)]
struct SumAccumulator {
	moments: Moments,
}

impl WindowAccumulator for SumAccumulator {
	type Contribution = f64;
	type Output = OrdF64;

	fn add(&mut self, contribution: &f64) {
		self.moments.add(*contribution);
	}

	fn remove(&mut self, contribution: &f64) {
		self.moments.remove(*contribution);
	}

	fn finalize(&self) -> Option<OrdF64> {
		(!self.moments.is_empty()).then(|| OrdF64::new(self.moments.sum()).expect("finite"))
	}

	fn is_empty(&self) -> bool {
		self.moments.is_empty()
	}
}

#[derive(Clone, Debug, PartialEq)]
struct SumOut {
	group: String,
	total: f64,
}

row!(SumOut {
	group: String,
	total: f64
});

struct ReadsSizeAsInt;

impl OperatorMetadata for ReadsSizeAsInt {
	const NAME: &'static str = "reads_size_as_int";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for ReadsSizeAsInt {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = SumAccumulator;
	type Output = SumOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Result<Option<DateTime>> {
		Ok(row.row_time())
	}

	fn extract(&self, _ctx: &mut impl GuestContext<Windowed>, row: &impl RowView) -> Result<Option<(String, f64)>> {
		let (Some(group), Some(size)) = (row.utf8("group")?, row.i32("size")?) else {
			return Ok(None);
		};
		Ok(Some((group.to_string(), size as f64)))
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> SumAccumulator {
		SumAccumulator::default()
	}
}

impl Emit for ReadsSizeAsInt {
	type Kinds = NoRolling;

	fn build_output(&self, group: &String, _span: WindowSpan<DateTime>, value: &OrdF64) -> Option<SumOut> {
		Some(SumOut {
			group: group.clone(),
			total: value.get(),
		})
	}
}

fn float_size_row() -> CoreRow {
	TestOperatorRowBuilder::new(1)
		.with_values(vec![Value::Utf8("BTC".into()), Value::float8(10.0)])
		.with_fields(vec![
			RowShapeField::unconstrained("group", ValueType::Utf8),
			RowShapeField::unconstrained("size", ValueType::Float8),
		])
		.with_time(DateTime::from_millis(0))
		.build()
}

#[test]
fn a_read_error_in_extract_fails_the_apply_instead_of_skipping_the_row() {
	// Skipping would drop the row from every window and emit plausible totals built on missing data.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<ReadsSizeAsInt>>>::new()
		.with(ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(millis(60)),
			}),
			lateness: Some(WithSpan::Duration(millis(3_600_000))),
			immutable: None,
			retention: None,
			throttle: None,
		})
		.build()
		.expect("harness");
	let result = h.apply(TestChangeBuilder::new().insert(float_size_row()).build());
	let err = result.expect_err("reading a float8 column as i32 must fail the apply");
	assert!(err.to_string().contains("size"), "the error must name the column, got {err}");
}
