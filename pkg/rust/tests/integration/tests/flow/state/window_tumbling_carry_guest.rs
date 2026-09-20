// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration as StdDuration;

use reifydb::{
	ConfigKey, Value, WithSubsystem,
	core::{
		interface::{catalog::flow::OperatorId, flow::OperatorCapability},
		metrics::heap::HeapSize,
		operator_with::ApplyWith,
	},
	embedded,
	sdk::{
		error::Result as SdkResult,
		flow::operator::{
			OperatorMetadata,
			column::operator::OperatorColumn,
			context::{GuestContext, Windowed},
			view::RowView,
			windowed::operator::{CarryEmit, WindowSettings, WindowedOperator},
		},
		row,
	},
	testing::db::TestDb,
	window::{accumulator::invertible::moments::Moments, span::WindowSpan},
};
use reifydb_value::{
	config::ExtensionParams,
	value::{constraint::TypeConstraint, datetime::DateTime, value_type::ValueType},
};

const TIMEOUT: StdDuration = StdDuration::from_secs(15);

#[reifydb::r#macro::operator_state]
#[derive(Clone, Debug, PartialEq, HeapSize)]
struct GuestCarryWindow {
	g: i32,
	total: i64,
	carried_in: i64,
}

row!(GuestCarryWindow {
	g: i32,
	total: i64,
	carried_in: i64
});

struct GuestCarry;

impl WindowedOperator for GuestCarry {
	type Coord = DateTime;
	type GroupKey = i32;
	type Accumulator = Moments;
	type Output = GuestCarryWindow;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, _ctx: &mut impl GuestContext<Windowed>, row: &impl RowView) -> Option<(i32, f64)> {
		Some((row.i32("g")?, row.i32("v")? as f64))
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> Moments {
		Moments::default()
	}
}

impl CarryEmit for GuestCarry {
	type Carry = f64;

	fn build_output(
		&self,
		group: &i32,
		_span: WindowSpan<DateTime>,
		value: &Moments,
		prev: Option<&f64>,
	) -> Option<GuestCarryWindow> {
		Some(GuestCarryWindow {
			g: *group,
			total: value.sum() as i64,
			carried_in: prev.copied().unwrap_or(0.0) as i64,
		})
	}

	fn carry_forward(&self, value: &Moments, prev: Option<&f64>) -> Option<f64> {
		Some(prev.copied().unwrap_or(0.0) + value.sum())
	}
}

impl OperatorMetadata for GuestCarry {
	const NAME: &'static str = "tumbling_carry_guest";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Sums v per g over one-second tumbling windows, carrying the running total";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[
		OperatorColumn {
			name: "g",
			type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
			description: "Group key",
		},
		OperatorColumn {
			name: "v",
			type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
			description: "Summed value",
		},
	];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[
		OperatorColumn {
			name: "g",
			type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
			description: "Group key",
		},
		OperatorColumn {
			name: "total",
			type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
			description: "Window sum",
		},
		OperatorColumn {
			name: "carried_in",
			type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
			description: "Running total of the earlier windows",
		},
	];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

fn setup() -> TestDb {
	// The guest operator is registered in process, so no dylib is built and the ABI plays no part here.
	TestDb::from(
		embedded::memory()
			.with_flow(|f| f.register_windowed_operator::<GuestCarry, _>())
			.with_config(ConfigKey::MetricsFlushInterval, Value::duration_milliseconds(10))
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with a registered carry guest operator"),
	)
}

#[test]
fn a_carry_guest_registers_and_carries_across_windows() {
	// A carry guest resolved as a plain one would publish every window with no carry in.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW app::w { g: int4, total: int8, carried_in: int8 } AS {
				FROM app::t
					| apply tumbling_carry_guest{} with { window: tumbling, duration: 1s }
			}"#);

	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.command(r#"INSERT app::t [{ id: 2, g: 1, v: 7, ts: "2026-01-01T00:00:01.500Z" }]"#);
	db.await_row_count("FROM app::w", 2, TIMEOUT);

	assert_eq!(
		db.row_count("FROM app::w FILTER { total == 5 and carried_in == 0 }"),
		1,
		"the first window has nothing to carry in; view now: {:?}",
		db.query("FROM app::w")
	);
	assert_eq!(
		db.row_count("FROM app::w FILTER { total == 7 and carried_in == 5 }"),
		1,
		"the second window must start from the first window's total; view now: {:?}",
		db.query("FROM app::w")
	);
}
