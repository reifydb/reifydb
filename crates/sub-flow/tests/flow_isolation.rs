// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The core claim of the per-flow deferred architecture: a slow flow must not stall a fast one. Each
// deferred flow pulls, computes and commits on its own actor, so an operator blocked in compute
// holds back only its own view. A shared barrier would stall the fast view alongside it.

use std::{thread, time::Duration as StdDuration};

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_core::interface::{catalog::flow::OperatorId, flow::OperatorCapability};
use reifydb_sdk::{
	error::Result as SdkResult,
	flow::operator::{
		OperatorLogic, OperatorMetadata,
		column::operator::OperatorColumn,
		context::OperatorContext,
		state::{RawStatefulOperator, utils::empty_state_key},
		view::{ChangeView, ColumnsView, DiffView},
	},
	row,
};
use reifydb_value::{
	config::Config,
	value::{constraint::TypeConstraint, row_number::RowNumber, value_type::ValueType},
};

const SLOW_APPLY: StdDuration = StdDuration::from_secs(5);

// Sleeps SLOW_APPLY inside apply, then tallies the rows it has seen. The sleep blocks only this
// flow's actor; the fast view's actor runs elsewhere.
struct SlowCounter;

impl RawStatefulOperator for SlowCounter {}

struct CountRow {
	seen: i64,
}

row!(CountRow {
	seen: i64
});

const COUNT_OUTPUT_COLUMNS: &[OperatorColumn] = &[OperatorColumn {
	name: "seen",
	type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
	description: "Rows seen so far (emitted only after a deliberate delay)",
}];

impl OperatorMetadata for SlowCounter {
	const NAME: &'static str = "slow_counter";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Sleeps, then tallies rows seen - used to prove flow isolation";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = COUNT_OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl OperatorLogic for SlowCounter {
	fn create(_operator_id: OperatorId, _config: &Config) -> SdkResult<Self> {
		Ok(SlowCounter)
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> SdkResult<()> {
		thread::sleep(SLOW_APPLY);

		let mut seen = 0i64;
		for i in 0..change.diff_count() {
			if let Some(diff) = change.diff(i) {
				seen += diff.post().map(|c| c.row_count()).unwrap_or(0) as i64;
			}
		}

		let key = empty_state_key();
		let total = self.state_get::<i64>(ctx, &key)?.unwrap_or(0) + seen;
		self.state_set(ctx, &key, &total)?;
		ctx.emit_insert(
			&[CountRow {
				seen: total,
			}],
			&[RowNumber(1)],
		)
	}
}

fn setup() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_flow(|f| f.register_operator::<SlowCounter>())
			.build()
			.expect("build memory db with flow"),
	)
}

#[test]
fn slow_flow_does_not_stall_fast_flow() {
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4 }");
	db.admin("CREATE DEFERRED VIEW app::fast { id: int4 } AS { FROM app::t MAP { id } }");
	db.admin("CREATE DEFERRED VIEW app::slow { seen: int8 } AS { FROM app::t APPLY slow_counter{} }");

	db.command("INSERT app::t [{ id: 1 }, { id: 2 }, { id: 3 }]");

	// The fast view must materialize all three rows well before the slow operator's 5s sleep finishes.
	let fast = db.await_row_count("FROM app::fast", 3, StdDuration::from_secs(3));
	assert_eq!(
		fast, 3,
		"fast deferred view must materialize all 3 rows within 3s even though an independent slow flow \
		 is blocked in compute; got {fast} (a lower count means the slow flow is stalling the fast one - \
		 the isolation the per-flow architecture is supposed to guarantee is broken)"
	);

	// The decisive check: the slow actor is still asleep inside apply, so fast is complete while
	// slow has committed nothing.
	let slow_now = db.row_count("FROM app::slow");
	assert_eq!(
		slow_now, 0,
		"the slow view must still be empty while the fast view is already complete (proves the two flows \
		 progress independently); got {slow_now}"
	);

	// And the slow flow must still eventually catch up on its own once its sleep completes.
	let slow_final = db.await_row_count("FROM app::slow", 1, StdDuration::from_secs(15));
	assert!(slow_final >= 1, "the slow view must eventually materialize its tally row; got {slow_final}");
}
