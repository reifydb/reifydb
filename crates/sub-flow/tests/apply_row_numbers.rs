// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::{WindowRequirements, WindowSizeDomain},
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	key::operator::state::GroupId,
	operator_with::ApplyWith,
};
use reifydb_sdk::{
	error::Result as SdkResult,
	flow::operator::{
		OperatorMetadata, UnmanagedOperator,
		column::operator::OperatorColumn,
		context::{GuestContext, Unmanaged},
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
	row,
};
use reifydb_value::{
	config::ExtensionParams,
	value::{constraint::TypeConstraint, diff_type::DiffType, duration::Duration, value_type::ValueType},
};

const TIMEOUT: Duration = Duration::from_seconds_const(20);

struct ProbeRow {
	id: i32,
	seen: i64,
}

row!(ProbeRow {
	id: i32,
	seen: i64
});

const PROBE_INPUT: &[OperatorColumn] = &[OperatorColumn {
	name: "id",
	type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
	description: "the source row's id",
}];

const PROBE_OUTPUT: &[OperatorColumn] = &[
	OperatorColumn {
		name: "id",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
		description: "the source row's id",
	},
	OperatorColumn {
		name: "seen",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "the #rownum the operator was handed, or -1 when it was handed none",
	},
];

struct RownumProbe;

impl OperatorMetadata for RownumProbe {
	const NAME: &'static str = "rownum_probe";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only operator that reports the row number of every inserted row";
	const INPUT_COLUMNS: &'static [OperatorColumn] = PROBE_INPUT;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = PROBE_OUTPUT;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl UnmanagedOperator for RownumProbe {
	const UNMANAGED_BECAUSE: &'static str = "test operator";
	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: false,
		kinds: &[],
		domain: WindowSizeDomain::Time,
		needs_pane: false,
		throttles: false,
	};

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(RownumProbe)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Unmanaged>, change: impl ChangeView) -> SdkResult<()> {
		for i in 0..change.diff_count() {
			let Some(diff) = change.diff(i) else {
				continue;
			};
			if !matches!(diff.kind(), DiffType::Insert) {
				continue;
			}
			let Some(post) = diff.post() else {
				continue;
			};
			for r in 0..post.row_count() {
				let row = post.row(r).expect("row");
				let id = row.i32("id").unwrap().expect("id");
				let seen = row.row_number().map(|number| number.value() as i64).unwrap_or(-1);
				let key = EncodedKey::new(id.to_be_bytes());
				let (row_number, _) =
					ctx.get_or_create_row_numbers(GroupId::of(&key), &[key])?.remove(0);
				ctx.emit_insert(
					&[ProbeRow {
						id,
						seen,
					}],
					&[row_number],
				)?;
			}
		}
		Ok(())
	}
}

fn setup() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_flow(|f| f.register_unmanaged_operator::<RownumProbe>())
			.build()
			.expect("build memory db with flow"),
	)
}

#[test]
fn a_guest_apply_is_handed_the_row_numbers_of_its_input() {
	// A guest keys state by #rownum, which the view never names, so the flow source must still build it.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4 }");
	db.admin(
		"CREATE DEFERRED VIEW app::v { id: int4, seen: int8 } AS { FROM app::t MAP {id} APPLY rownum_probe{} }",
	);

	db.command("INSERT app::t [{ id: 10 }, { id: 20 }]");
	db.await_row_count("FROM app::v", 2, TIMEOUT);

	let frames = db.query_as_root("FROM app::v | sort { id:ASC }", ()).expect("query view");
	let seen: Vec<(i32, i64)> = frames
		.iter()
		.flat_map(|frame| frame.rows())
		.map(|row| (row.get::<i32>("id").unwrap().unwrap(), row.get::<i64>("seen").unwrap().unwrap()))
		.collect();
	assert_eq!(seen, vec![(10, 1), (20, 2)], "the guest must be handed each input row's own #rownum");
}
