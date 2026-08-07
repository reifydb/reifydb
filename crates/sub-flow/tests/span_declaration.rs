// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A retention span the substrate cannot honor is refused at registration: accepted in silence, the
// node keeps every row it ever saw while the catalog claims it has a ttl.

use reifydb::testing::db::TestDb;
use reifydb::{WithSubsystem, embedded};
use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_group_state::{Keyspace, OperatorGroupStateKey},
};
use reifydb_sdk::{
	config::Config,
	error::Result as SdkResult,
	operator::{
		OperatorLogic, OperatorMetadata,
		column::operator::OperatorColumn,
		context::OperatorContext,
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
	row,
	state::RawStatefulOperator,
};
use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

const HOARDER_STATE: Keyspace = Keyspace::FIRST_CUSTOM;

struct HoarderRow {
	g: i32,
	total: i64,
}

row!(HoarderRow {
	g: i32,
	total: i64
});

const HOARDER_COLUMNS: &[OperatorColumn] = &[
	OperatorColumn {
		name: "g",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
		description: "group key",
	},
	OperatorColumn {
		name: "total",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "running total per group",
	},
];

// A guest operator with per-group state: a declared span on the apply node wrapping it is aged by
// the apply wrapper's own floors, so the declaration is accepted without any capability handshake.
struct Hoarder;

impl RawStatefulOperator for Hoarder {}

impl OperatorMetadata for Hoarder {
	const NAME: &'static str = "hoarder";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only per-group tally that cannot drop what it accumulates";
	const INPUT_COLUMNS: &'static [OperatorColumn] = HOARDER_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = HOARDER_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

fn tally_apply(
	operator: &impl RawStatefulOperator,
	ctx: &mut impl OperatorContext,
	change: impl ChangeView,
) -> SdkResult<()> {
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

		let mut rows = Vec::new();
		let mut row_numbers = Vec::new();
		for r in 0..post.row_count() {
			let row = post.row(r).expect("row");
			let g = row.i32("g").expect("g");

			let key = EncodedKey::new(g.to_be_bytes());
			let group = ctx.intern_group(&key)?;
			let state_key = OperatorGroupStateKey::inner_encoded(group, HOARDER_STATE, []);

			let total: i64 = operator.state_get(ctx, &state_key)?.unwrap_or(0) + 1;
			operator.state_set(ctx, &state_key, &total)?;

			let (row_number, _is_new) = ctx.get_or_create_row_number(group, &key)?;
			row_numbers.push(row_number);
			rows.push(HoarderRow {
				g,
				total,
			});
		}
		if !rows.is_empty() {
			ctx.emit_insert(&rows, &row_numbers)?;
		}
	}
	Ok(())
}

impl OperatorLogic for Hoarder {
	fn create(_operator_id: OperatorId, _config: &Config) -> SdkResult<Self> {
		Ok(Hoarder)
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> SdkResult<()> {
		tally_apply(self, ctx, change)
	}
}

fn setup_with_custom_operators() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_flow(|f| f.register_operator::<Hoarder>())
			.build()
			.expect("build memory db with flow"),
	)
}

fn rejection(db: &TestDb, rql: &str) -> Option<String> {
	match db.try_admin(rql) {
		Ok(_) => None,
		Err(err) => Some(err.diagnostic().code),
	}
}

#[test]
fn a_span_on_a_node_that_keeps_no_state_is_refused() {
	// A span is consulted only on join, distinct, append, apply and aggregate nodes; on a map it
	// would be accepted and never read, telling the author their data ages when nothing does. The
	// grammar refuses it here, and registration guards the route the grammar cannot see.
	let db = setup();
	db.admin("CREATE NAMESPACE sp");
	db.admin("CREATE TABLE sp::t { id: int4, v: int4 }");

	assert_eq!(
		rejection(
			&db,
			"CREATE DEFERRED VIEW sp::v { id: int4, v: int4 } AS { \
			 FROM sp::t map { id, v } with { ttl: { duration: \"1s\" } } }"
		)
		.as_deref(),
		Some("AST_009"),
		"a span on a stateless node must be refused rather than accepted and ignored"
	);
}

#[test]
fn a_span_on_a_stateful_node_that_can_age_is_accepted() {
	// The control. Append consults a declared span and the capability arm gates apply nodes only,
	// so the same span is legitimate here; otherwise the test above would pass against a rule that
	// refused every span.
	let db = setup();
	db.admin("CREATE NAMESPACE sp");
	db.admin("CREATE TABLE sp::a { id: int4, v: int4 }");
	db.admin("CREATE TABLE sp::b { id: int4, v: int4 }");

	assert_eq!(
		rejection(
			&db,
			"CREATE DEFERRED VIEW sp::u { id: int4, v: int4 } AS { \
			 FROM sp::a append { FROM sp::b } with { ttl: { duration: \"1s\" } } }"
		),
		None,
		"append keeps keyed state and can reclaim, so its span is honored and must be accepted"
	);
}

#[test]
fn a_span_on_an_apply_operator_is_accepted_and_aged_by_the_wrapper() {
	// A guest operator needs no capability handshake for a declared span: the apply wrapper's own
	// floors age the guest's state at the declared ttl, so the declaration is accepted for any
	// registered operator rather than refused for want of a capability bit.
	let db = setup_with_custom_operators();
	db.admin("CREATE NAMESPACE sp");
	db.admin("CREATE TABLE sp::t { id: int4, g: int4 }");

	assert_eq!(
		rejection(
			&db,
			"CREATE DEFERRED VIEW sp::h { g: int4, total: int8 } AS { \
			 FROM sp::t APPLY hoarder{} with { ttl: { duration: \"1s\" } } }"
		),
		None,
		"the apply wrapper enforces the span itself, so the declaration must be accepted"
	);
}

#[test]
fn a_statically_registered_operator_reaches_the_operator_catalog() {
	let db = setup_with_custom_operators();

	assert_eq!(
		db.row_count("FROM system::operator_libraries FILTER { operator == 'hoarder' }"),
		1,
		"a statically registered operator must be listed; catalog now: {:?}",
		db.query("FROM system::operator_libraries map { operator }")
	);
	assert_eq!(
		db.row_count("FROM system::operator_libraries FILTER { operator == 'hoarder' and cap_insert == true }"),
		1,
		"the capability bits must survive the trip, not just the name; catalog now: {:?}",
		db.query("FROM system::operator_libraries")
	);
	assert_eq!(
		db.row_count("FROM system::operator_library_inputs FILTER { operator == 'hoarder' and name == 'g' }"),
		1,
		"the declared columns travel with the operator, not just its name and capabilities; inputs now: {:?}",
		db.query("FROM system::operator_library_inputs")
	);
	assert_eq!(
		db.row_count(
			"FROM system::operator_library_outputs FILTER { operator == 'hoarder' and name == 'total' }"
		),
		1,
		"outputs are published too; outputs now: {:?}",
		db.query("FROM system::operator_library_outputs")
	);
}
