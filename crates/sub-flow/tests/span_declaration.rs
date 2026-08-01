// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A retention span that the substrate cannot honor used to be accepted in silence: the node kept
// every row it ever saw while the catalog claimed it had a ttl. Registration now refuses both shapes
// that produce that outcome.

use reifydb::{WithSubsystem, embedded};
use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	key::operator_state::{Keyspace, OperatorStateKey},
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
use reifydb_test_harness::db::TestDb;
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

// Keeps per-group state exactly the way Tally in custom_operator_reclaim does, and differs from it in
// one respect only: it declares STANDARD instead of STANDARD_WITH_RECLAIM. That single difference is
// what the test below is about, so the state-keeping has to be real - an operator that touched no
// state would be refused by FLOW_045 first and the FLOW_044 route would never be reached.
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

// Identical state-keeping, identical column shape, one bit different: it declares Reclaim. It is the
// control for the FLOW_044 test - without it that test would pass just as well against a rule that
// refused every span on an apply node, which would be a worse defect than the one being guarded.
struct Sweeper;

impl RawStatefulOperator for Sweeper {}

impl OperatorMetadata for Sweeper {
	const NAME: &'static str = "sweeper";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only per-group tally that can drop what it accumulates";
	const INPUT_COLUMNS: &'static [OperatorColumn] = HOARDER_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = HOARDER_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD_WITH_RECLAIM;
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
			let state_key = OperatorStateKey::inner_encoded(group, HOARDER_STATE, []);

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
	fn create(_operator_id: FlowNodeId, _config: &Config) -> SdkResult<Self> {
		Ok(Hoarder)
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> SdkResult<()> {
		tally_apply(self, ctx, change)
	}
}

impl OperatorLogic for Sweeper {
	fn create(_operator_id: FlowNodeId, _config: &Config) -> SdkResult<Self> {
		Ok(Sweeper)
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> SdkResult<()> {
		tally_apply(self, ctx, change)
	}
}

fn setup_with_custom_operators() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_flow(|f| f.register_operator::<Hoarder>().register_operator::<Sweeper>())
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
	// Intent: spans only mean something on operators that hold keyed state. Declared on a map the
	// engine resolves the horizon to Perpetual and never consults the span again, so the author is
	// told their data ages when nothing does.
	// The grammar is the first line of defence and refuses this shape outright, which is what this
	// test pins - a grammar change that started accepting it would land here first. Registration
	// carries its own guard (FLOW_045) for the route the grammar cannot see: a DAG reloaded from
	// the catalog on restart, which is the same reason check_time_domain re-runs at registration.
	// Mutation: let the grammar accept `with { ttl }` on a map and this assertion fails, at which
	// point the registration guard is what stops the span being silently dropped.
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
	// The control. Append holds keyed state and declares Reclaim, so the same span is legitimate
	// there - without this the test above would pass equally well against a rule that refused
	// every span, which would be a far worse defect than the one it fixes.
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
fn a_span_on_an_operator_that_cannot_reclaim_is_refused() {
	// FLOW_044. The node type is stateful and its declared horizon is a Span, so FLOW_045 passes it
	// through; the refusal has to come from the instantiated operator's capability set instead. An
	// apply node is the only route to that check, because every built-in stateful operator declares
	// Reclaim - the reachable failure is a guest operator whose author declared a ttl the operator
	// has no code to honor.
	// Intent: a span the substrate cannot act on is worse than no span. Accepted and ignored, the
	// catalog reports a ttl on a node whose state grows forever, so the one surface an operator
	// would consult to notice the leak actively denies it.
	// Mutation: drop the capability arm of check_declared_span and this returns None - registration
	// succeeds, the view is created, and nothing downstream ever reports the span as unhonored.
	let db = setup_with_custom_operators();
	db.admin("CREATE NAMESPACE sp");
	db.admin("CREATE TABLE sp::t { id: int4, g: int4 }");

	assert_eq!(
		rejection(
			&db,
			"CREATE DEFERRED VIEW sp::h { g: int4, total: int8 } AS { \
			 FROM sp::t APPLY hoarder{} with { ttl: { duration: \"1s\" } } }"
		)
		.as_deref(),
		Some("FLOW_044"),
		"a span on an operator without Reclaim must be refused, not accepted and silently ignored"
	);
}

#[test]
fn the_same_span_on_an_operator_that_can_reclaim_is_accepted() {
	// The control for the test above. Sweeper differs from Hoarder only by declaring Reclaim, and
	// the RQL is otherwise identical, so a rule that refused spans on apply nodes wholesale - or one
	// that refused every custom operator - fails here instead of passing both.
	let db = setup_with_custom_operators();
	db.admin("CREATE NAMESPACE sp");
	db.admin("CREATE TABLE sp::t { id: int4, g: int4 }");

	assert_eq!(
		rejection(
			&db,
			"CREATE DEFERRED VIEW sp::s { g: int4, total: int8 } AS { \
			 FROM sp::t APPLY sweeper{} with { ttl: { duration: \"1s\" } } }"
		),
		None,
		"an operator that declares Reclaim can honor the span, so its declaration must be accepted"
	);
}

#[test]
fn a_statically_registered_operator_reaches_the_operator_catalog() {
	let db = setup_with_custom_operators();

	assert_eq!(
		db.row_count("FROM system::operators FILTER { operator == 'hoarder' }"),
		1,
		"a statically registered operator must be listed; catalog now: {:?}",
		db.query("FROM system::operators map { operator }")
	);
	assert_eq!(
		db.row_count("FROM system::operators FILTER { operator == 'sweeper' and cap_reclaim == true }"),
		1,
		"the capability bits must survive the trip, not just the name; catalog now: {:?}",
		db.query("FROM system::operators")
	);
	assert_eq!(
		db.row_count("FROM system::operator_inputs FILTER { operator == 'hoarder' and name == 'g' }"),
		1,
		"the declared columns travel with the operator, not just its name and capabilities; inputs now: {:?}",
		db.query("FROM system::operator_inputs")
	);
	assert_eq!(
		db.row_count("FROM system::operator_outputs FILTER { operator == 'hoarder' and name == 'total' }"),
		1,
		"outputs are published too; outputs now: {:?}",
		db.query("FROM system::operator_outputs")
	);
	assert_eq!(
		db.row_count("FROM system::operators FILTER { operator == 'hoarder' and cap_reclaim == true }"),
		0,
		"hoarder declares STANDARD, so its Reclaim bit must be false - if both operators reported the \
		 same capabilities the span check could not tell them apart"
	);
}
