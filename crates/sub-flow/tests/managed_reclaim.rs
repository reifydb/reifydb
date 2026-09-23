// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration as StdDuration;

use reifydb::{
	ConfigKey, SqliteConfig, Value, WithSubsystem, embedded,
	testing::db::{TempDbPath, TestDb, await_value},
};
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::{WindowRequirements, WindowSizeDomain},
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	key::operator::state::{GroupId, managed_key_in},
	operator_with::ApplyWith,
};
use reifydb_runtime::{RuntimeConfig, fatal::FatalConfig};
use reifydb_sdk::{
	error::Result as SdkResult,
	flow::operator::{
		ManagedOperator, OperatorMetadata, UnmanagedOperator,
		column::operator::OperatorColumn,
		context::{ClassState, GuestContext, Managed, Unmanaged},
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
};
use reifydb_test_harness::assert::column_values;
use reifydb_value::{
	config::ExtensionParams,
	value::{constraint::TypeConstraint, value_type::ValueType},
};

const TIMEOUT: StdDuration = StdDuration::from_secs(20);

const MANAGED_KEYS: &str = "from system::metrics::flow::state::current filter { keyspace == 'CUSTOM_MANAGED' }";

const MANAGED_VIEW: &str =
	"CREATE DEFERRED VIEW app::v { g: int4 } AS { FROM app::t APPLY tally{} WITH { lateness: 2s } }";

const G_COLUMNS: &[OperatorColumn] = &[OperatorColumn {
	name: "g",
	type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
	description: "group key",
}];

struct Tally;

impl OperatorMetadata for Tally {
	const NAME: &'static str = "tally";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only managed operator that keeps one key per group and one in ROOT";
	const INPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl ManagedOperator for Tally {
	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(Tally)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Managed>, change: impl ChangeView) -> SdkResult<()> {
		// A ROOT key no group owns must survive every sweep, otherwise the operator loses its own bookkeeping.
		ctx.state().set(&managed_key_in(GroupId::ROOT, &[]).expect("an empty id fits the keyspace"), &1i64)?;
		for i in 0..change.diff_count() {
			let Some(diff) = change.diff(i) else {
				continue;
			};
			let Some(post) = diff.post() else {
				continue;
			};
			for r in 0..post.row_count() {
				let g = post.row(r).expect("row").i32("g").expect("g");
				let group = GroupId::of(&EncodedKey::new(g.to_be_bytes()));
				ctx.state().set(
					&managed_key_in(group, &[]).expect("an empty id fits the keyspace"),
					&1i64,
				)?;
			}
		}
		Ok(())
	}
}

struct Untracked;

impl OperatorMetadata for Untracked {
	const NAME: &'static str = "untracked";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only unmanaged operator that does nothing";
	const INPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl UnmanagedOperator for Untracked {
	const UNMANAGED_BECAUSE: &'static str = "test operator";
	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: false,
		kinds: &[],
		domain: WindowSizeDomain::Time,
		needs_pane: false,
		throttles: false,
	};

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(Untracked)
	}

	fn apply(&mut self, _ctx: &mut impl GuestContext<Unmanaged>, _change: impl ChangeView) -> SdkResult<()> {
		Ok(())
	}
}

fn runtime() -> RuntimeConfig {
	RuntimeConfig::default().fatal(FatalConfig::disarmed())
}

fn memory() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_runtime_config(runtime())
			.with_flow(|f| {
				f.register_managed_operator::<Tally>().register_unmanaged_operator::<Untracked>()
			})
			.with_config(ConfigKey::MetricsFlushInterval, Value::duration_milliseconds(10))
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with flow"),
	)
}

fn sqlite(path: &TempDbPath) -> TestDb {
	TestDb::from(
		embedded::sqlite(SqliteConfig::new(path))
			.with_runtime_config(runtime())
			.with_flow(|f| {
				f.register_managed_operator::<Tally>().register_unmanaged_operator::<Untracked>()
			})
			.with_config(ConfigKey::MetricsFlushInterval, Value::duration_milliseconds(10))
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build sqlite db with flow"),
	)
}

fn managed_view(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { time: event(ts) }");
	db.admin(MANAGED_VIEW);
}

fn managed_view_with(db: &TestDb, with: &str) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { time: event(ts) }");
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::v {{ g: int4 }} AS {{ FROM app::t APPLY tally{{}} WITH {{ {with} }} }}"
	));
}

fn managed_keys(db: &TestDb) -> u64 {
	db.query(MANAGED_KEYS)
		.iter()
		.flat_map(|frame| column_values(frame, "keys"))
		.map(|value| match value {
			Value::Uint8(keys) => keys,
			other => panic!("the keys measure must be an unsigned count, found {other:?}"),
		})
		.sum()
}

fn await_managed_keys(db: &TestDb, want: u64) -> u64 {
	await_value(want, TIMEOUT, || managed_keys(db))
}

fn advance_to(db: &TestDb, instant: &str) {
	db.admin(&format!("call storage::advance(app::t, cast('{instant}', datetime))"));
	assert!(db.await_all_flows(TIMEOUT), "the flow must drain before its state can be judged");
}

#[test]
fn a_managed_apply_over_a_source_without_event_time_is_rejected_at_create() {
	// Without an event time the watermark never passes a write, so a managed group would never be freed.
	for time in ["", " with { time: processing }"] {
		let db = memory();
		db.admin("CREATE NAMESPACE app");
		db.admin(&format!("CREATE TABLE app::t {{ id: int4, g: int4, ts: datetime }}{time}"));

		let Err(err) = db.try_admin(MANAGED_VIEW) else {
			panic!("a managed apply over a table declared with '{time}' must be refused at create");
		};

		let diagnostic = err.diagnostic();
		assert_eq!(diagnostic.code, "FLOW_073", "table declared with '{time}': {diagnostic:?}");
	}
}

#[test]
fn only_a_managed_apply_needs_event_time() {
	// A check that also refused unmanaged operators would break every time-less view that holds no managed state.
	let db = memory();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::timed { id: int4, g: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE TABLE app::plain { id: int4, g: int4 }");

	let managed = db.try_admin(
		"CREATE DEFERRED VIEW app::m { g: int4 } AS { FROM app::timed APPLY tally{} WITH { lateness: 2s } }",
	);
	let unmanaged =
		db.try_admin("CREATE DEFERRED VIEW app::u { g: int4 } AS { FROM app::plain APPLY untracked{} }");

	assert!(managed.is_ok(), "a managed apply over an event-time source must be accepted: {managed:?}");
	assert!(unmanaged.is_ok(), "an unmanaged apply needs no event time: {unmanaged:?}");
}

#[test]
fn managed_state_is_freed_once_the_watermark_passes_its_write_plus_lateness_and_not_before() {
	// A group freed before its write plus lateness loses state a late row inside the lateness still needs.
	let db = memory();
	managed_view(&db);

	db.command(
		r#"INSERT app::t [
			{ id: 1, g: 1, ts: "2026-01-01T00:00:00Z" },
			{ id: 2, g: 2, ts: "2026-01-01T00:00:00Z" },
			{ id: 3, g: 3, ts: "2026-01-01T00:00:00Z" }
		]"#,
	);
	db.command(r#"INSERT app::t [{ id: 4, g: 4, ts: "2026-01-01T00:00:02.999Z" }]"#);

	assert_eq!(await_managed_keys(&db, 5), 5, "at 2.999s no group is due yet: 4 groups plus ROOT");

	advance_to(&db, "2026-01-01T00:00:03Z");
	assert_eq!(await_managed_keys(&db, 2), 2, "groups 1 to 3 are due at 3s; group 4 and ROOT must stay");

	advance_to(&db, "2026-01-01T00:00:05Z");
	assert_eq!(await_managed_keys(&db, 1), 1, "group 4 is due at 5s; ROOT must never be freed");
}

#[test]
fn a_capped_reclaim_fire_finishes_the_rest_on_its_retry() {
	// Without the retry the groups past the batch stay until some unrelated write arms a new timer.
	let db = memory();
	managed_view(&db);
	let rows: Vec<String> =
		(1..=300).map(|g| format!(r#"{{ id: {g}, g: {g}, ts: "2026-01-01T00:00:00Z" }}"#)).collect();

	db.command(&format!("INSERT app::t [{}]", rows.join(", ")));
	assert_eq!(await_managed_keys(&db, 301), 301, "300 groups plus ROOT");

	advance_to(&db, "2026-01-01T00:00:03Z");
	assert_eq!(await_managed_keys(&db, 45), 45, "one fire frees at most 256 groups: 44 left plus ROOT");

	advance_to(&db, "2026-01-01T00:00:03.001Z");
	assert_eq!(await_managed_keys(&db, 1), 1, "the retry 1ms later frees the other 44");
}

#[test]
fn a_due_entry_armed_before_a_restart_still_frees_its_group_after_it() {
	// Due entries and the reclaim timer must be stored state, otherwise a restart leaks every armed group.
	let path = TempDbPath::new("managed_reclaim_restart");
	{
		let mut db = sqlite(&path);
		managed_view(&db);
		db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "2026-01-01T00:00:00Z" }]"#);
		assert_eq!(await_managed_keys(&db, 2), 2, "group 1 plus ROOT before the restart");
		db.stop();
	}

	let mut db = sqlite(&path);
	assert_eq!(await_managed_keys(&db, 2), 2, "the census must see the stored keys before the sweep can be judged");

	advance_to(&db, "2026-01-01T00:00:03Z");
	assert_eq!(await_managed_keys(&db, 1), 1, "group 1 armed before the restart must be freed after it");
	db.stop();
}

#[test]
fn a_retention_above_the_lateness_frees_later_than_the_lateness_would() {
	// Reclaim by the lateness would free the group at 2s, a second before the declared retention allows.
	let db = memory();
	managed_view_with(&db, "lateness: 1s, retention: 2s");

	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "2026-01-01T00:00:00Z" }]"#);
	assert_eq!(await_managed_keys(&db, 2), 2, "group 1 plus ROOT");

	advance_to(&db, "2026-01-01T00:00:02Z");
	db.command(r#"INSERT app::t [{ id: 2, g: 2, ts: "2026-01-01T00:00:02Z" }]"#);
	assert_eq!(await_managed_keys(&db, 3), 3, "at 2s the lateness has passed but group 1 is inside its retention");

	advance_to(&db, "2026-01-01T00:00:03Z");
	assert_eq!(await_managed_keys(&db, 2), 2, "group 1 is due at 3s; group 2 and ROOT must stay");
}

#[test]
fn a_zero_retention_frees_at_the_next_gate_step() {
	// Zero is a bound, not an absence: the group must go as soon as the watermark passes its write.
	let db = memory();
	managed_view_with(&db, "retention: 0s");

	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "2026-01-01T00:00:00.500Z" }]"#);
	assert_eq!(await_managed_keys(&db, 2), 2, "group 1 plus ROOT");

	advance_to(&db, "2026-01-01T00:00:01Z");
	assert_eq!(await_managed_keys(&db, 1), 1, "0.5s plus 1ms rounds up to 1s; ROOT must never be freed");
}

#[test]
fn a_group_rewritten_inside_its_retention_is_freed_from_its_last_write_not_its_first() {
	// Freeing a rewritten group at its first write's due drops the state the rewrite just extended.
	let db = memory();
	managed_view(&db);

	db.command(
		r#"INSERT app::t [
			{ id: 1, g: 1, ts: "2026-01-01T00:00:00Z" },
			{ id: 2, g: 2, ts: "2026-01-01T00:00:00Z" }
		]"#,
	);
	db.command(r#"INSERT app::t [{ id: 3, g: 1, ts: "2026-01-01T00:00:02Z" }]"#);

	assert_eq!(await_managed_keys(&db, 3), 3, "two groups plus ROOT before anything is due");

	advance_to(&db, "2026-01-01T00:00:03Z");
	assert_eq!(
		await_managed_keys(&db, 2),
		2,
		"group 2 is due at 3s; group 1 was rewritten at 2s and stays with ROOT"
	);

	advance_to(&db, "2026-01-01T00:00:05Z");
	assert_eq!(await_managed_keys(&db, 1), 1, "group 1 is due at 5s from its last write; ROOT must never be freed");
}
