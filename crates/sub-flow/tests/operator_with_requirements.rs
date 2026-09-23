// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_core::{
	common::{OperatorClass, WindowRequirements, WindowSizeDomain},
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::HeapSize,
	operator_with::ApplyWith,
};
use reifydb_flow::window::{
	accumulator::invertible::last_value::LastValue, coord::OrdinalCoord, settings::WindowSettings, span::WindowSpan,
};
use reifydb_runtime::{RuntimeConfig, fatal::FatalConfig};
use reifydb_sdk::{
	error::Result as SdkResult,
	flow::operator::{
		ManagedMount, ManagedOperator, MountedOperator, NostateMount, NostateOperator, OperatorMetadata,
		UnmanagedMount, UnmanagedOperator,
		column::operator::OperatorColumn,
		context::{GuestContext, Managed, Nostate, Unmanaged, Windowed},
		view::{ChangeView, RowView},
		windowed::operator::{AllKinds, Emit, NoRolling, PlainMarker, TopKMarker, WindowedOperator},
	},
	row,
};
use reifydb_sub_api::subsystem::HealthStatus;
use reifydb_test_harness::assert::column_values;
use reifydb_value::{
	config::ExtensionParams,
	value::{constraint::TypeConstraint, datetime::DateTime, value_type::ValueType},
};

const G_COLUMNS: &[OperatorColumn] = &[OperatorColumn {
	name: "g",
	type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
	description: "group key",
}];

const NOSTATE_VIEW: &str =
	"CREATE DEFERRED VIEW app::v { g: int4 } AS { FROM app::t APPLY nostate_probe{} WITH { lateness: 2s } }";

struct NostateProbe;

impl OperatorMetadata for NostateProbe {
	const NAME: &'static str = "nostate_probe";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only nostate operator that does nothing";
	const INPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl NostateOperator for NostateProbe {
	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(NostateProbe)
	}

	fn apply(&mut self, _ctx: &mut impl GuestContext<Nostate>, _change: impl ChangeView) -> SdkResult<()> {
		Ok(())
	}
}

struct ManagedProbe;

impl OperatorMetadata for ManagedProbe {
	const NAME: &'static str = "managed_probe";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only managed operator that does nothing";
	const INPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl ManagedOperator for ManagedProbe {
	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(ManagedProbe)
	}

	fn apply(&mut self, _ctx: &mut impl GuestContext<Managed>, _change: impl ChangeView) -> SdkResult<()> {
		Ok(())
	}
}

struct UnmanagedProbe;

impl OperatorMetadata for UnmanagedProbe {
	const NAME: &'static str = "unmanaged_probe";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only unmanaged operator that does nothing";
	const INPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl UnmanagedOperator for UnmanagedProbe {
	const UNMANAGED_BECAUSE: &'static str = "test operator";
	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: false,
		kinds: &[],
		domain: WindowSizeDomain::Time,
		needs_pane: false,
	};

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(UnmanagedProbe)
	}

	fn apply(&mut self, _ctx: &mut impl GuestContext<Unmanaged>, _change: impl ChangeView) -> SdkResult<()> {
		Ok(())
	}
}

struct UnmanagedWindowProbe;

impl OperatorMetadata for UnmanagedWindowProbe {
	const NAME: &'static str = "unmanaged_window_probe";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only unmanaged operator that takes a tumbling window";
	const INPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl UnmanagedOperator for UnmanagedWindowProbe {
	const UNMANAGED_BECAUSE: &'static str = "test operator";
	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: true,
		kinds: &["tumbling"],
		domain: WindowSizeDomain::Time,
		needs_pane: false,
	};

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(UnmanagedWindowProbe)
	}

	fn apply(&mut self, _ctx: &mut impl GuestContext<Unmanaged>, _change: impl ChangeView) -> SdkResult<()> {
		Ok(())
	}
}

fn runtime() -> RuntimeConfig {
	RuntimeConfig::default().fatal(FatalConfig::disarmed())
}

fn memory() -> TestDb {
	// Every class the create check branches on must be registered, or a skipped branch would read as a pass.
	TestDb::from(
		embedded::memory()
			.with_runtime_config(runtime())
			.with_flow(|f| {
				f.register_nostate_operator::<NostateProbe>()
					.register_managed_operator::<ManagedProbe>()
					.register_unmanaged_operator::<UnmanagedProbe>()
					.register_unmanaged_operator::<UnmanagedWindowProbe>()
			})
			.build()
			.expect("build memory db with flow"),
	)
}

fn event_time_source(db: &TestDb) {
	// A managed apply is refused with FLOW_073 unless its source carries event time, which would mask FLOW_072.
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { time: event(ts) }");
}

#[test]
fn a_nostate_apply_with_a_with_block_fails_the_create() {
	// A nostate operator honours no seal, so a with block on it must be refused before any flow exists.
	let db = memory();
	event_time_source(&db);

	let Err(err) = db.try_admin(NOSTATE_VIEW) else {
		panic!("a nostate apply carrying a with block must be refused at create");
	};

	let diagnostic = err.diagnostic();
	assert_eq!(diagnostic.code, "FLOW_071", "{diagnostic:?}");
}

#[test]
fn a_managed_apply_without_lateness_fails_the_create() {
	// Without lateness a managed operator's state has no bound, and flow start is too late to say so.
	let db = memory();
	event_time_source(&db);

	let Err(err) = db.try_admin("CREATE DEFERRED VIEW app::v { g: int4 } AS { FROM app::t APPLY managed_probe{} }")
	else {
		panic!("a managed apply without lateness must be refused at create");
	};

	let diagnostic = err.diagnostic();
	assert_eq!(diagnostic.code, "FLOW_072", "{diagnostic:?}");
}

#[test]
fn a_managed_apply_with_zero_lateness_creates() {
	// Zero lateness is a real bound now: no output hold, and the state is freed at the next gate step.
	let db = memory();
	event_time_source(&db);

	let created = db.try_admin(
		"CREATE DEFERRED VIEW app::v { g: int4 } AS { FROM app::t APPLY managed_probe{} WITH { lateness: 0s } }",
	);

	assert!(created.is_ok(), "a managed apply with zero lateness must create: {created:?}");
}

#[test]
fn an_unmanaged_apply_with_a_with_block_creates_without_a_with_check() {
	// A check that also caught unmanaged operators would refuse every view that legitimately declares lateness.
	let db = memory();
	event_time_source(&db);

	let created = db.try_admin(
		"CREATE DEFERRED VIEW app::v { g: int4 } AS { FROM app::t APPLY unmanaged_probe{} WITH { lateness: 2s } }",
	);

	assert!(created.is_ok(), "an unmanaged apply takes its with block untouched: {created:?}");
}

#[test]
fn a_rejected_create_registers_no_flow() {
	// create_flow runs before these checks, so a refused create must roll its flow row back or leave a half-built
	// view.
	let db = memory();
	event_time_source(&db);

	let Err(err) = db.try_admin(NOSTATE_VIEW) else {
		panic!("a nostate apply carrying a with block must be refused at create");
	};
	let diagnostic = err.diagnostic();
	assert_eq!(diagnostic.code, "FLOW_071", "{diagnostic:?}");

	let rows: usize = db
		.query("from system::flows filter { name == 'v' }")
		.iter()
		.map(|frame| column_values(frame, "name").len())
		.sum();

	assert_eq!(rows, 0, "a refused create must leave no flow row, found {rows}");
}

#[test]
fn an_unmanaged_apply_declaring_a_window_creates_with_one() {
	// Without this an unmanaged operator can never be given the window it needs and its flow dies at start.
	let db = memory();
	event_time_source(&db);

	let created = db.try_admin(
		"CREATE DEFERRED VIEW app::v { g: int4 } AS { FROM app::t APPLY unmanaged_window_probe{} WITH { window: tumbling, duration: 5s } }",
	);

	assert!(created.is_ok(), "an unmanaged apply declaring a window must take one: {created:?}");
}

#[test]
fn an_unmanaged_apply_declaring_a_window_is_refused_without_one() {
	// Otherwise the missing window surfaces only when the operator is built, long after create returned ok.
	let db = memory();
	event_time_source(&db);

	let Err(err) = db
		.try_admin("CREATE DEFERRED VIEW app::v { g: int4 } AS { FROM app::t APPLY unmanaged_window_probe{} }")
	else {
		panic!("an unmanaged apply that needs a window must be refused without one");
	};

	let diagnostic = err.diagnostic();
	assert_eq!(diagnostic.code, "FLOW_065", "{diagnostic:?}");
}

#[test]
fn an_unmanaged_apply_declaring_a_window_is_refused_with_another_kind() {
	// A declared kind list no one checks would hand a tumbling-only operator a sliding window.
	let db = memory();
	event_time_source(&db);

	let Err(err) = db.try_admin(
		"CREATE DEFERRED VIEW app::v { g: int4 } AS { FROM app::t APPLY unmanaged_window_probe{} WITH { window: sliding, duration: 5s, slide: 1s } }",
	) else {
		panic!("a tumbling-only operator must refuse a sliding window");
	};

	let diagnostic = err.diagnostic();
	assert_eq!(diagnostic.code, "FLOW_066", "{diagnostic:?}");
}

#[test]
fn an_unmanaged_apply_declaring_no_window_is_still_refused_with_one() {
	// Forwarding the declaration must never open the gate for unmanaged operators that take no window.
	let db = memory();
	event_time_source(&db);

	let Err(err) = db.try_admin(
		"CREATE DEFERRED VIEW app::v { g: int4 } AS { FROM app::t APPLY unmanaged_probe{} WITH { window: tumbling, duration: 5s } }",
	) else {
		panic!("an unmanaged apply that takes no window must be refused with one");
	};

	let diagnostic = err.diagnostic();
	assert_eq!(diagnostic.code, "FLOW_067", "{diagnostic:?}");
}

#[test]
fn nostate_managed_and_unmanaged_probes_each_publish_takes_window_false() {
	// a mount that claimed a window would let a view give it one at create, which the mount then refuses
	let no_window = WindowRequirements {
		takes_window: false,
		kinds: &[],
		domain: WindowSizeDomain::Time,
		needs_pane: false,
	};
	assert_eq!(<NostateMount<NostateProbe> as MountedOperator>::WINDOW, no_window);
	assert_eq!(<ManagedMount<ManagedProbe> as MountedOperator>::WINDOW, no_window);
	assert_eq!(<UnmanagedMount<UnmanagedProbe> as MountedOperator>::WINDOW, no_window);
}

#[test]
fn only_an_unmanaged_operator_reaches_the_library_with_its_reason() {
	// a reason lost on the way, or one published for another class, makes the census name the wrong owner
	let db = memory();
	let library = db.engine().operator_store();
	let published = |name: &str| library.get(name).map(|info| (info.class, info.unmanaged_because));
	assert_eq!(published("unmanaged_probe"), Some((OperatorClass::Unmanaged, Some("test operator".to_string()))));
	assert_eq!(published("managed_probe"), Some((OperatorClass::Managed, None)));
	assert_eq!(published("nostate_probe"), Some((OperatorClass::Nostate, None)));
}

struct GRow {
	g: i32,
}

row!(GRow {
	g: i32
});

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, PartialEq, HeapSize)]
struct RankedRow {
	g: i32,
}

row!(RankedRow {
	g: i32
});

macro_rules! window_probe {
	($probe:ident, $name:literal, $coord:ty, $kinds:ty, $output:ty) => {
		struct $probe;

		impl OperatorMetadata for $probe {
			const NAME: &'static str = $name;
			const VERSION: &'static str = "0.0.1";
			const DESCRIPTION: &'static str = "test-only windowed operator that emits nothing";
			const INPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
			const OUTPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
			const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
		}

		impl WindowedOperator for $probe {
			type Coord = $coord;
			type GroupKey = u32;
			type Accumulator = LastValue<i64>;
			type Output = $output;

			fn create(_: OperatorId, _: &ExtensionParams, _: &ApplyWith) -> SdkResult<Self> {
				Ok(Self)
			}

			fn coord(&self, _: &impl RowView) -> Option<$coord> {
				None
			}

			fn extract(&self, _: &mut impl GuestContext<Windowed>, _: &impl RowView) -> Option<(u32, i64)> {
				None
			}

			fn new_accumulator(&self, _: &WindowSettings<$coord>) -> LastValue<i64> {
				LastValue::default()
			}
		}

		impl Emit for $probe {
			type Kinds = $kinds;

			fn build_output(&self, _: &u32, _: WindowSpan<$coord>, _: &i64) -> Option<$output> {
				None
			}
		}
	};
}

window_probe!(TimeWindowProbe, "time_window_probe", DateTime, NoRolling, GRow);
window_probe!(SlotWindowProbe, "slot_window_probe", OrdinalCoord, NoRolling, GRow);
window_probe!(RollingProbe, "rolling_probe", DateTime, AllKinds, GRow);
window_probe!(TopKProbe, "top_k_probe", DateTime, AllKinds, BTreeMap<u32, RankedRow>);

fn windowed_memory() -> TestDb {
	// Every window shape the create check branches on must be registered, or a skipped branch would read as a pass.
	TestDb::from(
		embedded::memory()
			.with_runtime_config(runtime())
			.with_flow(|f| {
				f.register_nostate_operator::<NostateProbe>()
					.register_managed_operator::<ManagedProbe>()
					.register_unmanaged_operator::<UnmanagedProbe>()
					.register_windowed_operator::<TimeWindowProbe, PlainMarker>()
					.register_windowed_operator::<SlotWindowProbe, PlainMarker>()
					.register_windowed_operator::<RollingProbe, PlainMarker>()
					.register_windowed_operator::<TopKProbe, TopKMarker>()
			})
			.build()
			.expect("build memory db with flow"),
	)
}

fn view(name: &str, apply: &str) -> String {
	format!("CREATE DEFERRED VIEW app::{name} {{ g: int4 }} AS {{ FROM app::t APPLY {apply} }}")
}

fn refused_code(db: &TestDb, apply: &str) -> String {
	let statement = view("v", apply);
	let Err(err) = db.try_admin(&statement) else {
		panic!("the create must be refused: {statement}");
	};
	err.diagnostic().code
}

fn flow_rows(db: &TestDb, name: &str) -> usize {
	db.query(&format!("from system::flows filter {{ name == '{name}' }}"))
		.iter()
		.map(|frame| column_values(frame, "name").len())
		.sum()
}

fn poisoned(db: &TestDb) -> Option<String> {
	match db.get_all_component_health().remove("flow").expect("the flow subsystem is registered").status {
		HealthStatus::Degraded {
			description,
		} if description.contains("poisoned") => Some(description),
		_ => None,
	}
}

#[test]
fn a_windowed_apply_without_a_window_fails_the_create() {
	// A windowed operator with no window has nothing to bucket by, and flow start is too late to say so.
	let db = windowed_memory();
	event_time_source(&db);

	assert_eq!(refused_code(&db, "time_window_probe{}"), "FLOW_065");
}

#[test]
fn a_no_rolling_operator_on_a_rolling_window_fails_the_create() {
	// A tumbling-only operator given a rolling window would otherwise poison its flow at start.
	let db = windowed_memory();
	event_time_source(&db);

	assert_eq!(
		refused_code(&db, "time_window_probe{} WITH { window: rolling, duration: 1h, pane: 1s, lateness: 2s }"),
		"FLOW_066"
	);
}

#[test]
fn a_window_on_an_operator_that_takes_none_fails_the_create() {
	// An unmanaged operator never reads a window, so accepting one would silently ignore the view's intent.
	let db = windowed_memory();
	event_time_source(&db);

	assert_eq!(refused_code(&db, "unmanaged_probe{} WITH { window: tumbling, duration: 1m }"), "FLOW_067");
}

#[test]
fn a_time_operator_given_a_slot_size_fails_the_create() {
	// A count read as a duration would size every window in the wrong unit.
	let db = windowed_memory();
	event_time_source(&db);

	assert_eq!(
		refused_code(&db, "time_window_probe{} WITH { window: tumbling, slots: 4, lateness: 2 }"),
		"FLOW_068"
	);
}

#[test]
fn a_slot_operator_given_a_duration_size_fails_the_create() {
	// A duration read as a slot count would size every window in the wrong unit.
	let db = windowed_memory();
	event_time_source(&db);

	assert_eq!(
		refused_code(&db, "slot_window_probe{} WITH { window: tumbling, duration: 1m, lateness: 2s }"),
		"FLOW_069"
	);
}

#[test]
fn a_slot_operator_given_a_duration_lateness_fails_the_create() {
	// A slot operator can only count late slots, so a duration lateness must be refused, not truncated.
	let db = windowed_memory();
	event_time_source(&db);

	assert_eq!(
		refused_code(&db, "slot_window_probe{} WITH { window: tumbling, slots: 4, lateness: 30s }"),
		"AST_005"
	);
}

#[test]
fn a_rolling_window_without_a_pane_fails_the_create_on_a_top_k_operator() {
	// A top-k operator buckets rows by pane, so without one it fails every window at runtime.
	let db = windowed_memory();
	event_time_source(&db);

	assert_eq!(refused_code(&db, "top_k_probe{} WITH { window: rolling, duration: 1h, lateness: 2s }"), "FLOW_075");
}

#[test]
fn a_rolling_window_without_a_pane_creates_on_an_operator_that_needs_none() {
	// The pane check must key on the operator's need, or every paneless rolling view would be refused.
	let db = windowed_memory();
	event_time_source(&db);

	let statement = view("v", "rolling_probe{} WITH { window: rolling, duration: 1h, lateness: 2s }");
	assert!(db.try_admin(&statement).is_ok(), "a plain rolling operator needs no pane: {statement}");
}

#[test]
fn a_session_window_with_a_lateness_fails_the_create() {
	// A session seals on its gap, so an accepted lateness would be silently ignored at runtime.
	let db = windowed_memory();
	event_time_source(&db);

	assert_eq!(
		refused_code(&db, "time_window_probe{} WITH { window: session, gap: 10s, lateness: 1ms }"),
		"FLOW_078"
	);
}

#[test]
fn a_session_window_with_no_or_zero_lateness_creates() {
	// Refusing every declared lateness would reject the lateness: 0s that the FLOW_078 help tells users to write.
	let db = windowed_memory();
	event_time_source(&db);

	for (name, apply) in [
		("none_v", "time_window_probe{} WITH { window: session, gap: 10s }"),
		("zero_v", "time_window_probe{} WITH { window: session, gap: 10s, lateness: 0s }"),
	] {
		let statement = view(name, apply);
		if let Err(err) = db.try_admin(&statement) {
			panic!("a session with no positive lateness must create: {statement}: {:?}", err.diagnostic());
		}
	}
}

#[test]
fn a_slot_operator_given_a_session_window_fails_the_create() {
	// A session needs a time gap; a slot operator that took one would fail its flow at start instead.
	let db = windowed_memory();
	event_time_source(&db);

	assert_eq!(refused_code(&db, "slot_window_probe{} WITH { window: session, gap: 10s }"), "FLOW_066");
	assert_eq!(
		refused_code(&db, "slot_window_probe{} WITH { window: session, gap: 0s }"),
		"FLOW_066",
		"the kinds check runs before the session check"
	);
}

#[test]
fn a_session_window_with_a_zero_gap_fails_the_create() {
	// A zero gap gives every session an empty span, so the create must fail before any row is published.
	let db = windowed_memory();
	event_time_source(&db);

	assert_eq!(refused_code(&db, "time_window_probe{} WITH { window: session, gap: 0s }"), "FLOW_079");
}

#[test]
fn well_formed_time_and_slot_windows_create() {
	// The window checks must not refuse a view that matches its operator exactly.
	let db = windowed_memory();
	event_time_source(&db);

	for (name, apply) in [
		("time_v", "time_window_probe{} WITH { window: tumbling, duration: 1m, lateness: 2s }"),
		("slot_v", "slot_window_probe{} WITH { window: tumbling, slots: 4, lateness: 2 }"),
		("top_v", "top_k_probe{} WITH { window: rolling, duration: 1h, pane: 1s, lateness: 2s }"),
	] {
		let statement = view(name, apply);
		if let Err(err) = db.try_admin(&statement) {
			panic!("a well formed window must create: {statement}: {:?}", err.diagnostic());
		}
	}
}

#[test]
fn every_create_time_refusal_leaves_no_flow_and_no_poison() {
	// A refusal that still registered a flow would poison it at start, the exact failure these checks replace.
	let db = windowed_memory();
	event_time_source(&db);

	for (code, apply) in [
		("FLOW_065", "time_window_probe{}"),
		("FLOW_066", "time_window_probe{} WITH { window: rolling, duration: 1h, pane: 1s, lateness: 2s }"),
		("FLOW_067", "unmanaged_probe{} WITH { window: tumbling, duration: 1m }"),
		("FLOW_068", "time_window_probe{} WITH { window: tumbling, slots: 4, lateness: 2 }"),
		("FLOW_069", "slot_window_probe{} WITH { window: tumbling, duration: 1m, lateness: 2s }"),
		("AST_005", "slot_window_probe{} WITH { window: tumbling, slots: 4, lateness: 30s }"),
		("FLOW_071", "nostate_probe{} WITH { lateness: 2s }"),
		("FLOW_072", "managed_probe{}"),
		("FLOW_075", "top_k_probe{} WITH { window: rolling, duration: 1h, lateness: 2s }"),
		("FLOW_078", "time_window_probe{} WITH { window: session, gap: 10s, lateness: 1ms }"),
		("FLOW_079", "time_window_probe{} WITH { window: session, gap: 0s }"),
		("FLOW_080", "unmanaged_probe{} WITH { retention: 1h }"),
		("AST_005", "managed_probe{} WITH { lateness: 2s, retention: 1s }"),
	] {
		assert_eq!(refused_code(&db, apply), code, "{apply}");
		assert_eq!(flow_rows(&db, "v"), 0, "{code}: a refused create must register no flow");
		assert_eq!(poisoned(&db), None, "{code}: a refused create must poison nothing");
	}
}

#[test]
fn a_retention_alone_creates_a_managed_apply() {
	// Retention is the bound the managed reclaim reads, so it must satisfy the create without any lateness.
	let db = memory();
	event_time_source(&db);

	for (name, apply) in [
		("hour_v", "managed_probe{} WITH { retention: 1h }"),
		("zero_v", "managed_probe{} WITH { retention: 0s }"),
		("both_v", "managed_probe{} WITH { lateness: 2s, retention: 1h }"),
	] {
		let statement = view(name, apply);
		if let Err(err) = db.try_admin(&statement) {
			panic!("a managed apply with a retention must create: {statement}: {:?}", err.diagnostic());
		}
	}
}

#[test]
fn a_retention_on_an_unmanaged_or_windowed_apply_fails_the_create() {
	// Nothing but the managed reclaim reads retention, so accepting it elsewhere leaves a bound nobody honours.
	let db = windowed_memory();
	event_time_source(&db);

	assert_eq!(refused_code(&db, "unmanaged_probe{} WITH { retention: 1h }"), "FLOW_080");
	assert_eq!(
		refused_code(
			&db,
			"time_window_probe{} WITH { window: tumbling, duration: 1m, lateness: 2s, retention: 1h }"
		),
		"FLOW_080"
	);
}

#[test]
fn a_retention_below_the_lateness_fails_the_create() {
	// A retention under the lateness frees a group while a timer inside the hold can still fire for it.
	let db = memory();
	event_time_source(&db);

	assert_eq!(refused_code(&db, "managed_probe{} WITH { lateness: 2s, retention: 1s }"), "AST_005");
}
