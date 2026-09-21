// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	operator_with::ApplyWith,
};
use reifydb_runtime::{RuntimeConfig, fatal::FatalConfig};
use reifydb_sdk::{
	error::Result as SdkResult,
	flow::operator::{
		ManagedOperator, NostateOperator, OperatorMetadata, UnmanagedOperator,
		column::operator::OperatorColumn,
		context::{GuestContext, Managed, Nostate, Unmanaged},
		view::ChangeView,
	},
};
use reifydb_test_harness::assert::column_values;
use reifydb_value::{
	config::ExtensionParams,
	value::{constraint::TypeConstraint, value_type::ValueType},
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
	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(UnmanagedProbe)
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
fn a_managed_apply_with_zero_lateness_fails_the_create() {
	// Zero lateness is the absence of a bound, not a declared one, so it must fail exactly as a missing key does.
	let db = memory();
	event_time_source(&db);

	let Err(err) = db.try_admin(
		"CREATE DEFERRED VIEW app::v { g: int4 } AS { FROM app::t APPLY managed_probe{} WITH { lateness: 0s } }",
	) else {
		panic!("a managed apply with zero lateness must be refused at create");
	};

	let diagnostic = err.diagnostic();
	assert_eq!(diagnostic.code, "FLOW_072", "{diagnostic:?}");
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
