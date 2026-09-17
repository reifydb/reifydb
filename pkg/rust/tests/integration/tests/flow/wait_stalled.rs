// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::{
		Arc, Condvar, Mutex,
		atomic::{AtomicBool, Ordering},
	},
	thread,
	time::{Duration, Instant},
};

use reifydb::{
	RuntimeConfig, WithSubsystem,
	core::{interface::{catalog::flow::OperatorId, flow::OperatorCapability}, operator_with::ApplyWith},
	embedded,
	runtime::context::clock::{Clock, MockClock},
	sdk::{
		error::Result as SdkResult,
		flow::operator::{
			GuestOperator, OperatorMetadata, column::operator::OperatorColumn, context::GuestContext,
			view::ChangeView,
		},
	},
	sub::subsystem::HealthStatus,
	testing::db::TestDb,
	value::{config::ExtensionParams, value::duration::Duration as ValueDuration},
};
use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

const TICK: Duration = Duration::from_millis(20);

const JUMP: Duration = Duration::from_secs(31);

static GATE: Gate = Gate::new();

struct Gate {
	state: Mutex<GateState>,
	changed: Condvar,
}

struct GateState {
	entered: bool,
	open: bool,
}

impl Gate {
	const fn new() -> Self {
		Self {
			state: Mutex::new(GateState {
				entered: false,
				open: false,
			}),
			changed: Condvar::new(),
		}
	}

	fn pass(&self) {
		let mut state = self.state.lock().unwrap();
		state.entered = true;
		self.changed.notify_all();
		while !state.open {
			state = self.changed.wait(state).unwrap();
		}
	}

	fn await_entered(&self, timeout: Duration) -> bool {
		let state = self.state.lock().unwrap();
		let (state, _) = self.changed.wait_timeout_while(state, timeout, |state| !state.entered).unwrap();
		state.entered
	}

	fn open(&self) {
		self.state.lock().unwrap().open = true;
		self.changed.notify_all();
	}
}

struct OpenOnDrop;

impl Drop for OpenOnDrop {
	fn drop(&mut self) {
		// a gate left shut keeps the flow actor blocked, so the database drop would hang instead of failing
		GATE.open();
	}
}

struct Wedged;

impl OperatorMetadata for Wedged {
	const NAME: &'static str = "wedged";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Blocks in apply until the test opens its gate";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[OperatorColumn {
		name: "id",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
		description: "Never emitted",
	}];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl GuestOperator for Wedged {
	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(Wedged)
	}

	fn apply(&mut self, _ctx: &mut impl GuestContext, _change: impl ChangeView) -> SdkResult<()> {
		GATE.pass();
		Ok(())
	}
}

#[test]
fn waiting_on_a_flow_wedged_inside_an_operator_fails_fast_naming_the_stall() {
	// the wedge blocks the flow's own actor, so only a check running off that actor can ever report it
	let clock = MockClock::from_millis(0);
	let db = TestDb::from(
		embedded::memory()
			.with_runtime_config(RuntimeConfig::default().clock(Clock::Mock(clock.clone())))
			.with_flow(|f| f.register_operator::<Wedged>())
			.build()
			.unwrap(),
	);
	let _open = OpenOnDrop;
	db.admin("create namespace test");
	db.admin("create table test::src { id: int4 }");
	db.admin("create deferred view test::wedged { id: int4 } as { from test::src apply wedged{} }");
	db.command("insert test::src [{ id: 1 }]");
	assert!(GATE.await_entered(Duration::from_secs(10)), "the flow never reached the wedged operator");

	// the watch must see the flow waiting before a jump counts, so one jump taken too early would never expire
	let ticking = Arc::new(AtomicBool::new(true));
	let ticker = {
		let ticking = Arc::clone(&ticking);
		let clock = clock.clone();
		thread::spawn(move || {
			while ticking.load(Ordering::SeqCst) {
				clock.advance_millis(JUMP.as_millis() as u64);
				thread::sleep(TICK);
			}
		})
	};

	let target = db.watermarks().tx().current().unwrap();
	let started = Instant::now();
	let result = db.watermarks().cdc().wait_for_flow_consumer(target, ValueDuration::from_seconds(30).unwrap());
	let elapsed = started.elapsed();
	ticking.store(false, Ordering::SeqCst);
	ticker.join().unwrap();

	let err = result.expect_err("a wedged flow must make the wait an error, never a plain timeout");
	assert!(elapsed < Duration::from_secs(10), "the wait must fail soon after the stall, took {elapsed:?}");
	let message = format!("{err}");
	assert!(
		message.contains("stalled with input pending"),
		"the error must name the stall rather than some other failure, got: {message}"
	);

	// a monitor that only reads health status must see the same wedge the wait just reported
	let status = db.get_all_component_health().remove("flow").expect("the flow subsystem is registered").status;
	let HealthStatus::Degraded {
		description,
	} = &status
	else {
		panic!("a wedged flow must degrade the flow subsystem, got {status:?}");
	};
	assert!(
		description.contains("stalled with input pending"),
		"the degraded status must name the stall, got: {description}"
	);
}
