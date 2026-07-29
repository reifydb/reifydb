// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A custom operator arms a timer and is called back when the flow watermark passes it. This is the
//! whole timer service driven through a real flow: the guest arms through the host callback, the
//! wheel persists the instant, the executor advances the event watermark from arriving rows, and the
//! due timer fires inside the same flow transaction with its emission routed to the sink view. The
//! operator never reads a clock and never receives a timestamp on apply - it reads the row's #time
//! (populated from the table's declared ts column) and hands the substrate an instant to call it
//! back at.

use std::time::Duration as StdDuration;

use reifydb::{ConfigKey, Value, WithSubsystem, embedded};
use reifydb_abi::{
	flow::diff::DiffType,
	operator::{capabilities::OperatorCapability, timer::TimerKind},
};
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
		timer::Timer,
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
	row,
	state::RawStatefulOperator,
};
use reifydb_test_harness::db::TestDb;
use reifydb_value::value::{constraint::TypeConstraint, datetime::DateTime, value_type::ValueType};

const TIMEOUT: StdDuration = StdDuration::from_secs(20);

// How long after a row's own event time the operator asks to be woken.
const DELAY_MS: u64 = 1_000;

// Far beyond anything this test's event times reach, so the retention pass never reclaims a group
// underneath the assertions. It is declared only because it is what puts the node in the event
// domain, which is what makes the substrate stamp event-time positions.
const SEAL_AFTER_MS: u64 = 3_600_000;

const ALARM_STATE: Keyspace = Keyspace::FIRST_CUSTOM;

struct AlarmRow {
	g: i32,
	fired_at: i64,
}

row!(AlarmRow {
	g: i32,
	fired_at: i64
});

const ALARM_COLUMNS: &[OperatorColumn] = &[
	OperatorColumn {
		name: "g",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
		description: "group key carried through the timer key",
	},
	OperatorColumn {
		name: "fired_at",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "the instant the timer was armed for, as handed back on the callback",
	},
];

struct Alarm {
	seal_after_ms: u64,
}

impl RawStatefulOperator for Alarm {}

impl OperatorMetadata for Alarm {
	const NAME: &'static str = "alarm";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only operator that emits solely from timer callbacks";
	const INPUT_COLUMNS: &'static [OperatorColumn] = ALARM_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = ALARM_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD_WITH_RECLAIM;
}

fn group_key(g: i32) -> EncodedKey {
	EncodedKey::new(g.to_be_bytes())
}

impl OperatorLogic for Alarm {
	fn create(_operator_id: FlowNodeId, config: &Config) -> SdkResult<Self> {
		Ok(Alarm {
			seal_after_ms: config.u64_or("seal", SEAL_AFTER_MS),
		})
	}

	fn seal_after_ms(&self) -> Option<u64> {
		Some(self.seal_after_ms)
	}

	// Emits nothing. Every row this operator ever produces comes out of on_timer, so a view row is
	// proof that a callback ran - not that a change passed through.
	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> SdkResult<()> {
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
				let g = row.i32("g").expect("g");
				// The row's own event time, which the substrate populated from the
				// table's declared ts column. The operator is never told "now".
				let at = row
					.row_time()
					.expect("the substrate must populate #time on an event-time source");
				let key = group_key(g);
				// Interning here is what carries the node's retention position forward, exactly as
				// in any stateful operator: the substrate stamps this row's event time.
				ctx.intern_group(&key)?;
				let wake = DateTime::from_millis(at.to_millis() + DELAY_MS);
				ctx.arm_timer(wake, TimerKind::Seal, &key)?;
			}
		}
		Ok(())
	}

	fn on_timer(&mut self, ctx: &mut impl OperatorContext, timer: Timer<'_>) -> SdkResult<()> {
		let g = i32::from_be_bytes(timer.key.try_into().expect("the timer key round-trips the group key"));
		let key = group_key(g);
		let group = ctx.intern_group(&key)?;

		// Per-group state, so the group has something for the retention pass to erase once it ages
		// past its horizon. Without it a group is nothing but an identity and reclaim has no work.
		let fired_at = timer.at.to_millis() as i64;
		self.state_set(ctx, &OperatorStateKey::inner_encoded(group, ALARM_STATE, []), &fired_at)?;

		let (row_number, _is_new) = ctx.get_or_create_row_number(group, &key)?;
		ctx.emit_insert(
			&[AlarmRow {
				g,
				fired_at,
			}],
			&[row_number],
		)?;
		Ok(())
	}
}

fn setup() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_flow(|f| f.register_operator::<Alarm>())
			// The retention ledger is the only surface that reports what the reclaim pass
			// actually erased; without a refresh cadence it stays empty (none means off).
			.with_config(ConfigKey::MetricsLifecycleRefreshInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with flow"),
	)
}

fn declare(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { ts: ts }");
	db.admin(
		"CREATE DEFERRED VIEW app::v { g: int4, fired_at: int8 } with { time: event } AS { FROM app::t APPLY alarm{} }",
	);
}

#[test]
fn an_armed_timer_fires_only_once_the_event_watermark_passes_it() {
	// The core contract of the timer service, and the reason none of this reads a wall clock: a
	// timer armed for event instant T must stay silent while the flow's event watermark sits below
	// T, no matter how much real time passes or how many other rows arrive. Arriving rows are what
	// move time forward. Mutation: fire on arrival (dispatch without comparing against the
	// watermark) and the first assertion sees a row that has not come due yet.
	let db = setup();
	declare(&db);

	// Arms a callback for event instant 1000. The watermark is 0, so nothing is due.
	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "1970-01-01T00:00:00Z" }]"#);

	// A second row well short of the armed instant. It advances the watermark to 500 - still under
	// 1000 - so it must not release the timer, which is what separates "the watermark passed it"
	// from "another change came through".
	db.command(r#"INSERT app::t [{ id: 2, g: 2, ts: "1970-01-01T00:00:00.500Z" }]"#);
	assert!(db.await_all_flows(TIMEOUT), "the flow must drain before the view can be judged empty");

	assert_eq!(db.row_count("FROM app::v"), 0, "no timer is due at watermark 500; the view must still be empty");

	// Crosses both armed instants (1000 and 1500) at once.
	db.command(r#"INSERT app::t [{ id: 3, g: 3, ts: "1970-01-01T00:01:00Z" }]"#);

	db.await_row_count("FROM app::v", 2, TIMEOUT);
}

#[test]
fn a_fired_timer_hands_back_the_instant_and_key_it_was_armed_with() {
	// The callback is the only channel through which an operator learns anything about time, so the
	// (at, key) pair it receives has to be exactly what was armed. Millis are the wire unit end to
	// end for this reason: arm(X) must come back as on_timer(X) byte for byte, because operators
	// key state by the instant. Mutation: truncate or rescale the instant anywhere along the wheel
	// key, the FFI wire, or the callback and fired_at stops matching ts + DELAY_MS.
	let db = setup();
	declare(&db);

	db.command(r#"INSERT app::t [{ id: 1, g: 7, ts: "1970-01-01T00:00:02.250Z" }]"#);
	// Carries the watermark past group 7's armed instant, and arms its own for 601000.
	db.command(r#"INSERT app::t [{ id: 2, g: 9, ts: "1970-01-01T00:10:00Z" }]"#);
	// Carries the watermark past group 9's. Its own timer (661000) stays armed and unfired, which
	// is what holds the view at exactly two rows.
	db.command(r#"INSERT app::t [{ id: 3, g: 11, ts: "1970-01-01T00:11:00Z" }]"#);

	db.await_row_count("FROM app::v", 2, TIMEOUT);

	let armed = |g: i32, fired_at: i64| {
		let rql = format!("FROM app::v filter {{ g == {g} and fired_at == {fired_at} }}");
		assert_eq!(
			db.row_count(&rql),
			1,
			"group {g} must be called back at exactly {fired_at}; view now: {:?}",
			db.query_as_root("FROM app::v", ())
		);
	};

	// 2250 + 1000, sub-second precision intact: a millisecond lost anywhere on the wire shows here.
	armed(7, 3_250);
	armed(9, 601_000);

	assert_eq!(
		db.row_count("FROM app::v"),
		2,
		"only timers the watermark has passed may fire; group 11's is still armed"
	);
}

#[test]
fn a_row_emitted_from_a_timer_carries_the_firing_instant_as_its_event_time() {
	// A timer emission is an event-time fact: it happened at the instant the timer was armed for,
	// not at the wall-clock moment the callback ran. If the emitted row is stamped from the wall
	// clock instead, every downstream event-time consumer of this view has its watermark yanked to
	// present-day - the runaway-watermark failure the whole design exists to avoid, and it is
	// invisible in the view's own columns. Mutation: drop the timer-instant stamping and #time comes
	// back as the wall clock, decades away from the assertion.
	let db = setup();
	declare(&db);

	db.command(r#"INSERT app::t [{ id: 1, g: 7, ts: "1970-01-01T00:00:02.250Z" }]"#);
	db.command(r#"INSERT app::t [{ id: 2, g: 9, ts: "1970-01-01T00:10:00Z" }]"#);

	db.await_row_count("FROM app::v", 1, TIMEOUT);

	let frames = db.query_as_root("FROM app::v filter { g == 7 }", ()).expect("query view");
	let stamps: Vec<DateTime> = frames.iter().flat_map(|frame| frame.time().iter().copied()).collect();

	assert_eq!(
		stamps,
		vec![DateTime::from_millis(3_250)],
		"the emitted row's #time must be the instant the timer fired for, not the wall clock"
	);
}

#[test]
fn interning_inside_a_callback_stamps_the_firing_instant_not_the_change_that_woke_it() {
	// A callback runs inside the transaction of whatever change advanced the watermark, so the
	// coordinate left over from that change is sitting right there - and stamping an intern with it
	// backdates nothing and post-dates everything: the group looks as fresh as the newest unrelated
	// row in the system and outlives its own retention horizon indefinitely. Groups touched only by
	// timers would then never be reclaimed, which no view query can reveal.
	//
	// The node ages groups against the highest position it has stamped, which the second row pins at
	// 600000, putting the seal cutoff at 599000. Group 1's callback fires at 1000 and re-interns:
	// stamped with the firing instant it sits far below the cutoff and its state is reclaimed, while
	// stamped with the coordinate of the row that woke it (600000) it sits above and survives.
	//
	// Mutation: drop the per-timer set_change_coordinate in the dispatch loop and this times out
	// with an empty ledger.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { ts: ts }");
	db.admin(
		"CREATE DEFERRED VIEW app::v { g: int4, fired_at: int8 } with { time: event } AS { FROM app::t APPLY alarm{ seal: 1000 } }",
	);

	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "1970-01-01T00:00:00Z" }]"#);
	// Releases group 1's timer, which re-interns group 1 from inside the callback.
	db.command(r#"INSERT app::t [{ id: 2, g: 2, ts: "1970-01-01T00:10:00Z" }]"#);

	db.await_row_count("FROM app::v filter { g == 1 }", 1, TIMEOUT);

	let reclaimed = db.await_row_count(
		"from system::metrics::lifecycle::current filter { class == 'operator-group-data' and work_done > 0 }",
		1,
		TIMEOUT,
	);
	assert_eq!(
		reclaimed, 1,
		"a group last touched by a timer callback must age from the firing instant, so its state \
		 becomes reclaimable once the watermark passes that instant plus the seal span"
	);
}

const SNOOZE_ARMED: Keyspace = Keyspace::FIRST_CUSTOM;

struct Snooze {
	disarm_offset_ms: u64,
}

impl RawStatefulOperator for Snooze {}

impl OperatorMetadata for Snooze {
	const NAME: &'static str = "snooze";
	const API: u32 = 2;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str =
		"test-only operator that re-arms one timer per group, cancelling the instant it armed before";
	const INPUT_COLUMNS: &'static [OperatorColumn] = ALARM_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = ALARM_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD_WITH_RECLAIM;
}

impl OperatorLogic for Snooze {
	fn create(_operator_id: FlowNodeId, config: &Config) -> SdkResult<Self> {
		Ok(Snooze {
			disarm_offset_ms: config.u64_or("disarm_offset", 0),
		})
	}

	fn seal_after_ms(&self) -> Option<u64> {
		Some(SEAL_AFTER_MS)
	}

	// This is the session-window shape reduced to its essentials: every row pushes the group's
	// wake-up later, so the instant armed a moment ago must be cancelled rather than left to fire.
	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> SdkResult<()> {
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
				let g = row.i32("g").expect("g");
				let at = row
					.row_time()
					.expect("the substrate must populate #time on an event-time source");
				let key = group_key(g);
				let group = ctx.intern_group(&key)?;
				let armed_key = OperatorStateKey::inner_encoded(group, SNOOZE_ARMED, []);

				if let Some(prior) = self.state_get::<i64>(ctx, &armed_key)? {
					// disarm_offset_ms is 0 in the honest case. A non-zero value aims the
					// disarm one millisecond past what was armed, which is how the control
					// test proves the wheel matches on the exact instant.
					let target = prior as u64 + self.disarm_offset_ms;
					ctx.disarm_timer(DateTime::from_millis(target), TimerKind::Seal, &key)?;
				}

				let wake = at.to_millis() + DELAY_MS;
				ctx.arm_timer(DateTime::from_millis(wake), TimerKind::Seal, &key)?;
				self.state_set(ctx, &armed_key, &(wake as i64))?;
			}
		}
		Ok(())
	}

	fn on_timer(&mut self, ctx: &mut impl OperatorContext, timer: Timer<'_>) -> SdkResult<()> {
		let g = i32::from_be_bytes(timer.key.try_into().expect("the timer key round-trips the group key"));
		let group = ctx.intern_group(&group_key(g))?;
		let fired_at = timer.at.to_millis() as i64;

		// Keyed by the firing instant rather than by the group, so a timer that should have been
		// cancelled surfaces as an extra row instead of overwriting the surviving timer's row. Without
		// this both tests below would read identically no matter what disarm did.
		let row_key = EncodedKey::new(timer.at.to_millis().to_be_bytes());
		let (row_number, _is_new) = ctx.get_or_create_row_number(group, &row_key)?;
		ctx.emit_insert(
			&[AlarmRow {
				g,
				fired_at,
			}],
			&[row_number],
		)?;
		Ok(())
	}
}

fn setup_snooze() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_flow(|f| f.register_operator::<Snooze>())
			.build()
			.expect("build memory db with flow"),
	)
}

fn declare_snooze(db: &TestDb, config: &str) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { ts: ts }");
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::v {{ g: int4, fired_at: int8 }} with {{ time: event }} AS {{ FROM app::t APPLY snooze{{{}}} }}",
		config
	));
}

// Group 1 arms at 1000, then re-arms at 1500 and cancels the 1000. Group 99 exists only to carry the
// watermark past both instants without touching group 1's armed timer.
fn feed_snooze(db: &TestDb) {
	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "1970-01-01T00:00:00Z" }]"#);
	db.command(r#"INSERT app::t [{ id: 2, g: 1, ts: "1970-01-01T00:00:00.500Z" }]"#);
	db.command(r#"INSERT app::t [{ id: 3, g: 99, ts: "1970-01-01T00:01:00Z" }]"#);
}

#[test]
fn a_guest_can_disarm_a_timer_it_armed() {
	// Until now a guest could only arm. Every window kind re-arms as its last event time rises, and
	// session windows re-arm on every single row, so without disarm the wheel accumulates one stale
	// timer per row and each one fires a seal for a window that has already moved on. This is the
	// guest half of the host proof at flow/src/transaction/timer.rs::
	// a_disarmed_timer_does_not_fire_and_its_replacement_does, driven all the way through the FFI
	// callback rather than against the wheel directly.
	//
	// Mutation: make host_disarm_timer a no-op returning FFI_OK and the cancelled 1000 fires too,
	// so the view carries two rows instead of one.
	let db = setup_snooze();
	declare_snooze(&db, "");
	feed_snooze(&db);

	db.await_row_count("FROM app::v", 1, TIMEOUT);

	assert_eq!(
		db.row_count("FROM app::v filter { fired_at == 1500 }"),
		1,
		"the surviving re-armed timer must fire at the instant the last row pushed it to"
	);
	assert_eq!(
		db.row_count("FROM app::v filter { fired_at == 1000 }"),
		0,
		"the instant the operator cancelled must never fire"
	);
}

#[test]
fn a_guest_disarm_must_match_the_exact_instant_it_armed() {
	// The control for the test above, and the reason it cannot pass for the wrong reason. A disarm
	// carries an instant across the FFI boundary as millis; if that instant were rounded, rescaled or
	// ignored on the way through, a disarm aimed anywhere near the armed timer would still cancel it
	// and the test above would stay green while the wheel had become a blunt instrument. Here the
	// operator aims one millisecond past what it armed, which must miss, leaving both timers live.
	//
	// Mutation: match on anything looser than the exact (at, kind, key) triple - a range, a
	// truncation to seconds, or ignoring the instant - and the 1000 disappears, dropping this to one
	// row.
	let db = setup_snooze();
	declare_snooze(&db, " disarm_offset: 1 ");
	feed_snooze(&db);

	db.await_row_count("FROM app::v", 2, TIMEOUT);

	assert_eq!(
		db.row_count("FROM app::v filter { fired_at == 1000 }"),
		1,
		"a disarm aimed one millisecond off must leave the armed timer alone"
	);
	assert_eq!(db.row_count("FROM app::v filter { fired_at == 1500 }"), 1, "the re-armed timer fires regardless");
}
