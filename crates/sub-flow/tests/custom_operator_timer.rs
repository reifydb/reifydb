// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A custom operator arms a timer and is called back when the flow watermark passes it. The
//! operator never reads a clock and is never handed a timestamp on apply: it arms off the row's own
//! #time, and the due timer fires inside the flow transaction that carried the watermark past it.

use std::time::Duration as StdDuration;

use reifydb::{ConfigKey, Value, WithSubsystem, embedded, testing::db::TestDb};
use reifydb_abi::{
	flow::diff::DiffType,
	operator::{capabilities::OperatorCapability, timer::TimerKind},
};
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
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
use reifydb_value::value::{constraint::TypeConstraint, datetime::DateTime, duration::Duration, value_type::ValueType};

const TIMEOUT: StdDuration = StdDuration::from_secs(20);

// How long after a row's own event time the operator asks to be woken.
const DELAY_MS: u64 = 1_000;

// Far beyond anything these event times reach, so retention never reclaims a group underneath an
// assertion. It is declared only because declaring it is what puts the node in the event domain.
const SEAL_AFTER_MS: u64 = 3_600_000;

const ALARM_STATE: Keyspace = Keyspace::CUSTOM;

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
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

fn group_key(g: i32) -> EncodedKey {
	EncodedKey::new(g.to_be_bytes())
}

impl OperatorLogic for Alarm {
	fn create(_operator_id: OperatorId, config: &Config) -> SdkResult<Self> {
		Ok(Alarm {
			seal_after_ms: config.u64_or("seal", SEAL_AFTER_MS),
		})
	}

	fn seal_after(&self) -> Option<Duration> {
		Some(Duration::from_milliseconds_const(self.seal_after_ms as i64))
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> SdkResult<()> {
		// Emits nothing. Every row this operator produces comes out of on_timer, so a view row is
		// proof that a callback ran - not that a change passed through.
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
				// The row's own event time; the operator is never told "now".
				let at = row
					.row_time()
					.expect("the substrate must populate #time on an event-time source");
				let key = group_key(g);
				// Interning carries the node's retention position forward at this row's event time.
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
			// actually erased; a short sample cadence keeps the polls inside their timeouts.
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with flow"),
	)
}

fn declare(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE DEFERRED VIEW app::v { g: int4, fired_at: int8 } AS { FROM app::t APPLY alarm{} }");
}

#[test]
fn an_armed_timer_fires_only_once_the_event_watermark_passes_it() {
	// A timer armed for event instant T must stay silent while the flow's event watermark sits
	// below T, however much real time passes or however many other rows arrive. Arriving rows are
	// the only thing that moves time forward.
	let db = setup();
	declare(&db);

	// Arms a callback for event instant 1000. The watermark is 0, so nothing is due.
	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "1970-01-01T00:00:00Z" }]"#);

	// Advances the watermark to 500, still under 1000, separating "the watermark passed it" from
	// "another change came through".
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
	// (at, key) pair must come back byte for byte as armed - operators key state by the instant, so
	// a truncation or rescale anywhere on the wire corrupts it silently.
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
	// A timer emission is an event-time fact: it happened at the instant armed for, not at the
	// wall-clock moment the callback ran. Stamping from the clock yanks every downstream
	// event-time consumer's watermark to the present, invisibly in the view's own columns.
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
	// A callback runs inside the transaction of whatever change advanced the watermark, so stamping
	// an intern with that change's coordinate makes the group look as fresh as the newest unrelated
	// row and outlive its horizon forever - a leak no view query can reveal.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { time: event(ts) }");
	db.admin(
		"CREATE DEFERRED VIEW app::v { g: int4, fired_at: int8 } AS { FROM app::t APPLY alarm{ seal: 1000 } }",
	);

	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "2026-01-01T00:00:00Z" }]"#);
	// Releases group 1's timer, which re-interns group 1 from inside the callback.
	db.command(r#"INSERT app::t [{ id: 2, g: 2, ts: "2026-01-01T00:10:00Z" }]"#);

	db.await_row_count("FROM app::v filter { g == 1 }", 1, TIMEOUT);

	let reclaimed = db.await_row_count(
		"from system::metrics::runtime::operators::current filter { metric == 'state_compaction_dropped' and value > 0.0 }",
		1,
		TIMEOUT,
	);
	assert_eq!(
		reclaimed, 1,
		"a group last touched by a timer callback must age from the firing instant, so its state \
		 becomes reclaimable once the watermark passes that instant plus the seal span"
	);
}

const SNOOZE_ARMED: Keyspace = Keyspace::CUSTOM;

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
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl OperatorLogic for Snooze {
	fn create(_operator_id: OperatorId, config: &Config) -> SdkResult<Self> {
		Ok(Snooze {
			disarm_offset_ms: config.u64_or("disarm_offset", 0),
		})
	}

	fn seal_after(&self) -> Option<Duration> {
		Some(Duration::from_milliseconds_const(SEAL_AFTER_MS as i64))
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> SdkResult<()> {
		// The session-window shape reduced to essentials: every row pushes the group's wake-up
		// later, so the instant armed a moment ago must be cancelled rather than left to fire.
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
					// Zero in the honest case; non-zero aims the disarm past what
					// was armed, which is how the control test proves the wheel
					// matches on the exact instant.
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

		// Keyed by the firing instant, not the group, so a timer that should have been cancelled
		// surfaces as an extra row instead of overwriting the surviving timer's.
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
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { time: event(ts) }");
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::v {{ g: int4, fired_at: int8 }} AS {{ FROM app::t APPLY snooze{{{}}} }}",
		config
	));
}

fn feed_snooze(db: &TestDb) {
	// Group 1 arms at 1000, then re-arms at 1500 and cancels the 1000. Group 99 exists only to
	// carry the watermark past both instants without touching group 1's armed timer.
	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "1970-01-01T00:00:00Z" }]"#);
	db.command(r#"INSERT app::t [{ id: 2, g: 1, ts: "1970-01-01T00:00:00.500Z" }]"#);
	db.command(r#"INSERT app::t [{ id: 3, g: 99, ts: "1970-01-01T00:01:00Z" }]"#);
}

#[test]
fn a_guest_can_disarm_a_timer_it_armed() {
	// Every window kind re-arms as its last event time rises, and session windows re-arm on every
	// row, so without disarm the wheel accumulates one stale timer per row and each fires a seal
	// for a window that has already moved on. Driven through the FFI callback, not the wheel.
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
	// The control for the test above. A disarm carries its instant across the FFI boundary as
	// millis; rounded, rescaled or ignored, a disarm aimed anywhere near would still cancel and
	// the test above stays green over a blunt wheel. One millisecond off must miss.
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
