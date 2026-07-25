// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The window operator interns every (partition, window) pair as a substrate group, so a window that
//! goes idle past its seal horizon can be erased by the flow tick's reclamation pass rather than being
//! retained forever. Every link in that chain is one nobody would notice failing: an operator that does
//! not declare `Reclaim` is skipped and counted perpetual, a group stamped in the wrong position domain
//! either never comes due or comes due instantly, and a node whose state stayed at node scope has
//! nothing inside any group range to reclaim. All three leave the same signature - a healthy-looking
//! report over state that grows without bound - which is why this is driven end to end through a real
//! flow rather than against the driver.

use std::time::Duration as StdDuration;

use reifydb::{ConfigKey, Value, WithSubsystem, embedded};
use reifydb_test_harness::db::TestDb;

const TIMEOUT: StdDuration = StdDuration::from_secs(20);

fn setup() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_flow(|f| f)
			// The retention ledger is the only surface that reports what the tick pass actually
			// reclaimed; without a refresh cadence it stays empty (none means off).
			.with_config(ConfigKey::MetricsLifecycleRefreshInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with flow"),
	)
}

const RECLAIMED_A_GROUP: &str =
	"from system::metrics::lifecycle::current filter { class == 'operator-group-data' and work_done > 0 }";

#[test]
fn a_deferred_flow_drains_a_window_group_left_behind_by_retraction() {
	drains_a_stranded_window_group("DEFERRED");
}

#[test]
fn a_transactional_flow_drains_a_window_group_left_behind_by_retraction() {
	// The two tick paths reach reclamation through different call sites and hand it different
	// checkpoints (the engine's flow watermark versus the deferred actor's durable cursor), so a
	// wiring that only landed on one of them would leave half the flows retaining forever while the
	// other half looked healthy.
	drains_a_stranded_window_group("TRANSACTIONAL");
}

fn drains_a_stranded_window_group(view_kind: &str) {
	// A window emptied by retraction publishes its terminal Remove and drops its expiry index entry,
	// but the accumulator row it was holding is written back empty and then nothing in the operator
	// ever looks at it again: the expire sweep only walks the due index it just left. That row is the
	// leak this step exists to close, and reclamation is the only mechanism that can reach it - which
	// makes it the sharpest available probe that a due group really is drained through process_tick.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: int4, ts: datetime }");
	db.admin(&format!(r#"CREATE {view_kind} VIEW app::w {{ g: int4, total: int8 }} AS {{
			FROM app::t
				| window tumbling {{ total: math::sum(v) }}
					with {{ interval: "1s", grace: "1s", ts: "ts" }}
					by {{ g }}
		}}"#));

	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::w", 1, TIMEOUT);

	// Emptying the window is what strands its accumulator: the operator withdraws the published row
	// and un-indexes the window, so no later expiry pass will revisit it.
	db.command("DELETE app::t FILTER { id == 1 }");
	db.await_exact_row_count("FROM app::w", 0, TIMEOUT);

	// Push the event watermark well past that window's horizon (interval + grace + lateness = 2s), so
	// the stranded group is unambiguously idle while the new one is not.
	db.command(r#"INSERT app::t [{ id: 2, g: 1, v: 7, ts: "2026-01-01T00:05:00Z" }]"#);
	db.await_row_count("FROM app::w", 1, TIMEOUT);

	let reclaimed = db.await_row_count(RECLAIMED_A_GROUP, 1, TIMEOUT);
	assert_eq!(
		reclaimed,
		1,
		"no window group was ever reclaimed: the operator's state is either still addressed at node scope \
		 (nothing inside a group range to erase), stamped in a domain the seal cutoff cannot read, or the \
		 node was skipped for want of the Reclaim capability; ledger now: {:?}",
		db.query_as_root(
			"from system::metrics::lifecycle::current filter { class == 'operator-group-data' }",
			()
		)
	);

	// Reclamation must take only the idle group. The live window still owns its state and must keep
	// accumulating onto it, or the pass has erased something a batch was still using.
	db.command(r#"INSERT app::t [{ id: 3, g: 1, v: 4, ts: "2026-01-01T00:05:00.500Z" }]"#);
	let rows = db.await_row_count("FROM app::w FILTER { total == 11 }", 1, TIMEOUT);
	assert_eq!(
		rows,
		1,
		"the surviving window lost its accumulator: reclamation reached a group that was still live; view \
		 now: {:?}",
		db.query_as_root("FROM app::w", ())
	);
}
