// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
// End-to-end authorization guard for running uptime probes under their own
// CREATE SERVICE identity instead of root. These tests load the REAL uptime
// migrations (via #[path] on src/schema.rs, including 0003_probe_service_policies)
// so they pin the actual policy DDL the app ships, not a copy of it.
//
// The intent being verified: a probe's queue + write operations must be
// authorized ONLY for service identities. If the policies regress (dropped,
// widened to `true`, or scoped to the wrong op) these tests fail because a
// non-service identity would gain access, or a service would lose it.

#[path = "../src/schema.rs"]
mod schema;

use std::collections::HashMap;

use reifydb::{
	Database, IdentityId, Value, WithSubsystem, server,
	value::{
		params::Params,
		value::{datetime::DateTime, frame::frame::Frame, into::IntoValue, uuid::Uuid7},
	},
};

fn build() -> Database {
	server::memory().with_flow(|f| f).with_migrations(schema::migrations()).build().expect("build memory db")
}

fn admin(db: &Database, rql: &str) {
	let r = db.engine().admin_as(IdentityId::root(), rql, Params::None);
	if let Some(e) = r.error {
		panic!("admin failed for [{rql}]: {e:?}");
	}
}

fn query_as(db: &Database, id: IdentityId, rql: &str, params: Params) -> Result<Vec<Frame>, String> {
	let r = db.engine().query_as(id, rql, params);
	match r.error {
		Some(e) => Err(format!("{e:?}")),
		None => Ok(r.frames),
	}
}

fn command_as(db: &Database, id: IdentityId, rql: &str, params: Params) -> Result<Vec<Frame>, String> {
	let r = db.engine().command_as(id, rql, params);
	match r.error {
		Some(e) => Err(format!("{e:?}")),
		None => Ok(r.frames),
	}
}

fn root_cmd(db: &Database, rql: &str, params: Params) {
	let r = db.engine().command_as(IdentityId::root(), rql, params);
	if let Some(e) = r.error {
		panic!("root command failed for [{rql}]: {e:?}");
	}
}

fn lookup_identity(db: &Database, name: &str) -> IdentityId {
	let frames = query_as(
		db,
		IdentityId::root(),
		"from system::identities filter { name == $name } map { id }",
		params(&[("name", Value::Utf8(name.to_string()))]),
	)
	.expect("identity lookup");
	let frame = frames.first().expect("identity frame");
	let col = frame.columns.iter().find(|c| c.name == "id").expect("id column");
	match col.data.get_value(0) {
		Value::IdentityId(id) => id,
		other => panic!("unexpected identity value for {name}: {other:?}"),
	}
}

fn params(entries: &[(&str, Value)]) -> Params {
	let map: HashMap<String, Value> = entries.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect();
	Params::from(map)
}

fn rows(frames: &[Frame]) -> usize {
	frames.first().map(Frame::row_count).unwrap_or(0)
}

struct Fixture {
	db: Database,
	service: IdentityId,
	user: IdentityId,
	owner: IdentityId,
	monitor_id: Uuid7,
	region_id: Uuid7,
}

// A monitor owned by a real user, with one region in `unknown` state and one
// enqueued job, so the probe operations have something real to act on.
fn setup() -> Fixture {
	let db = build();
	let clock = db.clock().clone();
	let rng = db.engine().rng().clone();

	admin(&db, "CREATE SERVICE probe_svc");
	admin(&db, "CREATE USER alice");
	let service = lookup_identity(&db, "probe_svc");
	let owner = lookup_identity(&db, "alice");
	let user = owner;

	let monitor_id = Uuid7::generate(&clock, &rng);
	let region_id = Uuid7::generate(&clock, &rng);
	let now = clock.now();

	root_cmd(
		&db,
		"INSERT uptime::regions [{ id: $id, label: \"test\" }]",
		params(&[("id", region_id.into_value())]),
	);

	root_cmd(
		&db,
		"INSERT uptime::monitors [{ id: $id, owner: $owner, name: \"m\", kind: \"http\", target: \"http://x\", \
		 interval: $iv, timeout: $iv, http_method: none, expected_status: none, keyword: none, expected_ip: none, \
		 failure_threshold: 2, enabled: true, created_at: $now, last_checked_at: none, consecutive_failures: 0, \
		 status: \"unknown\" }]",
		params(&[
			("id", monitor_id.into_value()),
			("owner", owner.into_value()),
			("iv", reifydb::value::value::duration::Duration::from_seconds(30).unwrap().into_value()),
			("now", now.into_value()),
		]),
	);

	root_cmd(
		&db,
		"INSERT uptime::monitor_regions [{ monitor_id: $m, owner: $owner, region_id: $r, status: \"unknown\", \
		 last_checked_at: none, consecutive_failures: 0 }]",
		params(&[("m", monitor_id.into_value()), ("owner", owner.into_value()), ("r", region_id.into_value())]),
	);

	enqueue(&db, monitor_id, region_id);

	Fixture {
		db,
		service,
		user,
		owner,
		monitor_id,
		region_id,
	}
}

fn enqueue(db: &Database, monitor_id: Uuid7, region_id: Uuid7) {
	let clock = db.clock().clone();
	let rng = db.engine().rng().clone();
	let job_id = Uuid7::generate(&clock, &rng);
	root_cmd(
		db,
		"CALL uptime::enqueue_job($job_id, $monitor_id, $region_id)",
		params(&[
			("job_id", job_id.into_value()),
			("monitor_id", monitor_id.into_value()),
			("region_id", region_id.into_value()),
		]),
	);
}

fn report_params(f: &Fixture, success: bool) -> Params {
	let clock = f.db.clock().clone();
	let rng = f.db.engine().rng().clone();
	let result_id = Uuid7::generate(&clock, &rng);
	let now = clock.now();
	params(&[
		("result_id", result_id.into_value()),
		("monitor_id", f.monitor_id.into_value()),
		("owner", f.owner.into_value()),
		("region_id", f.region_id.into_value()),
		("probe", f.service.into_value()),
		("checked_at", now.into_value()),
		("success", Value::Boolean(success)),
		("response_time", Value::none()),
		("status_code", Value::none()),
		("error", Value::none()),
	])
}

const REPORT_CALL: &str = "CALL uptime::report_result($result_id, $monitor_id, $owner, $region_id, $probe, \
	 $checked_at, $success, $response_time, $status_code, $error)";

#[test]
fn service_reads_pending_jobs_but_user_sees_none() {
	// The `from` half of the jobs ringbuffer policy: a service must see enqueued
	// jobs; a user is filtered to zero rows (read deny = empty, not error). If the
	// policy widened to `true`, the user would start seeing jobs and this fails.
	let f = setup();

	let svc_jobs = query_as(&f.db, f.service, "from uptime::jobs map { monitor_id }", Params::None)
		.expect("service jobs read");
	assert_eq!(rows(&svc_jobs), 1, "service must see the enqueued job");

	let user_jobs =
		query_as(&f.db, f.user, "from uptime::jobs map { monitor_id }", Params::None).expect("user jobs read");
	assert_eq!(rows(&user_jobs), 0, "user must be filtered to zero jobs");
}

#[test]
fn service_claims_job_user_denied() {
	let f = setup();

	let mp = params(&[("monitor_id", f.monitor_id.into_value())]);

	// User CALL is rejected at the procedure call gate (not merely empty).
	let denied = command_as(&f.db, f.user, "CALL uptime::claim_job($monitor_id)", mp.clone());
	assert!(denied.is_err(), "user must be denied claim_job, got: {denied:?}");

	// Service CALL passes the gate and pops the job.
	let claimed = command_as(&f.db, f.service, "CALL uptime::claim_job($monitor_id)", mp).expect("service claim");
	assert_eq!(rows(&claimed), 1, "service claim must return the popped job");

	// The pop actually removed it.
	let remaining = query_as(&f.db, f.service, "from uptime::jobs map { monitor_id }", Params::None)
		.expect("service jobs read");
	assert_eq!(rows(&remaining), 0, "claimed job must be gone");
}

#[test]
fn service_reports_result_user_denied() {
	let f = setup();

	// User cannot CALL the procedure...
	let denied = command_as(&f.db, f.user, REPORT_CALL, report_params(&f, true));
	assert!(denied.is_err(), "user must be denied report_result CALL, got: {denied:?}");

	// ...nor forge a result row directly (no insert policy grants a user).
	let forged = command_as(
		&f.db,
		f.user,
		"INSERT uptime::results [{ id: $id, monitor_id: $m, owner: $owner, region_id: $r, probe: none, \
		 requirement_id: none, checked_at: $now, success: true, response_time: none, status_code: none, \
		 error: none }]",
		params(&[
			("id", Uuid7::generate(&f.db.clock().clone(), &f.db.engine().rng().clone()).into_value()),
			("m", f.monitor_id.into_value()),
			("owner", f.owner.into_value()),
			("r", f.region_id.into_value()),
			("now", f.db.clock().now().into_value()),
		]),
	);
	assert!(forged.is_err(), "user must be denied a direct results insert, got: {forged:?}");

	// Service CALL succeeds and drives the full write path.
	command_as(&f.db, f.service, REPORT_CALL, report_params(&f, false)).expect("service report (down)");

	let results = query_as(
		&f.db,
		IdentityId::root(),
		"from uptime::results filter { monitor_id == $m } map { success }",
		params(&[("m", f.monitor_id.into_value())]),
	)
	.expect("root results read");
	assert_eq!(rows(&results), 1, "service report must have inserted exactly one result");

	// The inner UPDATE on monitor_regions ran under the service too: one failure
	// recorded (threshold 2, so still `up`), proving the update policy authorized it.
	let region = query_as(
		&f.db,
		IdentityId::root(),
		"from uptime::monitor_regions filter { monitor_id == $m } map { consecutive_failures }",
		params(&[("m", f.monitor_id.into_value())]),
	)
	.expect("root region read");
	let cf = region.first().and_then(|fr| fr.columns.iter().find(|c| c.name == "consecutive_failures"));
	let cf = cf.expect("consecutive_failures column").data.get_value(0);
	assert_eq!(cf, Value::Int4(1), "one failure must have been recorded by the service report");
}

#[test]
fn service_registers_and_heartbeats_user_denied() {
	let f = setup();
	let clock = f.db.clock().clone();

	let reg = params(&[
		("probe", f.service.into_value()),
		("name", Value::Utf8("probe_svc".to_string())),
		("seen", clock.now().into_value()),
	]);

	// User denied at both call gates.
	let dr = command_as(&f.db, f.user, "CALL uptime::register_probe($probe, $name, $seen)", reg.clone());
	assert!(dr.is_err(), "user must be denied register_probe, got: {dr:?}");
	let dh = command_as(
		&f.db,
		f.user,
		"CALL uptime::probe_heartbeat($probe, $seen)",
		params(&[("probe", f.service.into_value()), ("seen", clock.now().into_value())]),
	);
	assert!(dh.is_err(), "user must be denied probe_heartbeat, got: {dh:?}");

	// Service registers itself (insert+delete on probes) then heartbeats (update).
	command_as(&f.db, f.service, "CALL uptime::register_probe($probe, $name, $seen)", reg)
		.expect("service register");

	let later = DateTime::from_nanos(clock.now().to_nanos() + 1_000_000_000);
	command_as(
		&f.db,
		f.service,
		"CALL uptime::probe_heartbeat($probe, $seen)",
		params(&[("probe", f.service.into_value()), ("seen", later.into_value())]),
	)
	.expect("service heartbeat");

	let probes = query_as(
		&f.db,
		IdentityId::root(),
		"from uptime::probes filter { id == $p } map { name }",
		params(&[("p", f.service.into_value())]),
	)
	.expect("root probes read");
	assert_eq!(rows(&probes), 1, "service must have exactly one registered probe row");
}

#[test]
fn service_finds_monitor_across_owner_user_denied() {
	// A proc body compiles under the caller's from-policies, so the service reads a monitor it does not own only
	// because 0006 widened the owner filter; a denied body read yields a `value: none` frame that still counts as
	// one row, so the owner column must be asserted.
	let f = setup();
	let mp = params(&[("monitor_id", f.monitor_id.into_value())]);

	let denied = command_as(&f.db, f.user, "CALL uptime::find_monitor($monitor_id)", mp.clone());
	assert!(denied.is_err(), "user must be denied find_monitor, got: {denied:?}");

	let found = command_as(&f.db, f.service, "CALL uptime::find_monitor($monitor_id)", mp).expect("service find");
	assert_eq!(rows(&found), 1, "service must read the monitor it does not own via the procedure");
	let owner = found.first().and_then(|fr| fr.columns.iter().find(|c| c.name == "owner")).expect("owner column");
	assert_eq!(owner.data.get_value(0), Value::IdentityId(f.owner), "row must be the alice-owned monitor");
}

#[test]
fn root_still_bypasses_probe_policies() {
	// Regression guard: adding the service policies must not break the root path
	// the backend REST routes and scheduler still use.
	let f = setup();
	let claimed = command_as(
		&f.db,
		IdentityId::root(),
		"CALL uptime::claim_job($monitor_id)",
		params(&[("monitor_id", f.monitor_id.into_value())]),
	)
	.expect("root claim");
	assert_eq!(rows(&claimed), 1, "root must still be able to claim jobs");
}
