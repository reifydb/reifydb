// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
// End-to-end proof of the standalone probe path: a real reifydb WebSocket server
// (public, auth-required port) + the reifydb-client WsClient authenticating with only
// a service token, then driving the exact RQL the standalone probe issues. This
// exercises the wire transport, token login, `$identity` resolution, and every probe
// procedure/query under real over-the-wire policy enforcement (migrations 0003 + 0004)
// - the parts the in-process engine tests in probe_service_policies.rs cannot reach.
//
// It loads the real migrations via #[path] so the shipped policy DDL is what gets tested.

#[path = "../src/schema.rs"]
mod schema;

use std::collections::HashMap;

use reifydb::{
	Database, Value, WithSubsystem, server,
	value::value::{duration::Duration, into::IntoValue, uuid::Uuid7},
};
use reifydb_client::{Params, WireFormat, WsClient};
use tokio::runtime::Runtime;

fn params(entries: &[(&str, Value)]) -> Params {
	let map: HashMap<String, Value> = entries.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect();
	Params::from(map)
}

fn build_server() -> Database {
	// uptime enables reqwest's `rustls-no-provider`, unified across the workspace, so the
	// TLS backend must be installed process-wide before the client stack builds.
	let _ = rustls::crypto::ring::default_provider().install_default();
	server::memory()
		.with_flow(|f| f)
		.with_ws(|ws| ws.bind_addr("[::1]:0"))
		.with_migrations(schema::migrations())
		.build()
		.expect("build ws server")
}

#[test]
fn standalone_probe_runs_the_full_cycle_over_the_wire_with_only_a_token() {
	let runtime = Runtime::new().unwrap();
	let _guard = runtime.enter();
	let mut server = build_server();

	// Provision the probe service + its token credential exactly as ensure_probe_identity does.
	server.admin_as_root("CREATE SERVICE probe_wire", Params::None).unwrap();
	server.admin_as_root(
		"CREATE AUTHENTICATION FOR probe_wire { method: token; token: 'wire-secret' }",
		Params::None,
	)
	.unwrap();

	// An owner (regular user) + a monitor it owns + a region + one enqueued job.
	server.admin_as_root("CREATE USER alice", Params::None).unwrap();
	let owner = match server
		.query_as_root("from system::identities filter { name == 'alice' } map { id }", Params::None)
		.unwrap()
		.first()
		.and_then(|f| f.columns.iter().find(|c| c.name == "id").map(|c| c.data.get_value(0)))
	{
		Some(Value::IdentityId(id)) => id,
		other => panic!("unexpected alice id: {other:?}"),
	};

	let clock = server.clock().clone();
	let rng = server.engine().rng().clone();
	let monitor_id = Uuid7::generate(&clock, &rng);
	let region_id = Uuid7::generate(&clock, &rng);
	let job_id = Uuid7::generate(&clock, &rng);
	let now = clock.now();

	server.command_as_root(
		"INSERT uptime::regions [{ id: $id, label: \"wire\" }]",
		params(&[("id", region_id.into_value())]),
	)
	.unwrap();
	server.command_as_root(
		"INSERT uptime::monitors [{ id: $id, owner: $owner, name: \"m\", kind: \"http\", \
			 target: \"http://x\", interval: $iv, timeout: $iv, http_method: none, expected_status: none, \
			 keyword: none, expected_ip: none, failure_threshold: 2, enabled: true, created_at: $now, \
			 last_checked_at: none, consecutive_failures: 0, status: \"unknown\" }]",
		params(&[
			("id", monitor_id.into_value()),
			("owner", owner.into_value()),
			("iv", Duration::from_seconds(30).unwrap().into_value()),
			("now", now.into_value()),
		]),
	)
	.unwrap();
	server.command_as_root(
		"INSERT uptime::monitor_regions [{ monitor_id: $m, owner: $owner, region_id: $r, \
			 status: \"unknown\", last_checked_at: none, consecutive_failures: 0 }]",
		params(&[("m", monitor_id.into_value()), ("owner", owner.into_value()), ("r", region_id.into_value())]),
	)
	.unwrap();
	server.command_as_root(
		"CALL uptime::enqueue_job($job_id, $monitor_id, $region_id)",
		params(&[
			("job_id", job_id.into_value()),
			("monitor_id", monitor_id.into_value()),
			("region_id", region_id.into_value()),
		]),
	)
	.unwrap();

	let ws_port = server.sub_server_ws().unwrap().port().unwrap();

	runtime.block_on(async move {
		let mut client = WsClient::connect(&format!("ws://[::1]:{ws_port}"), WireFormat::Frames)
			.await
			.expect("connect ws");

		// Token-only login: no name supplied, identity resolved server-side.
		let login = client.login_with_token("wire-secret").await.expect("token login");
		assert!(!login.identity.is_empty(), "login must yield an identity");

		// The probe resolves its own id + name from the authenticated session.
		let self_frames =
			client.query("map { id: $identity.id, name: $identity.name }", None).await.expect("self query");
		let self_frame = self_frames.first().expect("self frame");
		let name = self_frame.columns.iter().find(|c| c.name == "name").expect("name column").data.get_value(0);
		assert_eq!(name, Value::Utf8("probe_wire".to_string()), "identity name must come from the token");
		let probe_id = match self_frame
			.columns
			.iter()
			.find(|c| c.name == "id")
			.expect("id column")
			.data
			.get_value(0)
		{
			Value::IdentityId(id) => id,
			other => panic!("unexpected identity id: {other:?}"),
		};

		// Register self (insert into probes) - authorized by 0003 over the wire.
		client.command(
			"CALL uptime::register_probe($probe, $name, $seen)",
			Some(params(&[
				("probe", probe_id.into_value()),
				("name", Value::Utf8("probe_wire".to_string())),
				("seen", now.into_value()),
			])),
		)
		.await
		.expect("register over wire");

		// See the enqueued job (ringbuffer from-policy admits the service over the wire).
		let jobs = client.query("from uptime::jobs map { monitor_id }", None).await.expect("jobs query");
		assert_eq!(
			jobs.first().map(|f| f.row_count()).unwrap_or(0),
			1,
			"service must see the job over the wire"
		);

		// Claim it (procedure call policy + ringbuffer delete policy).
		let claimed = client
			.command(
				"CALL uptime::claim_job($monitor_id)",
				Some(params(&[("monitor_id", monitor_id.into_value())])),
			)
			.await
			.expect("claim over wire");
		assert_eq!(
			claimed.first().map(|f| f.row_count()).unwrap_or(0),
			1,
			"service must claim the job over the wire"
		);

		// Read the monitor it does not own, via the find_monitor procedure (0004).
		let mon = client
			.command(
				"CALL uptime::find_monitor($monitor_id)",
				Some(params(&[("monitor_id", monitor_id.into_value())])),
			)
			.await
			.expect("find_monitor over wire");
		assert_eq!(
			mon.first().map(|f| f.row_count()).unwrap_or(0),
			1,
			"service must read the monitor over the wire"
		);

		// Report a result (insert results + update monitor_regions + monitors, all under 0003).
		let result_id = Uuid7::generate(&clock, &rng);
		client.command(
			"CALL uptime::report_result($result_id, $monitor_id, $owner, $region_id, $probe, \
				 $checked_at, $success, $response_time, $status_code, $error)",
			Some(params(&[
				("result_id", result_id.into_value()),
				("monitor_id", monitor_id.into_value()),
				("owner", owner.into_value()),
				("region_id", region_id.into_value()),
				("probe", probe_id.into_value()),
				("checked_at", now.into_value()),
				("success", Value::Boolean(false)),
				("response_time", Value::none()),
				("status_code", Value::none()),
				("error", Value::none()),
			])),
		)
		.await
		.expect("report over wire");

		client.close().await.ok();
	});

	// The over-the-wire report actually landed and the rollup ran under the service identity.
	let results = server
		.query_as_root(
			"from uptime::results filter { monitor_id == $m } map { success }",
			params(&[("m", monitor_id.into_value())]),
		)
		.unwrap();
	assert_eq!(
		results.first().map(|f| f.row_count()).unwrap_or(0),
		1,
		"the wire report must have inserted a result"
	);

	let _ = server.stop();
}
