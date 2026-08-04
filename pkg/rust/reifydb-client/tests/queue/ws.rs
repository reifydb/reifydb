// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::Arc,
	time::{Duration, Instant},
};

use reifydb_client::{QueueClaimRequest, WireFormat, WsClient};
use tokio::runtime::Runtime;

use super::{QUEUE, row_count};
use crate::common::{cleanup_server, create_server_instance, start_server_and_get_ws_port};

fn claim(worker: &str, wait_for: Option<&str>) -> QueueClaimRequest {
	QueueClaimRequest {
		queue: "app::jobs".to_string(),
		worker: worker.to_string(),
		max_n: Some(10),
		lease_ttl: Some("30s".to_string()),
		wait_for: wait_for.map(str::to_string),
	}
}

fn run<F, Fut>(test_fn: F)
where
	F: FnOnce(WsClient, u16) -> Fut,
	Fut: std::future::Future<Output = ()>,
{
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
	server.admin_as_root("CREATE NAMESPACE app", reifydb_value::params::Params::None).unwrap();
	server.admin_as_root(QUEUE, reifydb_value::params::Params::None).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Rbcf).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();
		test_fn(client, port).await;
	});

	cleanup_server(Some(server));
}

#[test]
fn a_parked_ws_claim_does_not_wedge_the_socket() {
	// This is the invariant that forced the spawned-task design: WS handles one message at a time
	// inline, so a claim parked in the connection loop would freeze every other request on that
	// socket. Issuing a Query while a 3s claim is parked, and requiring the Query to answer
	// quickly, is the only thing that catches a regression back to inline handling.
	run(|client, port| async move {
		let mut inserter = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Rbcf).await.unwrap();
		inserter.authenticate("mysecrettoken").await.unwrap();

		let claim_client = Arc::new(client);
		let parked = {
			let claim_client = claim_client.clone();
			tokio::spawn(async move { claim_client.queue_claim(claim("w1", Some("3s"))).await })
		};

		tokio::time::sleep(Duration::from_millis(200)).await;

		let started = Instant::now();
		let queried = claim_client.query("MAP { v: 1 }", None).await.unwrap();
		assert_eq!(row_count(&queried), 1, "the socket must still serve queries while a claim is parked");
		assert!(
			started.elapsed() < Duration::from_secs(1),
			"the query must not wait behind the parked claim: {:?}",
			started.elapsed()
		);

		inserter.command("INSERT app::jobs [{ id: 1 }]", None).await.unwrap();

		let frames = parked.await.unwrap().unwrap();
		assert_eq!(row_count(&frames), 1, "the parked claim must still receive its item");

		inserter.close().await.unwrap();
	});
}

#[test]
fn a_ws_claim_that_times_out_returns_zero_rows() {
	// The deferred response has to arrive even when nothing wakes it, or a timed-out worker hangs
	// forever waiting on a reply that the server never sends.
	run(|client, _| async move {
		let started = Instant::now();
		let frames = client.queue_claim(claim("w1", Some("1s"))).await.unwrap();

		assert_eq!(row_count(&frames), 0);
		assert!(started.elapsed() >= Duration::from_secs(1), "the claim must wait out its budget");
		client.close().await.unwrap();
	});
}
