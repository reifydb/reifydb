// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::Arc,
	time::{Duration, Instant},
};

use reifydb_client::{HttpClient, QueueClaimRequest, WireFormat};
use tokio::runtime::Runtime;

use super::{QUEUE, row_count};
use crate::common::{cleanup_server, create_server_instance, start_server_and_get_http_port};

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
	F: FnOnce(HttpClient, u16) -> Fut,
	Fut: std::future::Future<Output = ()>,
{
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_http_port(&runtime, &mut server).unwrap();
	server.admin_as_root("CREATE NAMESPACE app", reifydb_value::params::Params::None).unwrap();
	server.admin_as_root(QUEUE, reifydb_value::params::Params::None).unwrap();

	runtime.block_on(async {
		let mut client =
			HttpClient::connect(&format!("http://[::1]:{}", port), WireFormat::Rbcf).await.unwrap();
		client.authenticate("mysecrettoken");
		test_fn(client, port).await;
	});

	cleanup_server(Some(server));
}

#[test]
fn parked_http_claim_is_released_by_a_concurrent_insert() {
	// The end-to-end promise of the whole step, across a real socket: a worker asks with a long
	// budget and a second client's INSERT hands it the work immediately. The 10s budget against
	// a 3s bound is the discriminator - without the wake path the request can only come back at
	// 10s, and axum would still return 200 with zero rows, so only the timing catches it.
	run(|client, port| async move {
		let mut inserter =
			HttpClient::connect(&format!("http://[::1]:{}", port), WireFormat::Rbcf).await.unwrap();
		inserter.authenticate("mysecrettoken");

		let started = Instant::now();
		let parked = tokio::spawn(async move { client.queue_claim(claim("w1", Some("10s"))).await });

		tokio::time::sleep(Duration::from_millis(200)).await;
		inserter.command("INSERT app::jobs [{ id: 1 }]", None).await.unwrap();

		let frames = parked.await.unwrap().unwrap();
		let elapsed = started.elapsed();

		assert_eq!(row_count(&frames), 1, "the parked claim must receive the inserted item");
		assert!(
			elapsed < Duration::from_secs(3),
			"the claim must return on the wake, not the budget: {elapsed:?}"
		);
	});
}

#[test]
fn an_http_claim_that_times_out_returns_zero_rows_with_a_success_status() {
	// Clients re-poll on empty, so a timeout has to be an ordinary 200 with no rows. Turning it
	// into an error (or a 504) would make every idle worker log a failure once a second.
	run(|client, _| async move {
		let started = Instant::now();
		let frames = client.queue_claim(claim("w1", Some("1s"))).await.unwrap();
		let elapsed = started.elapsed();

		assert_eq!(row_count(&frames), 0);
		assert!(elapsed >= Duration::from_secs(1), "the claim must wait out its budget: {elapsed:?}");
	});
}

#[test]
fn an_http_claim_without_wait_for_does_not_park() {
	// The default has to stay non-blocking: an absent wait_for is the shape every plain poller
	// sends, and parking it would stall callers that never asked to wait.
	run(|client, _| async move {
		let started = Instant::now();
		let frames = client.queue_claim(claim("w1", None)).await.unwrap();

		assert_eq!(row_count(&frames), 0);
		assert!(started.elapsed() < Duration::from_millis(500), "an absent wait_for must return at once");
	});
}
