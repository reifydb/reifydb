// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::Arc,
	time::{Duration, Instant},
};

use reifydb_client::{GrpcClient, QueueClaimRequest, WireFormat};
use tokio::runtime::Runtime;

use super::{QUEUE, row_count};
use crate::common::{cleanup_server, create_server_instance, start_server_and_get_grpc_port};

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
	F: FnOnce(GrpcClient, u16) -> Fut,
	Fut: std::future::Future<Output = ()>,
{
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();
	server.admin_as_root("CREATE NAMESPACE app", reifydb_value::params::Params::None).unwrap();
	server.admin_as_root(QUEUE, reifydb_value::params::Params::None).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Rbcf).await.unwrap();
		client.authenticate("mysecrettoken");
		test_fn(client, port).await;
	});

	cleanup_server(Some(server));
}

#[test]
fn parked_grpc_claim_is_released_by_a_concurrent_insert() {
	// Same promise as HTTP over a unary gRPC call: the wake must cross the transport boundary, not
	// just the embedded path. A regression makes the call sit for its full 10s budget and return
	// an empty (still OK) response, so only the elapsed bound distinguishes wake from timeout.
	run(|client, port| async move {
		let mut inserter =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Rbcf).await.unwrap();
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
fn a_grpc_claim_that_times_out_returns_zero_rows_without_an_error_status() {
	// A timed-out long-poll must not surface as a gRPC error status; workers re-poll on it and a
	// DEADLINE_EXCEEDED would make every idle cycle look like a transport failure.
	run(|client, _| async move {
		let started = Instant::now();
		let frames = client.queue_claim(claim("w1", Some("1s"))).await.unwrap();

		assert_eq!(row_count(&frames), 0);
		assert!(started.elapsed() >= Duration::from_secs(1), "the claim must wait out its budget");
	});
}

#[test]
fn a_malformed_grpc_duration_is_an_invalid_argument() {
	// The duration strings are the only free-form field on the wire; a bad one has to come back as
	// a client error, not a five-minute park or an internal error.
	run(|client, _| async move {
		let err = client.queue_claim(claim("w1", Some("whenever"))).await.unwrap_err();
		assert!(format!("{err:?}").contains("wait_for"), "the error must name the field: {err:?}");
	});
}
