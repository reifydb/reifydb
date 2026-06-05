// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

mod exec;
pub mod git;

use std::process;

use reifydb_client::{Frame, GrpcClient, GrpcSubscription, SubscriptionConfig, WireFormat};
use tokio::{runtime::Runtime, spawn};
use tracing::{debug, error};
use tracing_subscriber::fmt as tracing_fmt;

fn process_frames(frames: &[Frame], client: &GrpcClient) {
	for frame in frames {
		for row in frame.rows() {
			let job_run_id = match row.get_value("id") {
				Some(v) => v.to_string(),
				None => continue,
			};
			let job_id = match row.get_value("job_id") {
				Some(v) => v.to_string(),
				None => continue,
			};
			let run_id = match row.get_value("run_id") {
				Some(v) => v.to_string(),
				None => continue,
			};

			debug!("Picking up job_run {}", job_run_id);
			let client = client.clone();
			spawn(async move {
				if let Err(e) = exec::execute_job(&client, &job_run_id, &job_id, &run_id).await {
					error!("Job run {} failed: {}", job_run_id, e);
				}
			});
		}
	}
}

pub fn start(url: &str) {
	tracing_fmt().with_target(true).with_env_filter("debug,reifydb=trace").init();

	let rt = Runtime::new().unwrap();
	rt.block_on(async move {
		debug!("Forge runner connecting to orchestrator at {}", url);

		let mut client: GrpcClient = match GrpcClient::connect(url, WireFormat::Json).await {
			Ok(c) => c,
			Err(e) => {
				error!("Failed to connect to orchestrator: {}", e);
				process::exit(1);
			}
		};

		client.authenticate("mysecrettoken");

		debug!("Connected to orchestrator, subscribing to pending job_runs...");

		let mut subscription: GrpcSubscription = match client
			.subscribe("FROM forge::job_runs | FILTER status == \"pending\"", SubscriptionConfig::default())
			.await
		{
			Ok(s) => s,
			Err(e) => {
				error!("Failed to subscribe: {}", e);
				process::exit(1);
			}
		};

		debug!("Subscribed (id={}), waiting for work...", subscription.subscription_id());

		// Process any already-pending job_runs that existed before subscription
		match client.query("FROM forge::job_runs | FILTER status == \"pending\"", None).await {
			Ok(result) => {
				if !result.is_empty() {
					debug!("Found existing pending job_runs, processing...");
					process_frames(&result, &client);
				}
			}
			Err(e) => {
				error!("Failed to query existing job_runs: {}", e);
			}
		}

		loop {
			match subscription.recv().await {
				Some(change) => {
					process_frames(&change.frames, &client);
				}
				None => {
					error!(
						"Subscription stream closed unexpectedly. The orchestrator may have shut down."
					);
					process::exit(1);
				}
			}
		}
	});
}
