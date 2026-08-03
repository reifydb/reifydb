// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::disallowed_methods)]

mod client;
mod config;
mod metrics;
mod output;
mod runner;
mod worker;

use std::{
	process,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use clap::Parser;
use client::Client;
use config::{Config, Protocol};
use metrics::Metrics;
use num_cpus::get as get_num_cpus;
use output::{clear_progress, print_header, print_progress, print_scenarios, print_summary};
use rand::random;
use reifydb::allocator;
use reifydb_testing_scenario::{profile::StopCondition, registry};
use reifydb_value::value::duration::Duration;
use reqwest::Client as ReqwestClient;
use rustls::crypto::ring::default_provider;

allocator::set_global_allocator!();
use runner::{Runner, select_query};
use tokio::{runtime::Builder, spawn, task::JoinSet, time};
use worker::Worker;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn main() {
	allocator::verify();
	let _ = default_provider().install_default();

	Builder::new_multi_thread()
		.worker_threads(get_num_cpus())
		.max_blocking_threads(256)
		.thread_name("load-test")
		.enable_all()
		.build()
		.unwrap()
		.block_on(async {
			if let Err(e) = async_main().await {
				eprintln!("Error: {}", e);
				process::exit(1);
			}
		});
}

fn is_already_exists(message: &str) -> bool {
	message.contains("already exists") || message.contains("ALREADY_EXISTS")
}

async fn async_main() -> Result<()> {
	let config = Config::parse();

	if config.list {
		print_scenarios(&registry::all());
		return Ok(());
	}

	let scenario = registry::by_name(&config.scenario).ok_or_else(|| {
		format!("unknown scenario '{}'; available: {}", config.scenario, registry::names().join(", "))
	})?;
	scenario.validate()?;

	let resolved = config.resolve(&scenario)?;
	let query = select_query(&scenario, config.query.as_deref())?;
	let runner = Arc::new(Runner::new(scenario, query, resolved.scale));
	let metrics = Arc::new(Metrics::new());

	if !config.quiet {
		print_header(&config, &resolved, &runner.description());
	}

	if !config.quiet {
		println!("Setting up scenario...");
	}

	let setup_operations = runner.setup_operations();
	if !setup_operations.is_empty() {
		let setup_client =
			Client::connect(config.protocol, &config.admin_url(), config.token.as_deref()).await?;

		for operation in setup_operations {
			if let Err(e) = setup_client.execute(&operation).await {
				// Tolerating an existing table keeps a rerun after an aborted run working;
				// every other failure means the workload would measure a dataset that was
				// never created, so it has to stop the run rather than print and continue.
				let message = e.to_string();
				if !is_already_exists(&message) {
					setup_client.close().await?;
					return Err(
						format!("setup failed on `{}`: {}", operation.rql(), message).into()
					);
				}
			}
		}

		setup_client.close().await?;
	}

	if !config.quiet {
		println!("Creating {} connections...", resolved.connections);
	}

	let shared_http_client = if matches!(config.protocol, Protocol::Http) {
		Some(ReqwestClient::builder()
			.pool_max_idle_per_host(resolved.connections)
			.timeout(Duration::from_seconds(30).unwrap().to_std())
			.build()?)
	} else {
		None
	};

	let seed = config.seed.unwrap_or_else(random);
	let mut workers = Vec::with_capacity(resolved.connections);

	for i in 0..resolved.connections {
		let client = Client::connect_with_http_client(
			config.protocol,
			&config.url(),
			config.token.as_deref(),
			shared_http_client.clone(),
		)
		.await?;

		workers.push(Worker::new(i, client, Arc::clone(&runner), Arc::clone(&metrics), seed));
	}

	if config.warmup > 0 {
		if !config.quiet {
			println!("Warming up ({} requests)...", config.warmup);
		}

		let warmup_per_worker = config.warmup / resolved.connections as u64;
		let mut warmup_tasks = JoinSet::new();

		for mut worker in workers.drain(..) {
			warmup_tasks.spawn(async move {
				worker.run_requests(warmup_per_worker).await;
				worker
			});
		}

		while let Some(result) = warmup_tasks.join_next().await {
			workers.push(result?);
		}

		metrics.reset();
	}

	if !config.quiet {
		println!("Running benchmark...");
		println!();
	}

	metrics.start();

	let stop_signal = Arc::new(AtomicBool::new(false));
	let mut benchmark_tasks = JoinSet::new();

	let progress_handle = if !config.quiet {
		let progress_metrics = Arc::clone(&metrics);
		let progress_stop = Arc::clone(&stop_signal);

		Some(spawn(async move {
			let mut last_count = 0u64;
			loop {
				time::sleep(Duration::from_seconds(1).unwrap().to_std()).await;

				if progress_stop.load(Ordering::Relaxed) {
					break;
				}

				let current = progress_metrics.current_count();
				let rate = current - last_count;
				last_count = current;

				print_progress(current, rate);
			}
		}))
	} else {
		None
	};

	match resolved.stop {
		StopCondition::Duration(duration) => {
			for mut worker in workers.drain(..) {
				let stop = Arc::clone(&stop_signal);
				benchmark_tasks.spawn(async move {
					worker.run_duration(duration, stop).await;
					worker
				});
			}
		}
		StopCondition::Iterations(requests) => {
			let requests_per_worker = requests / resolved.connections as u64;
			let extra = requests % resolved.connections as u64;

			for (i, mut worker) in workers.drain(..).enumerate() {
				let count = requests_per_worker
					+ if (i as u64) < extra {
						1
					} else {
						0
					};
				benchmark_tasks.spawn(async move {
					worker.run_requests(count).await;
					worker
				});
			}
		}
	}

	while let Some(result) = benchmark_tasks.join_next().await {
		workers.push(result?);
	}

	for worker in &workers {
		metrics.merge_histogram(worker.histogram());
	}

	stop_signal.store(true, Ordering::Relaxed);

	if let Some(handle) = progress_handle {
		// Give progress reporter time to notice the stop signal
		time::sleep(Duration::from_milliseconds(100).unwrap().to_std()).await;
		handle.abort();
	}

	if !config.quiet {
		clear_progress();
	}

	let summary = metrics.summary();
	print_summary(&summary, &runner.description());

	let teardown_operations = runner.teardown_operations();
	if !teardown_operations.is_empty() {
		if !config.quiet {
			println!();
			println!("Cleaning up...");
		}

		let teardown_client =
			Client::connect(config.protocol, &config.admin_url(), config.token.as_deref()).await?;

		for operation in teardown_operations {
			if let Err(e) = teardown_client.execute(&operation).await {
				eprintln!("Cleanup failed on `{}`: {}", operation.rql(), e);
			}
		}

		teardown_client.close().await?;
	}

	for worker in workers {
		let _ = worker.into_client().close().await;
	}

	Ok(())
}
