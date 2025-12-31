// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod client;
mod config;
mod metrics;
mod output;
mod worker;
mod workload;

use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use clap::Parser;
use client::{Client, Operation};
use config::{Config, Protocol};
use metrics::Metrics;
use output::{clear_progress, print_header, print_progress, print_summary};
use tokio::task::JoinSet;
use worker::Worker;
use workload::create_workload;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn main() {
	tokio::runtime::Builder::new_multi_thread()
		.worker_threads(num_cpus::get())
		.max_blocking_threads(256)
		.thread_name("load-test")
		.enable_all()
		.build()
		.unwrap()
		.block_on(async {
			if let Err(e) = async_main().await {
				eprintln!("Error: {}", e);
				std::process::exit(1);
			}
		});
}

async fn async_main() -> Result<()> {
	let config = Config::parse();

	// Create workload and metrics
	let workload = create_workload(config.workload, &config);
	let metrics = Arc::new(Metrics::new());

	// Print header
	if !config.quiet {
		print_header(&config, workload.description());
	}

	// Setup phase - run setup queries using a single connection
	if !config.quiet {
		println!("Setting up workload...");
	}

	let setup_queries = workload.setup_queries();
	if !setup_queries.is_empty() {
		let setup_client = Client::connect(config.protocol, &config.url(), config.token.as_deref()).await?;

		for query in setup_queries {
			let operation = if query.is_command {
				Operation::Command(query.rql)
			} else {
				Operation::Query(query.rql)
			};

			if let Err(e) = setup_client.execute(&operation).await {
				// Ignore "already exists" errors during setup
				let err_str = e.to_string();
				if !err_str.contains("already exists") && !err_str.contains("ALREADY_EXISTS") {
					eprintln!("Setup error: {}", e);
				}
			}
		}

		setup_client.close().await?;
	}

	// Create worker connections
	if !config.quiet {
		println!("Creating {} connections...", config.connections);
	}

	// Create shared HTTP client for connection pooling (HTTP only)
	let shared_http_client = if matches!(config.protocol, Protocol::Http) {
		Some(reqwest::Client::builder()
			.pool_max_idle_per_host(config.connections)
			.timeout(std::time::Duration::from_secs(30))
			.build()?)
	} else {
		None
	};

	let seed = config.seed.unwrap_or_else(rand::random);
	let mut workers = Vec::with_capacity(config.connections);

	for i in 0..config.connections {
		let client = Client::connect_with_http_client(
			config.protocol,
			&config.url(),
			config.token.as_deref(),
			shared_http_client.clone(),
		)
		.await?;

		workers.push(Worker::new(i, client, Arc::clone(&workload), Arc::clone(&metrics), seed));
	}

	// Warmup phase
	if config.warmup > 0 {
		if !config.quiet {
			println!("Warming up ({} requests)...", config.warmup);
		}

		let warmup_per_worker = config.warmup / config.connections as u64;
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

		// Reset metrics after warmup
		metrics.reset();
	}

	// Benchmark phase
	if !config.quiet {
		println!("Running benchmark...");
		println!();
	}

	// Start metrics timer
	metrics.start();

	let stop_signal = Arc::new(AtomicBool::new(false));
	let mut benchmark_tasks = JoinSet::new();

	// Spawn progress reporter if not quiet
	let progress_handle = if !config.quiet {
		let progress_metrics = Arc::clone(&metrics);
		let progress_stop = Arc::clone(&stop_signal);

		Some(tokio::spawn(async move {
			let mut last_count = 0u64;
			loop {
				tokio::time::sleep(Duration::from_secs(1)).await;

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

	// Start benchmark
	if let Some(duration) = config.duration {
		// Duration-based run
		for mut worker in workers.drain(..) {
			let stop = Arc::clone(&stop_signal);
			benchmark_tasks.spawn(async move {
				worker.run_duration(duration, stop).await;
				worker
			});
		}
	} else {
		// Request-count-based run
		let requests_per_worker = config.requests / config.connections as u64;
		let extra = config.requests % config.connections as u64;

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

	// Wait for all workers to complete
	while let Some(result) = benchmark_tasks.join_next().await {
		workers.push(result?);
	}

	// Merge all worker histograms into global metrics
	for worker in &workers {
		metrics.merge_histogram(worker.histogram());
	}

	// Signal stop and wait for progress reporter
	stop_signal.store(true, Ordering::Relaxed);

	if let Some(handle) = progress_handle {
		// Give progress reporter time to notice the stop signal
		tokio::time::sleep(Duration::from_millis(100)).await;
		handle.abort();
	}

	if !config.quiet {
		clear_progress();
	}

	// Print results
	let summary = metrics.summary();
	print_summary(&summary, workload.description());

	// Teardown phase
	let teardown_queries = workload.teardown_queries();
	if !teardown_queries.is_empty() {
		if !config.quiet {
			println!();
			println!("Cleaning up...");
		}

		let teardown_client = Client::connect(config.protocol, &config.url(), config.token.as_deref()).await?;

		for rql in teardown_queries {
			// Ignore teardown errors
			let _ = teardown_client.execute(&Operation::Command(rql)).await;
		}

		teardown_client.close().await?;
	}

	// Close worker connections
	for worker in workers {
		let _ = worker.into_client().close().await;
	}

	Ok(())
}
