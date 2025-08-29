use std::{
	sync::{
		Arc,
		atomic::{AtomicU64, AtomicUsize, Ordering},
	},
	time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Serialize)]
struct RequestMsg {
	q: String,
}

#[derive(Deserialize)]
struct ResponseMsg {
	ok: bool,
	result: String,
}

#[derive(Clone)]
struct Config {
	server_url: String,
	concurrent_connections: usize,
	requests_per_connection: usize,
	query_payload: String,
	duration_secs: Option<u64>,
}

impl Default for Config {
	fn default() -> Self {
		Self {
			server_url: "ws://127.0.0.1:8091".to_string(),
			concurrent_connections: 10,
			requests_per_connection: 100,
			query_payload: "benchmark query".to_string(),
			duration_secs: None,
		}
	}
}

struct Stats {
	total_requests: AtomicU64,
	successful_requests: AtomicU64,
	failed_requests: AtomicU64,
	total_latency_ns: AtomicU64,
	min_latency_ns: AtomicU64,
	max_latency_ns: AtomicU64,
	connection_errors: AtomicUsize,
}

impl Stats {
	fn new() -> Self {
		Self {
			total_requests: AtomicU64::new(0),
			successful_requests: AtomicU64::new(0),
			failed_requests: AtomicU64::new(0),
			total_latency_ns: AtomicU64::new(0),
			min_latency_ns: AtomicU64::new(u64::MAX),
			max_latency_ns: AtomicU64::new(0),
			connection_errors: AtomicUsize::new(0),
		}
	}

	fn record_request(&self, latency_ns: u64, success: bool) {
		self.total_requests.fetch_add(1, Ordering::Relaxed);
		if success {
			self.successful_requests
				.fetch_add(1, Ordering::Relaxed);
		} else {
			self.failed_requests.fetch_add(1, Ordering::Relaxed);
		}

		self.total_latency_ns.fetch_add(latency_ns, Ordering::Relaxed);

		// Update min latency
		let mut min = self.min_latency_ns.load(Ordering::Relaxed);
		while latency_ns < min {
			match self.min_latency_ns.compare_exchange_weak(
				min,
				latency_ns,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => break,
				Err(x) => min = x,
			}
		}

		// Update max latency
		let mut max = self.max_latency_ns.load(Ordering::Relaxed);
		while latency_ns > max {
			match self.max_latency_ns.compare_exchange_weak(
				max,
				latency_ns,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => break,
				Err(x) => max = x,
			}
		}
	}

	fn record_connection_error(&self) {
		self.connection_errors.fetch_add(1, Ordering::Relaxed);
	}

	fn snapshot(&self) -> StatsSnapshot {
		let total = self.total_requests.load(Ordering::Relaxed);
		let successful =
			self.successful_requests.load(Ordering::Relaxed);
		let failed = self.failed_requests.load(Ordering::Relaxed);
		let total_latency =
			self.total_latency_ns.load(Ordering::Relaxed);
		let min_latency = self.min_latency_ns.load(Ordering::Relaxed);
		let max_latency = self.max_latency_ns.load(Ordering::Relaxed);
		let conn_errors =
			self.connection_errors.load(Ordering::Relaxed);

		StatsSnapshot {
			total_requests: total,
			successful_requests: successful,
			failed_requests: failed,
			avg_latency_ms: if total > 0 {
				(total_latency / total) as f64 / 1_000_000.0
			} else {
				0.0
			},
			min_latency_ms: if min_latency != u64::MAX {
				min_latency as f64 / 1_000_000.0
			} else {
				0.0
			},
			max_latency_ms: max_latency as f64 / 1_000_000.0,
			connection_errors: conn_errors,
		}
	}
}

struct StatsSnapshot {
	total_requests: u64,
	successful_requests: u64,
	failed_requests: u64,
	avg_latency_ms: f64,
	min_latency_ms: f64,
	max_latency_ms: f64,
	connection_errors: usize,
}

async fn benchmark_connection(
	config: Config,
	stats: Arc<Stats>,
	shutdown_signal: Arc<std::sync::atomic::AtomicBool>,
) -> Result<()> {
	let (ws_stream, _) = match connect_async(&config.server_url).await {
		Ok(result) => result,
		Err(e) => {
			stats.record_connection_error();
			return Err(anyhow!("Failed to connect: {}", e));
		}
	};

	let (mut ws_sender, mut ws_receiver) = ws_stream.split();
	let request_msg = RequestMsg {
		q: config.query_payload.clone(),
	};
	let request_json = serde_json::to_string(&request_msg)?;

	let mut requests_sent = 0;
	let target_requests = if config.duration_secs.is_some() {
		usize::MAX // Run indefinitely until shutdown
	} else {
		config.requests_per_connection
	};

	loop {
		// Check for shutdown signal
		if shutdown_signal.load(Ordering::Relaxed) {
			break;
		}

		if requests_sent >= target_requests {
			break;
		}

		let start = Instant::now();

		// Send request
		if let Err(e) = ws_sender
			.send(Message::Text(request_json.clone().into()))
			.await
		{
			stats.record_request(0, false);
			eprintln!("Send error: {}", e);
			continue;
		}

		// Receive response
		match ws_receiver.next().await {
			Some(Ok(Message::Text(response_text))) => {
				let latency_ns =
					start.elapsed().as_nanos() as u64;

				match serde_json::from_str::<ResponseMsg>(
					&response_text,
				) {
					Ok(response) => {
						stats.record_request(
							latency_ns,
							response.ok,
						);
					}
					Err(_) => {
						stats.record_request(
							latency_ns, false,
						);
					}
				}
			}
			Some(Ok(_)) => {
				// Non-text message
				stats.record_request(
					start.elapsed().as_nanos() as u64,
					false,
				);
			}
			Some(Err(e)) => {
				eprintln!("Receive error: {}", e);
				stats.record_request(0, false);
				break;
			}
			None => {
				// Connection closed
				stats.record_request(0, false);
				break;
			}
		}

		requests_sent += 1;
	}

	Ok(())
}

async fn print_progress(stats: Arc<Stats>, duration: Duration) {
	use std::time::SystemTime;
	let mut last_print = SystemTime::now();
	let start_time = Instant::now();
	let mut last_requests = 0u64;

	loop {
		std::thread::sleep(std::time::Duration::from_secs(1));

		let snapshot = stats.snapshot();
		let elapsed = start_time.elapsed().as_secs_f64();
		let current_requests = snapshot.total_requests;
		let requests_this_second =
			current_requests.saturating_sub(last_requests);

		println!(
			"[{:6.1}s] Total: {} | Success: {} | Failed: {} | RPS: {} | Avg: {:.2}ms | Min: {:.2}ms | Max: {:.2}ms | Conn Errors: {}",
			elapsed,
			snapshot.total_requests,
			snapshot.successful_requests,
			snapshot.failed_requests,
			requests_this_second,
			snapshot.avg_latency_ms,
			snapshot.min_latency_ms,
			snapshot.max_latency_ms,
			snapshot.connection_errors
		);

		last_requests = current_requests;

		if elapsed >= duration.as_secs_f64() {
			break;
		}
	}
}

fn main() -> Result<()> {
	let rt = tokio::runtime::Runtime::new()?;
	rt.block_on(async_main())?;
	Ok(())
}

async fn async_main() -> Result<()> {
	let mut config = Config::default();

	// Parse command line arguments
	let args: Vec<String> = std::env::args().collect();
	let mut i = 1;
	while i < args.len() {
		match args[i].as_str() {
			"--url" => {
				if i + 1 < args.len() {
					config.server_url = args[i + 1].clone();
					i += 1;
				}
			}
			"--connections" | "-c" => {
				if i + 1 < args.len() {
					config.concurrent_connections = args
						[i + 1]
					.parse()
					.unwrap_or(10);
					i += 1;
				}
			}
			"--requests" | "-r" => {
				if i + 1 < args.len() {
					config.requests_per_connection = args
						[i + 1]
					.parse()
					.unwrap_or(100);
					i += 1;
				}
			}
			"--duration" | "-d" => {
				if i + 1 < args.len() {
					config.duration_secs = Some(args
						[i + 1]
					.parse()
					.unwrap_or(10));
					i += 1;
				}
			}
			"--query" | "-q" => {
				if i + 1 < args.len() {
					config.query_payload =
						args[i + 1].clone();
					i += 1;
				}
			}
			"--help" | "-h" => {
				println!("WebSocket Benchmark Client");
				println!("Usage: {} [options]", args[0]);
				println!("Options:");
				println!(
					"  --url <url>           WebSocket server URL (default: ws://127.0.0.1:8091)"
				);
				println!(
					"  -c, --connections <n> Number of concurrent connections (default: 10)"
				);
				println!(
					"  -r, --requests <n>    Requests per connection (default: 100)"
				);
				println!(
					"  -d, --duration <s>    Run for duration in seconds (overrides --requests)"
				);
				println!(
					"  -q, --query <text>    Query payload (default: 'benchmark query')"
				);
				println!(
					"  -h, --help            Show this help"
				);
				return Ok(());
			}
			_ => {}
		}
		i += 1;
	}

	println!("WebSocket Benchmark Configuration:");
	println!("  Server URL: {}", config.server_url);
	println!("  Concurrent connections: {}", config.concurrent_connections);
	if let Some(duration) = config.duration_secs {
		println!("  Duration: {} seconds", duration);
	} else {
		println!(
			"  Requests per connection: {}",
			config.requests_per_connection
		);
	}
	println!("  Query payload: '{}'", config.query_payload);
	println!();

	let stats = Arc::new(Stats::new());
	let start_time = Instant::now();

	// Create shutdown signal
	let shutdown_signal =
		Arc::new(std::sync::atomic::AtomicBool::new(false));
	let mut handles = Vec::new();

	// Start connections
	for _ in 0..config.concurrent_connections {
		let config_clone = config.clone();
		let stats_clone = stats.clone();
		let shutdown_clone = shutdown_signal.clone();

		let handle = tokio::spawn(async move {
			if let Err(e) = benchmark_connection(
				config_clone,
				stats_clone,
				shutdown_clone,
			)
			.await
			{
				eprintln!("Connection error: {}", e);
			}
		});
		handles.push(handle);
	}

	// Start progress reporting
	let progress_stats = stats.clone();
	let progress_duration =
		Duration::from_secs(config.duration_secs.unwrap_or(3600));
	let progress_handle = tokio::spawn(async move {
		print_progress(progress_stats, progress_duration).await;
	});

	// Wait for completion
	if let Some(duration_secs) = config.duration_secs {
		tokio::time::sleep(Duration::from_secs(duration_secs)).await;
		// Send shutdown signal to all connections
		shutdown_signal.store(true, Ordering::Relaxed);
	}

	// Wait for all connections to finish
	for handle in handles {
		let _ = handle.await;
	}

	// Stop progress reporting
	progress_handle.abort();

	// Final statistics
	let final_stats = stats.snapshot();
	let total_duration = start_time.elapsed();

	println!("\n=== Final Results ===");
	println!("Total duration: {:.2}s", total_duration.as_secs_f64());
	println!("Total requests: {}", final_stats.total_requests);
	println!("Successful requests: {}", final_stats.successful_requests);
	println!("Failed requests: {}", final_stats.failed_requests);
	println!("Connection errors: {}", final_stats.connection_errors);
	println!(
		"Success rate: {:.2}%",
		if final_stats.total_requests > 0 {
			(final_stats.successful_requests as f64
				/ final_stats.total_requests as f64)
				* 100.0
		} else {
			0.0
		}
	);
	println!(
		"Average RPS: {:.2}",
		final_stats.total_requests as f64
			/ total_duration.as_secs_f64()
	);
	println!("Average latency: {:.2}ms", final_stats.avg_latency_ms);
	println!("Min latency: {:.2}ms", final_stats.min_latency_ms);
	println!("Max latency: {:.2}ms", final_stats.max_latency_ms);

	Ok(())
}
