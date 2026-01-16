// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	error::Error as StdError,
	fmt::Write,
	path::Path,
	sync::{Arc, Condvar, Mutex},
	time::Duration,
};

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{
		encoded::EncodedValues,
		key::{EncodedKey, EncodedKeyRange},
	},
	event::{EventBus, EventListener, store::StatsProcessed},
	interface::store::{MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionValues},
	runtime::compute::ComputePool,
	util::encoding::{binary::decode_binary, format, format::Formatter},
};
use reifydb_metric::{
	cdc::{CdcStats, CdcStatsReader},
	multi::{MultiStorageStats, StorageStatsReader, Tier},
	worker::{CdcStatsListener, MetricsWorker, MetricsWorkerConfig, StorageStatsListener},
};
use reifydb_store_multi::{
	config::{HotConfig, MultiStoreConfig},
	hot::storage::HotStorage,
	store::StandardMultiStore,
};
use reifydb_store_single::store::StandardSingleStore;
use reifydb_testing::{
	tempdir::temp_dir,
	testscript::{
		command::Command,
		runner::{self, Runner as TestRunner},
	},
};
use reifydb_type::cow_vec;
use test_each_file::test_each_path;

test_each_path! { in "crates/metric/tests/scripts/integration" as metric_memory => test_memory }
test_each_path! { in "crates/metric/tests/scripts/integration" as metric_sqlite => test_sqlite }

fn test_memory(path: &Path) {
	let compute_pool = ComputePool::new(2, 8);
	let data_storage = HotStorage::memory(compute_pool);
	let event_bus = EventBus::new();
	let metrics_storage = StandardSingleStore::testing_memory_with_eventbus(event_bus.clone());
	let stats_waiter = StatsWaiter::new();
	event_bus.register::<StatsProcessed, _>(stats_waiter.clone());
	runner::run_path(&mut Runner::new(data_storage, metrics_storage, event_bus, stats_waiter), path)
		.expect("test failed")
}

fn test_sqlite(path: &Path) {
	temp_dir(|_db_path| {
		let data_storage = HotStorage::sqlite_in_memory();
		let event_bus = EventBus::new();
		let metrics_storage = StandardSingleStore::testing_memory_with_eventbus(event_bus.clone());
		let stats_waiter = StatsWaiter::new();
		event_bus.register::<StatsProcessed, _>(stats_waiter.clone());
		runner::run_path(&mut Runner::new(data_storage, metrics_storage, event_bus, stats_waiter), path)
	})
	.expect("test failed")
}

/// Waiter for stats processing events.
/// Allows tests to wait until stats have been processed up to a specific version.
#[derive(Clone)]
struct StatsWaiter {
	inner: Arc<StatsWaiterInner>,
}

struct StatsWaiterInner {
	processed_up_to: Mutex<CommitVersion>,
	condvar: Condvar,
}

impl StatsWaiter {
	fn new() -> Self {
		Self {
			inner: Arc::new(StatsWaiterInner {
				processed_up_to: Mutex::new(CommitVersion(0)),
				condvar: Condvar::new(),
			}),
		}
	}

	/// Wait until stats have been processed up to the given version.
	fn wait_until(&self, version: CommitVersion, timeout: Duration) -> bool {
		let guard = self.inner.processed_up_to.lock().unwrap();
		let result = self.inner.condvar.wait_timeout_while(guard, timeout, |v| *v < version).unwrap();
		!result.1.timed_out()
	}
}

impl EventListener<StatsProcessed> for StatsWaiter {
	fn on(&self, event: &StatsProcessed) {
		let mut v = self.inner.processed_up_to.lock().unwrap();
		if event.up_to > *v {
			*v = event.up_to;
		}
		self.inner.condvar.notify_all();
	}
}

/// Test runner for metric integration tests.
///
/// Coordinates between:
/// - StandardMultiStore for data operations (set, remove, drop, get, scan)
/// - MetricsWorker for background stats processing
/// - StorageStatsReader and CdcStatsReader for querying stats
pub struct Runner {
	/// Multi-version store for data operations
	multi_store: StandardMultiStore,
	/// Metrics storage backend
	_metrics_storage: StandardSingleStore,
	/// Background metrics worker
	_metrics_worker: MetricsWorker,
	/// Reader for storage stats
	storage_reader: StorageStatsReader<StandardSingleStore>,
	/// Reader for CDC stats
	cdc_reader: CdcStatsReader<StandardSingleStore>,
	/// Waiter for async stats processing
	stats_waiter: StatsWaiter,
	/// Current version integration
	version: CommitVersion,
}

impl Runner {
	fn new(
		data_storage: HotStorage,
		metrics_storage: StandardSingleStore,
		event_bus: EventBus,
		stats_waiter: StatsWaiter,
	) -> Self {
		// Create multi-version store for data operations
		let multi_store = StandardMultiStore::new(MultiStoreConfig {
			hot: Some(HotConfig {
				storage: data_storage,
				retention_period: Duration::from_millis(200),
			}),
			warm: None,
			cold: None,
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus: event_bus.clone(),
		})
		.unwrap();

		// Create metrics worker (single writer)
		let metrics_worker = MetricsWorker::new(
			MetricsWorkerConfig::default(),
			metrics_storage.clone(),
			multi_store.clone(),
			event_bus.clone(),
		);

		// Register event listeners to forward events to metrics worker
		let storage_listener = StorageStatsListener::new(metrics_worker.sender());
		event_bus.register(storage_listener);

		let cdc_listener = CdcStatsListener::new(metrics_worker.sender());
		event_bus.register(cdc_listener);

		// Create readers for querying stats
		let storage_stats_reader = StorageStatsReader::new(metrics_storage.clone());
		let cdc_stats_reader = CdcStatsReader::new(metrics_storage.clone());

		Self {
			multi_store,
			_metrics_storage: metrics_storage,
			_metrics_worker: metrics_worker,
			storage_reader: storage_stats_reader,
			cdc_reader: cdc_stats_reader,
			stats_waiter,
			version: CommitVersion(0),
		}
	}
}

impl TestRunner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();
		match command.name.as_str() {
			// ==================== Data Operations ====================

			// get KEY [version=VERSION]
			"get" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				let value = self
					.multi_store
					.get(&key, version)?
					.map(|sv: MultiVersionValues| sv.values.to_vec());

				writeln!(output, "{}", format::raw::Raw::key_maybe_value(&key, value))?;
			}

			// contains KEY [version=VERSION]
			"contains" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;
				let contains = self.multi_store.contains(&key, version)?;
				writeln!(output, "{} => {}", format::raw::Raw::key(&key), contains)?;
			}

			// scan [reverse=BOOL] [version=VERSION]
			"scan" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				if !reverse {
					let items: Vec<_> = self
						.multi_store
						.range(EncodedKeyRange::all(), version, 1024)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				} else {
					let items: Vec<_> = self
						.multi_store
						.range_rev(EncodedKeyRange::all(), version, 1024)
						.collect::<Result<Vec<_>, _>>()?;

					print(&mut output, items.into_iter())
				};
			}

			// set KEY=VALUE [version=VERSION]
			"set" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey(decode_binary(&kv.key.unwrap()));
				let values = EncodedValues(decode_binary(&kv.value));
				let version = if let Some(v) = args.lookup_parse("version")? {
					let v = CommitVersion(v);
					// Update self.version to track highest version used
					if v > self.version {
						self.version = v;
					}
					v
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				self.multi_store.commit(
					cow_vec![
						(Delta::Set {
							key,
							values
						})
					],
					version,
				)?
			}

			// remove KEY [version=VERSION]
			// remove KEY [version=VERSION]
			"remove" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = if let Some(v) = args.lookup_parse("version")? {
					let v = CommitVersion(v);
					if v > self.version {
						self.version = v;
					}
					v
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				// Get the current value to pass to Unset for metrics tracking
				let prev_version = CommitVersion(version.0.saturating_sub(1));
				let current_values = self
					.multi_store
					.get(&key, prev_version)?
					.map(|mv| mv.values)
					.unwrap_or_else(|| EncodedValues(cow_vec![]));

				self.multi_store.commit(
					cow_vec![
						(Delta::Unset {
							key,
							values: current_values
						})
					],
					version,
				)?
			}

			// drop KEY [up_to_version=V] [keep_last_versions=N] [version=VERSION]
			"drop" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let up_to_version = args.lookup_parse::<u64>("up_to_version")?.map(CommitVersion);
				let keep_last_versions = args.lookup_parse::<usize>("keep_last_versions")?;
				let version = if let Some(v) = args.lookup_parse("version")? {
					let v = CommitVersion(v);
					if v > self.version {
						self.version = v;
					}
					v
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				self.multi_store.commit(
					cow_vec![
						(Delta::Drop {
							key,
							up_to_version,
							keep_last_versions,
						})
					],
					version,
				)?
			}

			// ==================== Stats Query Commands ====================

			// stats - outputs all integration stats for hot tier
			"stats" => {
				let args = command.consume_args();
				args.reject_rest()?;

				// Flush drop worker to ensure deferred drops are processed
				self.multi_store.flush_drop_worker();

				// Auto-sync before reading stats
				if !self.stats_waiter.wait_until(self.version, Duration::from_secs(5)) {
					return Err("timeout waiting for stats".into());
				}

				// Aggregate all storage stats (Hot tier only)
				let storage_entries = self.storage_reader.scan_tier(Tier::Hot)?;
				let mut total_storage = MultiStorageStats::default();
				for (_, stats) in storage_entries {
					total_storage += stats;
				}

				// Aggregate all CDC stats
				let cdc_entries = self.cdc_reader.scan_all()?;
				let mut total_cdc = CdcStats::default();
				for (_, stats) in cdc_entries {
					total_cdc += stats;
				}

				// Output in original format
				writeln!(output, "current_count: {}", total_storage.current_count)?;
				writeln!(output, "current_key_bytes: {}", total_storage.current_key_bytes)?;
				writeln!(output, "current_value_bytes: {}", total_storage.current_value_bytes)?;
				writeln!(output, "historical_count: {}", total_storage.historical_count)?;
				writeln!(output, "historical_key_bytes: {}", total_storage.historical_key_bytes)?;
				writeln!(output, "historical_value_bytes: {}", total_storage.historical_value_bytes)?;
				writeln!(output, "cdc_count: {}", total_cdc.entry_count)?;
				writeln!(output, "cdc_key_bytes: {}", total_cdc.key_bytes)?;
				writeln!(output, "cdc_value_bytes: {}", total_cdc.value_bytes)?;
				writeln!(output, "total_bytes: {}", total_storage.total_bytes())?;
			}

			// stats_current - outputs only current stats (for brevity)
			"stats_current" => {
				let args = command.consume_args();
				args.reject_rest()?;

				// Flush drop worker to ensure deferred drops are processed
				self.multi_store.flush_drop_worker();

				// Auto-sync before reading stats
				if !self.stats_waiter.wait_until(self.version, Duration::from_secs(5)) {
					return Err("timeout waiting for stats".into());
				}

				// Aggregate all storage stats
				let storage_entries = self.storage_reader.scan_tier(Tier::Hot)?;
				let mut total_storage = MultiStorageStats::default();
				for (_, stats) in storage_entries {
					total_storage += stats;
				}

				writeln!(output, "current_count: {}", total_storage.current_count)?;
				writeln!(output, "current_key_bytes: {}", total_storage.current_key_bytes)?;
				writeln!(output, "current_value_bytes: {}", total_storage.current_value_bytes)?;
			}

			// stats_historical - outputs only historical stats
			"stats_historical" => {
				let args = command.consume_args();
				args.reject_rest()?;

				// Flush drop worker to ensure deferred drops are processed
				self.multi_store.flush_drop_worker();

				// Auto-sync before reading stats
				if !self.stats_waiter.wait_until(self.version, Duration::from_secs(5)) {
					return Err("timeout waiting for stats".into());
				}

				// Aggregate all storage stats
				let storage_entries = self.storage_reader.scan_tier(Tier::Hot)?;
				let mut total_storage = MultiStorageStats::default();
				for (_, stats) in storage_entries {
					total_storage += stats;
				}

				writeln!(output, "historical_count: {}", total_storage.historical_count)?;
				writeln!(output, "historical_key_bytes: {}", total_storage.historical_key_bytes)?;
				writeln!(output, "historical_value_bytes: {}", total_storage.historical_value_bytes)?;
			}

			// stats_cdc - outputs only CDC stats
			"stats_cdc" => {
				let args = command.consume_args();
				args.reject_rest()?;

				// Flush drop worker to ensure deferred drops are processed
				self.multi_store.flush_drop_worker();

				// Auto-sync before reading stats
				if !self.stats_waiter.wait_until(self.version, Duration::from_secs(5)) {
					return Err("timeout waiting for stats".into());
				}

				// Aggregate all CDC stats
				let cdc_entries = self.cdc_reader.scan_all()?;
				let mut total_cdc = CdcStats::default();
				for (_, stats) in cdc_entries {
					total_cdc += stats;
				}

				writeln!(output, "cdc_count: {}", total_cdc.entry_count)?;
				writeln!(output, "cdc_key_bytes: {}", total_cdc.key_bytes)?;
				writeln!(output, "cdc_value_bytes: {}", total_cdc.value_bytes)?;
			}

			// stats_totals - outputs computed totals for invariant checks
			"stats_totals" => {
				let args = command.consume_args();
				args.reject_rest()?;

				// Flush drop worker to ensure deferred drops are processed
				self.multi_store.flush_drop_worker();

				// Auto-sync before reading stats
				if !self.stats_waiter.wait_until(self.version, Duration::from_secs(5)) {
					return Err("timeout waiting for stats".into());
				}

				// Aggregate all storage stats
				let storage_entries = self.storage_reader.scan_tier(Tier::Hot)?;
				let mut total_storage = MultiStorageStats::default();
				for (_, stats) in storage_entries {
					total_storage += stats;
				}

				let total_count = total_storage.current_count + total_storage.historical_count;
				let current_bytes = total_storage.current_key_bytes + total_storage.current_value_bytes;
				let historical_bytes =
					total_storage.historical_key_bytes + total_storage.historical_value_bytes;
				let total_bytes = total_storage.total_bytes();

				writeln!(output, "total_count: {}", total_count)?;
				writeln!(output, "current_bytes: {}", current_bytes)?;
				writeln!(output, "historical_bytes: {}", historical_bytes)?;
				writeln!(output, "total_bytes: {}", total_bytes)?;
			}

			// sync_stats - waits until stats have been processed up to current version
			"sync_stats" => {
				let args = command.consume_args();
				args.reject_rest()?;

				// Flush drop worker to ensure deferred drops are processed
				self.multi_store.flush_drop_worker();

				// Wait for stats to be processed up to current version
				if !self.stats_waiter.wait_until(self.version, Duration::from_secs(5)) {
					return Err("timeout waiting for stats to be processed".into());
				}
			}

			name => {
				return Err(format!("invalid command {name}").into());
			}
		}
		Ok(output)
	}
}

fn print<I: Iterator<Item = MultiVersionValues>>(output: &mut String, iter: I) {
	for item in iter {
		let fmtkv = format::raw::Raw::key_value(&item.key, item.values.as_slice());
		writeln!(output, "{fmtkv}").unwrap();
	}
}
