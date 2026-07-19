// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	error::Error as StdError,
	fmt::Write,
	path::Path,
	sync::{Arc, Condvar, Mutex},
};

use reifydb_codec::{
	encoded::row::EncodedRow,
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	event::{
		EventBus, EventListener,
		metric::{
			CdcEvictedEvent, CdcEviction, CdcWrite, CdcWrittenEvent, MultiCommittedEvent,
			RequestExecutedEvent,
		},
		store::MetricsProcessedEvent,
	},
	interface::store::{MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionRow, Tier},
	util::encoding::{binary::decode_binary, format, format::Formatter},
};
use reifydb_metrics::{
	accumulator::StatementMetricsAccumulator,
	storage::{
		cdc::{CdcMetrics, CdcMetricsReader},
		multi::{MultiStorageMetrics, StorageMetricsReader},
	},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_store_multi::{
	MultiStore, MultiVersionScope,
	config::{CommitBufferConfig, MultiStoreConfig},
	store::StandardMultiStore,
	tier::commit::buffer::MultiCommitBufferTier,
};
use reifydb_store_single::SingleStore;
use reifydb_sub_metrics::{
	actor::MetricsFlushActor,
	listener::{CdcEvictedListener, CdcWrittenListener, MultiCommittedListener, RequestMetricsEventListener},
};
use reifydb_testing::testscript::{
	command::Command,
	runner::{self, Runner as TestRunner},
};
use reifydb_value::{cow_vec, util::cowvec::CowVec, value::duration::Duration};
use test_each_file::test_each_path;

test_each_path! { in "crates/sub-metrics/tests/scripts/storage" as metric_memory => test_memory }

fn test_memory(path: &Path) {
	let data_storage = MultiCommitBufferTier::memory();
	runner::run_path(&mut Runner::new(data_storage), path).expect("test failed")
}

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

	fn wait_until(&self, version: CommitVersion, timeout: Duration) -> bool {
		let guard = self.inner.processed_up_to.lock().unwrap();
		let result = self.inner.condvar.wait_timeout_while(guard, timeout.to_std(), |v| *v < version).unwrap();
		!result.1.timed_out()
	}
}

impl EventListener<MetricsProcessedEvent> for StatsWaiter {
	fn on(&self, event: &MetricsProcessedEvent) {
		let mut v = self.inner.processed_up_to.lock().unwrap();
		if *event.up_to() > *v {
			*v = *event.up_to();
		}
		self.inner.condvar.notify_all();
	}
}

pub struct Runner {
	multi_store: MultiStore,
	storage_reader: StorageMetricsReader<SingleStore>,
	cdc_reader: CdcMetricsReader<SingleStore>,
	stats_waiter: StatsWaiter,
	event_bus: EventBus,
	version: CommitVersion,
}

impl Runner {
	fn new(data_storage: MultiCommitBufferTier) -> Self {
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		let spawner = actor_system.spawner();
		std::mem::forget(actor_system);
		let event_bus = EventBus::new(&spawner);

		let metrics_storage = SingleStore::testing_memory();

		let multi_store = MultiStore::Standard(
			StandardMultiStore::new(MultiStoreConfig {
				commit: Some(CommitBufferConfig {
					storage: data_storage,
				}),
				persistent: None,
				retention: Default::default(),
				merge_config: Default::default(),
				event_bus: event_bus.clone(),
				spawner: spawner.clone(),
				clock: Clock::Real,
			})
			.unwrap(),
		);

		let actor = MetricsFlushActor::new(
			Arc::new(StatementMetricsAccumulator::new()),
			event_bus.clone(),
			metrics_storage.clone(),
			multi_store.clone(),
		)
		.with_flush_interval(Duration::from_milliseconds(10).unwrap());

		let handle = spawner.spawn_coordination("metrics-flush", actor);
		let actor_ref = handle.actor_ref().clone();

		event_bus.register::<MultiCommittedEvent, _>(MultiCommittedListener::new(actor_ref.clone()));
		event_bus.register::<CdcWrittenEvent, _>(CdcWrittenListener::new(actor_ref.clone()));
		event_bus.register::<CdcEvictedEvent, _>(CdcEvictedListener::new(actor_ref.clone()));
		event_bus.register::<RequestExecutedEvent, _>(RequestMetricsEventListener::new(actor_ref));

		let stats_waiter = StatsWaiter::new();
		event_bus.register::<MetricsProcessedEvent, _>(stats_waiter.clone());

		let storage_reader = StorageMetricsReader::new(metrics_storage.clone());
		let cdc_reader = CdcMetricsReader::new(metrics_storage);

		Self {
			multi_store,
			storage_reader,
			cdc_reader,
			stats_waiter,
			event_bus,
			version: CommitVersion(0),
		}
	}
}

impl TestRunner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();
		match command.name.as_str() {
			"get" => {
				let mut args = command.consume_args();
				let key =
					EncodedKey::new(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				let value =
					self.multi_store.get(&key, version)?.map(|sv: MultiVersionRow| sv.row.to_vec());

				writeln!(output, "{}", format::raw::Raw::key_maybe_value(&key, value))?;
			}

			"contains" => {
				let mut args = command.consume_args();
				let key =
					EncodedKey::new(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;
				let contains = self.multi_store.contains(&key, version)?;
				writeln!(output, "{} => {}", format::raw::Raw::key(&key), contains)?;
			}

			"scan" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				let scope = MultiVersionScope::AsOf {
					read: version,
				};
				if !reverse {
					let items: Vec<_> = self
						.multi_store
						.range(EncodedKeyRange::all(), scope, 1024)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				} else {
					let items: Vec<_> = self
						.multi_store
						.range_rev(EncodedKeyRange::all(), scope, 1024)
						.collect::<Result<Vec<_>, _>>()?;

					print(&mut output, items.into_iter())
				};
			}

			"set" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey::new(decode_binary(&kv.key.unwrap()));
				let row = EncodedRow(CowVec::new(decode_binary(&kv.value)));
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

				MultiVersionCommit::commit(
					&self.multi_store,
					cow_vec![
						(Delta::Set {
							key,
							row
						})
					],
					version,
				)?
			}

			"remove" => {
				let mut args = command.consume_args();
				let key =
					EncodedKey::new(decode_binary(&args.next_pos().ok_or("key not given")?.value));
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

				let prev_version = CommitVersion(version.0.saturating_sub(1));
				let current_values = self
					.multi_store
					.get(&key, prev_version)?
					.map(|mv| mv.row)
					.unwrap_or_else(|| EncodedRow(cow_vec![]));

				MultiVersionCommit::commit(
					&self.multi_store,
					cow_vec![
						(Delta::Unset {
							key,
							row: current_values
						})
					],
					version,
				)?
			}

			// drop KEY [version=VERSION]
			"drop" => {
				let mut args = command.consume_args();
				let key =
					EncodedKey::new(decode_binary(&args.next_pos().ok_or("key not given")?.value));
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

				MultiVersionCommit::commit(
					&self.multi_store,
					cow_vec![
						(Delta::Drop {
							key,
						})
					],
					version,
				)?
			}

			"stats" => {
				let args = command.consume_args();
				args.reject_rest()?;

				if !self.stats_waiter.wait_until(self.version, Duration::from_seconds(5).unwrap()) {
					return Err("timeout waiting for stats".into());
				}

				let storage_entries = self.storage_reader.scan_tier(Tier::Buffer)?;
				let mut total_storage = MultiStorageMetrics::default();
				for (_, stats) in storage_entries {
					total_storage += stats;
				}

				let cdc_entries = self.cdc_reader.scan_all()?;
				let mut total_cdc = CdcMetrics::default();
				for (_, stats) in cdc_entries {
					total_cdc += stats;
				}

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

			"stats_current" => {
				let args = command.consume_args();
				args.reject_rest()?;

				if !self.stats_waiter.wait_until(self.version, Duration::from_seconds(5).unwrap()) {
					return Err("timeout waiting for stats".into());
				}

				let storage_entries = self.storage_reader.scan_tier(Tier::Buffer)?;
				let mut total_storage = MultiStorageMetrics::default();
				for (_, stats) in storage_entries {
					total_storage += stats;
				}

				writeln!(output, "current_count: {}", total_storage.current_count)?;
				writeln!(output, "current_key_bytes: {}", total_storage.current_key_bytes)?;
				writeln!(output, "current_value_bytes: {}", total_storage.current_value_bytes)?;
			}

			"stats_historical" => {
				let args = command.consume_args();
				args.reject_rest()?;

				if !self.stats_waiter.wait_until(self.version, Duration::from_seconds(5).unwrap()) {
					return Err("timeout waiting for stats".into());
				}

				let storage_entries = self.storage_reader.scan_tier(Tier::Buffer)?;
				let mut total_storage = MultiStorageMetrics::default();
				for (_, stats) in storage_entries {
					total_storage += stats;
				}

				writeln!(output, "historical_count: {}", total_storage.historical_count)?;
				writeln!(output, "historical_key_bytes: {}", total_storage.historical_key_bytes)?;
				writeln!(output, "historical_value_bytes: {}", total_storage.historical_value_bytes)?;
			}

			"stats_cdc" => {
				let args = command.consume_args();
				args.reject_rest()?;

				if !self.stats_waiter.wait_until(self.version, Duration::from_seconds(5).unwrap()) {
					return Err("timeout waiting for stats".into());
				}

				let cdc_entries = self.cdc_reader.scan_all()?;
				let mut total_cdc = CdcMetrics::default();
				for (_, stats) in cdc_entries {
					total_cdc += stats;
				}

				writeln!(output, "cdc_count: {}", total_cdc.entry_count)?;
				writeln!(output, "cdc_key_bytes: {}", total_cdc.key_bytes)?;
				writeln!(output, "cdc_value_bytes: {}", total_cdc.value_bytes)?;
			}

			"stats_totals" => {
				let args = command.consume_args();
				args.reject_rest()?;

				if !self.stats_waiter.wait_until(self.version, Duration::from_seconds(5).unwrap()) {
					return Err("timeout waiting for stats".into());
				}

				let storage_entries = self.storage_reader.scan_tier(Tier::Buffer)?;
				let mut total_storage = MultiStorageMetrics::default();
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

			"sync_stats" => {
				let args = command.consume_args();
				args.reject_rest()?;

				if !self.stats_waiter.wait_until(self.version, Duration::from_seconds(5).unwrap()) {
					return Err("timeout waiting for stats to be processed".into());
				}
			}

			"cdc_write" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey::new(decode_binary(&kv.key.unwrap()));
				let value_bytes = decode_binary(&kv.value).len() as u64;
				let version = if let Some(v) = args.lookup_parse("version")? {
					CommitVersion(v)
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				let entries = vec![CdcWrite {
					key,
					value_bytes,
				}];
				self.event_bus.emit(CdcWrittenEvent::new(entries, version));
				writeln!(output, "ok")?;
			}

			"cdc_drop" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value_bytes not given")?.clone();
				let key = EncodedKey::new(decode_binary(&kv.key.unwrap()));
				let value_bytes: u64 = String::from_utf8_lossy(&decode_binary(&kv.value)).parse()?;
				let version = if let Some(v) = args.lookup_parse("version")? {
					CommitVersion(v)
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				let entries = vec![CdcEviction {
					key,
					value_bytes,
				}];
				self.event_bus.emit(CdcEvictedEvent::new(entries, version));
				writeln!(output, "ok")?;
			}

			name => {
				return Err(format!("invalid command {name}").into());
			}
		}
		Ok(output)
	}
}

fn print<I: Iterator<Item = MultiVersionRow>>(output: &mut String, iter: I) {
	for item in iter {
		let fmtkv = format::raw::Raw::key_value(&item.key, item.row.as_slice());
		writeln!(output, "{fmtkv}").unwrap();
	}
}
