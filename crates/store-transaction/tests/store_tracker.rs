// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Comprehensive test suite for storage tracker functionality.
//!
//! Tests verify that the StorageTracker correctly tracks:
//! - Insert operations (new keys)
//! - Update operations (existing keys, historical accumulation)
//! - Delete operations (tombstone tracking)
//! - Drop operations (physical removal, historical reduction)
//! - CDC tracking (change attribution)
//! - Persistence (checkpoint/restore round-trip)
//! - Invariants (byte totals, count totals, no negative counts)

use std::{error::Error as StdError, fmt::Write, path::Path, time::Duration};

use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange, async_cow_vec,
	delta::Delta,
	interface::MultiVersionValues,
	util::encoding::{binary::decode_binary, format, format::Formatter},
	value::encoded::EncodedValues,
};
use reifydb_store_transaction::{
	BackendConfig, MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionRange,
	MultiVersionRangeRev, StandardTransactionStore, TransactionStoreConfig, backend::BackendStorage,
};
use reifydb_testing::{tempdir::temp_dir, testscript};
use test_each_file::test_each_path;

test_each_path! { in "crates/store-transaction/tests/scripts/tracker" as store_tracker_memory => test_memory }
test_each_path! { in "crates/store-transaction/tests/scripts/tracker" as store_tracker_sqlite => test_sqlite }

fn test_memory(path: &Path) {
	let runtime = tokio::runtime::Runtime::new().unwrap();
	let storage = runtime.block_on(async { BackendStorage::memory() });
	testscript::run_path(&mut Runner::new_with_runtime(storage, runtime), path).expect("test failed")
}

fn test_sqlite(path: &Path) {
	temp_dir(|_db_path| {
		let runtime = tokio::runtime::Runtime::new().unwrap();
		let storage = runtime.block_on(async { BackendStorage::sqlite_in_memory() });
		testscript::run_path(&mut Runner::new_with_runtime(storage, runtime), path)
	})
	.expect("test failed")
}

/// Test runner for storage tracker tests.
pub struct Runner {
	store: StandardTransactionStore,
	version: CommitVersion,
	runtime: tokio::runtime::Runtime,
}

impl Runner {
	fn new_with_runtime(storage: BackendStorage, runtime: tokio::runtime::Runtime) -> Self {
		let store = runtime.block_on(async {
			StandardTransactionStore::new(TransactionStoreConfig {
				hot: Some(BackendConfig {
					storage,
					retention_period: Duration::from_millis(200),
				}),
				warm: None,
				cold: None,
				retention: Default::default(),
				merge_config: Default::default(),
				stats: Default::default(),
			})
			.unwrap()
		});
		Self {
			store,
			version: CommitVersion(0),
			runtime,
		}
	}
}

impl testscript::Runner for Runner {
	fn run(&mut self, command: &testscript::Command) -> Result<String, Box<dyn StdError>> {
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
					.runtime
					.block_on(async { self.store.get(&key, version).await })?
					.map(|sv: MultiVersionValues| sv.values.to_vec());

				writeln!(output, "{}", format::Raw::key_maybe_value(&key, value))?;
			}

			// contains KEY [version=VERSION]
			"contains" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;
				let contains =
					self.runtime.block_on(async { self.store.contains(&key, version).await })?;
				writeln!(output, "{} => {}", format::Raw::key(&key), contains)?;
			}

			// scan [reverse=BOOL] [version=VERSION]
			"scan" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				if !reverse {
					let batch = self.runtime.block_on(async {
						self.store.range(EncodedKeyRange::all(), version).await
					})?;
					print(&mut output, batch.items.into_iter())
				} else {
					let batch = self.runtime.block_on(async {
						self.store.range_rev(EncodedKeyRange::all(), version).await
					})?;
					print(&mut output, batch.items.into_iter())
				};
			}

			// set KEY=VALUE [version=VERSION]
			"set" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey(decode_binary(&kv.key.unwrap()));
				let values = EncodedValues(decode_binary(&kv.value));
				let version = if let Some(v) = args.lookup_parse("version")? {
					CommitVersion(v)
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				self.runtime.block_on(async {
					self.store
						.commit(
							async_cow_vec![
								(Delta::Set {
									key,
									values
								})
							],
							version,
						)
						.await
				})?;
			}

			// remove KEY [version=VERSION]
			"remove" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = if let Some(v) = args.lookup_parse("version")? {
					CommitVersion(v)
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				self.runtime.block_on(async {
					self.store
						.commit(
							async_cow_vec![
								(Delta::Remove {
									key
								})
							],
							version,
						)
						.await
				})?
			}

			// drop KEY [up_to_version=V] [keep_last_versions=N] [version=VERSION]
			"drop" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let up_to_version = args.lookup_parse::<u64>("up_to_version")?.map(CommitVersion);
				let keep_last_versions = args.lookup_parse::<usize>("keep_last_versions")?;
				let version = if let Some(v) = args.lookup_parse("version")? {
					CommitVersion(v)
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				self.runtime.block_on(async {
					self.store
						.commit(
							async_cow_vec![
								(Delta::Drop {
									key,
									up_to_version,
									keep_last_versions,
								})
							],
							version,
						)
						.await
				})?;
			}

			// ==================== Stats Query Commands ====================

			// stats - outputs all tracker stats for hot tier
			"stats" => {
				let args = command.consume_args();
				args.reject_rest()?;

				let stats = self.store.stats_tracker().total_stats();
				let hot = &stats.hot;

				writeln!(output, "current_count: {}", hot.current_count)?;
				writeln!(output, "current_key_bytes: {}", hot.current_key_bytes)?;
				writeln!(output, "current_value_bytes: {}", hot.current_value_bytes)?;
				writeln!(output, "historical_count: {}", hot.historical_count)?;
				writeln!(output, "historical_key_bytes: {}", hot.historical_key_bytes)?;
				writeln!(output, "historical_value_bytes: {}", hot.historical_value_bytes)?;
				writeln!(output, "cdc_count: {}", hot.cdc_count)?;
				writeln!(output, "cdc_key_bytes: {}", hot.cdc_key_bytes)?;
				writeln!(output, "cdc_value_bytes: {}", hot.cdc_value_bytes)?;
				writeln!(output, "total_bytes: {}", hot.total_bytes())?;
			}

			// stats_current - outputs only current stats (for brevity)
			"stats_current" => {
				let args = command.consume_args();
				args.reject_rest()?;

				let stats = self.store.stats_tracker().total_stats();
				let hot = &stats.hot;

				writeln!(output, "current_count: {}", hot.current_count)?;
				writeln!(output, "current_key_bytes: {}", hot.current_key_bytes)?;
				writeln!(output, "current_value_bytes: {}", hot.current_value_bytes)?;
			}

			// stats_historical - outputs only historical stats
			"stats_historical" => {
				let args = command.consume_args();
				args.reject_rest()?;

				let stats = self.store.stats_tracker().total_stats();
				let hot = &stats.hot;

				writeln!(output, "historical_count: {}", hot.historical_count)?;
				writeln!(output, "historical_key_bytes: {}", hot.historical_key_bytes)?;
				writeln!(output, "historical_value_bytes: {}", hot.historical_value_bytes)?;
			}

			// stats_cdc - outputs only CDC stats
			"stats_cdc" => {
				let args = command.consume_args();
				args.reject_rest()?;

				let stats = self.store.stats_tracker().total_stats();
				let hot = &stats.hot;

				writeln!(output, "cdc_count: {}", hot.cdc_count)?;
				writeln!(output, "cdc_key_bytes: {}", hot.cdc_key_bytes)?;
				writeln!(output, "cdc_value_bytes: {}", hot.cdc_value_bytes)?;
			}

			// stats_totals - outputs computed totals for invariant checks
			"stats_totals" => {
				let args = command.consume_args();
				args.reject_rest()?;

				let stats = self.store.stats_tracker().total_stats();
				let hot = &stats.hot;

				writeln!(output, "total_count: {}", hot.total_count())?;
				writeln!(output, "current_bytes: {}", hot.current_bytes())?;
				writeln!(output, "historical_bytes: {}", hot.historical_bytes())?;
				writeln!(output, "total_bytes: {}", hot.total_bytes())?;
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
		let fmtkv = format::Raw::key_value(&item.key, item.values.as_slice());
		writeln!(output, "{fmtkv}").unwrap();
	}
}
