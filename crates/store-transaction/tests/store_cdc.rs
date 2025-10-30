// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{error::Error as StdError, fmt::Write, ops::Bound, path::Path, time::Duration};

#[cfg(debug_assertions)]
use reifydb_core::util::{mock_time_advance, mock_time_set};
use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, async_cow_vec,
	delta::Delta,
	interface::{Cdc, CdcChange, CdcSequencedChange},
	util::encoding::{binary::decode_binary, format, format::Formatter},
	value::encoded::EncodedValues,
};
use reifydb_store_transaction::{
	BackendConfig, CdcCount, CdcGet, CdcRange, CdcScan, MultiVersionCommit, StandardTransactionStore,
	TransactionStoreConfig,
	backend::{Backend, cdc::BackendCdc, multi::BackendMulti, single::BackendSingle},
	memory::MemoryBackend,
	sqlite::{SqliteBackend, SqliteConfig},
};
use reifydb_testing::{tempdir::temp_dir, testscript};
use test_each_file::test_each_path;

test_each_path! { in "crates/store-transaction/tests/scripts/store/cdc" as backend_cdc_memory => test_memory }
test_each_path! { in "crates/store-transaction/tests/scripts/store/cdc" as backend_cdc_sqlite => test_sqlite }

fn test_memory(path: &Path) {
	#[cfg(debug_assertions)]
	mock_time_set(1000);

	let backend = MemoryBackend::default();

	let config = TransactionStoreConfig {
		hot: Some(BackendConfig {
			backend: Backend {
				multi: BackendMulti::Memory(backend.clone()),
				single: BackendSingle::Memory(backend.clone()),
				cdc: BackendCdc::Memory(backend),
			},
			retention_period: Duration::from_secs(300),
		}),
		warm: None,
		cold: None,
		..Default::default()
	};

	testscript::run_path(&mut Runner::new(StandardTransactionStore::new(config).unwrap()), path)
		.expect("test failed")
}

fn test_sqlite(path: &Path) {
	temp_dir(|db_path| {
		#[cfg(debug_assertions)]
		mock_time_set(1000);

		let backend = SqliteBackend::new(SqliteConfig::fast(db_path));

		let config = TransactionStoreConfig {
			hot: Some(BackendConfig {
				backend: Backend {
					multi: BackendMulti::Sqlite(backend.clone()),
					single: BackendSingle::Sqlite(backend.clone()),
					cdc: BackendCdc::Sqlite(backend),
				},
				retention_period: Duration::from_secs(86400),
			}),
			warm: None,
			cold: None,
			..Default::default()
		};

		testscript::run_path(&mut Runner::new(StandardTransactionStore::new(config).unwrap()), path)
	})
	.expect("test failed")
}

/// Runs CDC tests for storage implementations
pub struct Runner {
	store: StandardTransactionStore,
	next_version: CommitVersion,
	/// Buffer of deltas to be committed
	deltas: Vec<Delta>,
}

impl Runner {
	fn new(store: StandardTransactionStore) -> Self {
		Self {
			store,
			next_version: CommitVersion(1),
			deltas: Vec::new(),
		}
	}

	fn format_cdc_change(change: &CdcChange) -> String {
		let format_value = |values: &EncodedValues| format::Raw::bytes(values.as_slice());
		let format_option_value = |row_opt: &Option<EncodedValues>| match row_opt {
			Some(values) => format::Raw::bytes(values.as_slice()),
			None => "\"<deleted>\"".to_string(),
		};

		match change {
			CdcChange::Insert {
				key,
				post,
			} => {
				format!(
					"Insert {{ key: {}, post: {} }}",
					format::Raw::key(key.as_slice()),
					format_value(post)
				)
			}
			CdcChange::Update {
				key,
				pre,
				post,
			} => {
				format!(
					"Update {{ key: {}, pre: {}, post: {} }}",
					format::Raw::key(key.as_slice()),
					format_value(pre),
					format_value(post)
				)
			}
			CdcChange::Delete {
				key,
				pre,
			} => {
				format!(
					"Delete {{ key: {}, pre: {} }}",
					format::Raw::key(key.as_slice()),
					format_option_value(pre)
				)
			}
		}
	}

	fn format_cdc(cdc: &Cdc) -> String {
		let changes_str = cdc
			.changes
			.iter()
			.map(|c| format!("{{ seq: {}, change: {} }}", c.sequence, Self::format_cdc_change(&c.change)))
			.collect::<Vec<_>>()
			.join(", ");

		format!("Cdc {{ version: {}, ts: {}, changes: [{}] }}", cdc.version, cdc.timestamp, changes_str)
	}

	fn format_sequenced_change(change: &CdcSequencedChange) -> String {
		format!("Change {{ seq: {}, change: {} }}", change.sequence, Self::format_cdc_change(&change.change))
	}
}

impl testscript::Runner for Runner {
	fn run(&mut self, command: &testscript::Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();
		match command.name.as_str() {
			// Apply a change with versioning (generates CDC)
			// apply VERSION KEY=VALUE
			"apply" => {
				let mut args = command.consume_args();
				let version = args
					.next_pos()
					.ok_or("version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid version")?;

				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey(decode_binary(&kv.key.unwrap()));
				let values = EncodedValues(decode_binary(&kv.value));
				args.reject_rest()?;

				self.store.commit(
					async_cow_vec![
						(Delta::Set {
							key,
							values
						})
					],
					version,
				)?;
				writeln!(output, "ok")?;
			}

			// insert VERSION KEY=VALUE
			"insert" => {
				let mut args = command.consume_args();
				let version = args
					.next_pos()
					.ok_or("version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid version")?;
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey(decode_binary(&kv.key.unwrap()));
				let values = EncodedValues(decode_binary(&kv.value));
				args.reject_rest()?;

				// Update next_version to match the given
				// version
				self.next_version = version;
				// Buffer the delta
				self.deltas.push(Delta::Set {
					key,
					values,
				});
			}

			// update VERSION KEY=VALUE
			"update" => {
				let mut args = command.consume_args();
				let version = args
					.next_pos()
					.ok_or("version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid version")?;
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey(decode_binary(&kv.key.unwrap()));
				let values = EncodedValues(decode_binary(&kv.value));
				args.reject_rest()?;

				// Update next_version to match the given
				// version
				self.next_version = version;
				// Buffer the delta
				self.deltas.push(Delta::Set {
					key,
					values,
				});
			}

			// delete VERSION KEY
			"delete" => {
				let mut args = command.consume_args();
				let version = args
					.next_pos()
					.ok_or("version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid version")?;
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;

				// Update next_version to match the given
				// version
				self.next_version = version;
				// Buffer the delta
				self.deltas.push(Delta::Remove {
					key,
				});
			}

			// commit - commits all buffered deltas
			"commit" => {
				let args = command.consume_args();
				args.reject_rest()?;

				if !self.deltas.is_empty() {
					let version = self.next_version;
					let deltas = CowVec::new(std::mem::take(&mut self.deltas));
					self.store.commit(deltas, version)?;
					self.next_version.0 += 1;
				}
				writeln!(output, "ok")?;
			}

			// cdc_get VERSION [SEQUENCE] - get entire transaction or specific change
			"cdc_get" => {
				let mut args = command.consume_args();
				let version = args
					.next_pos()
					.ok_or("version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid version")?;
				let sequence = args
					.next_pos()
					.map(|arg| arg.value.parse::<u16>())
					.transpose()
					.map_err(|_| "invalid sequence")?;
				args.reject_rest()?;

				let cdc = CdcGet::get(&self.store, version)?;

				match (cdc, sequence) {
					(Some(cdc), Some(seq)) => {
						// Find specific change by sequence
						if let Some(change) = cdc.changes.iter().find(|c| c.sequence == seq) {
							writeln!(output, "{}", Self::format_sequenced_change(change))?;
						} else {
							writeln!(output, "None")?;
						}
					}
					(Some(cdc), None) => {
						// Return entire transaction
						writeln!(output, "{}", Self::format_cdc(&cdc))?;
					}
					(None, _) => {
						writeln!(output, "None")?;
					}
				}
			}

			// cdc_range VERSION_START VERSION_END
			"cdc_range" => {
				let mut args = command.consume_args();
				let start_version = args
					.next_pos()
					.ok_or("start version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid start version")?;
				let end_version = args
					.next_pos()
					.ok_or("end version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid end version")?;
				args.reject_rest()?;

				let cdcs = CdcRange::range(
					&self.store,
					Bound::Included(start_version),
					Bound::Included(end_version),
				)?;
				for cdc in cdcs {
					for change in &cdc.changes {
						writeln!(
							output,
							"v{} {}",
							cdc.version,
							Self::format_sequenced_change(change)
						)?;
					}
				}
			}

			// cdc_range_unbounded - get all CDC transactions
			"cdc_range_unbounded" => {
				let args = command.consume_args();
				args.reject_rest()?;

				let cdcs = CdcRange::range(&self.store, Bound::Unbounded, Bound::Unbounded)?;
				for cdc in cdcs {
					for change in &cdc.changes {
						writeln!(
							output,
							"v{} {}",
							cdc.version,
							Self::format_sequenced_change(change)
						)?;
					}
				}
			}

			// cdc_range_included START END - [start, end]
			"cdc_range_included" => {
				let mut args = command.consume_args();
				let start_version = args
					.next_pos()
					.ok_or("start version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid start version")?;
				let end_version = args
					.next_pos()
					.ok_or("end version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid end version")?;
				args.reject_rest()?;

				let cdcs = CdcRange::range(
					&self.store,
					Bound::Included(start_version),
					Bound::Included(end_version),
				)?;
				for cdc in cdcs {
					for change in &cdc.changes {
						writeln!(
							output,
							"v{} {}",
							cdc.version,
							Self::format_sequenced_change(change)
						)?;
					}
				}
			}

			// cdc_range_included_excluded START END - [start, end)
			"cdc_range_included_excluded" => {
				let mut args = command.consume_args();
				let start_version = args
					.next_pos()
					.ok_or("start version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid start version")?;
				let end_version = args
					.next_pos()
					.ok_or("end version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid end version")?;
				args.reject_rest()?;

				let cdcs = CdcRange::range(
					&self.store,
					Bound::Included(start_version),
					Bound::Excluded(end_version),
				)?;
				for cdc in cdcs {
					for change in &cdc.changes {
						writeln!(
							output,
							"v{} {}",
							cdc.version,
							Self::format_sequenced_change(change)
						)?;
					}
				}
			}

			// cdc_range_excluded_included START END - (start, end]
			"cdc_range_excluded_included" => {
				let mut args = command.consume_args();
				let start_version = args
					.next_pos()
					.ok_or("start version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid start version")?;
				let end_version = args
					.next_pos()
					.ok_or("end version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid end version")?;
				args.reject_rest()?;

				let cdcs = CdcRange::range(
					&self.store,
					Bound::Excluded(start_version),
					Bound::Included(end_version),
				)?;
				for cdc in cdcs {
					for change in &cdc.changes {
						writeln!(
							output,
							"v{} {}",
							cdc.version,
							Self::format_sequenced_change(change)
						)?;
					}
				}
			}

			// cdc_range_excluded_excluded START END - (start, end)
			"cdc_range_excluded_excluded" => {
				let mut args = command.consume_args();
				let start_version = args
					.next_pos()
					.ok_or("start version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid start version")?;
				let end_version = args
					.next_pos()
					.ok_or("end version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid end version")?;
				args.reject_rest()?;

				let cdcs = CdcRange::range(
					&self.store,
					Bound::Excluded(start_version),
					Bound::Excluded(end_version),
				)?;
				for cdc in cdcs {
					for change in &cdc.changes {
						writeln!(
							output,
							"v{} {}",
							cdc.version,
							Self::format_sequenced_change(change)
						)?;
					}
				}
			}

			// cdc_range_to_included END - ..=end
			"cdc_range_to_included" => {
				let mut args = command.consume_args();
				let end_version = args
					.next_pos()
					.ok_or("end version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid end version")?;
				args.reject_rest()?;

				let cdcs =
					CdcRange::range(&self.store, Bound::Unbounded, Bound::Included(end_version))?;
				for cdc in cdcs {
					for change in &cdc.changes {
						writeln!(
							output,
							"v{} {}",
							cdc.version,
							Self::format_sequenced_change(change)
						)?;
					}
				}
			}

			// cdc_range_to_excluded END - ..<end
			"cdc_range_to_excluded" => {
				let mut args = command.consume_args();
				let end_version = args
					.next_pos()
					.ok_or("end version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid end version")?;
				args.reject_rest()?;

				let cdcs =
					CdcRange::range(&self.store, Bound::Unbounded, Bound::Excluded(end_version))?;
				for cdc in cdcs {
					for change in &cdc.changes {
						writeln!(
							output,
							"v{} {}",
							cdc.version,
							Self::format_sequenced_change(change)
						)?;
					}
				}
			}

			// cdc_range_from_included START - start..
			"cdc_range_from_included" => {
				let mut args = command.consume_args();
				let start_version = args
					.next_pos()
					.ok_or("start version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid start version")?;
				args.reject_rest()?;

				let cdcs =
					CdcRange::range(&self.store, Bound::Included(start_version), Bound::Unbounded)?;
				for cdc in cdcs {
					for change in &cdc.changes {
						writeln!(
							output,
							"v{} {}",
							cdc.version,
							Self::format_sequenced_change(change)
						)?;
					}
				}
			}

			// cdc_range_from_excluded START - start>..
			"cdc_range_from_excluded" => {
				let mut args = command.consume_args();
				let start_version = args
					.next_pos()
					.ok_or("start version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid start version")?;
				args.reject_rest()?;

				let cdcs =
					CdcRange::range(&self.store, Bound::Excluded(start_version), Bound::Unbounded)?;
				for cdc in cdcs {
					for change in &cdc.changes {
						writeln!(
							output,
							"v{} {}",
							cdc.version,
							Self::format_sequenced_change(change)
						)?;
					}
				}
			}

			// cdc_scan
			"cdc_scan" => {
				let args = command.consume_args();
				args.reject_rest()?;

				let cdcs = CdcScan::scan(&self.store)?;
				for cdc in cdcs {
					for change in &cdc.changes {
						writeln!(
							output,
							"v{} {}",
							cdc.version,
							Self::format_sequenced_change(change)
						)?;
					}
				}
			}

			// cdc_count VERSION
			"cdc_count" => {
				let mut args = command.consume_args();
				let version = args
					.next_pos()
					.ok_or("version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid version")?;
				args.reject_rest()?;

				let count = self.store.count(version)?;
				writeln!(output, "count: {}", count)?;
			}

			// advance_clock MILLIS
			"advance_clock" => {
				let mut args = command.consume_args();
				let millis = args
					.next_pos()
					.ok_or("millis not given")?
					.value
					.parse::<u64>()
					.map_err(|_| "invalid millis")?;
				args.reject_rest()?;

				#[cfg(debug_assertions)]
				mock_time_advance(millis);
				writeln!(output, "ok")?;
			}

			// next_version
			"next_version" => {
				let version = self.next_version;
				self.next_version.0 += 1;
				writeln!(output, "{}", version)?;
			}

			// bulk_insert VERSION COUNT - Insert COUNT events with
			// the same version
			"bulk_insert" => {
				let mut args = command.consume_args();

				let version = args
					.next_pos()
					.ok_or("version not given")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid version")?;

				let count = args
					.next_pos()
					.ok_or("count not given")?
					.value
					.parse::<usize>()
					.map_err(|_| "invalid count")?;

				args.reject_rest()?;

				// Create all events in a single transaction to
				// test sequence boundaries
				let mut deltas = Vec::new();

				for i in 0..count {
					let key = EncodedKey(CowVec::new(format!("bulk_{}", i).into_bytes()));
					let values = EncodedValues(CowVec::new(i.to_string().into_bytes()));
					deltas.push(Delta::Set {
						key,
						values,
					});
				}

				self.store.commit(CowVec::new(deltas), version)?;
				writeln!(output, "ok")?;
			}

			name => {
				return Err(format!("invalid command {name}").into());
			}
		}

		// Ensure output ends with newline if not empty
		if !output.is_empty() && !output.ends_with('\n') {
			writeln!(output)?;
		}

		Ok(output)
	}
}
