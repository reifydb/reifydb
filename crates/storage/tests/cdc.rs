// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{error::Error as StdError, fmt::Write, ops::Bound, path::Path};

#[cfg(debug_assertions)]
use reifydb_core::util::{mock_time_advance, mock_time_set};
use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, async_cow_vec,
	delta::Delta,
	interface::{
		CdcChange, CdcEvent, CdcGet, CdcRange, CdcScan, CdcStorage, TransactionId, VersionedCommit,
		VersionedGet, VersionedStorage,
	},
	row::EncodedRow,
	util::encoding::{binary::decode_binary, format, format::Formatter},
};
use reifydb_storage::{
	memory::Memory,
	sqlite::{Sqlite, SqliteConfig},
};
use reifydb_testing::{tempdir::temp_dir, testscript};
use test_each_file::test_each_path;

test_each_path! { in "crates/storage/tests/scripts/cdc" as cdc_memory => test_memory }
test_each_path! { in "crates/storage/tests/scripts/cdc" as cdc_sqlite => test_sqlite }

fn test_memory(path: &Path) {
	#[cfg(debug_assertions)]
	mock_time_set(1000);
	let storage = Memory::new();
	testscript::run_path(&mut Runner::new(storage), path).expect("test failed")
}

fn test_sqlite(path: &Path) {
	temp_dir(|db_path| {
		#[cfg(debug_assertions)]
		mock_time_set(1000);
		let storage = Sqlite::new(SqliteConfig::fast(db_path));
		testscript::run_path(&mut Runner::new(storage), path)
	})
	.expect("test failed")
}

/// Runs CDC tests for storage implementations
pub struct Runner<VS: VersionedStorage + VersionedCommit + VersionedGet + CdcStorage> {
	storage: VS,
	next_version: CommitVersion,
	/// Buffer of deltas to be committed
	deltas: Vec<Delta>,
}

impl<VS: VersionedStorage + VersionedCommit + VersionedGet + CdcStorage> Runner<VS> {
	fn new(storage: VS) -> Self {
		Self {
			storage,
			next_version: 1,
			deltas: Vec::new(),
		}
	}

	fn format_cdc_event(event: &CdcEvent) -> String {
		let format_value = |row: &EncodedRow| {
			if row.is_deleted() {
				"\"<deleted>\"".to_string()
			} else {
				format::Raw::bytes(row.as_slice())
			}
		};

		let change_str = match &event.change {
			CdcChange::Insert {
				key,
				after,
			} => {
				format!("Insert {{ key: {}, after: {} }}", format::Raw::key(key), format_value(after))
			}
			CdcChange::Update {
				key,
				before,
				after,
			} => {
				format!(
					"Update {{ key: {}, before: {}, after: {} }}",
					format::Raw::key(key),
					format_value(before),
					format_value(after)
				)
			}
			CdcChange::Delete {
				key,
				before,
			} => {
				format!("Delete {{ key: {}, before: {} }}", format::Raw::key(key), format_value(before))
			}
		};

		format!(
			"CdcEvent {{ version: {}, seq: {}, ts: {}, change: {} }}",
			event.version, event.sequence, event.timestamp, change_str
		)
	}
}

impl<VS: VersionedStorage + VersionedCommit + VersionedGet + CdcStorage> testscript::Runner for Runner<VS> {
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
				let row = EncodedRow(decode_binary(&kv.value));
				args.reject_rest()?;

				self.storage.commit(
					async_cow_vec![
						(Delta::Set {
							key,
							row
						})
					],
					version,
					TransactionId::default(),
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
				let row = EncodedRow(decode_binary(&kv.value));
				args.reject_rest()?;

				// Update next_version to match the given
				// version
				self.next_version = version;
				// Buffer the delta
				self.deltas.push(Delta::Set {
					key,
					row,
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
				let row = EncodedRow(decode_binary(&kv.value));
				args.reject_rest()?;

				// Update next_version to match the given
				// version
				self.next_version = version;
				// Buffer the delta
				self.deltas.push(Delta::Set {
					key,
					row,
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
					self.storage.commit(deltas, version, TransactionId::default())?;
					self.next_version += 1;
				}
				writeln!(output, "ok")?;
			}

			// cdc_get VERSION SEQUENCE
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
					.ok_or("sequence not given")?
					.value
					.parse::<u16>()
					.map_err(|_| "invalid sequence")?;
				args.reject_rest()?;

				// Get all events for the version and find the
				// one with matching sequence
				let events = CdcGet::get(&self.storage, version)?;
				let event = events.into_iter().find(|e| e.sequence == sequence);

				if let Some(event) = event {
					writeln!(output, "{}", Self::format_cdc_event(&event))?;
				} else {
					writeln!(output, "None")?;
				}
			}

			// cdc_range VERSION_START VERSION_END (for backward
			// compatibility)
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

				let events = CdcRange::range(
					&self.storage,
					Bound::Included(start_version),
					Bound::Included(end_version),
				)?;
				for event in events {
					writeln!(output, "{}", Self::format_cdc_event(&event))?;
				}
			}

			// cdc_range_unbounded - get all CDC events
			"cdc_range_unbounded" => {
				let args = command.consume_args();
				args.reject_rest()?;

				let events = CdcRange::range(&self.storage, Bound::Unbounded, Bound::Unbounded)?;
				for event in events {
					writeln!(output, "{}", Self::format_cdc_event(&event))?;
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

				let events = CdcRange::range(
					&self.storage,
					Bound::Included(start_version),
					Bound::Included(end_version),
				)?;
				for event in events {
					writeln!(output, "{}", Self::format_cdc_event(&event))?;
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

				let events = CdcRange::range(
					&self.storage,
					Bound::Included(start_version),
					Bound::Excluded(end_version),
				)?;
				for event in events {
					writeln!(output, "{}", Self::format_cdc_event(&event))?;
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

				let events = CdcRange::range(
					&self.storage,
					Bound::Excluded(start_version),
					Bound::Included(end_version),
				)?;
				for event in events {
					writeln!(output, "{}", Self::format_cdc_event(&event))?;
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

				let events = CdcRange::range(
					&self.storage,
					Bound::Excluded(start_version),
					Bound::Excluded(end_version),
				)?;
				for event in events {
					writeln!(output, "{}", Self::format_cdc_event(&event))?;
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

				let events =
					CdcRange::range(&self.storage, Bound::Unbounded, Bound::Included(end_version))?;
				for event in events {
					writeln!(output, "{}", Self::format_cdc_event(&event))?;
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

				let events =
					CdcRange::range(&self.storage, Bound::Unbounded, Bound::Excluded(end_version))?;
				for event in events {
					writeln!(output, "{}", Self::format_cdc_event(&event))?;
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

				let events = CdcRange::range(
					&self.storage,
					Bound::Included(start_version),
					Bound::Unbounded,
				)?;
				for event in events {
					writeln!(output, "{}", Self::format_cdc_event(&event))?;
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

				let events = CdcRange::range(
					&self.storage,
					Bound::Excluded(start_version),
					Bound::Unbounded,
				)?;
				for event in events {
					writeln!(output, "{}", Self::format_cdc_event(&event))?;
				}
			}

			// cdc_scan
			"cdc_scan" => {
				let args = command.consume_args();
				args.reject_rest()?;

				let events = CdcScan::scan(&self.storage)?;
				for event in events {
					writeln!(output, "{}", Self::format_cdc_event(&event))?;
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

				let count = self.storage.count(version)?;
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
				self.next_version += 1;
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
					let row = EncodedRow(CowVec::new(i.to_string().into_bytes()));
					deltas.push(Delta::Set {
						key,
						row,
					});
				}

				self.storage.commit(CowVec::new(deltas), version, TransactionId::default())?;
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
