// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{collections::HashMap, error::Error as StdError, fmt::Write};

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	event::EventBus,
	interface::store::{EntryKind, MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionRow},
	util::encoding::{
		binary::decode_binary,
		format::{Formatter, raw::Raw},
	},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_store_multi::{
	config::{HotConfig, MultiStoreConfig},
	hot::storage::HotStorage,
	store::{
		StandardMultiStore,
		router::classify_key,
		version::{VersionedGetResult, get_at_version},
	},
	tier::TierStorage,
};
use reifydb_testing::testscript;
use reifydb_type::{cow_vec, util::cowvec::CowVec};
use testscript::command::Command;

/// Shared testscript runner used by every per-backend test binary
/// (memory / sqlite / tiered / tiered_snapshot).
///
/// `auto_flush`:
/// - `true` (default via `from_store` and `new`): every committing command is followed by `flush_pending_blocking()`.
///   Used by memory/sqlite/tiered parity tests where reads must always see the latest commits in warm.
/// - `false` (via `from_store_no_auto_flush`): commits do not implicitly flush; the explicit `flush` testscript command
///   is the only way to move data into warm. Used by the tier-snapshot defect-hunting suite.
pub struct Runner {
	pub store: StandardMultiStore,
	pub version: CommitVersion,
	pub auto_flush: bool,
}

impl Runner {
	/// Hot-only constructor (memory or sqlite hot, no warm).
	///
	/// Each integration test binary compiles its own copy of `common`; this
	/// constructor is only consumed by `store_multi.rs`, so other binaries
	/// see it as unused.
	#[allow(dead_code)]
	pub fn new(storage: HotStorage) -> Self {
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		let store = StandardMultiStore::new(MultiStoreConfig {
			hot: Some(HotConfig {
				storage,
			}),
			warm: None,
			cold: None,
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus: EventBus::new(&actor_system),
			actor_system,
			clock: Clock::Real,
		})
		.unwrap();
		Self::from_store(store)
	}

	/// Reuse an externally built store with auto-flush enabled.
	#[allow(dead_code)]
	pub fn from_store(store: StandardMultiStore) -> Self {
		Self {
			store,
			version: CommitVersion(0),
			auto_flush: true,
		}
	}

	/// Reuse an externally built store WITHOUT auto-flush. Used by the
	/// tier-snapshot suite to control flush timing precisely.
	#[allow(dead_code)]
	pub fn from_store_no_auto_flush(store: StandardMultiStore) -> Self {
		Self {
			store,
			version: CommitVersion(0),
			auto_flush: false,
		}
	}

	#[inline]
	fn maybe_flush(&self) {
		if self.auto_flush {
			self.store.flush_pending_blocking();
		}
	}
}

impl testscript::runner::Runner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();
		match command.name.as_str() {
			"get" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				let value = self.store.get(&key, version)?.map(|sv: MultiVersionRow| sv.row.to_vec());

				writeln!(output, "{}", Raw::key_maybe_value(&key, value))?;
			}
			"contains" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;
				let contains = self.store.contains(&key, version)?;
				writeln!(output, "{} => {}", Raw::key(&key), contains)?;
			}

			"scan" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				if !reverse {
					let items: Vec<_> = self
						.store
						.range(EncodedKeyRange::all(), version, 1024)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				} else {
					let items: Vec<_> = self
						.store
						.range_rev(EncodedKeyRange::all(), version, 1024)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				};
			}
			"range" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let range = EncodedKeyRange::parse(
					args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."),
				);
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				if !reverse {
					let items: Vec<_> = self
						.store
						.range(range, version, 1024)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				} else {
					let items: Vec<_> = self
						.store
						.range_rev(range, version, 1024)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				};
			}

			"prefix" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				let prefix =
					EncodedKey(decode_binary(&args.next_pos().ok_or("prefix not given")?.value));
				args.reject_rest()?;

				let range = EncodedKeyRange::prefix(&prefix.0);
				if !reverse {
					let items: Vec<_> = self
						.store
						.range(range, version, 1024)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				} else {
					let items: Vec<_> = self
						.store
						.range_rev(range, version, 1024)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				};
			}

			"set" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey(decode_binary(&kv.key.unwrap()));
				let row = EncodedRow(decode_binary(&kv.value));
				let version = if let Some(v) = args.lookup_parse("version")? {
					v
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				self.store.commit(
					cow_vec![
						(Delta::Set {
							key,
							row
						})
					],
					version,
				)?;
				self.maybe_flush();
			}

			"remove" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = if let Some(v) = args.lookup_parse("version")? {
					v
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				self.store.commit(
					cow_vec![
						(Delta::Remove {
							key
						})
					],
					version,
				)?;
				self.maybe_flush();
			}

			"unset" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey(decode_binary(&kv.key.unwrap()));
				let row = EncodedRow(decode_binary(&kv.value));
				let version = if let Some(v) = args.lookup_parse("version")? {
					v
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				self.store.commit(
					cow_vec![
						(Delta::Unset {
							key,
							row
						})
					],
					version,
				)?;
				self.maybe_flush();
			}

			"flush" => {
				self.store.flush_pending_blocking();
				writeln!(output, "ok")?;
			}

			"hot_get" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				let hot = self.store.hot().ok_or("hot tier not configured")?;
				let table = classify_key(&key);
				let value = match get_at_version(hot, table, key.as_ref(), version)? {
					VersionedGetResult::Value {
						value,
						..
					} => Some(value.to_vec()),
					VersionedGetResult::Tombstone => None,
					VersionedGetResult::NotFound => None,
				};
				writeln!(output, "{}", Raw::key_maybe_value(&key, value))?;
			}

			"warm_get" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				let warm = self.store.warm().ok_or("warm tier not configured")?;
				let table = classify_key(&key);
				let value = warm.get(table, key.as_ref(), version)?.map(|v| v.to_vec());
				writeln!(output, "{}", Raw::key_maybe_value(&key, value))?;
			}

			"warm_set" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey(decode_binary(&kv.key.unwrap()));
				let value_bytes = decode_binary(&kv.value);
				let version = if let Some(v) = args.lookup_parse("version")? {
					CommitVersion(v)
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				let warm = self.store.warm().ok_or("warm tier not configured")?;
				let table = classify_key(&key);
				let mut batches: HashMap<EntryKind, Vec<(CowVec<u8>, Option<CowVec<u8>>)>> =
					HashMap::new();
				batches.entry(table).or_default().push((key.0.clone(), Some(value_bytes)));
				warm.set(version, batches)?;
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
		let fmtkv = Raw::key_value(&item.key, item.row.as_slice());
		writeln!(output, "{fmtkv}").unwrap();
	}
}
