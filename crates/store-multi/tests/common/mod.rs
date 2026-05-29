// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{collections::HashMap, error::Error as StdError, fmt::Write, sync::Arc};

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	event::EventBus,
	interface::store::{
		EntryKind, MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionRow, classify_key,
	},
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
	MultiVersionScope,
	config::{CommitBufferConfig, MultiStoreConfig, PersistentConfig},
	gc::EvictionWatermark,
	store::StandardMultiStore,
	tier::{TierStorage, VersionedGetResult, commit::buffer::MultiCommitBufferTier},
};
use reifydb_testing::testscript;
use reifydb_value::{cow_vec, util::cowvec::CowVec};
use testscript::command::Command;

/// Shared testscript runner used by every per-backend test binary
/// (memory / sqlite / tiered / tiered_snapshot).
///
/// `auto_flush`:
/// - `true` (default via `from_store` and `new`): every committing command is followed by `flush_pending_blocking()`.
///   Used by memory/sqlite/tiered parity tests where reads must always see the latest commits in persistent.
/// - `false` (via `from_store_no_auto_flush`): commits do not implicitly flush; the explicit `flush` testscript command
///   is the only way to move data into persistent. Used by the tier-snapshot defect-hunting suite.
pub struct Runner {
	pub store: StandardMultiStore,
	pub version: CommitVersion,
	pub auto_flush: bool,
}

impl Runner {
	/// Buffer-only constructor (memory or sqlite buffer, no persistent).
	///
	/// Each integration test binary compiles its own copy of `common`; this
	/// constructor is only consumed by `store_multi.rs`, so other binaries
	/// see it as unused.
	#[allow(dead_code)]
	pub fn new(storage: MultiCommitBufferTier) -> Self {
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		let store = StandardMultiStore::new(MultiStoreConfig {
			commit: Some(CommitBufferConfig {
				storage,
			}),
			persistent: None,
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus: EventBus::new(&actor_system),
			actor_system,
			clock: Clock::Real,
		})
		.unwrap();
		Self::from_store(store)
	}

	/// Persistent-only constructor (no buffer). Mirrors `new` for the unbuffered case.
	#[allow(dead_code)]
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_unbuffered(persistent: PersistentConfig) -> Self {
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		let event_bus = EventBus::new(&actor_system);
		let store = StandardMultiStore::new(MultiStoreConfig::sqlite_unbuffered(
			persistent,
			actor_system,
			Clock::Real,
			event_bus,
		))
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
				let key =
					EncodedKey::new(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				let value = self.store.get(&key, version)?.map(|sv: MultiVersionRow| sv.row.to_vec());

				writeln!(output, "{}", Raw::key_maybe_value(&key, value))?;
			}

			"get_many" => {
				let mut args = command.consume_args();
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				let keys: Vec<EncodedKey> = args
					.rest_pos()
					.into_iter()
					.map(|a| EncodedKey::new(decode_binary(&a.value)))
					.collect();
				args.reject_rest()?;

				let found = self.store.get_many(&keys, version)?;
				for key in &keys {
					let value = found.get(key).map(|row| row.row.to_vec());
					writeln!(output, "{}", Raw::key_maybe_value(key, value))?;
				}
			}
			"contains" => {
				let mut args = command.consume_args();
				let key =
					EncodedKey::new(decode_binary(&args.next_pos().ok_or("key not given")?.value));
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
						.range(
							EncodedKeyRange::all(),
							MultiVersionScope::AsOf {
								read: version,
							},
							1024,
						)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				} else {
					let items: Vec<_> = self
						.store
						.range_rev(
							EncodedKeyRange::all(),
							MultiVersionScope::AsOf {
								read: version,
							},
							1024,
						)
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
						.range(
							range,
							MultiVersionScope::AsOf {
								read: version,
							},
							1024,
						)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				} else {
					let items: Vec<_> = self
						.store
						.range_rev(
							range,
							MultiVersionScope::AsOf {
								read: version,
							},
							1024,
						)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				};
			}

			"prefix" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				let prefix = EncodedKey::new(decode_binary(
					&args.next_pos().ok_or("prefix not given")?.value,
				));
				args.reject_rest()?;

				let range = EncodedKeyRange::prefix(prefix.as_slice());
				if !reverse {
					let items: Vec<_> = self
						.store
						.range(
							range,
							MultiVersionScope::AsOf {
								read: version,
							},
							1024,
						)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				} else {
					let items: Vec<_> = self
						.store
						.range_rev(
							range,
							MultiVersionScope::AsOf {
								read: version,
							},
							1024,
						)
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
					v
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				MultiVersionCommit::commit(
					&self.store,
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
				let key =
					EncodedKey::new(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = if let Some(v) = args.lookup_parse("version")? {
					v
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				MultiVersionCommit::commit(
					&self.store,
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
				let key = EncodedKey::new(decode_binary(&kv.key.unwrap()));
				let row = EncodedRow(CowVec::new(decode_binary(&kv.value)));
				let version = if let Some(v) = args.lookup_parse("version")? {
					v
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				MultiVersionCommit::commit(
					&self.store,
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

			"watermark" => {
				let mut args = command.consume_args();
				let version = CommitVersion(
					args.next_pos().ok_or("watermark version not given")?.value.parse()?,
				);
				args.reject_rest()?;

				self.store.set_eviction_watermark(Arc::new(FixedWatermark(version)));
				writeln!(output, "ok")?;
			}

			"flush" => {
				self.store.flush_pending_blocking();
				writeln!(output, "ok")?;
			}

			"buffer_get" => {
				let mut args = command.consume_args();
				let key =
					EncodedKey::new(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				let buffer = self.store.commit().ok_or("buffer tier not configured")?;
				let table = classify_key(&key);
				let value = match buffer.get(table, key.as_ref(), version)? {
					VersionedGetResult::Value {
						value,
						..
					} => Some(value.to_vec()),
					VersionedGetResult::Tombstone => None,
					VersionedGetResult::NotFound => None,
				};
				writeln!(output, "{}", Raw::key_maybe_value(&key, value))?;
			}

			"persistent_get" => {
				let mut args = command.consume_args();
				let key =
					EncodedKey::new(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				let persistent = self.store.persistent().ok_or("persistent tier not configured")?;
				let table = classify_key(&key);
				let value = persistent.get(table, key.as_ref(), version)?.value().map(|v| v.to_vec());
				writeln!(output, "{}", Raw::key_maybe_value(&key, value))?;
			}

			"buffer_get_state" => {
				let mut args = command.consume_args();
				let key =
					EncodedKey::new(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				let buffer = self.store.commit().ok_or("buffer tier not configured")?;
				let table = classify_key(&key);
				let line = match buffer.get(table, key.as_ref(), version)? {
					VersionedGetResult::Value {
						value,
						version: found,
					} => format!(
						"{} => {} version={}",
						Raw::key(&key),
						Raw::bytes(value.as_ref()),
						found.0
					),
					VersionedGetResult::Tombstone => format!("{} => tombstone", Raw::key(&key)),
					VersionedGetResult::NotFound => format!("{} => notfound", Raw::key(&key)),
				};
				writeln!(output, "{line}")?;
			}

			"persistent_get_state" => {
				let mut args = command.consume_args();
				let key =
					EncodedKey::new(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				let persistent = self.store.persistent().ok_or("persistent tier not configured")?;
				let table = classify_key(&key);
				let line = match persistent.get(table, key.as_ref(), version)? {
					VersionedGetResult::Value {
						value,
						version: found,
					} => format!(
						"{} => {} version={}",
						Raw::key(&key),
						Raw::bytes(value.as_ref()),
						found.0
					),
					VersionedGetResult::Tombstone => format!("{} => tombstone", Raw::key(&key)),
					VersionedGetResult::NotFound => format!("{} => notfound", Raw::key(&key)),
				};
				writeln!(output, "{line}")?;
			}

			"persistent_set" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey::new(decode_binary(&kv.key.unwrap()));
				let value_bytes = decode_binary(&kv.value);
				let version = if let Some(v) = args.lookup_parse("version")? {
					CommitVersion(v)
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				let persistent = self.store.persistent().ok_or("persistent tier not configured")?;
				let table = classify_key(&key);
				let mut batches: HashMap<EntryKind, Vec<(EncodedKey, Option<CowVec<u8>>)>> =
					HashMap::new();
				batches.entry(table).or_default().push((key, Some(CowVec::new(value_bytes))));
				persistent.set(version, batches)?;
			}

			name => {
				return Err(format!("invalid command {name}").into());
			}
		}
		Ok(output)
	}
}

/// A constant eviction cutoff injected by the `watermark` testscript command.
///
/// Mirrors the `StaticWatermark` used by the store-multi unit/integration tests:
/// the store reads this through the `EvictionWatermark` trait when the flush actor
/// sweeps, so a script can pin the cutoff `W` before issuing `flush`. The store
/// stores the watermark in a `OnceLock`, so a single `watermark` command per script
/// is what takes effect.
struct FixedWatermark(CommitVersion);

impl EvictionWatermark for FixedWatermark {
	fn watermark(&self) -> CommitVersion {
		self.0
	}
}

fn print<I: Iterator<Item = MultiVersionRow>>(output: &mut String, iter: I) {
	for item in iter {
		let fmtkv = Raw::key_value(&item.key, item.row.as_slice());
		writeln!(output, "{fmtkv}").unwrap();
	}
}
