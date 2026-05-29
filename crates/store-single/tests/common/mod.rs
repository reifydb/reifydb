// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{error::Error as StdError, fmt::Write};

use reifydb_core::{
	delta::Delta,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	interface::store::{
		SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionRange,
		SingleVersionRangeRev, SingleVersionRow,
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
use reifydb_store_single::{
	buffer::tier::SingleBufferTier,
	config::{BufferConfig, PersistentConfig, SingleStoreConfig},
	store::StandardSingleStore,
	tier::TierStorage,
};
use reifydb_testing::testscript;
use reifydb_value::{cow_vec, util::cowvec::CowVec};
use testscript::command::Command;

/// Shared testscript runner used by every per-backend integration test
/// (memory / sqlite / tiered).
///
/// `auto_flush`:
/// - `true` (set via `from_store_auto_flush`): every committing command is followed by `flush_pending_blocking()`. Used
///   by tiered parity tests so reads always see the latest commits in persistent.
/// - `false` (default for buffer-only constructors and `from_store_no_auto_flush`): commits do not implicitly flush.
///   The explicit `flush` testscript command is the only way to move data into persistent. Used by tier-snapshot
///   defect-hunting suites.
pub struct Runner {
	pub store: StandardSingleStore,
	pub auto_flush: bool,
}

impl Runner {
	/// Buffer-only constructor (memory or sqlite buffer, no persistent).
	#[allow(dead_code)]
	pub fn new(storage: SingleBufferTier) -> Self {
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		let store = StandardSingleStore::new(SingleStoreConfig {
			buffer: Some(BufferConfig {
				storage,
			}),
			persistent: None,
			actor_system,
			clock: Clock::Real,
		})
		.unwrap();
		Self {
			store,
			auto_flush: false,
		}
	}

	/// Persistent-only constructor (no buffer). Mirrors `new` for the unbuffered case.
	#[allow(dead_code)]
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_unbuffered(persistent: PersistentConfig) -> Self {
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		let store = StandardSingleStore::new(SingleStoreConfig::sqlite_unbuffered(
			persistent,
			actor_system,
			Clock::Real,
		))
		.unwrap();
		Self {
			store,
			auto_flush: false,
		}
	}

	/// Reuse an externally built store with auto-flush enabled.
	#[allow(dead_code)]
	pub fn from_store_auto_flush(store: StandardSingleStore) -> Self {
		Self {
			store,
			auto_flush: true,
		}
	}

	/// Reuse an externally built store WITHOUT auto-flush. Used by tier-snapshot suites to control
	/// flush timing precisely.
	#[allow(dead_code)]
	pub fn from_store_no_auto_flush(store: StandardSingleStore) -> Self {
		Self {
			store,
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
				args.reject_rest()?;
				let value: Option<SingleVersionRow> = self.store.get(&key)?.into();
				let value = value.map(|sv| sv.row.to_vec());
				writeln!(output, "{}", Raw::key_maybe_value(&key, value))?;
			}
			"contains" => {
				let mut args = command.consume_args();
				let key =
					EncodedKey::new(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;
				let contains = self.store.contains(&key)?;
				writeln!(output, "{} => {}", Raw::key(&key), contains)?;
			}

			"scan" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				args.reject_rest()?;

				if !reverse {
					let batch = SingleVersionRange::range(&self.store, EncodedKeyRange::all())?;
					print(&mut output, batch.items.into_iter())
				} else {
					let batch =
						SingleVersionRangeRev::range_rev(&self.store, EncodedKeyRange::all())?;
					print(&mut output, batch.items.into_iter())
				};
			}

			"range" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let range = EncodedKeyRange::parse(
					args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."),
				);
				args.reject_rest()?;

				if !reverse {
					let batch = SingleVersionRange::range(&self.store, range)?;
					print(&mut output, batch.items.into_iter())
				} else {
					let batch = SingleVersionRangeRev::range_rev(&self.store, range)?;
					print(&mut output, batch.items.into_iter())
				};
			}

			"prefix" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let prefix = EncodedKey::new(decode_binary(
					&args.next_pos().ok_or("prefix not given")?.value,
				));
				args.reject_rest()?;

				if !reverse {
					let batch = SingleVersionRange::prefix(&self.store, &prefix)?;
					print(&mut output, batch.items.into_iter())
				} else {
					let batch = SingleVersionRangeRev::prefix_rev(&self.store, &prefix)?;
					print(&mut output, batch.items.into_iter())
				};
			}

			"set" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey::new(decode_binary(&kv.key.unwrap()));
				let row = EncodedRow(CowVec::new(decode_binary(&kv.value)));
				args.reject_rest()?;

				self.store.commit(cow_vec![
					(Delta::Set {
						key,
						row
					})
				])?;
				self.maybe_flush();
			}

			"remove" => {
				let mut args = command.consume_args();
				let key =
					EncodedKey::new(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;

				self.store.commit(cow_vec![
					(Delta::Remove {
						key
					})
				])?;
				self.maybe_flush();
			}

			"unset" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey::new(decode_binary(&kv.key.unwrap()));
				let row = EncodedRow(CowVec::new(decode_binary(&kv.value)));
				args.reject_rest()?;

				self.store.commit(cow_vec![
					(Delta::Unset {
						key,
						row
					})
				])?;
				self.maybe_flush();
			}

			"drop" => {
				let mut args = command.consume_args();
				let key =
					EncodedKey::new(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;

				self.store.commit(cow_vec![
					(Delta::Drop {
						key,
					})
				])?;
				self.maybe_flush();
			}

			"flush" => {
				self.store.flush_pending_blocking();
				writeln!(output, "ok")?;
			}

			"buffer_get" => {
				let mut args = command.consume_args();
				let key =
					EncodedKey::new(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;

				let buffer = self.store.buffer().ok_or("buffer tier not configured")?;
				let value = buffer.get_with_tombstone(key.as_ref())?;
				let value = match value {
					Some(Some(v)) => Some(v.to_vec()),
					Some(None) => None,
					None => None,
				};
				writeln!(output, "{}", Raw::key_maybe_value(&key, value))?;
			}

			"persistent_get" => {
				let mut args = command.consume_args();
				let key =
					EncodedKey::new(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;

				let persistent = self.store.persistent().ok_or("persistent tier not configured")?;
				let value = persistent.get(key.as_ref())?.map(|v| v.to_vec());
				writeln!(output, "{}", Raw::key_maybe_value(&key, value))?;
			}

			"persistent_set" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey::new(decode_binary(&kv.key.unwrap()));
				let value_bytes = CowVec::new(decode_binary(&kv.value));
				args.reject_rest()?;

				let persistent = self.store.persistent().ok_or("persistent tier not configured")?;
				let entries: Vec<(EncodedKey, Option<CowVec<u8>>)> = vec![(key, Some(value_bytes))];
				persistent.set(entries)?;
			}

			name => {
				return Err(format!("invalid command {name}").into());
			}
		}
		Ok(output)
	}
}

fn print<I: Iterator<Item = SingleVersionRow>>(output: &mut String, iter: I) {
	for item in iter {
		let fmtkv = Raw::key_value(&item.key, item.row.as_slice());
		writeln!(output, "{fmtkv}").unwrap();
	}
}
