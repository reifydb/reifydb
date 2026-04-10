// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{collections::HashMap, error::Error as StdError, fmt::Write as _, path::Path, sync::Arc};

use reifydb_core::{
	common::CommitVersion,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	event::EventBus,
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		store::MultiVersionRow,
	},
	util::encoding::{
		binary::decode_binary,
		format::{Formatter, raw::Raw},
	},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::{
		clock::{Clock, MockClock},
		rng::Rng,
	},
	pool::Pools,
};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_testing::testscript::{
	command::Command,
	runner::{Runner, run_path},
};
use reifydb_transaction::{
	multi::transaction::{
		MultiTransaction, read::MultiReadTransaction, replica::MultiReplicaTransaction,
		write::MultiWriteTransaction,
	},
	single::SingleTransaction,
};
use reifydb_type::value::Value;

/// A handle to either a read, write, or replica transaction for test tracking
enum TransactionHandle {
	Read(MultiReadTransaction),
	Write(MultiWriteTransaction),
	Replica(MultiReplicaTransaction),
}
use test_each_file::test_each_path;

test_each_path! { in "crates/transaction/tests/scripts/multi" as serializable_multi => test_serializable }
test_each_path! { in "crates/transaction/tests/scripts/all" as serializable_all => test_serializable }

fn test_serializable(path: &Path) {
	let multi_store = MultiStore::testing_memory();
	let single_store = SingleStore::testing_memory();
	let bus = EventBus::new(&ActorSystem::new(Pools::default(), Clock::Real));
	let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
	struct DefaultConfig;
	impl GetConfig for DefaultConfig {
		fn get_config(&self, key: ConfigKey) -> Value {
			key.default_value()
		}
		fn get_config_at(&self, key: ConfigKey, _version: CommitVersion) -> Value {
			key.default_value()
		}
	}
	let engine = MultiTransaction::new(
		multi_store,
		SingleTransaction::new(single_store, bus.clone()),
		bus,
		actor_system,
		Clock::Mock(MockClock::from_millis(1000)),
		Rng::seeded(42),
		Arc::new(DefaultConfig),
	)
	.unwrap();

	run_path(&mut MvccRunner::new(engine), path).expect("testfailed")
}

pub struct MvccRunner {
	engine: MultiTransaction,
	transactions: HashMap<String, TransactionHandle>,
}

impl MvccRunner {
	fn new(engine: MultiTransaction) -> Self {
		Self {
			engine,
			transactions: HashMap::new(),
		}
	}

	/// Fetches the named transaction from a command prefix.
	fn get_transaction(&mut self, prefix: &Option<String>) -> Result<&'_ mut TransactionHandle, Box<dyn StdError>> {
		let name = Self::tx_name(prefix)?;
		self.transactions.get_mut(name).ok_or(format!("unknown transaction {name}").into())
	}

	/// Fetches the tx name from a command prefix, or errors.
	fn tx_name(prefix: &Option<String>) -> Result<&str, Box<dyn StdError>> {
		prefix.as_deref().ok_or("no tx name".into())
	}

	/// Errors if a tx prefix is given.
	fn no_tx(command: &Command) -> Result<(), Box<dyn StdError>> {
		if let Some(name) = &command.prefix {
			return Err(format!("can't run {} with tx {name}", command.name).into());
		}
		Ok(())
	}
}

impl<'a> Runner for MvccRunner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();
		let tags = command.tags.clone();

		match command.name.as_str() {
			// tx: begin [readonly] [version=VERSION]
			"begin" => {
				let name = Self::tx_name(&command.prefix)?;
				if self.transactions.contains_key(name) {
					return Err(format!("tx {name} already exists").into());
				}
				let mut args = command.consume_args();
				let readonly = match args.next_pos().map(|a| a.value.as_str()) {
					Some("readonly") => true,
					None => false,
					Some(v) => {
						return Err(format!("invalid argument{v}").into());
					}
				};

				let version = args.lookup_parse("version")?;
				args.reject_rest()?;
				let t = match readonly {
					true => TransactionHandle::Read(
						MultiReadTransaction::new(self.engine.clone(), version).unwrap(),
					),
					false => TransactionHandle::Write(
						MultiWriteTransaction::new(self.engine.clone()).unwrap(),
					),
				};

				self.transactions.insert(name.to_string(), t);
			}

			// tx: begin_replica version=VERSION
			"begin_replica" => {
				let name = Self::tx_name(&command.prefix)?;
				if self.transactions.contains_key(name) {
					return Err(format!("tx {name} already exists").into());
				}
				let mut args = command.consume_args();
				let version: u64 =
					args.lookup_parse("version")?.ok_or("version required for begin_replica")?;
				args.reject_rest()?;
				let t = TransactionHandle::Replica(
					MultiReplicaTransaction::new(self.engine.clone(), CommitVersion(version))
						.unwrap(),
				);
				self.transactions.insert(name.to_string(), t);
			}

			// tx: commit
			"commit" => {
				let name = Self::tx_name(&command.prefix)?;
				let t = self.transactions.remove(name).ok_or(format!("unknown tx {name}"))?;
				command.consume_args().reject_rest()?;

				match t {
					TransactionHandle::Read(_) => {
						unreachable!("can not call commit on rx")
					}
					TransactionHandle::Write(mut tx) => {
						tx.commit()?;
					}
					TransactionHandle::Replica(_) => {
						unreachable!("use commit_replica for replica transactions")
					}
				}
			}

			// tx: commit_replica
			"commit_replica" => {
				let name = Self::tx_name(&command.prefix)?;
				let t = self.transactions.remove(name).ok_or(format!("unknown tx {name}"))?;
				command.consume_args().reject_rest()?;

				match t {
					TransactionHandle::Replica(mut tx) => {
						tx.commit_at_version()?;
					}
					_ => {
						return Err("commit_replica only works on replica transactions".into());
					}
				}
			}

			// advance_replica version=VERSION
			"advance_replica" => {
				Self::no_tx(command)?;
				let mut args = command.consume_args();
				let version: u64 =
					args.lookup_parse("version")?.ok_or("version required for advance_replica")?;
				args.reject_rest()?;
				self.engine.advance_version_for_replica(CommitVersion(version));
			}

			// tx: remove KEY...
			"remove" => {
				let t = self.get_transaction(&command.prefix)?;
				let mut args = command.consume_args();
				for arg in args.rest_pos() {
					let key = EncodedKey(decode_binary(&arg.value));

					match t {
						TransactionHandle::Read(_) => {
							unreachable!("can not call remove on rx")
						}
						TransactionHandle::Write(tx) => {
							tx.remove(&key).unwrap();
						}
						TransactionHandle::Replica(tx) => {
							tx.remove(&key).unwrap();
						}
					}
				}
				args.reject_rest()?;
			}

			// tx: unset KEY=VALUE...
			"unset" => {
				let t = self.get_transaction(&command.prefix)?;
				let mut args = command.consume_args();
				for kv in args.rest_key() {
					let key = EncodedKey(decode_binary(kv.key.as_ref().unwrap()));
					let row = EncodedRow(decode_binary(&kv.value));
					match t {
						TransactionHandle::Read(_) => {
							unreachable!("can not call unset on rx")
						}
						TransactionHandle::Write(tx) => {
							tx.unset(&key, row).unwrap();
						}
						TransactionHandle::Replica(tx) => {
							tx.unset(&key, row).unwrap();
						}
					}
				}
				args.reject_rest()?;
			}

			"version" => {
				command.consume_args().reject_rest()?;
				let t = self.get_transaction(&command.prefix)?;
				let version = match t {
					TransactionHandle::Read(rx) => rx.version(),
					TransactionHandle::Write(tx) => tx.version(),
					TransactionHandle::Replica(tx) => tx.version(),
				};
				writeln!(output, "{}", version)?;
			}

			// tx: get KEY...
			"get" => {
				let name = Self::tx_name(&command.prefix)?;
				let mut t =
					self.transactions.remove(name).ok_or(format!("unknown transaction {name}"))?;

				let mut args = command.consume_args();
				for arg in args.rest_pos() {
					let key = EncodedKey(decode_binary(&arg.value));

					let value = match &mut t {
						TransactionHandle::Read(rx) => {
							rx.get(&key).map(|r| r.and_then(|tv| Some(tv.row().to_vec())))
						}
						TransactionHandle::Write(tx) => {
							tx.get(&key).map(|r| r.and_then(|tv| Some(tv.row().to_vec())))
						}
						TransactionHandle::Replica(tx) => {
							tx.get(&key).map(|r| r.and_then(|tv| Some(tv.row().to_vec())))
						}
					}
					.unwrap();

					let fmtkv = Raw::key_maybe_value(&key, value.as_ref());
					writeln!(output, "{fmtkv}")?;
				}
				args.reject_rest()?;
				self.transactions.insert(name.to_string(), t);
			}

			// import KEY=VALUE...
			"import" => {
				Self::no_tx(command)?;
				let mut args = command.consume_args();

				let mut tx = MultiWriteTransaction::new(self.engine.clone()).unwrap();

				for kv in args.rest_key() {
					let key = EncodedKey(decode_binary(kv.key.as_ref().unwrap()));
					let row = EncodedRow(decode_binary(&kv.value));
					if row.is_empty() {
						tx.remove(&key).unwrap();
					} else {
						tx.set(&key, row).unwrap();
					}
				}
				args.reject_rest()?;
				tx.commit()?;
			}

			// tx: rollback
			"rollback" => {
				let name = Self::tx_name(&command.prefix)?;
				let t = self.transactions.remove(name).ok_or(format!("unknown tx {name}"))?;
				command.consume_args().reject_rest()?;

				match t {
					TransactionHandle::Read(_) => {
						unreachable!("can not call rollback on rx")
					}
					TransactionHandle::Write(mut tx) => {
						tx.rollback()?;
					}
					TransactionHandle::Replica(mut tx) => {
						tx.rollback()?;
					}
				}
			}

			// tx: scan
			"scan" => {
				let name = Self::tx_name(&command.prefix)?;
				let mut t =
					self.transactions.remove(name).ok_or(format!("unknown transaction {name}"))?;
				let args = command.consume_args();
				args.reject_rest()?;

				let mut kvs: Vec<(EncodedKey, Vec<u8>)> = Vec::new();
				match &mut t {
					TransactionHandle::Read(rx) => {
						let items: Vec<_> = rx
							.range(EncodedKeyRange::all(), 1024)
							.collect::<Result<Vec<_>, _>>()
							.unwrap();
						for multi in items {
							kvs.push((multi.key.clone(), multi.row.to_vec()));
						}
					}
					TransactionHandle::Write(tx) => {
						let items: Vec<_> = tx
							.range(EncodedKeyRange::all(), 1024)
							.collect::<Result<Vec<_>, _>>()
							.unwrap();
						for item in items {
							kvs.push((item.key.clone(), item.row.to_vec()));
						}
					}
					TransactionHandle::Replica(tx) => {
						let items: Vec<_> = tx
							.range(EncodedKeyRange::all(), 1024)
							.collect::<Result<Vec<_>, _>>()
							.unwrap();
						for item in items {
							kvs.push((item.key.clone(), item.row.to_vec()));
						}
					}
				}

				for (key, value) in kvs {
					writeln!(output, "{}", Raw::key_value(&key, &value))?;
				}
				self.transactions.insert(name.to_string(), t);
			}

			// range RANGE [reverse=BOOL]
			"range" => {
				let name = Self::tx_name(&command.prefix)?;
				let mut t =
					self.transactions.remove(name).ok_or(format!("unknown transaction {name}"))?;

				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let range = EncodedKeyRange::parse(
					args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."),
				);
				args.reject_rest()?;

				match &mut t {
					TransactionHandle::Read(rx) => {
						if !reverse {
							let items: Vec<_> = rx
								.range(range, 1024)
								.collect::<Result<Vec<_>, _>>()
								.unwrap();
							print_rx(&mut output, items.into_iter())
						} else {
							let items: Vec<_> = rx
								.range_rev(range, 1024)
								.collect::<Result<Vec<_>, _>>()
								.unwrap();
							print_rx(&mut output, items.into_iter())
						}
					}
					TransactionHandle::Write(tx) => {
						if !reverse {
							let items: Vec<_> = tx
								.range(range, 1024)
								.collect::<Result<Vec<_>, _>>()
								.unwrap();
							print_rx(&mut output, items.into_iter())
						} else {
							let items: Vec<_> = tx
								.range_rev(range, 1024)
								.collect::<Result<Vec<_>, _>>()
								.unwrap();
							print_rx(&mut output, items.into_iter())
						}
					}
					TransactionHandle::Replica(tx) => {
						if !reverse {
							let items: Vec<_> = tx
								.range(range, 1024)
								.collect::<Result<Vec<_>, _>>()
								.unwrap();
							print_rx(&mut output, items.into_iter())
						} else {
							let items: Vec<_> = tx
								.range_rev(range, 1024)
								.collect::<Result<Vec<_>, _>>()
								.unwrap();
							print_rx(&mut output, items.into_iter())
						}
					}
				}
				self.transactions.insert(name.to_string(), t);
			}

			// prefix PREFIX [reverse=BOOL] [version=VERSION]
			"prefix" => {
				let name = Self::tx_name(&command.prefix)?;
				let mut t =
					self.transactions.remove(name).ok_or(format!("unknown transaction {name}"))?;

				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let prefix =
					EncodedKey(decode_binary(&args.next_pos().ok_or("prefixnot given")?.value));
				args.reject_rest()?;

				match &mut t {
					TransactionHandle::Read(rx) => {
						if !reverse {
							let batch = rx.prefix(&prefix).unwrap();
							print_rx(&mut output, batch.items.into_iter())
						} else {
							let batch = rx.prefix_rev(&prefix).unwrap();
							print_rx(&mut output, batch.items.into_iter())
						}
					}
					TransactionHandle::Write(tx) => {
						if !reverse {
							let batch = tx.prefix(&prefix).unwrap();
							print_rx(&mut output, batch.items.into_iter())
						} else {
							let batch = tx.prefix_rev(&prefix).unwrap();
							print_rx(&mut output, batch.items.into_iter())
						}
					}
					TransactionHandle::Replica(tx) => {
						if !reverse {
							let batch = tx.prefix(&prefix).unwrap();
							print_rx(&mut output, batch.items.into_iter())
						} else {
							let batch = tx.prefix_rev(&prefix).unwrap();
							print_rx(&mut output, batch.items.into_iter())
						}
					}
				}
				self.transactions.insert(name.to_string(), t);
			}

			// tx: set KEY=VALUE...
			"set" => {
				let t = self.get_transaction(&command.prefix)?;
				let mut args = command.consume_args();
				for kv in args.rest_key() {
					let key = EncodedKey(decode_binary(kv.key.as_ref().unwrap()));
					let row = EncodedRow(decode_binary(&kv.value));
					match t {
						TransactionHandle::Read(_) => {
							unreachable!("can not call set on rx")
						}
						TransactionHandle::Write(tx) => {
							tx.set(&key, row).unwrap();
						}
						TransactionHandle::Replica(tx) => {
							tx.set(&key, row).unwrap();
						}
					}
				}
				args.reject_rest()?;
			}

			// tx: set_as_of_inclusive VERSION
			"set_as_of_inclusive" => {
				let t = self.get_transaction(&command.prefix)?;
				let mut args = command.consume_args();
				let version = args
					.next_pos()
					.ok_or("version not provided")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid version number")?;

				args.reject_rest()?;

				match t {
					TransactionHandle::Read(rx) => {
						rx.read_as_of_version_inclusive(version);
					}
					TransactionHandle::Write(tx) => {
						tx.read_as_of_version_inclusive(version)?;
					}
					TransactionHandle::Replica(_) => {
						return Err("set_as_of_inclusive not supported on Replica transaction"
							.into());
					}
				}
			}

			// tx: set_as_of_exclusive VERSION
			"set_as_of_exclusive" => {
				let t = self.get_transaction(&command.prefix)?;
				let mut args = command.consume_args();
				let version = args
					.next_pos()
					.ok_or("version not provided")?
					.value
					.parse::<CommitVersion>()
					.map_err(|_| "invalid version number")?;

				args.reject_rest()?;

				match t {
					TransactionHandle::Read(rx) => {
						rx.read_as_of_version_exclusive(version);
					}
					TransactionHandle::Write(tx) => {
						tx.read_as_of_version_exclusive(version);
					}
					TransactionHandle::Replica(_) => {
						return Err("set_as_of_exclusive not supported on Replica transaction"
							.into());
					}
				}
			}

			name => {
				return Err(format!("invalid command {name}").into());
			}
		}

		if let Some(tag) = tags.iter().next() {
			return Err(format!("unknown tag {tag}").into());
		}

		Ok(output)
	}
}

fn print_rx<I>(output: &mut String, mut iter: I)
where
	I: Iterator<Item = MultiVersionRow>,
{
	while let Some(sv) = iter.next() {
		let fmtkv = Raw::key_value(&sv.key, sv.row.as_slice());
		writeln!(output, "{fmtkv}").unwrap();
	}
}
