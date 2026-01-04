// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{collections::HashMap, error::Error as StdError, fmt::Write as _, path::Path};

use futures_util::TryStreamExt;
use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange,
	event::EventBus,
	interface::MultiVersionValues,
	util::encoding::{binary::decode_binary, format, format::Formatter},
	value::encoded::EncodedValues,
};
use reifydb_store_transaction::TransactionStore;
use reifydb_testing::testscript;
use reifydb_transaction::{
	multi::{
		TransactionMulti,
		transaction::{CommandTransaction, QueryTransaction},
	},
	single::{TransactionSingle, TransactionSvl},
};
use tokio::runtime::Runtime;

/// A handle to either a query or command transaction for test tracking
enum TransactionHandle {
	Query(QueryTransaction),
	Command(CommandTransaction),
}
use test_each_file::test_each_path;

test_each_path! { in "crates/transaction/tests/scripts/multi" as serializable_multi => test_serializable }
test_each_path! { in "crates/transaction/tests/scripts/all" as serializable_all => test_serializable }

fn test_serializable(path: &Path) {
	let runtime = Runtime::new().unwrap();

	let engine = runtime
		.block_on(async {
			// Create store inside runtime context because it spawns a background writer task
			let store = TransactionStore::testing_memory().await;
			let bus = EventBus::default();
			TransactionMulti::new(
				store.clone(),
				TransactionSingle::SingleVersionLock(TransactionSvl::new(store.clone(), bus.clone())),
				bus,
			)
			.await
		})
		.unwrap();

	testscript::run_path(&mut MvccRunner::new(engine, runtime), path).expect("testfailed")
}

pub struct MvccRunner {
	engine: TransactionMulti,
	transactions: HashMap<String, TransactionHandle>,
	runtime: Runtime,
}

impl MvccRunner {
	fn new(engine: TransactionMulti, runtime: Runtime) -> Self {
		Self {
			engine,
			transactions: HashMap::new(),
			runtime,
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
	fn no_tx(command: &testscript::Command) -> Result<(), Box<dyn StdError>> {
		if let Some(name) = &command.prefix {
			return Err(format!("can't run {} with tx {name}", command.name).into());
		}
		Ok(())
	}
}

impl<'a> testscript::Runner for MvccRunner {
	fn run(&mut self, command: &testscript::Command) -> Result<String, Box<dyn StdError>> {
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
					true => TransactionHandle::Query(
						self.runtime
							.block_on(async {
								QueryTransaction::new(self.engine.clone(), version)
									.await
							})
							.unwrap(),
					),
					false => TransactionHandle::Command(
						self.runtime
							.block_on(async {
								CommandTransaction::new(self.engine.clone()).await
							})
							.unwrap(),
					),
				};

				self.transactions.insert(name.to_string(), t);
			}

			// tx: commit
			"commit" => {
				let name = Self::tx_name(&command.prefix)?;
				let t = self.transactions.remove(name).ok_or(format!("unknown tx {name}"))?;
				command.consume_args().reject_rest()?;

				match t {
					TransactionHandle::Query(_) => {
						unreachable!("can not call commit on rx")
					}
					TransactionHandle::Command(mut tx) => {
						self.runtime.block_on(async { tx.commit().await })?;
					}
				}
			}

			// tx: remove KEY...
			"remove" => {
				let t = self.get_transaction(&command.prefix)?;
				let mut args = command.consume_args();
				for arg in args.rest_pos() {
					let key = EncodedKey(decode_binary(&arg.value));

					match t {
						TransactionHandle::Query(_) => {
							unreachable!("can not call remove on rx")
						}
						TransactionHandle::Command(tx) => {
							tx.remove(&key).unwrap();
						}
					}
				}
				args.reject_rest()?;
			}

			"version" => {
				command.consume_args().reject_rest()?;
				let t = self.get_transaction(&command.prefix)?;
				let version = match t {
					TransactionHandle::Query(rx) => rx.version(),
					TransactionHandle::Command(tx) => tx.version(),
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
						TransactionHandle::Query(rx) => self
							.runtime
							.block_on(async { rx.get(&key).await })
							.map(|r| r.and_then(|tv| Some(tv.values().to_vec()))),
						TransactionHandle::Command(tx) => self
							.runtime
							.block_on(async { tx.get(&key).await })
							.map(|r| r.and_then(|tv| Some(tv.values().to_vec()))),
					}
					.unwrap();

					let fmtkv = format::Raw::key_maybe_value(&key, value.as_ref());
					writeln!(output, "{fmtkv}")?;
				}
				args.reject_rest()?;
				self.transactions.insert(name.to_string(), t);
			}

			// import KEY=VALUE...
			"import" => {
				Self::no_tx(command)?;
				let mut args = command.consume_args();

				let mut tx = self
					.runtime
					.block_on(async { CommandTransaction::new(self.engine.clone()).await })
					.unwrap();

				for kv in args.rest_key() {
					let key = EncodedKey(decode_binary(kv.key.as_ref().unwrap()));
					let values = EncodedValues(decode_binary(&kv.value));
					if values.is_empty() {
						tx.remove(&key).unwrap();
					} else {
						tx.set(&key, values).unwrap();
					}
				}
				args.reject_rest()?;
				self.runtime.block_on(async { tx.commit().await })?;
			}

			// tx: rollback
			"rollback" => {
				let name = Self::tx_name(&command.prefix)?;
				let t = self.transactions.remove(name).ok_or(format!("unknown tx {name}"))?;
				command.consume_args().reject_rest()?;

				match t {
					TransactionHandle::Query(_) => {
						unreachable!("can not call rollback on rx")
					}
					TransactionHandle::Command(mut tx) => {
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
					TransactionHandle::Query(rx) => {
						let items: Vec<_> = self
							.runtime
							.block_on(async {
								rx.range(EncodedKeyRange::all(), 1024)
									.try_collect()
									.await
							})
							.unwrap();
						for multi in items {
							kvs.push((multi.key.clone(), multi.values.to_vec()));
						}
					}
					TransactionHandle::Command(tx) => {
						let items: Vec<_> = self
							.runtime
							.block_on(async {
								tx.range(EncodedKeyRange::all(), 1024)
									.try_collect()
									.await
							})
							.unwrap();
						for item in items {
							kvs.push((item.key.clone(), item.values.to_vec()));
						}
					}
				}

				for (key, value) in kvs {
					writeln!(output, "{}", format::Raw::key_value(&key, &value))?;
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
					TransactionHandle::Query(rx) => {
						if !reverse {
							let items: Vec<_> = self
								.runtime
								.block_on(async {
									rx.range(range, 1024).try_collect().await
								})
								.unwrap();
							print_rx(&mut output, items.into_iter())
						} else {
							let items: Vec<_> = self
								.runtime
								.block_on(async {
									rx.range_rev(range, 1024).try_collect().await
								})
								.unwrap();
							print_rx(&mut output, items.into_iter())
						}
					}
					TransactionHandle::Command(tx) => {
						if !reverse {
							let items: Vec<_> = self
								.runtime
								.block_on(async {
									tx.range(range, 1024).try_collect().await
								})
								.unwrap();
							print_rx(&mut output, items.into_iter())
						} else {
							let items: Vec<_> = self
								.runtime
								.block_on(async {
									tx.range_rev(range, 1024).try_collect().await
								})
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
					TransactionHandle::Query(rx) => {
						if !reverse {
							let batch = self
								.runtime
								.block_on(async { rx.prefix(&prefix).await })
								.unwrap();
							print_rx(&mut output, batch.items.into_iter())
						} else {
							let batch = self
								.runtime
								.block_on(async { rx.prefix_rev(&prefix).await })
								.unwrap();
							print_rx(&mut output, batch.items.into_iter())
						}
					}
					TransactionHandle::Command(tx) => {
						if !reverse {
							let batch = self
								.runtime
								.block_on(async { tx.prefix(&prefix).await })
								.unwrap();
							print_rx(&mut output, batch.items.into_iter())
						} else {
							let batch = self
								.runtime
								.block_on(async { tx.prefix_rev(&prefix).await })
								.unwrap();
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
					let values = EncodedValues(decode_binary(&kv.value));
					match t {
						TransactionHandle::Query(_) => {
							unreachable!("can not call set on rx")
						}
						TransactionHandle::Command(tx) => {
							tx.set(&key, values).unwrap();
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
					TransactionHandle::Query(rx) => {
						rx.read_as_of_version_inclusive(version);
					}
					TransactionHandle::Command(tx) => {
						tx.read_as_of_version_inclusive(version)?;
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
					TransactionHandle::Query(rx) => {
						rx.read_as_of_version_exclusive(version);
					}
					TransactionHandle::Command(tx) => {
						tx.read_as_of_version_exclusive(version);
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
	I: Iterator<Item = MultiVersionValues>,
{
	while let Some(sv) = iter.next() {
		let fmtkv = format::Raw::key_value(&sv.key, sv.values.as_slice());
		writeln!(output, "{fmtkv}").unwrap();
	}
}
