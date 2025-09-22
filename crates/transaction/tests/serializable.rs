// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{collections::HashMap, error::Error as StdError, fmt::Write as _, path::Path};

use reifydb_core::{
	EncodedKey, EncodedKeyRange,
	event::EventBus,
	interface::Versioned,
	util::encoding::{binary::decode_binary, format, format::Formatter},
	value::row::EncodedRow,
};
use reifydb_storage::memory::Memory;
use reifydb_testing::testscript;
use reifydb_transaction::{
	mvcc::{
		transaction::serializable::{CommandTransaction, QueryTransaction, Serializable, Transaction},
		types::TransactionValue,
	},
	svl::SingleVersionLock,
};
use test_each_file::test_each_path;

test_each_path! { in "crates/transaction/tests/scripts/mvcc" as mvcc => test_serializable }
test_each_path! { in "crates/transaction/tests/scripts/all" as all => test_serializable }

fn test_serializable(path: &Path) {
	let bus = EventBus::default();

	testscript::run_path(
		&mut MvccRunner::new(Serializable::new(
			Memory::new(),
			SingleVersionLock::new(Memory::new(), bus.clone()),
			bus,
		)),
		path,
	)
	.expect("testfailed")
}

pub struct MvccRunner {
	engine: Serializable<Memory, SingleVersionLock<Memory>>,
	transactions: HashMap<String, Transaction<Memory, SingleVersionLock<Memory>>>,
}

impl MvccRunner {
	fn new(serializable: Serializable<Memory, SingleVersionLock<Memory>>) -> Self {
		Self {
			engine: serializable,
			transactions: HashMap::new(),
		}
	}

	/// Fetches the named transaction from a command prefix.
	fn get_transaction(
		&mut self,
		prefix: &Option<String>,
	) -> Result<&'_ mut Transaction<Memory, SingleVersionLock<Memory>>, Box<dyn StdError>> {
		let name = Self::tx_name(prefix)?;
		self.transactions.get_mut(name).ok_or(format!("unknown transaction
{name}")
		.into())
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
					true => Transaction::Query(
						QueryTransaction::new(self.engine.clone(), version).unwrap(),
					),
					false => Transaction::Command(
						CommandTransaction::new(self.engine.clone()).unwrap(),
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
					Transaction::Query(_) => {
						unreachable!("can not call commit on rx")
					}
					Transaction::Command(mut tx) => {
						tx.commit()?;
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
						Transaction::Query(_) => {
							unreachable!("can not call remove on rx")
						}
						Transaction::Command(tx) => {
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
					Transaction::Query(rx) => rx.version(),
					Transaction::Command(tx) => tx.version(),
				};
				writeln!(output, "{}", version)?;
			}

			// tx: get KEY...
			"get" => {
				let mut args = command.consume_args();
				for arg in args.rest_pos() {
					let key = EncodedKey(decode_binary(&arg.value));
					let t = self.get_transaction(&command.prefix)?;

					let value = match t {
						Transaction::Query(rx) => {
							rx.get(&key).map(|r| r.and_then(|tv| Some(tv.row().to_vec())))
						}
						Transaction::Command(tx) => {
							tx.get(&key).map(|r| r.and_then(|tv| Some(tv.row().to_vec())))
						}
					}
					.unwrap();

					let fmtkv = format::Raw::key_maybe_row(&key, value.as_ref());
					writeln!(output, "{fmtkv}")?;
				}
				args.reject_rest()?;
			}

			// import KEY=VALUE...
			"import" => {
				Self::no_tx(command)?;
				let mut args = command.consume_args();

				let mut tx = CommandTransaction::new(self.engine.clone()).unwrap();

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
					Transaction::Query(_) => {
						unreachable!("can not call rollback on rx")
					}
					Transaction::Command(mut tx) => {
						tx.rollback()?;
					}
				}
			}

			// tx: scan
			"scan" => {
				let t = self.get_transaction(&command.prefix)?;
				let args = command.consume_args();
				args.reject_rest()?;

				let mut kvs = Vec::new();
				match t {
					Transaction::Query(rx) => {
						let iter = rx.scan().unwrap();
						for versioned in iter {
							kvs.push((versioned.key.clone(), versioned.row.to_vec()));
						}
					}
					Transaction::Command(tx) => {
						for item in tx.scan().unwrap().into_iter() {
							kvs.push((item.key().clone(), item.row().to_vec()));
						}
					}
				}

				for (key, value) in kvs {
					writeln!(output, "{}", format::Raw::key_row(&key, &value))?;
				}
			}

			// range RANGE [reverse=BOOL]
			"range" => {
				let t = self.get_transaction(&command.prefix)?;

				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let range = EncodedKeyRange::parse(
					args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."),
				);
				args.reject_rest()?;

				match t {
					Transaction::Query(rx) => {
						if !reverse {
							print_rx(&mut output, rx.range(range).unwrap())
						} else {
							print_rx(&mut output, rx.range_rev(range).unwrap())
						}
					}
					Transaction::Command(tx) => {
						if !reverse {
							print_tx(&mut output, tx.range(range).unwrap())
						} else {
							print_tx(&mut output, tx.range_rev(range).unwrap())
						}
					}
				}
			}

			// prefix PREFIX [reverse=BOOL] [version=VERSION]
			"prefix" => {
				let t = self.get_transaction(&command.prefix)?;

				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let prefix = EncodedKey(decode_binary(
					&args.next_pos()
						.ok_or("prefix
not given")?
						.value,
				));
				args.reject_rest()?;

				match t {
					Transaction::Query(rx) => {
						if !reverse {
							print_rx(&mut output, rx.prefix(&prefix).unwrap())
						} else {
							print_rx(&mut output, rx.prefix_rev(&prefix).unwrap())
						}
					}
					Transaction::Command(tx) => {
						if !reverse {
							print_tx(&mut output, tx.prefix(&prefix).unwrap())
						} else {
							print_tx(&mut output, tx.prefix_rev(&prefix).unwrap())
						}
					}
				}
			}

			// tx: set KEY=VALUE...
			"set" => {
				let t = self.get_transaction(&command.prefix)?;
				let mut args = command.consume_args();
				for kv in args.rest_key() {
					let key = EncodedKey(decode_binary(kv.key.as_ref().unwrap()));
					let row = EncodedRow(decode_binary(&kv.value));
					match t {
						Transaction::Query(_) => {
							unreachable!("can not call set on rx")
						}
						Transaction::Command(tx) => {
							tx.set(&key, row).unwrap();
						}
					}
				}
				args.reject_rest()?;
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
	I: Iterator<Item = Versioned>,
{
	while let Some(sv) = iter.next() {
		let fmtkv = format::Raw::key_row(&sv.key, sv.row.as_slice());
		writeln!(output, "{fmtkv}").unwrap();
	}
}

fn print_tx<I>(output: &mut String, mut iter: I)
where
	I: Iterator<Item = TransactionValue>,
{
	while let Some(tv) = iter.next() {
		let fmtkv = format::Raw::key_row(tv.key(), tv.row().as_slice());
		writeln!(output, "{fmtkv}").unwrap();
	}
}
