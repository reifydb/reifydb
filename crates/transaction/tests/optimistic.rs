// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_core::encoding::binary::decode_binary;
use reifydb_core::encoding::format;
use reifydb_core::encoding::format::Formatter;
use reifydb_storage::memory::Memory;
use reifydb_storage::{KeyRange, StoredValue};
use reifydb_testing::testscript;
use reifydb_transaction::Tx;
use reifydb_transaction::mvcc::transaction::optimistic::{
    Optimistic, Transaction, TransactionRx, TransactionTx,
};
use reifydb_transaction::mvcc::types::TransactionValue;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Write as _;
use std::ops::Deref;
use std::path::Path;
use test_each_file::test_each_path;

test_each_path! { in "crates/transaction/tests/scripts/mvcc" as mvcc => test_optimistic }
test_each_path! { in "crates/transaction/tests/scripts/all" as all => test_optimistic }

fn test_optimistic(path: &Path) {
    testscript::run_path(&mut MvccRunner::new(Optimistic::new(Memory::new())), path)
        .expect("test failed")
}

pub struct MvccRunner {
    engine: Optimistic<Memory>,
    transactions: HashMap<String, Transaction<Memory>>,
}

impl MvccRunner {
    fn new(optimistic: Optimistic<Memory>) -> Self {
        Self { engine: optimistic, transactions: HashMap::new() }
    }

    /// Fetches the named transaction from a command prefix.
    fn get_transaction(
        &mut self,
        prefix: &Option<String>,
    ) -> Result<&'_ mut Transaction<Memory>, Box<dyn StdError>> {
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
                    Some(v) => return Err(format!("invalid argument {v}").into()),
                };
                let version = args.lookup_parse("version")?;
                args.reject_rest()?;

                let t = match readonly {
                    true => Transaction::Rx(TransactionRx::new(self.engine.clone(), version)),
                    false => Transaction::Tx(TransactionTx::new(self.engine.clone())),
                };
                self.transactions.insert(name.to_string(), t);
            }

            // tx: commit
            "commit" => {
                let name = Self::tx_name(&command.prefix)?;
                let t = self.transactions.remove(name).ok_or(format!("unknown tx {name}"))?;
                command.consume_args().reject_rest()?;

                match t {
                    Transaction::Rx(_) => {
                        unreachable!("can not call commit on rx")
                    }
                    Transaction::Tx(tx) => {
                        tx.commit()?;
                    }
                }
            }

            // tx: remove KEY...
            "remove" => {
                let t = self.get_transaction(&command.prefix)?;
                let mut args = command.consume_args();
                for arg in args.rest_pos() {
                    let key = decode_binary(&arg.value);

                    match t {
                        Transaction::Rx(_) => {
                            unreachable!("can not call remove on rx")
                        }
                        Transaction::Tx(tx) => {
                            tx.remove(key).unwrap();
                        }
                    }
                }
                args.reject_rest()?;
            }

            "version" => {
                command.consume_args().reject_rest()?;
                let t = self.get_transaction(&command.prefix)?;
                let version = match t {
                    Transaction::Rx(rx) => rx.version(),
                    Transaction::Tx(tx) => tx.version(),
                };
                writeln!(output, "{}", version)?;
            }

            // tx: get KEY...
            "get" => {
                let t = self.get_transaction(&command.prefix)?;
                let mut args = command.consume_args();
                for arg in args.rest_pos() {
                    let key = decode_binary(&arg.value);
                    let t = self.get_transaction(&command.prefix)?;
                    let value = match t {
                        Transaction::Rx(rx) => rx.get(&key).map(|r| r.value().to_vec()),
                        Transaction::Tx(tx) => tx.get(&key).unwrap().map(|r| r.value().to_vec()),
                    };
                    let fmtkv = format::Raw::key_maybe_value(&key, value);
                    writeln!(output, "{fmtkv}")?;
                }
                args.reject_rest()?;
            }

            // import KEY=VALUE...
            "import" => {
                Self::no_tx(command)?;
                let mut args = command.consume_args();

                let mut tx = TransactionTx::new(self.engine.clone());

                for kv in args.rest_key() {
                    let key = decode_binary(kv.key.as_ref().unwrap());
                    let value = decode_binary(&kv.value);
                    if value.is_empty() {
                        tx.remove(key).unwrap();
                    } else {
                        tx.set(key, value).unwrap();
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
                    Transaction::Rx(_) => {
                        unreachable!("can not call rollback on rx")
                    }
                    Transaction::Tx(tx) => {
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
                    Transaction::Rx(rx) => {
                        for sv in rx.scan().into_iter() {
                            kvs.push((sv.key.clone(), sv.value.to_vec()));
                        }
                    }
                    Transaction::Tx(tx) => {
                        for item in tx.scan().unwrap().into_iter() {
                            kvs.push((item.key().clone(), item.value().to_vec()));
                        }
                    }
                };

                for (key, value) in kvs {
                    writeln!(output, "{}", format::Raw::key_value(&key, &value))?;
                }
            }

            // scan_range RANGE [reverse=BOOL]
            "scan_range" => {
                let t = self.get_transaction(&command.prefix)?;

                let mut args = command.consume_args();
                let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
                let range =
                    KeyRange::parse(args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."));
                args.reject_rest()?;

                match t {
                    Transaction::Rx(rx) => {
                        if !reverse {
                            print_rx(&mut output, rx.scan_range(range).into_iter())
                        } else {
                            print_rx(&mut output, rx.scan_range_rev(range).into_iter())
                        }
                    }
                    Transaction::Tx(tx) => {
                        if !reverse {
                            print_tx(&mut output, tx.scan_range(range).unwrap().into_iter())
                        } else {
                            print_tx(&mut output, tx.scan_range_rev(range).unwrap().into_iter())
                        }
                    }
                };
            }

            // scan_prefix PREFIX [reverse=BOOL] [version=VERSION]
            "scan_prefix" => {
                let t = self.get_transaction(&command.prefix)?;

                let mut args = command.consume_args();
                let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
                let prefix = decode_binary(&args.next_pos().ok_or("prefix not given")?.value);
                args.reject_rest()?;

                match t {
                    Transaction::Rx(rx) => {
                        if !reverse {
                            print_rx(&mut output, rx.scan_prefix(&prefix).into_iter())
                        } else {
                            print_rx(&mut output, rx.scan_prefix_rev(&prefix).into_iter())
                        }
                    }
                    Transaction::Tx(tx) => {
                        if !reverse {
                            print_tx(&mut output, tx.scan_prefix(&prefix).unwrap().into_iter())
                        } else {
                            print_tx(&mut output, tx.scan_prefix_rev(&prefix).unwrap().into_iter())
                        }
                    }
                };
            }

            // tx: set KEY=VALUE...
            "set" => {
                let t = self.get_transaction(&command.prefix)?;
                let mut args = command.consume_args();
                for kv in args.rest_key() {
                    let key = decode_binary(kv.key.as_ref().unwrap());
                    let value = decode_binary(&kv.value);
                    match t {
                        Transaction::Rx(_) => {
                            unreachable!("can not call set on rx")
                        }
                        Transaction::Tx(tx) => {
                            tx.set(key, value).unwrap();
                        }
                    }
                }
                args.reject_rest()?;
            }

            name => return Err(format!("invalid command {name}").into()),
        }

        if let Some(tag) = tags.iter().next() {
            return Err(format!("unknown tag {tag}").into());
        }

        Ok(output)
    }
}

fn print_rx<I: Iterator<Item = StoredValue>>(output: &mut String, mut iter: I) {
    while let Some(sv) = iter.next() {
        let fmtkv = format::Raw::key_value(&sv.key, &sv.value.deref());
        writeln!(output, "{fmtkv}").unwrap();
    }
}

fn print_tx<I: Iterator<Item = TransactionValue>>(output: &mut String, mut iter: I) {
    while let Some(tv) = iter.next() {
        let fmtkv = format::Raw::key_value(tv.key(), tv.value().deref());
        writeln!(output, "{fmtkv}").unwrap();
    }
}
