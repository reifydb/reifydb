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
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Write as _;

use reifydb_core::encoding::format;
use reifydb_core::encoding::format::Formatter;
use reifydb_core::{AsyncCowVec, CowVec};
use reifydb_testing::testscript;
use reifydb_testing::util::parse_key_range;
use reifydb_transaction::Tx;
use reifydb_transaction::mvcc::transaction::optimistic::{
    Optimistic, Transaction, TransactionRx, TransactionTx,
};
use std::path::Path;
use test_each_file::test_each_path;

test_each_path! { in "crates/transaction/tests/scripts/mvcc" as mvcc => test_optimistic }
test_each_path! { in "crates/transaction/tests/scripts/all" as all => test_optimistic }

fn test_optimistic(path: &Path) {
    testscript::run_path(&mut MvccRunner::new(Optimistic::new()), path).expect("test failed")
}

pub struct MvccRunner {
    engine: Optimistic,
    transactions: HashMap<String, Transaction>,
}

impl MvccRunner {
    fn new(optimistic: Optimistic) -> Self {
        Self { engine: optimistic, transactions: HashMap::new() }
    }

    /// Fetches the named transaction from a command prefix.
    fn get_transaction(
        &mut self,
        prefix: &Option<String>,
    ) -> Result<&'_ mut Transaction, Box<dyn StdError>> {
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
            // tx: begin [readonly]
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
                args.reject_rest()?;

                let tx = match readonly {
                    true => Transaction::Rx(TransactionRx::new(self.engine.clone())),
                    false => Transaction::Tx(TransactionTx::new(self.engine.clone())),
                };
                self.transactions.insert(name.to_string(), tx);
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

            // dump
            // "dump" => {
            //     command.consume_args().reject_rest()?;
            //     let mut persistence = self.mvcc.persistence.lock().unwrap();
            //     let mut scan = persistence.scan(..);
            //     while let Some((key, value)) = scan.next().transpose()? {
            //         let fmtkv = MVCC::<format::Raw>::key_value(&Keyey, &value);
            //         let rawkv = format::Raw::key_value(&Keyey, &value);
            //         writeln!(output, "{fmtkv} [{rawkv}]")?;
            //     }
            // }
            // tx: version
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

            // get_unversioned KEY...
            // "get_unversioned" => {
            //     Self::no_tx(command)?;
            //     let mut args = command.consume_args();
            //     for arg in args.rest_pos() {
            //         let key = decode_binary(&arg.value);
            //         let value = self.mvcc.get_unversioned(&Keyey)?;
            //         let fmtkv = format::Raw::key_maybe_value(&Keyey, value.as_deref());
            //         writeln!(output, "{fmtkv}")?;
            //     }
            //     args.reject_rest()?;
            // }

            // import [VERSION] KEY=VALUE...
            "import" => {
                Self::no_tx(command)?;
                let mut args = command.consume_args();
                let version = args.next_pos().map(|a| a.parse()).transpose()?;
                // let mut tx = self.mvcc.begin()?;
                let mut tx = TransactionTx::new(self.engine.clone());
                if let Some(version) = version {
                    if tx.version() > version {
                        return Err(format!("version {version} already used").into());
                    }
                    while tx.version() < version {
                        // tx = self.mvcc.begin()?;
                        tx = TransactionTx::new(self.engine.clone());
                    }
                }
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

            // tx: scan [RANGE]
            "scan" => {
                let t = self.get_transaction(&command.prefix)?;
                let mut args = command.consume_args();
                let range =
                    parse_key_range(args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."))?;
                args.reject_rest()?;

                let mut kvs = Vec::new();
                // for item in tx.range(range) {
                //     let (key, value) = item;
                //     kvs.push((key, value));
                // }
                //
                // for (key, value) in kvs {
                //     writeln!(output, "{}", format::Raw::key_value(&Keyey, &value))?;
                // }

                match t {
                    Transaction::Rx(rx) => {
                        for item in rx.range(range).into_iter() {
                            kvs.push((item.key().clone(), item.value().to_vec()));
                        }
                    }
                    Transaction::Tx(tx) => {
                        for item in tx.range(range).unwrap().into_iter() {
                            kvs.push((item.key().clone(), item.value().to_vec()));
                        }
                    }
                };

                for (key, value) in kvs {
                    writeln!(output, "{}", format::Raw::key_value(&key, &value))?;
                }
            }

            // tx: scan_prefix PREFIX
            // "scan_prefix" => {
            //     let tx = self.get_tx(&command.prefix)?;
            //     let mut args = command.consume_args();
            //     let prefix = decode_binary(&args.next_pos().ok_or("prefix not given")?.value);
            //     args.reject_rest()?;
            //
            //     let mut kvs = Vec::new();
            //     for item in tx.scan_prefix(&prefix) {
            //         let (key, value) = item?;
            //         kvs.push((key, value));
            //     }
            //
            //     for (key, value) in kvs {
            //         writeln!(output, "{}", format::Raw::key_value(&Keyey, &value))?;
            //     }
            // }

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

            // // set_unversioned KEY=VALUE...
            // "set_unversioned" => {
            //     Self::no_tx(command)?;
            //     let mut args = command.consume_args();
            //     for kv in args.rest_key() {
            //         let key = decode_binary(kv.key.as_ref().unwrap());
            //         let value = decode_binary(&Keyv.value);
            //         self.mvcc.set_unversioned(&Keyey, value)?;
            //     }
            //     args.reject_rest()?;
            // }

            // tx: state
            // "state" => {
            //     command.consume_args().reject_rest()?;
            //     let tx = self.get_tx(&command.prefix)?;
            //     let state = tx.state();
            //
            //     write!(
            //         output,
            //         "v{} {} active={{{}}}",
            //         state.version,
            //         if state.read_only { "ro" } else { "rw" },
            //         {
            //             let mut v: Vec<_> = state.active.iter().collect();
            //             v.sort();
            //             v.into_iter().map(ToString::to_string).collect::<Vec<_>>().join(",")
            //         }
            //     )?;
            // }

            // status
            // "status" => writeln!(output, "{:#?}", self.mvcc.status()?)?,
            name => return Err(format!("invalid command {name}").into()),
        }

        // If requested, output reifydb_engine operations.
        // if tags.remove("ops") {
        //     while let Ok(op) = self.operations.try_recv() {
        //         match op {
        //             Operation::Remove { key } => {
        //                 let fmtkey = MVCC::<format::Raw>::key(&Keyey);
        //                 let rawkey = format::Raw::key(&Keyey);
        //                 writeln!(output, "reifydb_engine remove {fmtkey} [{rawkey}]")?
        //             }
        //             Operation::Set { key, value } => {
        //                 let fmtkv = MVCC::<format::Raw>::key_value(&Keyey, &value);
        //                 let rawkv = format::Raw::key_value(&Keyey, &value);
        //                 writeln!(output, "reifydb_engine set {fmtkv} [{rawkv}]")?
        //             }
        //         }
        //     }
        // }

        if let Some(tag) = tags.iter().next() {
            return Err(format!("unknown tag {tag}").into());
        }

        Ok(output)
    }

    // Drain unhandled reifydb_engine operations.
    // fn end_command(&mut self, _: &testscript::Command) -> Result<String, Box<dyn StdError>> {
    //     while self.operations.try_recv().is_ok() {}
    //     Ok(String::new())
    // }
}
