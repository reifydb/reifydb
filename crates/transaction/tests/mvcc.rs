// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::encoding::Key as _;
use base::encoding::binary::decode_binary;
use base::encoding::format::Formatter;
use format::MVCC;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Write as _;
use std::ops::{Bound, RangeBounds};
use std::result::Result as StdResult;

use std::fmt::Write as _;
use std::path::Path;
use storage::{Engine as StorageEngine, Memory};
use test_each_file::test_each_path;
use testing::testscript;
use testing::util::parse_key_range;
use transaction::mvcc;
use transaction::mvcc::{Engine, Key, KeyPrefix, Transaction, TransactionState, Version, format};

// test_each_path! { in "crates/transaction/tests/mvcc" as mvcc => test_script }

test_each_path! { in "crates/transaction/tests/mvcc" => test_script }

fn test_script(path: &Path) {
    testscript::run_path(&mut MVCCRunner::new(), path).expect("test failed")
}

/// Tests that key prefixes are actually prefixes of keys.
// #[test_case(KeyPrefix::NextVersion, Key::NextVersion; "NextVersion")]
// #[test_case(KeyPrefix::TxActive, Key::TxActive(1); "TxActive")]
// #[test_case(KeyPrefix::TxActiveSnapshot, Key::TxActiveSnapshot(1); "TxActiveSnapshot")]
// #[test_case(KeyPrefix::TxWrite(1), Key::TxWrite(1, b"foo".as_slice().into()); "TxWrite")]
// #[test_case(KeyPrefix::Version(b"foo".as_slice().into()), Key::Version(b"foo".as_slice().into(), 1); "Version"
// 	)]
// #[test_case(KeyPrefix::Unversioned, Key::Unversioned(b"foo".as_slice().into()); "Unversioned")]
// fn key_prefix(prefix: KeyPrefix, key: Key) {
//     let prefix = prefix.encode();
//     let key = key.encode();
//     assert_eq!(prefix, key[..prefix.len()])
// }

/// Runs MVCC tests.
pub struct MVCCRunner {
    mvcc: mvcc::Engine<Memory>,
    txs: HashMap<String, Transaction<Memory>>,
    // op_rx: Receiver<Operation>,
    // _tempdir: TempDir,
}

// type TestEngine = Emit<Mirror<BitCask, Memory>>;

impl MVCCRunner {
    fn new() -> Self {
        // Use both a BitCask and a Memory engine, and mirror operations
        // across them. Emit engine operations to op_rx.
        // let (op_tx, op_rx) = crossbeam::channel::unbounded();
        // let tempdir = TempDir::with_prefix("toydb").expect("tempdir failed");
        // let bitcask = BitCask::new(tempdir.path().join("bitcask")).expect("bitcask failed");
        // let memory = Memory::new();
        // let engine = Emit::new(Mirror::new(bitcask, memory), op_tx);
        let mvcc = Engine::new(Memory::default());
        Self { mvcc, txs: HashMap::new() }
    }

    /// Fetches the named transaction from a command prefix.
    fn get_tx(
        &mut self,
        prefix: &Option<String>,
    ) -> Result<&'_ mut Transaction<Memory>, Box<dyn StdError>> {
        let name = Self::tx_name(prefix)?;
        self.txs.get_mut(name).ok_or(format!("unknown tx {name}").into())
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

impl testscript::Runner for MVCCRunner {
    fn run(&mut self, command: &testscript::Command) -> Result<String, Box<dyn StdError>> {
        let mut output = String::new();
        let mut tags = command.tags.clone();

        match command.name.as_str() {
            // tx: begin [readonly] [as_of=VERSION]
            "begin" => {
                let name = Self::tx_name(&command.prefix)?;
                if self.txs.contains_key(name) {
                    return Err(format!("tx {name} already exists").into());
                }
                let mut args = command.consume_args();
                let readonly = match args.next_pos().map(|a| a.value.as_str()) {
                    Some("readonly") => true,
                    None => false,
                    Some(v) => return Err(format!("invalid argument {v}").into()),
                };
                let as_of: Option<Version> = args.lookup_parse("as_of")?;
                args.reject_rest()?;
                let tx = match (readonly, as_of) {
                    (false, None) => self.mvcc.begin()?,
                    (true, None) => self.mvcc.begin_read_only()?,
                    (true, Some(v)) => self.mvcc.begin_read_only_as_of(v)?,
                    (false, Some(_)) => return Err("as_of only valid for read-only tx".into()),
                };
                self.txs.insert(name.to_string(), tx);
            }

            // tx: commit
            "commit" => {
                let name = Self::tx_name(&command.prefix)?;
                let tx = self.txs.remove(name).ok_or(format!("unknown tx {name}"))?;
                command.consume_args().reject_rest()?;
                tx.commit()?;
            }

            // tx: delete KEY...
            "delete" => {
                let tx = self.get_tx(&command.prefix)?;
                let mut args = command.consume_args();
                for arg in args.rest_pos() {
                    let key = decode_binary(&arg.value);
                    tx.delete(&key)?;
                }
                args.reject_rest()?;
            }

            // dump
            "dump" => {
                command.consume_args().reject_rest()?;
                let mut engine = self.mvcc.storage.lock().unwrap();
                let mut scan = engine.scan(..);
                while let Some((key, value)) = scan.next().transpose()? {
                    let fmtkv = MVCC::<format::Raw>::key_value(&key, &value);
                    let rawkv = format::Raw::key_value(&key, &value);
                    writeln!(output, "{fmtkv} [{rawkv}]")?;
                }
            }

            // tx: get KEY...
            "get" => {
                let tx = self.get_tx(&command.prefix)?;
                let mut args = command.consume_args();
                for arg in args.rest_pos() {
                    let key = decode_binary(&arg.value);
                    let value = tx.get(&key)?;
                    let fmtkv = format::Raw::key_maybe_value(&key, value.as_deref());
                    writeln!(output, "{fmtkv}")?;
                }
                args.reject_rest()?;
            }

            // get_unversioned KEY...
            "get_unversioned" => {
                Self::no_tx(command)?;
                let mut args = command.consume_args();
                for arg in args.rest_pos() {
                    let key = decode_binary(&arg.value);
                    let value = self.mvcc.get_unversioned(&key)?;
                    let fmtkv = format::Raw::key_maybe_value(&key, value.as_deref());
                    writeln!(output, "{fmtkv}")?;
                }
                args.reject_rest()?;
            }

            // import [VERSION] KEY=VALUE...
            "import" => {
                Self::no_tx(command)?;
                let mut args = command.consume_args();
                let version = args.next_pos().map(|a| a.parse()).transpose()?;
                let mut tx = self.mvcc.begin()?;
                if let Some(version) = version {
                    if tx.version() > version {
                        return Err(format!("version {version} already used").into());
                    }
                    while tx.version() < version {
                        tx = self.mvcc.begin()?;
                    }
                }
                for kv in args.rest_key() {
                    let key = decode_binary(kv.key.as_ref().unwrap());
                    let value = decode_binary(&kv.value);
                    if value.is_empty() {
                        tx.delete(&key)?;
                    } else {
                        tx.set(&key, value)?;
                    }
                }
                args.reject_rest()?;
                tx.commit()?;
            }

            // tx: resume JSON
            "resume" => {
                // let name = Self::tx_name(&command.prefix)?;
                // let mut args = command.consume_args();
                // let raw = &args.next_pos().ok_or("state not given")?.value;
                // args.reject_rest()?;
                // let state: TransactionState = serde_json::from_str(raw)?;
                // let tx = self.mvcc.resume(state)?;
                // self.txs.insert(name.to_string(), tx);
                unimplemented!()
            }

            // tx: rollback
            "rollback" => {
                let name = Self::tx_name(&command.prefix)?;
                let tx = self.txs.remove(name).ok_or(format!("unknown tx {name}"))?;
                command.consume_args().reject_rest()?;
                tx.rollback()?;
            }

            // tx: scan [RANGE]
            "scan" => {
                let tx = self.get_tx(&command.prefix)?;
                let mut args = command.consume_args();
                let range =
                    parse_key_range(args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."))?;
                args.reject_rest()?;

                let mut kvs = Vec::new();
                for item in tx.scan(range) {
                    let (key, value) = item?;
                    kvs.push((key, value));
                }

                for (key, value) in kvs {
                    writeln!(output, "{}", format::Raw::key_value(&key, &value))?;
                }
            }

            // tx: scan_prefix PREFIX
            "scan_prefix" => {
                let tx = self.get_tx(&command.prefix)?;
                let mut args = command.consume_args();
                let prefix = decode_binary(&args.next_pos().ok_or("prefix not given")?.value);
                args.reject_rest()?;

                let mut kvs = Vec::new();
                for item in tx.scan_prefix(&prefix) {
                    let (key, value) = item?;
                    kvs.push((key, value));
                }

                for (key, value) in kvs {
                    writeln!(output, "{}", format::Raw::key_value(&key, &value))?;
                }
            }

            // tx: set KEY=VALUE...
            "set" => {
                let tx = self.get_tx(&command.prefix)?;
                let mut args = command.consume_args();
                for kv in args.rest_key() {
                    let key = decode_binary(kv.key.as_ref().unwrap());
                    let value = decode_binary(&kv.value);
                    tx.set(&key, value)?;
                }
                args.reject_rest()?;
            }

            // set_unversioned KEY=VALUE...
            "set_unversioned" => {
                Self::no_tx(command)?;
                let mut args = command.consume_args();
                for kv in args.rest_key() {
                    let key = decode_binary(kv.key.as_ref().unwrap());
                    let value = decode_binary(&kv.value);
                    self.mvcc.set_unversioned(&key, value)?;
                }
                args.reject_rest()?;
            }

            // tx: state
            "state" => {
                command.consume_args().reject_rest()?;
                let tx = self.get_tx(&command.prefix)?;
                let state = tx.state();

                write!(
                    output,
                    "v{} {} active={{{}}}",
                    state.version,
                    if state.read_only { "ro" } else { "rw" },
                    {
                        let mut v: Vec<_> = state.active.iter().collect();
                        v.sort();
                        v.into_iter().map(ToString::to_string).collect::<Vec<_>>().join(",")
                    }
                )?;
            }

            // status
            "status" => writeln!(output, "{:#?}", self.mvcc.status()?)?,

            name => return Err(format!("invalid command {name}").into()),
        }

        // If requested, output engine operations.
        // if tags.remove("ops") {
        //     while let Ok(op) = self.op_rx.try_recv() {
        //         match op {
        //             Operation::Delete { key } => {
        //                 let fmtkey = MVCC::<format::Raw>::key(&key);
        //                 let rawkey = format::Raw::key(&key);
        //                 writeln!(output, "engine delete {fmtkey} [{rawkey}]")?
        //             }
        //             Operation::Flush => writeln!(output, "engine flush")?,
        //             Operation::Set { key, value } => {
        //                 let fmtkv = MVCC::<format::Raw>::key_value(&key, &value);
        //                 let rawkv = format::Raw::key_value(&key, &value);
        //                 writeln!(output, "engine set {fmtkv} [{rawkv}]")?
        //             }
        //         }
        //     }
        // }

        if let Some(tag) = tags.iter().next() {
            return Err(format!("unknown tag {tag}").into());
        }

        Ok(output)
    }

    // Drain unhandled engine operations.
    // fn end_command(&mut self, _: &testscript::Command) -> Result<String, Box<dyn Error>> {
    // while self.op_rx.try_recv().is_ok() {}
    // Ok(String::new())
    // }
}
