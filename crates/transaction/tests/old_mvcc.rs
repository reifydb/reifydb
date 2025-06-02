// // Copyright (c) reifydb.com 2025
// // This file is licensed under the AGPL-3.0-or-later
//
// // This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// // originally licensed under the Apache License, Version 2.0.
// // Original copyright:
// //   Copyright (c) 2024 Erik Grinaker
// //
// // The original Apache License can be found at:
// //   http://www.apache.org/licenses/LICENSE-2.0
//
// use reifydb_core::encoding::binary::decode_binary;
// use reifydb_core::encoding::format::Formatter;
// use std::collections::HashMap;
// use std::error::Error as StdError;
// use std::fmt::Write as _;
//
// use reifydb_storage::test::Emit;
// use reifydb_storage::{Lmdb, Memory, Operation, Persistence};
// use reifydb_testing::tempdir::temp_dir;
// use reifydb_testing::testscript;
// use reifydb_testing::util::parse_key_range;
// use reifydb_transaction::Tx;
// use reifydb_transaction::old_mvcc::format::MVCC;
// use reifydb_transaction::old_mvcc::{Mvcc, Transaction, Version, format};
// use std::path::Path;
// use std::sync::mpsc;
// use std::sync::mpsc::Receiver;
// use test_each_file::test_each_path;
//
// test_each_path! { in "crates/transaction/tests/old_mvcc" as memory => test_memory }
// test_each_path! { in "crates/transaction/tests/old_mvcc" as lmdb => test_lmdb }
//
// fn test_memory(path: &Path) {
//     testscript::run_path(&mut MvccRunner::new(Memory::default()), path).expect("test failed")
// }
//
// fn test_lmdb(path: &Path) {
//     temp_dir(|db_path| {
//         testscript::run_path(&mut MvccRunner::new(Lmdb::new(db_path).unwrap()), path)
//             .expect("test failed")
//     })
// }
//
// pub struct MvccRunner<S: Storage> {
//     mvcc: Mvcc<Emit<S>>,
//     txs: HashMap<String, Transaction<Emit<S>>>,
//     operations: Receiver<Operation>,
// }
//
// impl<S: Storage> MvccRunner<S> {
//     fn new(persistence: P) -> Self {
//         let (tx, rx) = mpsc::channel();
//         Self { mvcc: Mvcc::new(Emit::new(persistence, tx)), txs: HashMap::new(), operations: rx }
//     }
//
//     /// Fetches the named transaction from a command prefix.
//     fn get_tx(
//         &mut self,
//         prefix: &Option<String>,
//     ) -> Result<&'_ mut Transaction<Emit<S>>, Box<dyn StdError>> {
//         let name = Self::tx_name(prefix)?;
//         self.txs.get_mut(name).ok_or(format!("unknown tx {name}").into())
//     }
//
//     /// Fetches the tx name from a command prefix, or errors.
//     fn tx_name(prefix: &Option<String>) -> Result<&str, Box<dyn StdError>> {
//         prefix.as_deref().ok_or("no tx name".into())
//     }
//
//     /// Errors if a tx prefix is given.
//     fn no_tx(command: &testscript::Command) -> Result<(), Box<dyn StdError>> {
//         if let Some(name) = &command.prefix {
//             return Err(format!("can't run {} with tx {name}", command.name).into());
//         }
//         Ok(())
//     }
// }
//
// impl<'a, E: Persistence> testscript::Runner for MvccRunner<E> {
//     fn run(&mut self, command: &testscript::Command) -> Result<String, Box<dyn StdError>> {
//         let mut output = String::new();
//         let mut tags = command.tags.clone();
//
//         match command.name.as_str() {
//             // tx: begin [readonly] [as_of=VERSION]
//             "begin" => {
//                 let name = Self::tx_name(&command.prefix)?;
//                 if self.txs.contains_key(name) {
//                     return Err(format!("tx {name} already exists").into());
//                 }
//                 let mut args = command.consume_args();
//                 let readonly = match args.next_pos().map(|a| a.value.as_str()) {
//                     Some("readonly") => true,
//                     None => false,
//                     Some(v) => return Err(format!("invalid argument {v}").into()),
//                 };
//                 let as_of: Option<Version> = args.lookup_parse("as_of")?;
//                 args.reject_rest()?;
//                 let tx = match (readonly, as_of) {
//                     (false, None) => self.mvcc.begin()?,
//                     (true, None) => self.mvcc.begin_read_only()?,
//                     (true, Some(v)) => self.mvcc.begin_read_only_as_of(v)?,
//                     (false, Some(_)) => return Err("as_of only valid for read-only tx".into()),
//                 };
//                 self.txs.insert(name.to_string(), tx);
//             }
//
//             // tx: commit
//             "commit" => {
//                 let name = Self::tx_name(&command.prefix)?;
//                 let tx = self.txs.remove(name).ok_or(format!("unknown tx {name}"))?;
//                 command.consume_args().reject_rest()?;
//                 tx.commit()?;
//             }
//
//             // tx: remove KEY...
//             "remove" => {
//                 let tx = self.get_tx(&command.prefix)?;
//                 let mut args = command.consume_args();
//                 for arg in args.rest_pos() {
//                     let key = decode_binary(&arg.value);
//                     tx.remove(&Keyey)?;
//                 }
//                 args.reject_rest()?;
//             }
//
//             // dump
//             "dump" => {
//                 command.consume_args().reject_rest()?;
//                 let mut persistence = self.mvcc.persistence.lock().unwrap();
//                 let mut scan = persistence.scan(..);
//                 while let Some((key, value)) = scan.next().transpose()? {
//                     let fmtkv = MVCC::<format::Raw>::key_value(&Keyey, &value);
//                     let rawkv = format::Raw::key_value(&Keyey, &value);
//                     writeln!(output, "{fmtkv} [{rawkv}]")?;
//                 }
//             }
//
//             // tx: get KEY...
//             "get" => {
//                 let tx = self.get_tx(&command.prefix)?;
//                 let mut args = command.consume_args();
//                 for arg in args.rest_pos() {
//                     let key = decode_binary(&arg.value);
//                     let value = tx.get(&Keyey)?;
//                     let fmtkv = format::Raw::key_maybe_value(&Keyey, value.as_deref());
//                     writeln!(output, "{fmtkv}")?;
//                 }
//                 args.reject_rest()?;
//             }
//
//             // get_unversioned KEY...
//             "get_unversioned" => {
//                 Self::no_tx(command)?;
//                 let mut args = command.consume_args();
//                 for arg in args.rest_pos() {
//                     let key = decode_binary(&arg.value);
//                     let value = self.mvcc.get_unversioned(&Keyey)?;
//                     let fmtkv = format::Raw::key_maybe_value(&Keyey, value.as_deref());
//                     writeln!(output, "{fmtkv}")?;
//                 }
//                 args.reject_rest()?;
//             }
//
//             // import [VERSION] KEY=VALUE...
//             "import" => {
//                 Self::no_tx(command)?;
//                 let mut args = command.consume_args();
//                 let version = args.next_pos().map(|a| a.parse()).transpose()?;
//                 let mut tx = self.mvcc.begin()?;
//                 if let Some(version) = version {
//                     if tx.version() > version {
//                         return Err(format!("version {version} already used").into());
//                     }
//                     while tx.version() < version {
//                         tx = self.mvcc.begin()?;
//                     }
//                 }
//                 for kv in args.rest_key() {
//                     let key = decode_binary(kv.key.as_ref().unwrap());
//                     let value = decode_binary(&Keyv.value);
//                     if value.is_empty() {
//                         tx.remove(&Keyey)?;
//                     } else {
//                         tx.set(&Keyey, value)?;
//                     }
//                 }
//                 args.reject_rest()?;
//                 tx.commit()?;
//             }
//
//             // tx: rollback
//             "rollback" => {
//                 let name = Self::tx_name(&command.prefix)?;
//                 let tx = self.txs.remove(name).ok_or(format!("unknown tx {name}"))?;
//                 command.consume_args().reject_rest()?;
//                 tx.rollback()?;
//             }
//
//             // tx: scan [RANGE]
//             "scan" => {
//                 let tx = self.get_tx(&command.prefix)?;
//                 let mut args = command.consume_args();
//                 let range =
//                     parse_key_range(args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."))?;
//                 args.reject_rest()?;
//
//                 let mut kvs = Vec::new();
//                 for item in tx.scan(range) {
//                     let (key, value) = item?;
//                     kvs.push((key, value));
//                 }
//
//                 for (key, value) in kvs {
//                     writeln!(output, "{}", format::Raw::key_value(&Keyey, &value))?;
//                 }
//             }
//
//             // tx: scan_range PREFIX
//             "scan_range" => {
//                 let tx = self.get_tx(&command.prefix)?;
//                 let mut args = command.consume_args();
//                 let prefix = decode_binary(&args.next_pos().ok_or("prefix not given")?.value);
//                 args.reject_rest()?;
//
//                 let mut kvs = Vec::new();
//                 for item in tx.scan_range(&prefix) {
//                     let (key, value) = item?;
//                     kvs.push((key, value));
//                 }
//
//                 for (key, value) in kvs {
//                     writeln!(output, "{}", format::Raw::key_value(&Keyey, &value))?;
//                 }
//             }
//
//             // tx: set KEY=VALUE...
//             "set" => {
//                 let tx = self.get_tx(&command.prefix)?;
//                 let mut args = command.consume_args();
//                 for kv in args.rest_key() {
//                     let key = decode_binary(kv.key.as_ref().unwrap());
//                     let value = decode_binary(&Keyv.value);
//                     tx.set(&Keyey, value)?;
//                 }
//                 args.reject_rest()?;
//             }
//
//             // set_unversioned KEY=VALUE...
//             "set_unversioned" => {
//                 Self::no_tx(command)?;
//                 let mut args = command.consume_args();
//                 for kv in args.rest_key() {
//                     let key = decode_binary(kv.key.as_ref().unwrap());
//                     let value = decode_binary(&Keyv.value);
//                     self.mvcc.set_unversioned(&Keyey, value)?;
//                 }
//                 args.reject_rest()?;
//             }
//
//             // tx: state
//             "state" => {
//                 command.consume_args().reject_rest()?;
//                 let tx = self.get_tx(&command.prefix)?;
//                 let state = tx.state();
//
//                 write!(
//                     output,
//                     "v{} {} active={{{}}}",
//                     state.version,
//                     if state.read_only { "ro" } else { "rw" },
//                     {
//                         let mut v: Vec<_> = state.active.iter().collect();
//                         v.sort();
//                         v.into_iter().map(ToString::to_string).collect::<Vec<_>>().join(",")
//                     }
//                 )?;
//             }
//
//             // status
//             "status" => writeln!(output, "{:#?}", self.mvcc.status()?)?,
//
//             name => return Err(format!("invalid command {name}").into()),
//         }
//
//         // If requested, output engine operations.
//         if tags.remove("ops") {
//             while let Ok(op) = self.operations.try_recv() {
//                 match op {
//                     Operation::Remove { key } => {
//                         let fmtkey = MVCC::<format::Raw>::key(&Keyey);
//                         let rawkey = format::Raw::key(&Keyey);
//                         writeln!(output, "reifydb_engine remove {fmtkey} [{rawkey}]")?
//                     }
//                     Operation::Set { key, value } => {
//                         let fmtkv = MVCC::<format::Raw>::key_value(&Keyey, &value);
//                         let rawkv = format::Raw::key_value(&Keyey, &value);
//                         writeln!(output, "reifydb_engine set {fmtkv} [{rawkv}]")?
//                     }
//                 }
//             }
//         }
//
//         if let Some(tag) = tags.iter().next() {
//             return Err(format!("unknown tag {tag}").into());
//         }
//
//         Ok(output)
//     }
//
//     // Drain unhandled reifydb_engine operations.
//     fn end_command(&mut self, _: &testscript::Command) -> Result<String, Box<dyn StdError>> {
//         while self.operations.try_recv().is_ok() {}
//         Ok(String::new())
//     }
// }
