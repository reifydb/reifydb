// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes portions of code from https://github.com/erikgrinaker/toydb (Apache 2 License).
// Original Apache 2 License Copyright (c) erikgrinaker 2024.

//! This module implements MVCC (Multi-Version Concurrency Control), a widely
//! used method for ACID transactions and concurrency control. It allows
//! multiple concurrent transactions to access and modify the same dataset,
//! isolates them from each other, detects and handles conflicts, and commits
//! their writes atomically as a single unit. It uses an underlying storage
//! engine to store raw keys and values.
//!
//! VERSIONS
//! ========
//!
//! MVCC handles concurrency control by managing multiple historical versions of
//! keys, identified by a timestamp. Every write adds a new version at a higher
//! timestamp, with deletes having a special tombstone value. For example, the
//! keys a,b,c,d may have the following values at various logical timestamps (x
//! is tombstone):
//!
//! Time
//! 5
//! 4  a4
//! 3      b3      x
//! 2
//! 1  a1      c1  d1
//!    a   b   c   d   Keys
//!
//! A transaction t2 that started at T=2 will see the values a=a1, c=c1, d=d1. A
//! different transaction t5 running at T=5 will see a=a4, b=b3, c=c1.
//!
//! toyDB uses logical timestamps with a sequence number stored in
//! Key::NextVersion. Each new read-write transaction takes its timestamp from
//! the current value of Key::NextVersion and then increments the value for the
//! next transaction.
//!
//! ISOLATION
//! =========
//!
//! MVCC provides an isolation level called snapshot isolation. Briefly,
//! transactions see a consistent snapshot of the database state as of their
//! start time. Writes made by concurrent or subsequent transactions are never
//! visible to it. If two concurrent transactions write to the same key they
//! will conflict and one of them must retry. A transaction's writes become
//! atomically visible to subsequent transactions only when they commit, and are
//! rolled back on failure. Read-only transactions never conflict with other
//! transactions.
//!
//! Transactions write new versions at their timestamp, storing them as
//! Key::Version(key, version) => value. If a transaction writes to a key and
//! finds a newer version, it returns an error and the client must retry.
//!
//! Active (uncommitted) read-write transactions record their version in the
//! active set, stored as Key::Active(version). When new transactions begin, they
//! take a snapshot of this active set, and any key versions that belong to a
//! transaction in the active set are considered invisible (to anyone except that
//! transaction itself). Writes to keys that already have a past version in the
//! active set will also return an error.
//!
//! To commit, a transaction simply deletes its record in the active set. This
//! will immediately (and, crucially, atomically) make all of its writes visible
//! to subsequent transactions, but not ongoing ones. If the transaction is
//! cancelled and rolled back, it maintains a record of all keys it wrote as
//! Key::TxWrite(version, key), so that it can find the corresponding versions
//! and delete them before removing itself from the active set.
//!
//! Consider the following example, where we have two ongoing transactions at
//! time T=2 and T=5, with some writes that are not yet committed marked in
//! parentheses.
//!
//! Active set: [2, 5]
//!
//! Time
//! 5 (a5)
//! 4  a4
//! 3      b3      x
//! 2         (x)     (e2)
//! 1  a1      c1  d1
//!    a   b   c   d   e   Keys
//!
//! Here, t2 will see a=a1, d=d1, e=e2 (it sees its own writes). t5 will see
//! a=a5, b=b3, c=c1. t2 does not see any newer versions, and t5 does not see
//! the tombstone at c@2 nor the value e=e2, because version=2 is in its active
//! set.
//!
//! If t2 tries to write b=b2, it receives an error and must retry, because a
//! newer version exists. Similarly, if t5 tries to write e=e5, it receives an
//! error and must retry, because the version e=e2 is in its active set.
//!
//! To commit, t2 can remove itself from the active set. A new transaction t6
//! starting after the commit will then see c as deleted and e=e2. t5 will still
//! not see any of t2's writes, because it's still in its local snapshot of the
//! active set at the time it began.
//!
//! READ-ONLY AND TIME TRAVEL QUERIES
//! =================================
//!
//! Since MVCC stores historical versions, it can trivially support time travel
//! queries where a transaction reads at a past timestamp and has a consistent
//! view of the database at that time.
//!
//! This is done by a transaction simply using a past version, as if it had
//! started far in the past, ignoring newer versions like any other transaction.
//! This transaction cannot write, as it does not have a unique timestamp (the
//! original read-write transaction originally owned this timestamp).
//!
//! The only wrinkle is that the time-travel query must also know what the active
//! set was at that version. Otherwise, it may see past transactions that committed
//! after that time, which were not visible to the original transaction that wrote
//! at that version. Similarly, if a time-travel query reads at a version that is
//! still active, it should not see its in-progress writes, and after it commits
//! a different time-travel query should not see those writes either, to maintain
//! version consistency.
//!
//! To achieve this, every read-write transaction stores its active set snapshot
//! in the storage engine as well, as Key::TxActiveSnapshot, such that later
//! time-travel queries can restore its original snapshot. Furthermore, a
//! time-travel query can only see versions below the snapshot version, otherwise
//! it could see spurious in-progress or since-committed versions.
//!
//! In the following example, a time-travel query at version=3 would see a=a1,
//! c=c1, d=d1.
//!
//! Time
//! 5
//! 4  a4
//! 3      b3      x
//! 2
//! 1  a1      c1  d1
//!    a   b   c   d   Keys
//!
//! Read-only queries work similarly to time-travel queries, with one exception:
//! they read at the next (current) version, i.e. Key::NextVersion, and use the
//! current active set, storing the snapshot in memory only. Read-only queries
//! do not increment the version sequence number in Key::NextVersion.
//!
//! GARBAGE COLLECTION
//! ==================
//!
//! Normally, old versions would be garbage collected regularly, when they are
//! no longer needed by active transactions or time-travel queries. However,
//! toyDB does not implement garbage collection, instead keeping all history
//! forever, both out of laziness and also because it allows unlimited time
//! travel queries (it's a feature, not a bug!).

pub use version::Version;
pub use transaction::init;
mod catalog;
mod key;
mod scan;
mod schema;
mod store;
mod transaction;
mod version;

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::error::Error;
use std::ops::Sub;
use std::sync::{Arc, Mutex, OnceLock};

use crate::TransactionMut;
use crate::mvcc::key::{Key, KeyPrefix};
use base::encoding;
use base::encoding::{Key as _, Value};
use serde::{Deserialize, Serialize};
use storage::EngineMut;

// FIXME remove this
/// Constructs an Error::InvalidData for the given format string.
#[macro_export]
macro_rules! errdata {
    ($($args:tt)*) => {
        unimplemented!()
    };
}

// FIXME remove this
/// Constructs an Error::InvalidInput for the given format string.
#[macro_export]
macro_rules! errinput {
    ($($args:tt)*) => {
        unimplemented!()
    };
}

/// An MVCC-based transactional key-value engine. It wraps an underlying storage
/// engine that's used for raw key-value storage.
///
/// While it supports any number of concurrent transactions, individual read or
/// write operations are executed sequentially, serialized via a mutex. There
/// are two reasons for this: the storage engine itself is not thread-safe,
/// requiring serialized access, and the Raft state machine that manages the
/// MVCC engine applies commands one at a time from the Raft log, which will
/// serialize them anyway.
pub struct Engine<S: EngineMut> {
    pub storage: Arc<Mutex<S>>,
}

impl<'a, S: storage::EngineMut + 'a> crate::Engine<'a, S> for Engine<S> {
    type Rx = Transaction<S>;
    // type Tx = TransactionMut<S>;
    type Tx = Transaction<S>;

    fn begin(&'a self) -> crate::Result<Self::Tx> {
        // let guard = self.inner.write().unwrap();
        // Ok(TransactionMut::new(guard))
        Ok(Transaction::begin(self.storage.clone()).unwrap())
    }

    fn begin_read_only(&'a self) -> crate::Result<Self::Rx> {
        // let guard = self.inner.read().unwrap();
        // Ok(Transaction::new(guard))
        // unimplemented!()
        Ok(Transaction::begin_read_only(self.storage.clone(), None).unwrap())
    }
}

//FIXME
pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

static CATALOG: OnceLock<()> = OnceLock::new();

impl<S: EngineMut> Engine<S> {
    /// Creates a new MVCC engine with the given storage engine.
    pub fn new(engine: S) -> Self {
        CATALOG.get_or_init(||{
            init();
        });
        Self { storage: Arc::new(Mutex::new(engine)) }
    }

    /// Begins a new read-write transaction.
    pub fn begin(&self) -> Result<Transaction<S>> {
        Transaction::begin(self.storage.clone())
    }

    /// Begins a new read-only transaction at the latest version.
    pub fn begin_read_only(&self) -> Result<Transaction<S>> {
        Transaction::begin_read_only(self.storage.clone(), None)
    }

    /// Begins a new read-only transaction as of the given version.
    pub fn begin_as_of(&self, version: Version) -> Result<Transaction<S>> {
        Transaction::begin_read_only(self.storage.clone(), Some(version))
    }

    /// Resumes a transaction from the given transaction state.
    // pub fn resume(&self, state: TransactionState) -> Result<Transaction<S>> {
    //     Transaction::resume(self.engine.clone(), state)
    // }

    /// Fetches the value of an unversioned key.
    pub fn get_unversioned(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // self.engine.lock()?.get(&Key::Unversioned(key.into()).encode())
        // FIXME
        Ok(self.storage.lock().unwrap().get(&Key::Unversioned(key.into()).encode()).unwrap())
    }

    /// Sets the value of an unversioned key.
    pub fn set_unversioned(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        // self.engine.lock()?.set(&Key::Unversioned(key.into()).encode(), value)
        // FIXME
        Ok(self.storage.lock().unwrap().set(&Key::Unversioned(key.into()).encode(), value).unwrap())
    }

    /// Returns the status of the MVCC and storage engines.
    pub fn status(&self) -> Result<Status> {
        // FIXME
        // let mut engine = self.engine.lock()?;
        let mut engine = self.storage.lock().unwrap();
        let versions = match engine.get(&Key::NextVersion.encode())? {
            Some(ref v) => Version::decode(v)? - 1,
            None => Version(0),
        };
        let active_txs = engine.scan_prefix(&KeyPrefix::TxActive.encode()).count() as u64;
        Ok(Status { version: versions, active_txs: active_txs })
        // Ok(Status { versions, active_txs, storage: engine.status()? })
    }
}

/// MVCC engine status.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    /// The current MVCC.
    pub version: Version,
    /// Number of currently active transactions.
    pub active_txs: u64,
    // ///The storage engine.
    // pub storage: super::engine::Status,
}

impl encoding::Value for Status {}

/// An MVCC transaction.
pub struct Transaction<S: EngineMut> {
    /// The underlying engine, shared by all transactions.
    engine: Arc<Mutex<S>>,
    /// The transaction state.
    state: TransactionState,
}

/// A Transaction's state, which determines its write version and isolation. It
/// is separate from Transaction to allow it to be passed around independently
/// of the engine. There are two main motivations for this:
///
/// * It can be exported via Transaction.state(), (de)serialized, and later used
///   to instantiate a new functionally equivalent Transaction via
///   Transaction::resume(). This allows passing the transaction between the
///   storage engine and SQL engine (potentially running on a different node)
///   across the Raft state machine boundary.
///
/// * It can be borrowed independently of Engine, allowing references to it
///   in VisibleIterator, which would otherwise result in self-references.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TransactionState {
    /// The version this transaction is running at. Only one read-write
    /// transaction can run at a given version, since this identifies its
    /// writes.
    pub version: Version,
    /// If true, the transaction is read only.
    pub read_only: bool,
    /// The set of concurrent active (uncommitted) transactions, as of the start
    /// of this transaction. Their writes should be invisible to this
    /// transaction even if they're writing at a lower version, since they're
    /// not committed yet. Uses a BTreeSet for test determinism.
    pub active: BTreeSet<Version>,
}

impl encoding::Value for TransactionState {}

impl TransactionState {
    /// Checks whether the given version is visible to this transaction.
    ///
    /// Future versions, and versions belonging to active transactions as of
    /// the start of this transaction, are never visible.
    ///
    /// Read-write transactions see their own writes at their version.
    ///
    /// Read-only queries only see versions below the transaction's version,
    /// excluding the version itself. This is to ensure time-travel queries see
    /// a consistent version both before and after any active transaction at
    /// that version commits its writes. See the module documentation for
    /// details.
    fn is_visible(&self, version: Version) -> bool {
        if self.active.contains(&version) {
            false
        } else if self.read_only {
            version < self.version
        } else {
            version <= self.version
        }
    }
}

impl From<TransactionState> for Cow<'_, TransactionState> {
    fn from(tx: TransactionState) -> Self {
        Cow::Owned(tx)
    }
}

impl<'a> From<&'a TransactionState> for Cow<'a, TransactionState> {
    fn from(tx: &'a TransactionState) -> Self {
        Cow::Borrowed(tx)
    }
}

// /// Most storage tests are Goldenscripts under src/storage/testscripts.
// #[cfg(test)]
// pub mod tests {
//     use std::collections::HashMap;
//     use std::error::Error;
//     use std::fmt::Write as _;
//     use std::path::Path;
//     use std::result::Result;
//
//     use crossbeam::channel::Receiver;
//     use tempfile::TempDir;
//     use test_case::test_case;
//     use test_each_file::test_each_path;
//
//     use super::*;
//     use crate::encoding::format::{self, Formatter as _};
//     use crate::storage::engine::test::{Emit, Mirror, Operation, decode_binary, parse_key_range};
//     use crate::storage::{BitCask, Memory};
//
//     // Run goldenscript tests in src/storage/testscripts/mvcc.
//     test_each_path! { in "src/storage/testscripts/mvcc" as scripts => test_goldenscript }
//
//     fn test_goldenscript(path: &Path) {
//         goldenscript::run(&mut MVCCRunner::new(), path).expect("goldenscript failed")
//     }
//
//     /// Tests that key prefixes are actually prefixes of keys.
//     #[test_case(KeyPrefix::NextVersion, Key::NextVersion; "NextVersion")]
//     #[test_case(KeyPrefix::TxActive, Key::TxActive(1); "TxActive")]
//     #[test_case(KeyPrefix::TxActiveSnapshot, Key::TxActiveSnapshot(1); "TxActiveSnapshot")]
//     #[test_case(KeyPrefix::TxWrite(1), Key::TxWrite(1, b"foo".as_slice().into()); "TxWrite")]
//     #[test_case(KeyPrefix::Version(b"foo".as_slice().into()), Key::Version(b"foo".as_slice().into(), 1); "Version")]
//     #[test_case(KeyPrefix::Unversioned, Key::Unversioned(b"foo".as_slice().into()); "Unversioned")]
//     fn key_prefix(prefix: KeyPrefix, key: Key) {
//         let prefix = prefix.encode();
//         let key = key.encode();
//         assert_eq!(prefix, key[..prefix.len()])
//     }
//
//     /// Runs MVCC goldenscript tests.
//     pub struct MVCCRunner {
//         mvcc: MVCC<TestEngine>,
//         txs: HashMap<String, Transaction<TestEngine>>,
//         op_rx: Receiver<Operation>,
//         _tempdir: TempDir,
//     }
//
//     type TestEngine = Emit<Mirror<BitCask, Memory>>;
//
//     impl MVCCRunner {
//         fn new() -> Self {
//             // Use both a BitCask and a Memory engine, and mirror operations
//             // across them. Emit engine operations to op_rx.
//             let (op_tx, op_rx) = crossbeam::channel::unbounded();
//             let tempdir = TempDir::with_prefix("toydb").expect("tempdir failed");
//             let bitcask = BitCask::new(tempdir.path().join("bitcask")).expect("bitcask failed");
//             let memory = Memory::new();
//             let engine = Emit::new(Mirror::new(bitcask, memory), op_tx);
//             let mvcc = MVCC::new(engine);
//             Self { mvcc, op_rx, txs: HashMap::new(), _tempdir: tempdir }
//         }
//
//         /// Fetches the named transaction from a command prefix.
//         fn get_tx(
//             &mut self,
//             prefix: &Option<String>,
//         ) -> Result<&'_ mut Transaction<TestEngine>, Box<dyn Error>> {
//             let name = Self::tx_name(prefix)?;
//             self.txs.get_mut(name).ok_or(format!("unknown tx {name}").into())
//         }
//
//         /// Fetches the tx name from a command prefix, or errors.
//         fn tx_name(prefix: &Option<String>) -> Result<&str, Box<dyn Error>> {
//             prefix.as_deref().ok_or("no tx name".into())
//         }
//
//         /// Errors if a tx prefix is given.
//         fn no_tx(command: &goldenscript::Command) -> Result<(), Box<dyn Error>> {
//             if let Some(name) = &command.prefix {
//                 return Err(format!("can't run {} with tx {name}", command.name).into());
//             }
//             Ok(())
//         }
//     }
//
//     impl goldenscript::Runner for MVCCRunner {
//         fn run(&mut self, command: &goldenscript::Command) -> Result<String, Box<dyn Error>> {
//             let mut output = String::new();
//             let mut tags = command.tags.clone();
//
//             match command.name.as_str() {
//                 // tx: begin [readonly] [as_of=VERSION]
//                 "begin" => {
//                     let name = Self::tx_name(&command.prefix)?;
//                     if self.txs.contains_key(name) {
//                         return Err(format!("tx {name} already exists").into());
//                     }
//                     let mut args = command.consume_args();
//                     let readonly = match args.next_pos().map(|a| a.value.as_str()) {
//                         Some("readonly") => true,
//                         None => false,
//                         Some(v) => return Err(format!("invalid argument {v}").into()),
//                     };
//                     let as_of = args.lookup_parse("as_of")?;
//                     args.reject_rest()?;
//                     let tx = match (readonly, as_of) {
//                         (false, None) => self.mvcc.begin()?,
//                         (true, None) => self.mvcc.begin_read_only()?,
//                         (true, Some(v)) => self.mvcc.begin_as_of(v)?,
//                         (false, Some(_)) => return Err("as_of only valid for read-only tx".into()),
//                     };
//                     self.txs.insert(name.to_string(), tx);
//                 }
//
//                 // tx: commit
//                 "commit" => {
//                     let name = Self::tx_name(&command.prefix)?;
//                     let tx = self.txs.remove(name).ok_or(format!("unknown tx {name}"))?;
//                     command.consume_args().reject_rest()?;
//                     tx.commit()?;
//                 }
//
//                 // tx: delete KEY...
//                 "delete" => {
//                     let tx = self.get_tx(&command.prefix)?;
//                     let mut args = command.consume_args();
//                     for arg in args.rest_pos() {
//                         let key = decode_binary(&arg.value);
//                         tx.delete(&key)?;
//                     }
//                     args.reject_rest()?;
//                 }
//
//                 // dump
//                 "dump" => {
//                     command.consume_args().reject_rest()?;
//                     let mut engine = self.mvcc.engine.lock().unwrap();
//                     let mut scan = engine.scan(..);
//                     while let Some((key, value)) = scan.next().transpose()? {
//                         let fmtkv = format::MVCC::<format::Raw>::key_value(&key, &value);
//                         let rawkv = format::Raw::key_value(&key, &value);
//                         writeln!(output, "{fmtkv} [{rawkv}]")?;
//                     }
//                 }
//
//                 // tx: get KEY...
//                 "get" => {
//                     let tx = self.get_tx(&command.prefix)?;
//                     let mut args = command.consume_args();
//                     for arg in args.rest_pos() {
//                         let key = decode_binary(&arg.value);
//                         let value = tx.get(&key)?;
//                         let fmtkv = format::Raw::key_maybe_value(&key, value.as_deref());
//                         writeln!(output, "{fmtkv}")?;
//                     }
//                     args.reject_rest()?;
//                 }
//
//                 // get_unversioned KEY...
//                 "get_unversioned" => {
//                     Self::no_tx(command)?;
//                     let mut args = command.consume_args();
//                     for arg in args.rest_pos() {
//                         let key = decode_binary(&arg.value);
//                         let value = self.mvcc.get_unversioned(&key)?;
//                         let fmtkv = format::Raw::key_maybe_value(&key, value.as_deref());
//                         writeln!(output, "{fmtkv}")?;
//                     }
//                     args.reject_rest()?;
//                 }
//
//                 // import [VERSION] KEY=VALUE...
//                 "import" => {
//                     Self::no_tx(command)?;
//                     let mut args = command.consume_args();
//                     let version = args.next_pos().map(|a| a.parse()).transpose()?;
//                     let mut tx = self.mvcc.begin()?;
//                     if let Some(version) = version {
//                         if tx.version() > version {
//                             return Err(format!("version {version} already used").into());
//                         }
//                         while tx.version() < version {
//                             tx = self.mvcc.begin()?;
//                         }
//                     }
//                     for kv in args.rest_key() {
//                         let key = decode_binary(kv.key.as_ref().unwrap());
//                         let value = decode_binary(&kv.value);
//                         if value.is_empty() {
//                             tx.delete(&key)?;
//                         } else {
//                             tx.set(&key, value)?;
//                         }
//                     }
//                     args.reject_rest()?;
//                     tx.commit()?;
//                 }
//
//                 // tx: resume JSON
//                 "resume" => {
//                     let name = Self::tx_name(&command.prefix)?;
//                     let mut args = command.consume_args();
//                     let raw = &args.next_pos().ok_or("state not given")?.value;
//                     args.reject_rest()?;
//                     let state: TransactionState = serde_json::from_str(raw)?;
//                     let tx = self.mvcc.resume(state)?;
//                     self.txs.insert(name.to_string(), tx);
//                 }
//
//                 // tx: rollback
//                 "rollback" => {
//                     let name = Self::tx_name(&command.prefix)?;
//                     let tx = self.txs.remove(name).ok_or(format!("unknown tx {name}"))?;
//                     command.consume_args().reject_rest()?;
//                     tx.rollback()?;
//                 }
//
//                 // tx: scan [RANGE]
//                 "scan" => {
//                     let tx = self.get_tx(&command.prefix)?;
//                     let mut args = command.consume_args();
//                     let range =
//                         parse_key_range(args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."))?;
//                     args.reject_rest()?;
//
//                     let kvs: Vec<_> = tx.scan(range).try_collect()?;
//                     for (key, value) in kvs {
//                         writeln!(output, "{}", format::Raw::key_value(&key, &value))?;
//                     }
//                 }
//
//                 // tx: scan_prefix PREFIX
//                 "scan_prefix" => {
//                     let tx = self.get_tx(&command.prefix)?;
//                     let mut args = command.consume_args();
//                     let prefix = decode_binary(&args.next_pos().ok_or("prefix not given")?.value);
//                     args.reject_rest()?;
//
//                     let kvs: Vec<_> = tx.scan_prefix(&prefix).try_collect()?;
//                     for (key, value) in kvs {
//                         writeln!(output, "{}", format::Raw::key_value(&key, &value))?;
//                     }
//                 }
//
//                 // tx: set KEY=VALUE...
//                 "set" => {
//                     let tx = self.get_tx(&command.prefix)?;
//                     let mut args = command.consume_args();
//                     for kv in args.rest_key() {
//                         let key = decode_binary(kv.key.as_ref().unwrap());
//                         let value = decode_binary(&kv.value);
//                         tx.set(&key, value)?;
//                     }
//                     args.reject_rest()?;
//                 }
//
//                 // set_unversioned KEY=VALUE...
//                 "set_unversioned" => {
//                     Self::no_tx(command)?;
//                     let mut args = command.consume_args();
//                     for kv in args.rest_key() {
//                         let key = decode_binary(kv.key.as_ref().unwrap());
//                         let value = decode_binary(&kv.value);
//                         self.mvcc.set_unversioned(&key, value)?;
//                     }
//                     args.reject_rest()?;
//                 }
//
//                 // tx: state
//                 "state" => {
//                     command.consume_args().reject_rest()?;
//                     let tx = self.get_tx(&command.prefix)?;
//                     let state = tx.state();
//                     write!(
//                         output,
//                         "v{} {} active={{{}}}",
//                         state.version,
//                         if state.read_only { "ro" } else { "rw" },
//                         state.active.iter().sorted().join(",")
//                     )?;
//                 }
//
//                 // status
//                 "status" => writeln!(output, "{:#?}", self.mvcc.status()?)?,
//
//                 name => return Err(format!("invalid command {name}").into()),
//             }
//
//             // If requested, output engine operations.
//             if tags.remove("ops") {
//                 while let Ok(op) = self.op_rx.try_recv() {
//                     match op {
//                         Operation::Delete { key } => {
//                             let fmtkey = format::MVCC::<format::Raw>::key(&key);
//                             let rawkey = format::Raw::key(&key);
//                             writeln!(output, "engine delete {fmtkey} [{rawkey}]")?
//                         }
//                         Operation::Flush => writeln!(output, "engine flush")?,
//                         Operation::Set { key, value } => {
//                             let fmtkv = format::MVCC::<format::Raw>::key_value(&key, &value);
//                             let rawkv = format::Raw::key_value(&key, &value);
//                             writeln!(output, "engine set {fmtkv} [{rawkv}]")?
//                         }
//                     }
//                 }
//             }
//
//             if let Some(tag) = tags.iter().next() {
//                 return Err(format!("unknown tag {tag}").into());
//             }
//
//             Ok(output)
//         }
//
//         // Drain unhandled engine operations.
//         fn end_command(&mut self, _: &goldenscript::Command) -> Result<String, Box<dyn Error>> {
//             while self.op_rx.try_recv().is_ok() {}
//             Ok(String::new())
//         }
//     }
// }
