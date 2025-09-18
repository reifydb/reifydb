// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::HashSet, path::Path, sync::mpsc, thread};

use mpsc::Sender;
use reifydb_core::{CommitVersion, CowVec, EncodedKey, TransactionId, delta::Delta};
use reifydb_type::{Error, Result, return_error};
use rusqlite::{Connection, OpenFlags, Transaction, params_from_iter, types::Value};

use super::diagnostic::{from_rusqlite_error, transaction_failed};
use crate::{
	cdc::{CdcTransaction, CdcTransactionChange, generate_cdc_change},
	diagnostic::{connection_failed, sequence_exhausted},
	sqlite::{
		cdc::{fetch_pre_value, store_cdc_transaction},
		versioned::{ensure_table_exists, table_name},
	},
};

pub enum WriteCommand {
	UnversionedCommit {
		operations: Vec<(String, Vec<Value>)>,
		response: Sender<Result<()>>,
	},
	VersionedCommit {
		deltas: CowVec<Delta>,
		version: CommitVersion,
		transaction: TransactionId,
		timestamp: u64,
		respond_to: Sender<Result<()>>,
	},
	Shutdown,
}

pub struct Writer {
	receiver: mpsc::Receiver<WriteCommand>,
	conn: Connection,
	ensured_tables: HashSet<String>,
}

impl Writer {
	pub fn spawn(db_path: &Path, flags: OpenFlags) -> Result<Sender<WriteCommand>> {
		let conn = Connection::open_with_flags(db_path, flags)
			.map_err(|e| Error(connection_failed(db_path.display().to_string(), e.to_string())))?;

		let (sender, receiver) = mpsc::channel();

		thread::spawn(move || {
			let mut actor = Writer {
				receiver,
				conn,
				ensured_tables: HashSet::new(),
			};

			actor.run();
		});

		Ok(sender)
	}

	fn run(&mut self) {
		while let Ok(cmd) = self.receiver.recv() {
			match cmd {
				WriteCommand::UnversionedCommit {
					operations,
					response,
				} => {
					let result = self.handle_unversioned_commit(operations);

					let _ = response.send(result);
				}
				WriteCommand::VersionedCommit {
					deltas,
					version,
					transaction,
					timestamp,
					respond_to: response,
				} => {
					let result =
						self.handle_versioned_commit(deltas, version, transaction, timestamp);

					let _ = response.send(result);
				}
				WriteCommand::Shutdown => break,
			}
		}
	}

	fn handle_unversioned_commit(&mut self, operations: Vec<(String, Vec<Value>)>) -> Result<()> {
		let tx = self.conn.transaction().map_err(|e| Error(from_rusqlite_error(e)))?;

		for (rql, params) in operations {
			tx.execute(&rql, params_from_iter(params)).map_err(|e| Error(from_rusqlite_error(e)))?;
		}

		tx.commit().map_err(|e| Error(transaction_failed(e.to_string())))
	}

	fn handle_versioned_commit(
		&mut self,
		deltas: CowVec<Delta>,
		version: CommitVersion,
		transaction: TransactionId,
		timestamp: u64,
	) -> Result<()> {
		let mut tx = self.conn.transaction().map_err(|e| Error(from_rusqlite_error(e)))?;

		let cdc_changes = Self::apply_deltas(&mut tx, &deltas, version, &mut self.ensured_tables)?;

		if !cdc_changes.is_empty() {
			Self::store_cdc_changes(&tx, version, timestamp, transaction, cdc_changes)?;
		}

		tx.commit().map_err(|e| Error(transaction_failed(e.to_string())))?;

		Ok(())
	}

	fn apply_deltas(
		tx: &mut Transaction,
		deltas: &[Delta],
		version: CommitVersion,
		ensured_tables: &mut HashSet<String>,
	) -> Result<Vec<CdcTransactionChange>> {
		let mut cdc_changes = Vec::with_capacity(deltas.len());

		for (idx, delta) in deltas.iter().enumerate() {
			let sequence = match u16::try_from(idx + 1) {
				Ok(seq) => seq,
				Err(_) => return_error!(sequence_exhausted()),
			};

			let table = table_name(delta.key())?;
			let pre = fetch_pre_value(tx, delta.key(), table).ok().flatten();

			Self::apply_single_delta(tx, delta, version, ensured_tables)?;

			cdc_changes.push(CdcTransactionChange {
				sequence,
				change: generate_cdc_change(delta.clone(), pre),
			});
		}

		Ok(cdc_changes)
	}

	fn apply_single_delta(
		tx: &Transaction,
		delta: &Delta,
		version: CommitVersion,
		ensured_tables: &mut HashSet<String>,
	) -> Result<()> {
		match delta {
			Delta::Set {
				key,
				row,
			} => Self::apply_delta_set(tx, key, row, version, ensured_tables),
			Delta::Remove {
				key,
			} => Self::apply_delta_remove(tx, key, version, ensured_tables),
		}
	}

	fn apply_delta_set(
		tx: &Transaction,
		key: &[u8],
		row: &[u8],
		version: CommitVersion,
		ensured_tables: &mut HashSet<String>,
	) -> Result<()> {
		let encoded_key = EncodedKey::new(key.to_vec());
		let table = table_name(&encoded_key)?;
		Self::ensure_table_if_needed(tx, table, ensured_tables)?;

		let query = format!("INSERT OR REPLACE INTO {} (key, version, value) VALUES (?1, ?2, ?3)", table);

		tx.execute(&query, rusqlite::params![key.to_vec(), version, row.to_vec()])
			.map_err(|e| Error(from_rusqlite_error(e)))?;

		Ok(())
	}

	fn apply_delta_remove(
		tx: &Transaction,
		key: &[u8],
		version: CommitVersion,
		ensured_tables: &mut HashSet<String>,
	) -> Result<()> {
		let encoded_key = EncodedKey::new(key.to_vec());
		let table = table_name(&encoded_key)?;
		Self::ensure_table_if_needed(tx, table, ensured_tables)?;

		let query = format!("INSERT OR REPLACE INTO {} (key, version, value) VALUES (?1, ?2, NULL)", table);

		tx.execute(&query, rusqlite::params![key.to_vec(), version])
			.map_err(|e| Error(from_rusqlite_error(e)))?;

		Ok(())
	}

	fn ensure_table_if_needed(tx: &Transaction, table: &str, ensured_tables: &mut HashSet<String>) -> Result<()> {
		if table != "versioned" && !ensured_tables.contains(table) {
			ensure_table_exists(tx, table);
			ensured_tables.insert(table.to_string());
		}
		Ok(())
	}

	fn store_cdc_changes(
		tx: &Transaction,
		version: CommitVersion,
		timestamp: u64,
		transaction: TransactionId,
		cdc_changes: Vec<CdcTransactionChange>,
	) -> Result<()> {
		store_cdc_transaction(tx, CdcTransaction::new(version, timestamp, transaction, cdc_changes))
			.map_err(|e| Error(from_rusqlite_error(e)))
	}
}
