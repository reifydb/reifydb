// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{error::Error as StdError, fmt::Write as _, path::Path};

use reifydb_catalog::change::apply_system_change;
use reifydb_core::{
	delta::{Delta, RemoveAnnounce},
	interface::cdc::SystemChange,
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_testing::testscript::{
	command::Command,
	runner::{Runner, run_path},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction, replica::ReplicaTransaction};
use reifydb_value::{params::Params, value::identity::IdentityId};
use test_each_file::test_each_path;

test_each_path! { in "crates/catalog/tests/scripts/change" as change => test_catalog_change }

fn test_catalog_change(path: &Path) {
	let primary = TestEngine::new();
	let replica = TestEngine::new();
	let mut runner = CatalogRunner::new(primary, replica);
	run_path(&mut runner, path).expect("test failed")
}

struct CatalogRunner {
	primary: TestEngine,
	primary_txn: Option<AdminTransaction>,

	replica: TestEngine,
}

impl CatalogRunner {
	fn new(primary: TestEngine, replica: TestEngine) -> Self {
		let primary_txn = primary.begin_admin(IdentityId::system()).unwrap();
		Self {
			primary,
			primary_txn: Some(primary_txn),
			replica,
		}
	}

	fn primary_txn(&mut self) -> &mut AdminTransaction {
		self.primary_txn.as_mut().expect("no active primary transaction")
	}

	fn begin_primary_txn(&mut self) {
		self.primary_txn = Some(self.primary.begin_admin(IdentityId::system()).unwrap());
	}
}

impl Runner for CatalogRunner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();

		match command.name.as_str() {
			"admin" => {
				let mut args = command.consume_args();
				let rql = args.next_pos().ok_or("admin requires an RQL string")?.value.clone();
				args.reject_rest()?;

				let txn = self.primary_txn();
				txn.rql(&rql, Params::None).check()?;
			}

			"replicate" => {
				command.consume_args().reject_rest()?;

				// Deltas must be captured before the commit consumes the pending writes.
				let changes = deltas_to_system_changes(self.primary_txn());

				let mut txn = self.primary_txn.take().expect("no active primary transaction");
				let version = txn.commit()?;

				let replica_catalog = self.replica.catalog();
				let mut replica_txn = ReplicaTransaction::new(self.replica.multi_owned(), version)?;
				for change in &changes {
					apply_system_change(
						&replica_catalog,
						&mut Transaction::Replica(&mut replica_txn),
						change,
					)?;
				}
				replica_txn.commit_at_version()?;

				writeln!(output, "version: {}", version.0)?;

				self.begin_primary_txn();
			}

			"query" => {
				let mut args = command.consume_args();
				let rql = args.next_pos().ok_or("query requires an RQL string")?.value.clone();
				args.reject_rest()?;

				let result = self.replica.query_as(IdentityId::system(), &rql, Params::None).check()?;
				for frame in result.iter() {
					write!(output, "{}", frame)?;
				}
			}

			name => return Err(format!("unknown command: {name}").into()),
		}

		Ok(output)
	}
}

fn deltas_to_system_changes(txn: &AdminTransaction) -> Vec<SystemChange> {
	// Insertion order is load-bearing: column entries must reach the replica before a table
	// applier tries to list them.
	txn.pending_writes()
		.clone()
		.into_iter_insertion_order()
		.filter_map(|(_, pending)| match pending.delta {
			Delta::Set {
				key,
				row,
			} => Some(SystemChange::Insert {
				key,
				post: row,
			}),
			Delta::Remove {
				key,
				announce: RemoveAnnounce::Announced {
					pre,
				},
			} => Some(SystemChange::Delete {
				key,
				pre: Some(pre),
			}),
			Delta::Remove {
				announce: RemoveAnnounce::Silent,
				..
			} => None,
		})
		.collect()
}
