// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{error::Error as StdError, fmt::Write as _, path::Path};

use reifydb_catalog::{
	catalog::segment_tree::{SegmentTreeColumnToCreate, SegmentTreeToCreate},
	change::apply_system_change,
};
use reifydb_core::{
	delta::Delta,
	interface::{
		catalog::{
			key::{KeySpec, TimestampPrecision},
			segment_tree::SegmentTreeAggregate,
		},
		cdc::SystemChange,
	},
};
use reifydb_engine::test_harness::TestEngine;
use reifydb_testing::testscript::{
	command::Command,
	runner::{Runner, run_path},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction, replica::ReplicaTransaction};
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{constraint::TypeConstraint, identity::IdentityId, value_type::ValueType},
};
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

			// Test-only stand-in for `CREATE SEGMENTTREE` RQL (not landed until plan 4):
			// drives the catalog API directly so change-tracking/replication can be
			// exercised now. Fixed two-column schema (key_column: datetime, value: float8).
			// aggregates='name|monoid|column,name2|monoid2|column2' ('|' avoids clashing
			// with '::' inside monoid names like 'math::sum').
			"create_segment_tree" => {
				let mut args = command.consume_args();
				let path = args
					.next_pos()
					.ok_or("create_segment_tree requires 'namespace::name'")?
					.value
					.clone();
				let key_column = args
					.lookup("key")
					.map(|a| a.value.clone())
					.unwrap_or_else(|| "timestamp".to_string());
				let aggregates_arg = args.lookup("aggregates").map(|a| a.value.clone());
				args.reject_rest()?;

				let (namespace, name) = path.split_once("::").ok_or("expected 'namespace::name'")?;

				let catalog = self.primary.catalog();
				let ns_id = catalog
					.find_namespace_by_name(&mut Transaction::Admin(self.primary_txn()), namespace)?
					.ok_or_else(|| format!("namespace '{namespace}' not found"))?
					.id();

				let aggregates = aggregates_arg
					.map(|raw| {
						raw.split(',')
							.map(|triple| {
								let parts: Vec<&str> = triple.split('|').collect();
								SegmentTreeAggregate {
									name: parts[0].to_string(),
									monoid: parts[1].to_string(),
									column: parts[2].to_string(),
								}
							})
							.collect()
					})
					.unwrap_or_default();

				let to_create = SegmentTreeToCreate {
					name: Fragment::internal(name),
					namespace: ns_id,
					columns: vec![
						SegmentTreeColumnToCreate {
							name: Fragment::internal(&key_column),
							fragment: Fragment::None,
							constraint: TypeConstraint::unconstrained(ValueType::DateTime),
							properties: vec![],
							auto_increment: false,
							dictionary_id: None,
						},
						SegmentTreeColumnToCreate {
							name: Fragment::internal("value"),
							fragment: Fragment::None,
							constraint: TypeConstraint::unconstrained(ValueType::Float8),
							properties: vec![],
							auto_increment: false,
							dictionary_id: None,
						},
					],
					key: KeySpec::DateTime {
						column: key_column,
						precision: TimestampPrecision::Millisecond,
					},
					aggregates,
					partition_by: vec![],
					underlying: false,
				};

				catalog.create_segment_tree(self.primary_txn(), to_create)?;
			}

			"drop_segment_tree" => {
				let mut args = command.consume_args();
				let path = args
					.next_pos()
					.ok_or("drop_segment_tree requires 'namespace::name'")?
					.value
					.clone();
				args.reject_rest()?;

				let (namespace, name) = path.split_once("::").ok_or("expected 'namespace::name'")?;

				let catalog = self.primary.catalog();
				let ns_id = catalog
					.find_namespace_by_name(&mut Transaction::Admin(self.primary_txn()), namespace)?
					.ok_or_else(|| format!("namespace '{namespace}' not found"))?
					.id();
				let tree = catalog
					.find_segment_tree_by_name(
						&mut Transaction::Admin(self.primary_txn()),
						ns_id,
						name,
					)?
					.ok_or_else(|| format!("segment tree '{path}' not found"))?;

				catalog.drop_segment_tree(self.primary_txn(), tree)?;
			}

			"replicate" => {
				command.consume_args().reject_rest()?;

				// 1. Capture deltas from primary pending writes
				let changes = deltas_to_system_changes(self.primary_txn());

				// 2. Commit primary
				let mut txn = self.primary_txn.take().expect("no active primary transaction");
				let version = txn.commit()?;

				// 3. Apply to replica via ReplicaTransaction + apply_system_change
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

				// 4. Start a new primary transaction
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
	// Clone and consume in insertion order - this preserves the order the primary
	// wrote entries, which matters because e.g. column entries must exist before
	// table appliers try to list them.
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
			Delta::Unset {
				key,
				row,
			} => Some(SystemChange::Delete {
				key,
				pre: Some(row),
			}),
			Delta::Remove {
				key,
			} => Some(SystemChange::Delete {
				key,
				pre: None,
			}),
			Delta::Drop {
				key: _,
			} => None,
		})
		.collect()
}
