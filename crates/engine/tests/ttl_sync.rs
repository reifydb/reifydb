// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::change::apply_system_change;
use reifydb_core::{
	delta::Delta,
	interface::{
		catalog::{id::NamespaceId, shape::ShapeId},
		cdc::SystemChange,
	},
};
use reifydb_engine::test_harness::TestEngine;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction, replica::ReplicaTransaction};
use reifydb_type::value::identity::IdentityId;

#[test]
fn test_row_ttl_sync_to_materialized_catalog() {
	let engine = TestEngine::new();
	let catalog = engine.catalog();

	// 1. Create a namespace and table with TTL
	engine.admin("CREATE NAMESPACE test");
	engine.admin(r#"
		CREATE TABLE test::users { id: int4 } WITH {
			ttl: { duration: '1h', on: created, mode: drop }
		};
	"#);

	// 2. Check if TTL is in MaterializedCatalog immediately
	let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
	let ns_id = NamespaceId(16385); // 'test' namespace
	let table = catalog
		.find_table_by_name(&mut Transaction::Admin(&mut txn), ns_id, "users")
		.unwrap()
		.expect("table not found");
	let shape = ShapeId::Table(table.id);

	let ttl = catalog.materialized.find_row_ttl(shape).expect("TTL not found in materialized catalog");
	assert_eq!(ttl.duration_nanos, 3_600_000_000_000);
}

#[test]
fn test_row_ttl_replication_sync() {
	let primary = TestEngine::new();
	let replica = TestEngine::new();
	let replica_catalog = replica.catalog();

	// 1. Start transaction on primary
	let mut txn = primary.begin_admin(IdentityId::system()).unwrap();

	// 2. Create table with TTL
	let r = txn.rql("CREATE NAMESPACE test", Default::default());
	if let Some(e) = r.error {
		panic!("{e:?}");
	}
	let r = txn.rql(
		"CREATE TABLE test::users { id: int4 } WITH { ttl: { duration: '1m', on: created, mode: drop } }",
		Default::default(),
	);
	if let Some(e) = r.error {
		panic!("{e:?}");
	}

	// 3. Capture changes
	let changes = deltas_to_system_changes(&txn);

	// 4. Commit primary
	let version = txn.commit().unwrap();

	// 5. Apply to replica
	let mut replica_txn = ReplicaTransaction::new(replica.multi_owned(), version).unwrap();
	for change in &changes {
		apply_system_change(&replica_catalog, &mut Transaction::Replica(&mut replica_txn), change).unwrap();
	}
	replica_txn.commit_at_version().unwrap();

	// 6. Verify replica materialized catalog has the TTL
	// Namespace ID should be 1025
	let mut q_txn = replica.begin_admin(IdentityId::system()).unwrap();
	let table = replica_catalog
		.find_table_by_name(&mut Transaction::Admin(&mut q_txn), NamespaceId(16385), "users")
		.unwrap()
		.expect("table not found on replica");
	let shape = ShapeId::Table(table.id);

	let ttl = replica_catalog
		.materialized
		.find_row_ttl(shape)
		.expect("TTL not found in replica materialized catalog");
	assert_eq!(ttl.duration_nanos, 60_000_000_000);
}

fn deltas_to_system_changes(txn: &AdminTransaction) -> Vec<SystemChange> {
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
				..
			} => None,
		})
		.collect()
}
