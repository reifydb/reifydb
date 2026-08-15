// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::change::apply_system_change;
use reifydb_core::{
	delta::{Delta, RemoveAnnounce},
	interface::{
		catalog::{id::NamespaceId, storage::StorageId},
		cdc::SystemChange,
	},
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction, replica::ReplicaTransaction};
use reifydb_value::value::{duration::Duration, identity::IdentityId};

#[test]
fn test_row_settings_sync_to_catalog_cache() {
	let engine = TestEngine::new();
	let catalog = engine.catalog();

	engine.admin("CREATE NAMESPACE test");
	engine.admin(r#"
		CREATE TABLE test::users { id: int4 } WITH {
			time: processing,
			row: { ttl: 1h, announce: false }
		};
	"#);

	// The TTL must be in the cache immediately after DDL, not only after a reload.
	let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
	let ns_id = NamespaceId(16385); // 'test' namespace
	let table = catalog
		.find_table_by_name(&mut Transaction::Admin(&mut txn), ns_id, "users")
		.unwrap()
		.expect("table not found");
	let storage = StorageId::Table(table.id);

	let ttl = catalog
		.find_row_settings(&mut Transaction::Admin(&mut txn), storage)
		.unwrap()
		.expect("TTL not found in materialized catalog");
	assert_eq!(ttl.ttl.expect("ttl not set").duration, Duration::from_hours(1).unwrap());
}

#[test]
fn test_row_settings_replication_sync() {
	let primary = TestEngine::new();
	let replica = TestEngine::new();
	let replica_catalog = replica.catalog();

	let mut txn = primary.begin_admin(IdentityId::system()).unwrap();

	let r = txn.rql("CREATE NAMESPACE test", Default::default());
	if let Some(e) = r.error {
		panic!("{e:?}");
	}
	let r = txn.rql(
		"CREATE TABLE test::users { id: int4 } WITH { time: processing, row: { ttl: 1m, announce: false } }",
		Default::default(),
	);
	if let Some(e) = r.error {
		panic!("{e:?}");
	}

	// The TTL must ride the replicated system changes; a replica that misses it never expires
	// rows the primary does.
	let changes = deltas_to_system_changes(&txn);

	let version = txn.commit().unwrap();

	let mut replica_txn = ReplicaTransaction::new(replica.multi_owned(), version).unwrap();
	for change in &changes {
		apply_system_change(&replica_catalog, &mut Transaction::Replica(&mut replica_txn), change).unwrap();
	}
	replica_txn.commit_at_version().unwrap();

	let mut q_txn = replica.begin_admin(IdentityId::system()).unwrap();
	let table = replica_catalog
		.find_table_by_name(&mut Transaction::Admin(&mut q_txn), NamespaceId(16385), "users")
		.unwrap()
		.expect("table not found on replica");
	let storage = StorageId::Table(table.id);

	let ttl = replica_catalog
		.find_row_settings(&mut Transaction::Admin(&mut q_txn), storage)
		.unwrap()
		.expect("TTL not found in replica materialized catalog");
	assert_eq!(ttl.ttl.expect("ttl not set").duration, Duration::from_minutes(1).unwrap());
}

#[test]
fn test_operator_settings_sync_to_catalog_cache() {
	use reifydb_catalog::store::operator_settings::create::create_operator_settings;
	use reifydb_core::{
		interface::catalog::flow::OperatorId,
		lifecycle::operator::ListOperatorSettings,
		row::{OperatorLateness, OperatorSettings},
	};

	let engine = TestEngine::new();
	let catalog = engine.catalog();

	// The operator-TTL GC actor only ever reads the cache-backed list, so a write that reaches
	// storage without tracking the change leaves the list empty and silently disables GC for
	// every stateful operator.
	let operator_id = OperatorId(42);
	let settings = OperatorSettings {
		lateness: Some(OperatorLateness {
			duration: Duration::from_hours(1).unwrap(),
		}),
		join: None,
	};

	let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
	create_operator_settings(&mut txn, operator_id, &settings).unwrap();
	txn.commit().unwrap();

	let listed = catalog.list_operator_settings();
	assert_eq!(listed.len(), 1, "operator settings did not sync to the catalog cache");
	assert_eq!(listed[0].0, operator_id);
	assert_eq!(listed[0].1.lateness.as_ref().expect("ttl not set").duration, Duration::from_hours(1).unwrap());
}

fn deltas_to_system_changes(txn: &AdminTransaction) -> Vec<SystemChange> {
	txn.pending_writes()
		.clone()
		.into_iter_insertion_order()
		.filter_map(|(_, pending)| match pending.delta {
			Delta::Set {
				key,
				bytes,
			} => Some(SystemChange::Insert {
				key,
				post: bytes,
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
