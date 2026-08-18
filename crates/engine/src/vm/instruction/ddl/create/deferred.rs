// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{
	catalog::view::ViewToCreate,
	store::{row_settings::create::create_row_settings, view::create::ViewStorage},
};
use reifydb_core::{
	error::diagnostic::catalog::view_already_exists,
	interface::catalog::{change::CatalogTrackViewChangeOperations, storage::StorageId},
	row::RowSettings,
	value::column::columns::Columns,
};
use reifydb_rql::nodes::{CompiledViewStorageKind, CreateDeferredViewNode};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{return_error, value::Value};

use super::{create_deferred_view_flow, extract_view_sort};
use crate::{Result, vm::services::Services};

pub(crate) fn create_deferred_view(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateDeferredViewNode,
) -> Result<Columns> {
	if let Some(view) = services.catalog.find_view_by_name(
		&mut Transaction::Admin(txn),
		plan.namespace.id(),
		plan.view.text(),
	)? {
		if plan.if_not_exists {
			return Ok(Columns::single_row([
				("id", Value::Uint8(view.id().0)),
				("namespace", Value::Utf8(plan.namespace.name().to_string())),
				("view", Value::Utf8(plan.view.text().to_string())),
				("created", Value::Boolean(false)),
			]));
		}

		return_error!(view_already_exists(plan.view.clone(), plan.namespace.name(), view.name(),));
	}

	let storage = match &plan.storage_kind {
		CompiledViewStorageKind::Table {
			partition_by,
		} => ViewStorage::Table {
			partition_by: partition_by.clone(),
		},
		CompiledViewStorageKind::RingBuffer {
			capacity,
			partition_by,
		} => ViewStorage::RingBuffer {
			capacity: *capacity,
			partition_by: partition_by.clone(),
		},
		CompiledViewStorageKind::Series {
			key,
			partition_by,
		} => ViewStorage::Series {
			key: key.clone(),
			tag: None,
			partition_by: partition_by.clone(),
		},
	};

	let sort = extract_view_sort(&plan.as_clause, &plan.columns);

	let result = services.catalog.create_deferred_view(
		txn,
		ViewToCreate {
			name: plan.view.clone(),
			namespace: plan.namespace.id(),
			columns: plan.columns,
			storage,
			sort,
		},
	)?;
	txn.track_view_created(result.clone())?;

	if let Some(ttl) = &plan.ttl {
		create_row_settings(
			txn,
			StorageId::View(result.id()),
			&RowSettings {
				ttl: Some(ttl.clone()),
				persistent: plan.persistent,
			},
		)?;
	}

	create_deferred_view_flow(&services.catalog, &services.routines, txn, &result, *plan.as_clause)?;

	Ok(Columns::single_row([
		("id", Value::Uint8(result.id().0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("view", Value::Utf8(plan.view.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}

#[cfg(test)]
pub mod tests {
	use reifydb_test_harness::engine::create_test_admin_transaction_with_internal_shape;
	use reifydb_value::{params::Params, value::Value};

	use crate::vm::{Admin, executor::Executor};

	#[test]
	fn test_create_view() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction_with_internal_shape();

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE TABLE test_namespace::src { id: Int4 }",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE DEFERRED VIEW test_namespace::test_view { id: Int4 } AS { FROM test_namespace::src }",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		let frame = &r[0];

		assert_eq!(frame[0].get_value(0), Value::Uint8(16387));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("test_view".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Boolean(true));

		// A duplicate view name must fault rather than silently redefine the flow behind it.
		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE DEFERRED VIEW test_namespace::test_view { id: Int4 } AS { FROM test_namespace::src }",
				params: Params::default(),
			},
		);
		assert!(r.is_err());
		assert_eq!(r.error.unwrap().diagnostic().code, "CA_003");
	}

	#[test]
	fn test_create_same_view_in_different_shape() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction_with_internal_shape();

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE another_shape",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE TABLE test_namespace::src { id: Int4 }",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE TABLE another_shape::src { id: Int4 }",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE DEFERRED VIEW test_namespace::test_view { id: Int4 } AS { FROM test_namespace::src }",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		let frame = &r[0];

		assert_eq!(frame[0].get_value(0), Value::Uint8(16388));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("test_view".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Boolean(true));

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE DEFERRED VIEW another_shape::test_view { id: Int4 } AS { FROM another_shape::src }",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		let frame = &r[0];
		assert_eq!(frame[0].get_value(0), Value::Uint8(16389));
		assert_eq!(frame[1].get_value(0), Value::Utf8("another_shape".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("test_view".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Boolean(true));
	}
}
