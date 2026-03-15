// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::{
	catalog::{
		ringbuffer::{RingBufferColumnToCreate, RingBufferToCreate},
		series::{SeriesColumnToCreate, SeriesToCreate},
		table::{TableColumnToCreate, TableToCreate},
		view::ViewToCreate,
	},
	store::view::create::ViewStorageConfig,
};
use reifydb_core::{
	error::diagnostic::catalog::view_already_exists, interface::catalog::change::CatalogTrackViewChangeOperations,
	value::column::columns::Columns,
};
use reifydb_rql::nodes::{CompiledViewStorageKind, CreateDeferredViewNode};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{fragment::Fragment, return_error, value::Value};

use super::create_deferred_view_flow;
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
				("namespace", Value::Utf8(plan.namespace.name().to_string())),
				("view", Value::Utf8(plan.view.text().to_string())),
				("created", Value::Boolean(false)),
			]));
		}

		return_error!(view_already_exists(plan.view.clone(), &plan.namespace.name(), view.name(),));
	}

	let storage = create_underlying_primitive(services, txn, &plan)?;

	let result = services.catalog.create_deferred_view(
		txn,
		ViewToCreate {
			name: plan.view.clone(),
			namespace: plan.namespace.id(),
			columns: plan.columns,
			storage,
		},
	)?;
	txn.track_view_def_created(result.clone())?;

	create_deferred_view_flow(&services.catalog, txn, &result, *plan.as_clause)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("view", Value::Utf8(plan.view.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}

fn create_underlying_primitive(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: &CreateDeferredViewNode,
) -> Result<ViewStorageConfig> {
	let underlying_name = Fragment::internal(format!("__view_{}", plan.view.text()));
	let namespace = plan.namespace.id();

	match &plan.storage_kind {
		CompiledViewStorageKind::Table => {
			let columns: Vec<TableColumnToCreate> = plan
				.columns
				.iter()
				.map(|c| TableColumnToCreate {
					name: c.name.clone(),
					fragment: c.fragment.clone(),
					constraint: c.constraint.clone(),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				})
				.collect();

			let table = services.catalog.create_table(
				txn,
				TableToCreate {
					name: underlying_name,
					namespace,
					columns,
					retention_policy: None,
					primary_key_columns: None,
				},
			)?;

			Ok(ViewStorageConfig::Table {
				underlying: table.id,
			})
		}
		CompiledViewStorageKind::RingBuffer {
			capacity,
			propagate_evictions,
			partition_by,
		} => {
			let columns: Vec<RingBufferColumnToCreate> = plan
				.columns
				.iter()
				.map(|c| RingBufferColumnToCreate {
					name: c.name.clone(),
					fragment: c.fragment.clone(),
					constraint: c.constraint.clone(),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				})
				.collect();

			let ringbuffer = services.catalog.create_ringbuffer(
				txn,
				RingBufferToCreate {
					name: underlying_name,
					namespace,
					columns,
					capacity: *capacity,
					partition_by: partition_by.clone(),
				},
			)?;

			Ok(ViewStorageConfig::RingBuffer {
				underlying: ringbuffer.id,
				capacity: *capacity,
				propagate_evictions: *propagate_evictions,
			})
		}
		CompiledViewStorageKind::Series {
			timestamp_column,
			precision,
		} => {
			let columns: Vec<SeriesColumnToCreate> = plan
				.columns
				.iter()
				.map(|c| SeriesColumnToCreate {
					name: c.name.clone(),
					fragment: c.fragment.clone(),
					constraint: c.constraint.clone(),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				})
				.collect();

			let series = services.catalog.create_series(
				txn,
				SeriesToCreate {
					name: underlying_name,
					namespace,
					columns,
					tag: None,
					precision: *precision,
				},
			)?;

			Ok(ViewStorageConfig::Series {
				underlying: series.id,
				timestamp_column: timestamp_column.clone(),
				precision: *precision,
				tag: None,
			})
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::{params::Params, value::Value};

	use crate::{
		test_utils::create_test_admin_transaction_with_internal_schema,
		vm::{Admin, executor::Executor},
	};

	#[test]
	fn test_create_view() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction_with_internal_schema();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
			},
		)
		.unwrap();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE TABLE test_namespace::src { id: Int4 }",
				params: Params::default(),
			},
		)
		.unwrap();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE DEFERRED VIEW test_namespace::test_view { id: Int4 } AS { FROM test_namespace::src }",
					params: Params::default(),
				},
			)
			.unwrap();
		let frame = &frames[0];

		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_view".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));

		// Creating the same view again should return error
		let err = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE DEFERRED VIEW test_namespace::test_view { id: Int4 } AS { FROM test_namespace::src }",
					params: Params::default(),
				},
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_create_same_view_in_different_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction_with_internal_schema();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
			},
		)
		.unwrap();
		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE another_schema",
				params: Params::default(),
			},
		)
		.unwrap();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE TABLE test_namespace::src { id: Int4 }",
				params: Params::default(),
			},
		)
		.unwrap();
		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE TABLE another_schema::src { id: Int4 }",
				params: Params::default(),
			},
		)
		.unwrap();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE DEFERRED VIEW test_namespace::test_view { id: Int4 } AS { FROM test_namespace::src }",
					params: Params::default(),
				},
			)
			.unwrap();
		let frame = &frames[0];

		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_view".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE DEFERRED VIEW another_schema::test_view { id: Int4 } AS { FROM another_schema::src }",
					params: Params::default(),
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("another_schema".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_view".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));
	}
}
