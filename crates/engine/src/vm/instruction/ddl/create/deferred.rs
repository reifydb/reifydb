// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::view::ViewToCreate;
use reifydb_core::{interface::catalog::change::CatalogTrackViewChangeOperations, value::column::columns::Columns};
use reifydb_rql::nodes::CreateDeferredViewNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use super::create_deferred_view_flow;
use crate::{Result, vm::services::Services};

pub(crate) fn create_deferred_view(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateDeferredViewNode,
) -> Result<Columns> {
	if let Some(_) =
		services.catalog.find_view_by_name(&mut Transaction::Admin(txn), plan.namespace.id, plan.view.text())?
	{
		if plan.if_not_exists {
			return Ok(Columns::single_row([
				("namespace", Value::Utf8(plan.namespace.name.to_string())),
				("view", Value::Utf8(plan.view.text().to_string())),
				("created", Value::Boolean(false)),
			]));
		}
	}

	let result = services.catalog.create_deferred_view(
		txn,
		ViewToCreate {
			name: plan.view.clone(),
			namespace: plan.namespace.id,
			columns: plan.columns,
		},
	)?;
	txn.track_view_def_created(result.clone())?;

	create_deferred_view_flow(&services.catalog, txn, &result, *plan.as_clause)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace.name.to_string())),
		("view", Value::Utf8(plan.view.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::{
		params::Params,
		value::{Value, identity::IdentityId},
	};

	use crate::{
		test_utils::create_test_admin_transaction_with_internal_schema,
		vm::{Admin, executor::Executor},
	};

	#[test]
	fn test_create_view() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction_with_internal_schema();
		let identity = IdentityId::root();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
				identity,
			},
		)
		.unwrap();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE DEFERRED VIEW test_namespace::test_view { id: Int4 } AS { FROM [] }",
					params: Params::default(),
					identity,
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
					rql: "CREATE DEFERRED VIEW test_namespace::test_view { id: Int4 } AS { FROM [] }",
					params: Params::default(),
					identity,
				},
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_create_same_view_in_different_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction_with_internal_schema();
		let identity = IdentityId::root();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
				identity,
			},
		)
		.unwrap();
		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE another_schema",
				params: Params::default(),
				identity,
			},
		)
		.unwrap();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE DEFERRED VIEW test_namespace::test_view { id: Int4 } AS { FROM [] }",
					params: Params::default(),
					identity,
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
					rql: "CREATE DEFERRED VIEW another_schema::test_view { id: Int4 } AS { FROM [] }",
					params: Params::default(),
					identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("another_schema".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_view".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));
	}
}
