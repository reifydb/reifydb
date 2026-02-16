// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::dictionary::DictionaryToCreate;
use reifydb_core::{
	interface::catalog::change::CatalogTrackDictionaryChangeOperations, value::column::columns::Columns,
};
use reifydb_rql::nodes::CreateDictionaryNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn create_dictionary(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateDictionaryNode,
) -> crate::Result<Columns> {
	if let Some(_) = services.catalog.find_dictionary_by_name(txn, plan.namespace.id, plan.dictionary.text())? {
		if plan.if_not_exists {
			return Ok(Columns::single_row([
				("namespace", Value::Utf8(plan.namespace.name.clone())),
				("dictionary", Value::Utf8(plan.dictionary.text().to_string())),
				("created", Value::Boolean(false)),
			]));
		}
	}

	let result = services.catalog.create_dictionary(
		txn,
		DictionaryToCreate {
			name: plan.dictionary.clone(),
			namespace: plan.namespace.id,
			value_type: plan.value_type,
			id_type: plan.id_type,
		},
	)?;
	txn.track_dictionary_def_created(result)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace.name.clone())),
		("dictionary", Value::Utf8(plan.dictionary.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::auth::Identity;
	use reifydb_type::{params::Params, value::Value};

	use crate::{
		test_utils::create_test_admin_transaction,
		vm::{Admin, executor::Executor},
	};

	#[test]
	fn test_create_dictionary() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = Identity::root();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE DICTIONARY test_namespace.test_dictionary FOR Utf8 AS Uint4",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_dictionary".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE DICTIONARY IF NOT EXISTS test_namespace.test_dictionary FOR Utf8 AS Uint4",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_dictionary".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(false));

		let err = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE DICTIONARY test_namespace.test_dictionary FOR Utf8 AS Uint4",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_006");
	}

	#[test]
	fn test_create_same_dictionary_in_different_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = Identity::root();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();
		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE another_schema",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE DICTIONARY test_namespace.test_dictionary FOR Utf8 AS Uint4",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_dictionary".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE DICTIONARY another_schema.test_dictionary FOR Utf8 AS Uint4",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("another_schema".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_dictionary".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));
	}
}
