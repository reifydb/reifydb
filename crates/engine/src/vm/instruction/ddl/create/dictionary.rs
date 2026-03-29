// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::dictionary::DictionaryToCreate;
use reifydb_core::{
	interface::catalog::change::CatalogTrackDictionaryChangeOperations, value::column::columns::Columns,
};
use reifydb_rql::nodes::CreateDictionaryNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_dictionary(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateDictionaryNode,
) -> Result<Columns> {
	if let Some(existing) = services.catalog.find_dictionary_by_name(
		&mut Transaction::Admin(txn),
		plan.namespace.id(),
		plan.dictionary.text(),
	)? {
		if plan.if_not_exists {
			return Ok(Columns::single_row([
				("id", Value::Uint8(existing.id.0)),
				("namespace", Value::Utf8(plan.namespace.name().to_string())),
				("dictionary", Value::Utf8(plan.dictionary.text().to_string())),
				("created", Value::Boolean(false)),
			]));
		}
	}

	let result = services.catalog.create_dictionary(
		txn,
		DictionaryToCreate {
			name: plan.dictionary.clone(),
			namespace: plan.namespace.id(),
			value_type: plan.value_type,
			id_type: plan.id_type,
		},
	)?;
	let id = result.id;
	txn.track_dictionary_created(result)?;

	Ok(Columns::single_row([
		("id", Value::Uint8(id.0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("dictionary", Value::Utf8(plan.dictionary.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::{params::Params, value::Value};

	use crate::{
		test_harness::create_test_admin_transaction,
		vm::{Admin, executor::Executor},
	};

	#[test]
	fn test_create_dictionary() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
			},
		)
		.unwrap();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE DICTIONARY test_namespace::test_dictionary FOR Utf8 AS Uint4",
					params: Params::default(),
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Uint8(1025));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("test_dictionary".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Boolean(true));

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE DICTIONARY IF NOT EXISTS test_namespace::test_dictionary FOR Utf8 AS Uint4",
					params: Params::default(),
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Uint8(1025));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("test_dictionary".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Boolean(false));

		let err = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE DICTIONARY test_namespace::test_dictionary FOR Utf8 AS Uint4",
					params: Params::default(),
				},
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_006");
	}

	#[test]
	fn test_create_same_dictionary_in_different_shape() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

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
				rql: "CREATE NAMESPACE another_shape",
				params: Params::default(),
			},
		)
		.unwrap();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE DICTIONARY test_namespace::test_dictionary FOR Utf8 AS Uint4",
					params: Params::default(),
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Uint8(1025));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("test_dictionary".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Boolean(true));

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE DICTIONARY another_shape::test_dictionary FOR Utf8 AS Uint4",
					params: Params::default(),
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Uint8(1026));
		assert_eq!(frame[1].get_value(0), Value::Utf8("another_shape".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("test_dictionary".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Boolean(true));
	}
}
