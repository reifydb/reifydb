// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::dictionary::DictionaryToCreate;
use reifydb_core::{
	interface::catalog::change::CatalogTrackDictionaryChangeOperations, value::column::columns::Columns,
};
use reifydb_rql::plan::physical::CreateDictionaryNode;
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
			fragment: Some(plan.dictionary.clone()),
			dictionary: plan.dictionary.text().to_string(),
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
	use reifydb_catalog::test_utils::{create_namespace, ensure_test_namespace};
	use reifydb_rql::plan::physical::PhysicalPlan;
	use reifydb_type::{
		fragment::Fragment,
		params::Params,
		value::{Value, r#type::Type},
	};

	use crate::{
		test_utils::create_test_admin_transaction,
		vm::{executor::Executor, instruction::ddl::create::dictionary::CreateDictionaryNode},
	};

	#[test]
	fn test_create_dictionary() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		let namespace = ensure_test_namespace(&mut txn);

		let mut plan = CreateDictionaryNode {
			namespace: namespace.clone(),
			dictionary: Fragment::internal("test_dictionary"),
			if_not_exists: false,
			value_type: Type::Utf8,
			id_type: Type::Uint4,
		};

		// First creation should succeed
		let frames = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateDictionary(plan.clone()), Params::default())
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_dictionary".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));

		// Creating the same dictionary again with `if_not_exists = true`
		// should not error
		plan.if_not_exists = true;
		let frames = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateDictionary(plan.clone()), Params::default())
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_dictionary".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(false));

		// Creating the same dictionary again with `if_not_exists = false`
		// should return error
		plan.if_not_exists = false;
		let err = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateDictionary(plan), Params::default())
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_006");
	}

	#[test]
	fn test_create_same_dictionary_in_different_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		let namespace = ensure_test_namespace(&mut txn);
		let another_schema = create_namespace(&mut txn, "another_schema");

		let plan = CreateDictionaryNode {
			namespace: namespace.clone(),
			dictionary: Fragment::internal("test_dictionary"),
			if_not_exists: false,
			value_type: Type::Utf8,
			id_type: Type::Uint4,
		};

		let frames = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateDictionary(plan.clone()), Params::default())
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_dictionary".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));

		let plan = CreateDictionaryNode {
			namespace: another_schema.clone(),
			dictionary: Fragment::internal("test_dictionary"),
			if_not_exists: false,
			value_type: Type::Utf8,
			id_type: Type::Uint4,
		};

		let frames = instance
			.run_admin_plan(&mut txn, PhysicalPlan::CreateDictionary(plan.clone()), Params::default())
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("another_schema".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_dictionary".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));
	}
}
