// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{
	CatalogDictionaryCommandOperations, CatalogDictionaryQueryOperations,
	store::dictionary::create::DictionaryToCreate,
};
use reifydb_core::value::column::Columns;
use reifydb_rql::plan::physical::CreateDictionaryNode;
use reifydb_type::Value;

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) fn create_dictionary<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: CreateDictionaryNode,
	) -> crate::Result<Columns<'a>> {
		if let Some(_) = txn.find_dictionary_by_name(plan.namespace.id, plan.dictionary.text())? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					("namespace", Value::Utf8(plan.namespace.name.clone())),
					("dictionary", Value::Utf8(plan.dictionary.text().to_string())),
					("created", Value::Boolean(false)),
				]));
			}
			// The error will be returned by create_dictionary if the
			// dictionary exists
		}

		txn.create_dictionary(DictionaryToCreate {
			fragment: Some(plan.dictionary.clone().into_owned()),
			dictionary: plan.dictionary.text().to_string(),
			namespace: plan.namespace.id,
			value_type: plan.value_type,
			id_type: plan.id_type,
		})?;

		Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace.name.clone())),
			("dictionary", Value::Utf8(plan.dictionary.text().to_string())),
			("created", Value::Boolean(true)),
		]))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::test_utils::{create_namespace, ensure_test_namespace};
	use reifydb_core::interface::Params;
	use reifydb_rql::plan::physical::PhysicalPlan;
	use reifydb_type::{Fragment, Type, Value};

	use crate::{
		execute::{Executor, catalog::create::dictionary::CreateDictionaryNode},
		stack::Stack,
		test_utils::create_test_command_transaction,
	};

	#[test]
	fn test_create_dictionary() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction();

		let namespace = ensure_test_namespace(&mut txn);

		let mut plan = CreateDictionaryNode {
			namespace: namespace.clone(),
			dictionary: Fragment::owned_internal("test_dictionary"),
			if_not_exists: false,
			value_type: Type::Utf8,
			id_type: Type::Uint4,
		};

		// First creation should succeed
		let mut stack = Stack::new();
		let result = instance
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateDictionary(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.unwrap()
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_dictionary".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));

		// Creating the same dictionary again with `if_not_exists = true`
		// should not error
		plan.if_not_exists = true;
		let result = instance
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateDictionary(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.unwrap()
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_dictionary".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(false));

		// Creating the same dictionary again with `if_not_exists = false`
		// should return error
		plan.if_not_exists = false;
		let err = instance
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateDictionary(plan),
				Params::default(),
				&mut stack,
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_006");
	}

	#[test]
	fn test_create_same_dictionary_in_different_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction();

		let namespace = ensure_test_namespace(&mut txn);
		let another_schema = create_namespace(&mut txn, "another_schema");

		let plan = CreateDictionaryNode {
			namespace: namespace.clone(),
			dictionary: Fragment::owned_internal("test_dictionary"),
			if_not_exists: false,
			value_type: Type::Utf8,
			id_type: Type::Uint4,
		};

		let mut stack = Stack::new();
		let result = instance
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateDictionary(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.unwrap()
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_dictionary".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));

		let plan = CreateDictionaryNode {
			namespace: another_schema.clone(),
			dictionary: Fragment::owned_internal("test_dictionary"),
			if_not_exists: false,
			value_type: Type::Utf8,
			id_type: Type::Uint4,
		};

		let result = instance
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::CreateDictionary(plan.clone()),
				Params::default(),
				&mut stack,
			)
			.unwrap()
			.unwrap();
		assert_eq!(result.row(0)[0], Value::Utf8("another_schema".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("test_dictionary".to_string()));
		assert_eq!(result.row(0)[2], Value::Boolean(true));
	}
}
