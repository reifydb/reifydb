// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_catalog::catalog::primary_key::PrimaryKeyToCreate;
use reifydb_core::{
	error::diagnostic::{
		catalog::{primary_key_invalid_type, table_not_found},
		query::column_not_found,
	},
	interface::catalog::object::ObjectId,
	value::batch::single_row,
};
use reifydb_rql::nodes::CreatePrimaryKeyNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{
	return_error,
	value::{Value, value_type::ValueType},
};

use crate::{Result, vm::services::Services};

pub(crate) fn create_primary_key(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreatePrimaryKeyNode,
) -> Result<RecordBatch> {
	let namespace_id = plan.namespace.def().id();
	let table_name = plan.table.text();

	let Some(table) =
		services.catalog.find_table_by_name(&mut Transaction::Admin(txn), namespace_id, table_name)?
	else {
		return_error!(table_not_found(plan.table.clone(), plan.namespace.name(), table_name));
	};

	let table_columns = services.catalog.list_columns(&mut Transaction::Admin(txn), table.id)?;

	let mut column_ids = Vec::new();
	for pk_column in &plan.columns {
		let column_name = pk_column.column.text();

		let Some(column) = table_columns.iter().find(|col| col.name == column_name) else {
			return_error!(column_not_found(pk_column.column.clone()));
		};

		let ty = column.constraint.get_type();
		if column.dictionary_id.is_some()
			|| matches!(
				ty,
				ValueType::Option(_)
					| ValueType::Decimal { .. }
					| ValueType::Utf8
					| ValueType::Blob
					| ValueType::Any
					| ValueType::List(_)
					| ValueType::Record(_)
					| ValueType::Tuple(_)
					| ValueType::Digest { .. }
			) {
			return_error!(primary_key_invalid_type(pk_column.column.clone(), &column.name, ty));
		}

		column_ids.push(column.id);
	}

	services.catalog.create_primary_key(
		txn,
		PrimaryKeyToCreate {
			object: ObjectId::Table(table.id),
			column_ids,
		},
	)?;

	single_row([
		("operation", Value::Utf8("CREATE PRIMARY KEY".to_string())),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("table", Value::Utf8(table.name)),
	])
}
