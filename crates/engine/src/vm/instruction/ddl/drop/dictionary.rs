// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropDictionaryNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn drop_dictionary(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropDictionaryNode,
) -> crate::Result<Columns> {
	let Some(dictionary_id) = plan.dictionary_id else {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("dictionary", Value::Utf8(plan.dictionary_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	};

	let def = services.catalog.get_dictionary(&mut Transaction::Admin(txn), dictionary_id)?;
	services.catalog.drop_dictionary(txn, def)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("dictionary", Value::Utf8(plan.dictionary_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
