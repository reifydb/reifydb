// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropRelationshipNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn drop_relationship(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropRelationshipNode,
) -> Result<Columns> {
	let name = plan.name.text().to_string();

	match services.catalog.drop_relationship(txn, plan.namespace, plan.source_table, &name) {
		Ok(()) => Ok(Columns::single_row([("name", Value::Utf8(name)), ("dropped", Value::Boolean(true))])),
		Err(e) if plan.if_exists && e.0.code == "CA_024" => {
			Ok(Columns::single_row([("name", Value::Utf8(name)), ("dropped", Value::Boolean(false))]))
		}
		Err(e) => Err(e),
	}
}
