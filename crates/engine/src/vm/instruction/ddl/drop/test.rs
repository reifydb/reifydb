// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropTestNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn drop_test(services: &Services, txn: &mut AdminTransaction, plan: DropTestNode) -> Result<Columns> {
	let Some(test_id) = plan.test_id else {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("test", Value::Utf8(plan.test_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	};

	services.catalog.drop_test(txn, test_id)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("test", Value::Utf8(plan.test_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
