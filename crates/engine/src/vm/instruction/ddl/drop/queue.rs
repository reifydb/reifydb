// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::value::batch::single_row;
use reifydb_rql::nodes::DropQueueNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{error::ValueError, value::Value};

use crate::vm::services::Services;

pub(crate) fn drop_queue(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropQueueNode,
) -> Result<RecordBatch, ValueError> {
	let Some(queue_id) = plan.queue_id else {
		return single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("queue", Value::Utf8(plan.queue_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]);
	};

	let def = services.catalog.get_queue(&mut Transaction::Admin(txn), queue_id)?;

	services.catalog.drop_queue(txn, def)?;

	single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("queue", Value::Utf8(plan.queue_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	])
}
