// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::queue::QueueToCreate;
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::CreateQueueNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_queue(services: &Services, txn: &mut AdminTransaction, plan: CreateQueueNode) -> Result<Columns> {
	if let Some(existing) = services.catalog.find_queue_by_name(
		&mut Transaction::Admin(txn),
		plan.namespace.def().id(),
		plan.queue.text(),
	)? && plan.if_not_exists
	{
		return Ok(Columns::single_row([
			("id", Value::Uint8(existing.id.0)),
			("namespace", Value::Utf8(plan.namespace.name().to_string())),
			("queue", Value::Utf8(plan.queue.text().to_string())),
			("created", Value::Boolean(false)),
		]));
	}

	let result = services.catalog.create_queue(
		txn,
		QueueToCreate {
			name: plan.queue.clone(),
			namespace: plan.namespace.def().id(),
			columns: plan.columns,
			partitions: plan.partitions,
			ordered_by: plan.ordered_by,
			retention: plan.retention,
			retry: plan.retry,
			underlying: false,
		},
	)?;

	Ok(Columns::single_row([
		("id", Value::Uint8(result.id.0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("queue", Value::Utf8(plan.queue.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}
