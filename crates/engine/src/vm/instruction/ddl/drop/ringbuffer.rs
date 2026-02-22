// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropRingBufferNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn drop_ringbuffer(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropRingBufferNode,
) -> crate::Result<Columns> {
	let Some(ringbuffer_id) = plan.ringbuffer_id else {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("ringbuffer", Value::Utf8(plan.ringbuffer_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	};

	let def = services.catalog.get_ringbuffer(&mut Transaction::Admin(txn), ringbuffer_id)?;
	services.catalog.drop_ringbuffer(txn, def)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("ringbuffer", Value::Utf8(plan.ringbuffer_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
