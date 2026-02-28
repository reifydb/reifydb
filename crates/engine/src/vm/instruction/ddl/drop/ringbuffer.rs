// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::{flow::node::FlowNodeType, nodes::DropRingBufferNode};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use super::dependent::find_flow_dependents;
use crate::{Result, vm::services::Services};

pub(crate) fn drop_ringbuffer(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropRingBufferNode,
) -> Result<Columns> {
	let Some(ringbuffer_id) = plan.ringbuffer_id else {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("ringbuffer", Value::Utf8(plan.ringbuffer_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	};

	let def = services.catalog.get_ringbuffer(&mut Transaction::Admin(txn), ringbuffer_id)?;

	// Check for flows that reference this ring buffer
	let nodes = services.catalog.list_flow_nodes_all(&mut Transaction::Admin(txn))?;
	let flows = services.catalog.list_flows_all(&mut Transaction::Admin(txn))?;
	let dependents = find_flow_dependents(
		&services.catalog,
		txn,
		&nodes,
		&flows,
		|node_type| matches!(node_type, FlowNodeType::SourceRingBuffer { ringbuffer } if *ringbuffer == ringbuffer_id),
	)?;
	if !dependents.is_empty() {
		let dependents_str = dependents.join(", ");
		return Err(CatalogError::InUse {
			kind: CatalogObjectKind::RingBuffer,
			namespace: plan.namespace_name.text().to_string(),
			name: Some(plan.ringbuffer_name.text().to_string()),
			dependents: dependents_str,
			fragment: plan.ringbuffer_name.clone(),
		}
		.into());
	}

	services.catalog.drop_ringbuffer(txn, def)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("ringbuffer", Value::Utf8(plan.ringbuffer_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
