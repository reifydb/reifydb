// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::{interface::catalog::view::View, value::column::columns::Columns};
use reifydb_rql::{flow::node::FlowNodeType, nodes::DropViewNode};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use super::dependent::find_flow_dependents;
use crate::{Result, vm::services::Services};

pub(crate) fn drop_view(services: &Services, txn: &mut AdminTransaction, plan: DropViewNode) -> Result<Columns> {
	let Some(view_id) = plan.view_id else {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("view", Value::Utf8(plan.view_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	};

	let def = services.catalog.get_view(&mut Transaction::Admin(txn), view_id)?;

	let nodes = services.catalog.list_flow_nodes_all(&mut Transaction::Admin(txn))?;
	let flows = services.catalog.list_flows_all(&mut Transaction::Admin(txn))?;
	let own_flow_id = flows.iter().find(|f| f.namespace == def.namespace() && f.name == def.name()).map(|f| f.id);
	let external_nodes: Vec<_> = if let Some(own_id) = own_flow_id {
		nodes.iter().filter(|n| n.flow != own_id).cloned().collect()
	} else {
		nodes
	};
	let dependents = find_flow_dependents(&services.catalog, txn, &external_nodes, &flows, |node_type| {
		matches!(node_type, FlowNodeType::SourceView { view } if *view == view_id)
			|| matches!(node_type, FlowNodeType::SinkTableView { view, .. } | FlowNodeType::SinkRingBufferView { view, .. } | FlowNodeType::SinkSeriesView { view, .. } if *view == view_id)
	})?;
	if !dependents.is_empty() {
		let dependents_str = dependents.join(", ");
		return Err(CatalogError::InUse {
			kind: CatalogObjectKind::View,
			namespace: plan.namespace_name.text().to_string(),
			name: Some(plan.view_name.text().to_string()),
			dependents: dependents_str,
			fragment: plan.view_name.clone(),
		}
		.into());
	}

	drop_underlying_primitive(services, txn, &def)?;

	services.catalog.drop_view(txn, def)?;

	if let Some(own_id) = own_flow_id
		&& let Some(own_flow) = flows.iter().find(|f| f.id == own_id)
	{
		services.catalog.drop_flow(txn, own_flow.clone())?;
	}

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("view", Value::Utf8(plan.view_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}

fn drop_underlying_primitive(services: &Services, txn: &mut AdminTransaction, view: &View) -> Result<()> {
	match view {
		View::Table(t) => {
			if let Some(table) = services.catalog.find_table(&mut Transaction::Admin(txn), t.underlying)? {
				services.catalog.drop_table(txn, table)?;
			}
		}
		View::RingBuffer(rb) => {
			if let Some(ringbuffer) =
				services.catalog.find_ringbuffer(&mut Transaction::Admin(txn), rb.underlying)?
			{
				services.catalog.drop_ringbuffer(txn, ringbuffer)?;
			}
		}
		View::Series(s) => {
			if let Some(series) =
				services.catalog.find_series(&mut Transaction::Admin(txn), s.underlying)?
			{
				services.catalog.drop_series(txn, series)?;
			}
		}
	}
	Ok(())
}
