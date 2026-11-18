// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::{
	flow::operator::{LookupObject, OperatorDef},
	value::batch::single_row,
};
use reifydb_rql::nodes::{DropNamespaceNode, DropWindowedViewGroupNode};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{error::ValueError, fragment::Fragment, value::Value};

use super::{dependent::find_flow_dependents, namespace::drop_namespace};
use crate::vm::services::Services;

pub(crate) fn drop_windowed_view_group(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropWindowedViewGroupNode,
) -> Result<RecordBatch, ValueError> {
	let group = services.catalog.find_windowed_view_group_by_name(
		&mut Transaction::Admin(txn),
		plan.namespace.id(),
		plan.group.text(),
	)?;

	let Some(group) = group else {
		if plan.if_exists {
			return single_row([
				("namespace", Value::Utf8(plan.namespace.name().to_string())),
				("group", Value::Utf8(plan.group.text().to_string())),
				("dropped", Value::Boolean(false)),
			]);
		}
		return Err(CatalogError::NotFound {
			kind: CatalogObjectKind::WindowedViewGroup,
			namespace: plan.namespace.name().to_string(),
			name: plan.group.text().to_string(),
			fragment: plan.group.clone(),
		}
		.into());
	};

	let dags = services.catalog.list_flow_dags_asc(&mut Transaction::Admin(txn))?;
	let flows = services.catalog.list_flows_all(&mut Transaction::Admin(txn))?;
	let external_dags: Vec<_> = dags.into_iter().filter(|dag| dag.id() != group.flow).collect();
	let members = group.members.clone();
	let dependents = find_flow_dependents(&services.catalog, txn, &external_dags, &flows, |node_type| {
		matches!(node_type, OperatorDef::SourceView { view } if members.contains(view))
			|| matches!(node_type, OperatorDef::SinkTableView { view, .. } | OperatorDef::SinkRingBufferView { view, .. } | OperatorDef::SinkSeriesView { view, .. } | OperatorDef::SinkQueueView { view } if members.contains(view))
			|| matches!(node_type, OperatorDef::Lookup { right: LookupObject::View(view), .. } if members.contains(view))
	})?;
	if !dependents.is_empty() {
		return Err(CatalogError::InUse {
			kind: CatalogObjectKind::WindowedViewGroup,
			namespace: plan.namespace.name().to_string(),
			name: Some(plan.group.text().to_string()),
			dependents: dependents.join(", "),
			fragment: plan.group.clone(),
		}
		.into());
	}

	let own_name = format!("{}::{}", plan.namespace.name(), plan.group.text());
	let own = services.catalog.find_namespace_by_name(&mut Transaction::Admin(txn), &own_name)?;
	if let Some(ns) = &own
		&& let Some((holds, name)) =
			services.catalog.find_namespace_leftover(&mut Transaction::Admin(txn), ns.id())?
	{
		return Err(CatalogError::WindowedViewGroupNamespaceNotEmpty {
			group: own_name.clone(),
			namespace: own_name.clone(),
			holds,
			name,
			fragment: plan.group.clone(),
		}
		.into());
	}

	for view_id in &group.members {
		let view = services.catalog.get_view(&mut Transaction::Admin(txn), *view_id)?;
		services.catalog.drop_view(txn, view)?;
	}
	if let Some(flow) = flows.iter().find(|flow| flow.id == group.flow) {
		services.catalog.drop_flow(txn, flow.clone())?;
	}
	let group_id = group.id;
	services.catalog.drop_windowed_view_group(txn, group)?;
	if let Some(ns) = own {
		drop_namespace(
			services,
			txn,
			DropNamespaceNode {
				namespace_name: Fragment::internal(&own_name),
				namespace_id: Some(ns.id()),
				if_exists: false,
				cascade: false,
			},
		)?;
	}

	single_row([
		("id", Value::Uint8(group_id.0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("group", Value::Utf8(plan.group.text().to_string())),
		("dropped", Value::Boolean(true)),
	])
}
