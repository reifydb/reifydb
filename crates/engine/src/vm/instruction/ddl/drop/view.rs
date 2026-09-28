// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::{flow::operator::OperatorDef, value::column::columns::Columns};
use reifydb_rql::nodes::DropViewNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::Value;

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

	let dags = services.catalog.list_flow_dags_asc(&mut Transaction::Admin(txn))?;
	let flows = services.catalog.list_flows_all(&mut Transaction::Admin(txn))?;
	let own_flow_id = flows.iter().find(|f| f.namespace == def.namespace() && f.name == def.name()).map(|f| f.id);
	let external_dags: Vec<_> = if let Some(own_id) = own_flow_id {
		dags.iter().filter(|dag| dag.id() != own_id).cloned().collect()
	} else {
		dags
	};
	let dependents = find_flow_dependents(&services.catalog, txn, &external_dags, &flows, |node_type| {
		matches!(node_type, OperatorDef::SourceView { view } if *view == view_id)
			|| matches!(node_type, OperatorDef::SinkTableView { view, .. } | OperatorDef::SinkRingBufferView { view, .. } | OperatorDef::SinkSeriesView { view, .. } if *view == view_id)
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
