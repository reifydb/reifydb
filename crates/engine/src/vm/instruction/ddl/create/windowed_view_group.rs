// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_catalog::{
	catalog::{flow::FlowToCreate, view::ViewToCreate, windowed_view_group::WindowedViewGroupToCreate},
	error::{CatalogError, CatalogObjectKind},
	store::row_settings::create::create_row_settings,
};
use reifydb_core::{
	error::diagnostic::{catalog::view_already_exists, flow::flow_source_required},
	interface::catalog::{
		change::CatalogTrackViewChangeOperations, namespace::Namespace, storage::StorageId, view::View,
	},
	row::RowSettings,
	value::batch::single_row,
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_flow::compiler::compile_group_flow;
use reifydb_rql::{error::RqlError, nodes::CreateDeferredWindowedViewGroupNode, query::QueryPlan};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{error::Error, fragment::Fragment, return_error, value::Value};

use super::{
	check_flow_dag, count_windowed_nodes, extract_view_sort, prepare_flow_plan, split_at_windowed_node,
	view_storage,
};
use crate::{Result, vm::services::Services};

pub(crate) fn create_deferred_windowed_view_group(
	services: &Services,
	txn: &mut AdminTransaction,
	symbols: &SymbolTable,
	plan: CreateDeferredWindowedViewGroupNode,
) -> Result<RecordBatch> {
	reject_taken_group_name(services, txn, &plan.namespace, &plan.group)?;

	let mut member_plans: Vec<(QueryPlan, View)> = Vec::with_capacity(plan.members.len());
	let mut prefix: Option<QueryPlan> = None;
	for member in plan.members {
		if let Some(view) = services.catalog.find_view_by_name(
			&mut Transaction::Admin(txn),
			member.namespace.id(),
			member.view.text(),
		)? {
			return_error!(view_already_exists(member.view.clone(), member.namespace.name(), view.name()));
		}

		let mut as_clause = member.as_clause;
		prepare_flow_plan(&services.operators, symbols, &mut as_clause)?;
		if count_windowed_nodes(&mut as_clause) != 1 {
			return Err(RqlError::WindowedViewGroupRequiresOneWindow {
				fragment: plan.group.clone(),
			}
			.into());
		}
		let member_prefix = match split_at_windowed_node(&mut as_clause) {
			None => {
				return Err(RqlError::WindowedViewGroupWindowNotOnMainChain {
					fragment: plan.group.clone(),
				}
				.into());
			}
			Some(None) => return Err(Error(Box::new(flow_source_required()))),
			Some(Some(member_prefix)) => member_prefix,
		};
		if prefix.is_none() {
			prefix = Some(member_prefix);
		}

		let sort = extract_view_sort(&as_clause, &plan.columns);
		let view = services.catalog.create_deferred_view(
			txn,
			ViewToCreate {
				name: member.view.clone(),
				namespace: member.namespace.id(),
				columns: plan.columns.clone(),
				storage: view_storage(&plan.storage_kind),
				sort,
			},
		)?;
		txn.track_view_created(view.clone())?;

		if let Some(ttl) = &member.ttl {
			create_row_settings(
				txn,
				StorageId::View(view.id()),
				&RowSettings {
					ttl: Some(ttl.clone()),
					persistent: member.persistent,
				},
			)?;
			services.catalog.create_ttl_eviction(txn, StorageId::View(view.id()), member.ttl_eviction)?;
		}

		member_plans.push((as_clause, view));
	}

	let Some(prefix) = prefix else {
		return Err(RqlError::WindowedViewGroupNoMembers {
			fragment: plan.group.clone(),
		}
		.into());
	};

	let member_ids = member_plans.iter().map(|(_, view)| view.id()).collect::<Vec<_>>();
	let member_count = member_ids.len();
	let flow_id = services.catalog.next_flow_id(txn)?;
	let dag = compile_group_flow(&services.catalog, &services.routines, txn, prefix, member_plans, flow_id)?;
	check_flow_dag(&services.catalog, &services.operators, txn, &dag, &services.ioc)?;
	let flow = services.catalog.create_flow(
		txn,
		FlowToCreate {
			name: plan.group.clone(),
			namespace: plan.namespace.id(),
		},
		dag,
	)?;

	let group = services.catalog.create_windowed_view_group(
		txn,
		WindowedViewGroupToCreate {
			name: plan.group.clone(),
			namespace: plan.namespace.id(),
			flow: flow.id,
			members: member_ids,
		},
	)?;

	single_row([
		("id", Value::Uint8(group.id.0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("group", Value::Utf8(plan.group.text().to_string())),
		("flow", Value::Uint8(flow.id.0)),
		("members", Value::Uint8(member_count as u64)),
		("created", Value::Boolean(true)),
	])
}

fn reject_taken_group_name(
	services: &Services,
	txn: &mut AdminTransaction,
	namespace: &Namespace,
	group: &Fragment,
) -> Result<()> {
	let name = group.text();
	let taken = if services.catalog.find_view_by_name(&mut Transaction::Admin(txn), namespace.id(), name)?.is_some()
	{
		Some(CatalogObjectKind::View)
	} else if services
		.catalog
		.find_windowed_view_group_by_name(&mut Transaction::Admin(txn), namespace.id(), name)?
		.is_some()
	{
		Some(CatalogObjectKind::WindowedViewGroup)
	} else if services.catalog.find_flow_by_name(&mut Transaction::Admin(txn), namespace.id(), name)?.is_some() {
		Some(CatalogObjectKind::Flow)
	} else {
		None
	};
	match taken {
		Some(kind) => Err(CatalogError::AlreadyExists {
			kind,
			namespace: namespace.name().to_string(),
			name: name.to_string(),
			fragment: group.clone(),
		}
		.into()),
		None => Ok(()),
	}
}
