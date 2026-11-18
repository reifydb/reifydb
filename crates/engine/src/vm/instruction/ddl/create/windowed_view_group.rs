// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_catalog::{
	catalog::{
		flow::FlowToCreate,
		name::{NameAllowed, QualifiedName},
		namespace::NamespaceToCreate,
		view::ViewToCreate,
		windowed_view_group::WindowedViewGroupToCreate,
	},
	error::{CatalogError, CatalogObjectKind, NameKind},
	store::row_settings::create::create_row_settings,
};
use reifydb_core::{
	error::diagnostic::{catalog::view_already_exists, flow::flow_source_required},
	interface::catalog::{
		change::CatalogTrackViewChangeOperations, namespace::Namespace, storage::StorageId, view::View,
	},
	internal,
	row::RowSettings,
	value::batch::single_row,
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_flow::compiler::compile_group_flow;
use reifydb_rql::{error::RqlError, nodes::CreateDeferredWindowedViewGroupNode, query::QueryPlan};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{error, error::ValueError, return_error, value::Value};

use super::{
	check_flow_dag, count_windowed_nodes, extract_view_sort, prepare_flow_plan, split_at_windowed_node,
	view_storage,
};
use crate::vm::services::Services;

pub(crate) fn create_deferred_windowed_view_group(
	services: &Services,
	txn: &mut AdminTransaction,
	symbols: &SymbolTable,
	plan: CreateDeferredWindowedViewGroupNode,
) -> Result<RecordBatch, ValueError> {
	if services
		.catalog
		.find_windowed_view_group_by_name(&mut Transaction::Admin(txn), plan.namespace.id(), plan.group.text())?
		.is_some()
	{
		return Err(CatalogError::AlreadyExists {
			kind: CatalogObjectKind::WindowedViewGroup,
			namespace: plan.namespace.name().to_string(),
			name: plan.group.text().to_string(),
			fragment: plan.group.clone(),
		}
		.into());
	}
	services.catalog.reject_name_taken(
		&mut Transaction::Admin(txn),
		QualifiedName {
			namespace: plan.namespace.id(),
			name: &plan.group,
		},
		NameKind::WindowedViewGroup,
	)?;
	let own: Option<Namespace> = if plan.members.iter().any(|member| member.namespace.is_none()) {
		Some(services.catalog.create_namespace(
			txn,
			NamespaceToCreate {
				namespace_fragment: None,
				name: format!("{}::{}", plan.namespace.name(), plan.group.text()),
				local_name: plan.group.text().to_string(),
				parent_id: plan.namespace.id(),
				grpc: None,
				token: None,
			},
		)?)
	} else {
		None
	};

	let mut member_plans: Vec<(QueryPlan, View)> = Vec::with_capacity(plan.members.len());
	let mut prefix: Option<QueryPlan> = None;
	for member in plan.members {
		let Some(member_ns) = member.namespace.as_ref().or(own.as_ref()) else {
			return Err(error!(internal!("windowed view group member has no namespace")));
		};
		if let Some(view) = services.catalog.find_view_by_name(
			&mut Transaction::Admin(txn),
			member_ns.id(),
			member.view.text(),
		)? {
			return_error!(view_already_exists(member.view.clone(), member_ns.name(), view.name()));
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
			Some(None) => return Err(ValueError(Box::new(flow_source_required()))),
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
				namespace: member_ns.id(),
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
			allowed: NameAllowed {
				namespace: own.as_ref().map(|ns| ns.id()),
				..NameAllowed::default()
			},
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
