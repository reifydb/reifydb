// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::{Catalog, flow::FlowToCreate, view::ViewColumnToCreate};
use reifydb_core::{
	interface::catalog::{
		column::ColumnIndex,
		flow::FlowStatus,
		view::{View, ViewSortKey},
	},
	sort::SortKey,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::query::QueryPlan;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::fragment::Fragment;

use crate::{
	Result,
	flow::{compiler::compile_flow, span::check_declared_spans, time_domain::check_window_time_requirements},
};

fn outermost_sort(plan: &QueryPlan) -> Option<&Vec<SortKey>> {
	match plan {
		QueryPlan::Sort(node) => Some(&node.by),
		QueryPlan::Map(node) => node.input.as_deref().and_then(outermost_sort),
		QueryPlan::Extend(node) => node.input.as_deref().and_then(outermost_sort),
		QueryPlan::Filter(node) => outermost_sort(&node.input),
		QueryPlan::Take(node) => outermost_sort(&node.input),
		QueryPlan::Distinct(node) => outermost_sort(&node.input),
		_ => None,
	}
}

pub(crate) fn extract_view_sort(as_clause: &QueryPlan, columns: &[ViewColumnToCreate]) -> Vec<ViewSortKey> {
	let Some(by) = outermost_sort(as_clause) else {
		return Vec::new();
	};

	let mut resolved = Vec::with_capacity(by.len());
	for key in by {
		let Some(position) = columns.iter().position(|c| c.name.text() == key.column.text()) else {
			return Vec::new();
		};
		resolved.push(ViewSortKey {
			column: ColumnIndex(position as u8),
			direction: key.direction.clone(),
		});
	}
	resolved
}

pub mod authentication;
pub mod binding;
pub mod deferred;
pub mod dictionary;
pub mod event;

pub mod identity;
pub mod identity_attribute;
pub mod migration;
pub mod namespace;
pub mod policy;
pub mod primary_key;
pub mod procedure;
pub mod property;
pub mod queue;
pub mod relationship;
pub mod remote_namespace;
pub mod ringbuffer;
pub mod role;
pub mod series;
pub mod sink;
pub mod source;
pub mod subscription;
pub mod sumtype;
pub mod table;
pub mod tag;
pub mod test;
pub mod transactional;

pub(crate) fn create_deferred_view_flow(
	catalog: &Catalog,
	routines: &Routines,
	txn: &mut AdminTransaction,
	view: &View,
	plan: QueryPlan,
) -> Result<()> {
	let flow = catalog.create_flow(
		txn,
		FlowToCreate {
			name: Fragment::internal(view.name()),
			namespace: view.namespace(),
			status: FlowStatus::Active,
		},
	)?;

	let dag = compile_flow(catalog, routines, txn, plan, Some(view), flow.id)?;
	check_window_time_requirements(catalog, &mut Transaction::Admin(txn), &dag)?;
	check_declared_spans(catalog, &mut Transaction::Admin(txn), &dag)
}
