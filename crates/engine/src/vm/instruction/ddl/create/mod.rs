// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::{Catalog, flow::FlowToCreate, view::ViewColumnToCreate};
use reifydb_core::{
	error::diagnostic::catalog::persistent_requires_buffer,
	interface::catalog::{
		column::ColumnIndex,
		flow::FlowStatus,
		view::{View, ViewSortKey},
	},
	sort::SortKey,
};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::query::QueryPlan;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::{fragment::Fragment, return_error};

use crate::{Result, flow::compiler::compile_flow};

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

pub(crate) fn require_buffer_for_non_persistent(
	txn: &AdminTransaction,
	persistent: bool,
	fragment: Fragment,
	shape: &str,
) -> Result<()> {
	if !persistent && !txn.multi.has_buffer() {
		return_error!(persistent_requires_buffer(fragment, shape));
	}
	Ok(())
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

	let _flow = compile_flow(catalog, routines, txn, plan, Some(view), flow.id)?;
	Ok(())
}
