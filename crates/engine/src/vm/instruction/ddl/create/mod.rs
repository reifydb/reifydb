// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::{Catalog, flow::FlowToCreate};
use reifydb_core::interface::catalog::{flow::FlowStatus, view::View};
use reifydb_rql::query::QueryPlan;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::{fragment::Fragment, value::duration::Duration};

use crate::{Result, flow::compiler::compile_flow};

pub mod authentication;
pub mod binding;
pub mod deferred;
pub mod dictionary;
pub mod event;

pub mod identity;
pub mod migration;
pub mod namespace;
pub mod policy;
pub mod primary_key;
pub mod procedure;
pub mod property;
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

/// Creates a flow for a deferred view.
///
/// The flow entry is created first to obtain a FlowId, then the flow nodes
/// and edges are compiled and persisted with that same FlowId.
pub(crate) fn create_deferred_view_flow(
	catalog: &Catalog,
	txn: &mut AdminTransaction,
	view: &View,
	plan: QueryPlan,
	tick: Option<Duration>,
) -> Result<()> {
	let flow = catalog.create_flow(
		txn,
		FlowToCreate {
			name: Fragment::internal(view.name()),
			namespace: view.namespace(),
			status: FlowStatus::Active,
			tick,
		},
	)?;

	let _flow = compile_flow(catalog, txn, plan, Some(view), flow.id)?;
	Ok(())
}
