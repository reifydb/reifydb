// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::{Catalog, flow::FlowToCreate};
use reifydb_core::interface::catalog::{
	flow::{FlowId, FlowStatus},
	subscription::{Subscription, subscription_flow_name, subscription_flow_namespace},
	view::View,
};
use reifydb_rql::query::QueryPlan;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::{fragment::Fragment, value::duration::Duration};

use crate::{
	Result,
	flow::compiler::{compile_flow, compile_subscription_flow},
};

pub mod authentication;
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

/// Creates a flow for a subscription.
///
/// Since SubscriptionId == FlowId for subscription flows, we use the subscription ID
/// directly as the flow ID, avoiding the O(n) find_flow_by_name check.
pub(crate) fn create_subscription_flow(
	catalog: &Catalog,
	txn: &mut AdminTransaction,
	subscription: &Subscription,
	plan: QueryPlan,
) -> Result<()> {
	// FlowId == SubscriptionId for subscription flows
	let flow_id = FlowId(subscription.id.0);
	let flow = catalog.create_flow_with_id(
		txn,
		flow_id,
		FlowToCreate {
			name: Fragment::internal(subscription_flow_name(subscription.id)),
			namespace: subscription_flow_namespace(),
			status: FlowStatus::Active,
			tick: None,
		},
	)?;

	let _flow = compile_subscription_flow(catalog, txn, plan, subscription, flow.id)?;
	Ok(())
}
