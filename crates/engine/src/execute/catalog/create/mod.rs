// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::{CatalogStore, store::flow::create::FlowToCreate};
use reifydb_core::interface::{
	FlowStatus, SubscriptionDef, ViewDef, subscription_flow_name, subscription_flow_namespace,
};
use reifydb_rql::plan::physical::PhysicalPlan;

use crate::{
	StandardCommandTransaction,
	execute::Executor,
	flow::{compile_flow, compile_subscription_flow},
};

#[allow(dead_code)] // FIXME
mod deferred;
mod dictionary;
mod flow;
mod namespace;
mod ringbuffer;
mod subscription;
mod table;
#[allow(dead_code)] // FIXME
mod transactional;

impl Executor {
	/// Creates a flow for a deferred view.
	///
	/// The flow entry is created first to obtain a FlowId, then the flow nodes
	/// and edges are compiled and persisted with that same FlowId.
	pub(crate) async fn create_deferred_view_flow(
		&self,
		txn: &mut StandardCommandTransaction,
		view: &ViewDef,
		plan: Box<PhysicalPlan>,
	) -> crate::Result<()> {
		let flow_def = CatalogStore::create_flow(
			txn,
			FlowToCreate {
				fragment: None,
				name: view.name.to_string(),
				namespace: view.namespace,
				status: FlowStatus::Active,
			},
		)
		.await?;

		let _flow = compile_flow(txn, *plan, Some(view), flow_def.id).await?;
		Ok(())
	}

	/// Creates a flow for a subscription.
	///
	/// The flow entry is created first to obtain a FlowId, then the flow nodes
	/// and edges are compiled and persisted with that same FlowId.
	pub(crate) async fn create_subscription_flow(
		&self,
		txn: &mut StandardCommandTransaction,
		subscription: &SubscriptionDef,
		plan: PhysicalPlan,
	) -> crate::Result<()> {
		let flow_def = CatalogStore::create_flow(
			txn,
			FlowToCreate {
				fragment: None,
				name: subscription_flow_name(subscription.id),
				namespace: subscription_flow_namespace(),
				status: FlowStatus::Active,
			},
		)
		.await?;

		let _flow = compile_subscription_flow(txn, plan, subscription, flow_def.id).await?;
		Ok(())
	}
}
