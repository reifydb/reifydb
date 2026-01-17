// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::flow::FlowToCreate;
use reifydb_core::interface::catalog::{
	flow::FlowStatus,
	subscription::{SubscriptionDef, subscription_flow_name, subscription_flow_namespace},
	view::ViewDef,
};
use reifydb_rql::plan::physical::PhysicalPlan;
use reifydb_transaction::standard::command::StandardCommandTransaction;

use crate::{
	execute::Executor,
	flow::compiler::{compile_flow, compile_subscription_flow},
};

#[allow(dead_code)] // FIXME
pub mod deferred;
pub mod dictionary;
pub mod flow;
pub mod namespace;
pub mod ringbuffer;
pub mod subscription;
pub mod table;
#[allow(dead_code)] // FIXME
pub mod transactional;

impl Executor {
	/// Creates a flow for a deferred view.
	///
	/// The flow entry is created first to obtain a FlowId, then the flow nodes
	/// and edges are compiled and persisted with that same FlowId.
	pub(crate) fn create_deferred_view_flow(
		&self,
		txn: &mut StandardCommandTransaction,
		view: &ViewDef,
		plan: Box<PhysicalPlan>,
	) -> crate::Result<()> {
		let flow_def = self.catalog.create_flow(
			txn,
			FlowToCreate {
				fragment: None,
				name: view.name.to_string(),
				namespace: view.namespace,
				status: FlowStatus::Active,
			},
		)?;

		let _flow = compile_flow(&self.catalog, txn, *plan, Some(view), flow_def.id)?;
		Ok(())
	}

	/// Creates a flow for a subscription.
	///
	/// The flow entry is created first to obtain a FlowId, then the flow nodes
	/// and edges are compiled and persisted with that same FlowId.
	pub(crate) fn create_subscription_flow(
		&self,
		txn: &mut StandardCommandTransaction,
		subscription: &SubscriptionDef,
		plan: PhysicalPlan,
	) -> crate::Result<()> {
		let flow_def = self.catalog.create_flow(
			txn,
			FlowToCreate {
				fragment: None,
				name: subscription_flow_name(subscription.id),
				namespace: subscription_flow_namespace(),
				status: FlowStatus::Active,
			},
		)?;

		let _flow = compile_subscription_flow(&self.catalog, txn, plan, subscription, flow_def.id)?;
		Ok(())
	}
}
