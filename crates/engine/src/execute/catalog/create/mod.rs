// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::ViewDef;
use reifydb_rql::plan::physical::PhysicalPlan;

use crate::{StandardCommandTransaction, execute::Executor, flow::compile_flow};

#[allow(dead_code)] // FIXME
mod deferred;
mod dictionary;
mod flow;
mod namespace;
mod ringbuffer;
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
		use reifydb_catalog::{CatalogStore, store::flow::create::FlowToCreate};
		use reifydb_core::interface::FlowStatus;

		println!("[create_deferred_view_flow] Creating flow for view: {}", view.name);

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

		println!("[create_deferred_view_flow] Created flow with ID: {}", flow_def.id.0);

		// Compile flow with the obtained FlowId - nodes and edges are persisted by the compiler
		let _flow = compile_flow(txn, *plan, Some(view), flow_def.id).await?;

		println!("[create_deferred_view_flow] Compiled flow successfully");
		Ok(())
	}
}
