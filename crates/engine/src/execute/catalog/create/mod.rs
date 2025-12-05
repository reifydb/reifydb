// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::ViewDef;
use reifydb_rql::{flow::compile_flow, plan::physical::PhysicalPlan};

use crate::{StandardCommandTransaction, execute::Executor};

#[allow(dead_code)] // FIXME
mod deferred;
mod dictionary;
mod flow;
mod namespace;
mod ring_buffer;
mod table;
#[allow(dead_code)] // FIXME
mod transactional;

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
		use reifydb_catalog::{CatalogStore, store::flow::create::FlowToCreate};
		use reifydb_core::interface::FlowStatus;

		let flow_def = CatalogStore::create_flow(
			txn,
			FlowToCreate {
				fragment: None,
				name: view.name.to_string(),
				namespace: view.namespace,
				status: FlowStatus::Active,
			},
		)?;

		// Compile flow with the obtained FlowId - nodes and edges are persisted by the compiler
		let _flow = compile_flow(txn, *plan, Some(view), flow_def.id)?;
		Ok(())
	}
}
