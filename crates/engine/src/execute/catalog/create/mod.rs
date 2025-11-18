// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::ViewDef;
use reifydb_rql::{flow::compile_flow, plan::physical::PhysicalPlan};

use crate::{StandardCommandTransaction, execute::Executor};

#[allow(dead_code)] // FIXME
mod deferred;
mod flow;
mod namespace;
mod ring_buffer;
mod table;
#[allow(dead_code)] // FIXME
mod transactional;

impl Executor {
	// FIXME: This creates the internal flow representation for deferred views
	// TODO: Fix flow_id mismatch - compile_flow generates one ID, create_flow generates another
	pub(crate) fn create_deferred_view_flow(
		&self,
		txn: &mut StandardCommandTransaction,
		view: &ViewDef,
		plan: Box<PhysicalPlan>,
	) -> crate::Result<()> {
		use reifydb_catalog::{CatalogStore, store::flow::create::FlowToCreate};
		use reifydb_core::interface::FlowStatus;

		// Compile flow - nodes and edges are persisted by the compiler
		let _flow = compile_flow(txn, *plan, Some(view))?;

		// Create the flow entry in the catalog
		// Use the view name with "_flow" suffix as the flow name
		CatalogStore::create_flow(
			txn,
			FlowToCreate {
				fragment: None,
				name: format!("{}_flow", view.name),
				namespace: view.namespace,
				status: FlowStatus::Active,
			},
		)?;

		Ok(())
	}
}
