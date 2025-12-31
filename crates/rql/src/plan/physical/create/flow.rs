// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use PhysicalPlan::CreateFlow;
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::{Fragment, diagnostic::catalog::namespace_not_found, return_error};

use crate::plan::{
	logical,
	physical::{Compiler, CreateFlowNode, PhysicalPlan},
};

impl Compiler {
	pub(crate) async fn compile_create_flow<T: IntoStandardTransaction>(
		&self,
		rx: &mut T,
		create: logical::CreateFlowNode,
	) -> crate::Result<PhysicalPlan> {
		// Get namespace name from the MaybeQualified type
		let namespace_name = create.flow.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
		let Some(namespace) = self.catalog.find_namespace_by_name(rx, namespace_name).await? else {
			let ns_fragment = create
				.flow
				.namespace
				.clone()
				.unwrap_or_else(|| Fragment::internal("default".to_string()));
			return_error!(namespace_not_found(ns_fragment, namespace_name));
		};

		Ok(CreateFlow(CreateFlowNode {
			namespace,
			flow: create.flow.name.clone(), // Extract just the name Fragment
			if_not_exists: create.if_not_exists,
			as_clause: Box::pin(self.compile(rx, create.as_clause)).await?.map(Box::new).unwrap(), // FIXME
		}))
	}
}
