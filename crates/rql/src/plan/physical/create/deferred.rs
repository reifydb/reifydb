// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use PhysicalPlan::CreateDeferredView;
use reifydb_transaction::standard::IntoStandardTransaction;
use reifydb_core::error::diagnostic::catalog::namespace_not_found;
use reifydb_type::{fragment::Fragment, return_error};

use crate::plan::{
	logical,
	physical::{Compiler, CreateDeferredViewNode, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_deferred<T: IntoStandardTransaction>(
		&self,
		rx: &mut T,
		create: logical::CreateDeferredViewNode,
	) -> crate::Result<PhysicalPlan> {
		// Get namespace name from the MaybeQualified type
		let namespace_name = create.view.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
		let Some(namespace) = self.catalog.find_namespace_by_name(rx, namespace_name)? else {
			let ns_fragment = create
				.view
				.namespace
				.clone()
				.unwrap_or_else(|| Fragment::internal("default".to_string()));
			return_error!(namespace_not_found(ns_fragment, namespace_name));
		};

		Ok(CreateDeferredView(CreateDeferredViewNode {
			namespace,
			view: create.view.name.clone(), // Extract just the name Fragment
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			as_clause: self.compile(rx, create.as_clause)?.map(Box::new).unwrap(), // FIXME
			primary_key: create.primary_key,
		}))
	}
}
