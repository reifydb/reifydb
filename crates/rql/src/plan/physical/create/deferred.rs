// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use PhysicalPlan::CreateDeferredView;
use reifydb_catalog::CatalogStore;
use reifydb_core::interface::QueryTransaction;
use reifydb_type::{diagnostic::catalog::namespace_not_found, return_error};

use crate::plan::{
	logical,
	physical::{Compiler, CreateDeferredViewNode, PhysicalPlan},
};

impl Compiler {
	pub(crate) async fn compile_create_deferred(
		rx: &mut impl QueryTransaction,
		create: logical::CreateDeferredViewNode,
	) -> crate::Result<PhysicalPlan> {
		// Get namespace name from the MaybeQualified type
		let namespace_name = create.view.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
		let Some(namespace) = CatalogStore::find_namespace_by_name(rx, namespace_name).await? else {
			let ns_fragment = create.view.namespace.clone().unwrap_or_else(|| {
				use reifydb_type::Fragment;
				Fragment::internal("default".to_string())
			});
			return_error!(namespace_not_found(ns_fragment, namespace_name));
		};

		Ok(CreateDeferredView(CreateDeferredViewNode {
			namespace,
			view: create.view.name.clone(), // Extract just the name Fragment
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			as_clause: Self::compile(rx, create.as_clause).await?.map(Box::new).unwrap(), // FIXME
			primary_key: create.primary_key,
		}))
	}
}
