// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use PhysicalPlan::CreateDeferredView;
use reifydb_catalog::CatalogStore;
use reifydb_core::interface::QueryTransaction;
use reifydb_type::{diagnostic::catalog::namespace_not_found, return_error};

use crate::plan::{
	logical,
	physical::{Compiler, CreateDeferredViewNode, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_deferred<'a>(
		rx: &mut impl QueryTransaction,
		create: logical::CreateDeferredViewNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		// Get namespace name from the MaybeQualified type
		let namespace_name = create.view.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
		let Some(namespace) = CatalogStore::find_namespace_by_name(rx, namespace_name)? else {
			let ns_fragment = create.view.namespace.clone().unwrap_or_else(|| {
				use reifydb_type::Fragment;
				Fragment::owned_internal("default".to_string())
			});
			return_error!(namespace_not_found(ns_fragment, namespace_name));
		};

		Ok(CreateDeferredView(CreateDeferredViewNode {
			namespace,
			view: create.view.name.clone(), // Extract just the name Fragment
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			as_clause: Self::compile(rx, create.as_clause)?.map(Box::new).unwrap(), // FIXME
			primary_key: create.primary_key,
		}))
	}
}
