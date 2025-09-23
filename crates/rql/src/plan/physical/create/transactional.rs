// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use PhysicalPlan::CreateTransactionalView;
use reifydb_catalog::CatalogStore;
use reifydb_core::interface::QueryTransaction;
use reifydb_type::{diagnostic::catalog::namespace_not_found, return_error};

use crate::plan::{
	logical,
	physical::{Compiler, CreateTransactionalViewNode, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_transactional<'a>(
		rx: &mut impl QueryTransaction,
		create: logical::CreateTransactionalViewNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		let Some(namespace) = CatalogStore::find_namespace_by_name(rx, create.view.namespace.text())? else {
			return_error!(namespace_not_found(create.view.namespace.clone(), create.view.namespace.text()));
		};

		Ok(CreateTransactionalView(CreateTransactionalViewNode {
			namespace,
			view: create.view.name.clone(), // Extract just the name Fragment
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			with: Self::compile(rx, create.with)?.map(Box::new).unwrap(), // FIXME
		}))
	}
}
