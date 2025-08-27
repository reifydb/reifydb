// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use PhysicalPlan::CreateDeferredView;
use reifydb_catalog::CatalogStore;
use reifydb_core::{
	diagnostic::catalog::schema_not_found, interface::QueryTransaction,
	return_error,
};

use crate::plan::{
	logical::CreateDeferredViewNode,
	physical::{Compiler, CreateDeferredViewPlan, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_deferred(
		rx: &mut impl QueryTransaction,
		create: CreateDeferredViewNode,
	) -> crate::Result<PhysicalPlan> {
		let Some(schema) = CatalogStore::find_schema_by_name(
			rx,
			&create.schema.text(),
		)?
		else {
			return_error!(schema_not_found(
				Some(create.schema.clone()),
				&create.schema.text()
			));
		};

		Ok(CreateDeferredView(CreateDeferredViewPlan {
			schema,
			view: create.view,
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			with: Self::compile(rx, create.with)?
				.map(Box::new)
				.unwrap(), // FIXME
		}))
	}
}
