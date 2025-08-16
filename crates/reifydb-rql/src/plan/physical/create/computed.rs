// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use PhysicalPlan::CreateComputedView;
use reifydb_catalog::Catalog;
use reifydb_core::{
	diagnostic::catalog::schema_not_found,
	interface::VersionedQueryTransaction, return_error,
};

use crate::plan::{
	logical::CreateComputedViewNode,
	physical::{Compiler, CreateComputedViewPlan, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_computed(
		rx: &mut impl VersionedQueryTransaction,
		create: CreateComputedViewNode,
	) -> crate::Result<PhysicalPlan> {
		let Some(schema) = Catalog::get_schema_by_name(
			rx,
			&create.schema.fragment,
		)?
		else {
			return_error!(schema_not_found(
				Some(create.schema.clone()),
				&create.schema.fragment
			));
		};

		Ok(CreateComputedView(CreateComputedViewPlan {
			schema,
			view: create.view,
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			with: Self::compile(rx, create.with)?.map(Box::new),
		}))
	}
}
