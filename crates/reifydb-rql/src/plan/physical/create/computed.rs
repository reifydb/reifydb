// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use PhysicalPlan::CreateComputedView;
use reifydb_core::interface::VersionedQueryTransaction;

use crate::plan::{
	logical::CreateComputedViewNode,
	physical::{Compiler, CreateComputedViewPlan, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_computed(
		rx: &mut impl VersionedQueryTransaction,
		create: CreateComputedViewNode,
	) -> crate::Result<PhysicalPlan> {
		// FIXME validate with catalog
		Ok(CreateComputedView(CreateComputedViewPlan {
			schema: create.schema,
			view: create.view,
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			with: Self::compile(rx, create.with)?.map(Box::new),
		}))
	}
}
