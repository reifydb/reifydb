// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::plan::logical::CreateComputedViewNode;
use crate::plan::physical::{Compiler, CreateComputedViewPlan, PhysicalPlan};
use reifydb_core::interface::VersionedQueryTransaction;

impl Compiler {
    pub(crate) fn compile_create_computed(
        _rx: &mut impl VersionedQueryTransaction,
        create: CreateComputedViewNode,
    ) -> crate::Result<PhysicalPlan> {
        // FIXME validate with catalog
        Ok(PhysicalPlan::CreateComputedView(CreateComputedViewPlan {
            schema: create.schema,
            view: create.view,
            if_not_exists: create.if_not_exists,
            columns: create.columns,
        }))
    }
}
