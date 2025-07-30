// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::frame::Frame;
use crate::execute::Executor;
use reifydb_core::interface::{Tx, UnversionedStorage, VersionedStorage};
use reifydb_rql::plan::physical::CreateComputedViewPlan;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn create_computed_view(
        &mut self,
        _tx: &mut impl Tx<VS, US>,
        _plan: CreateComputedViewPlan,
    ) -> crate::Result<Frame> {
        // if plan.if_not_exists {
        //     unimplemented!()
        // } else {
        //     tx.dep_schema_mut(&plan.schema)?.create(StoreToCreate::ComputedView {
        //         view: plan.view.clone(),
        //         columns: plan.columns,
        //     })?;
        // }
        //
        // Ok(ExecutionResult::CreateComputedView { schema: plan.schema, view: plan.view.clone() })
        unimplemented!()
    }
}
