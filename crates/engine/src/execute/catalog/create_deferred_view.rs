// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Executor;
use crate::frame::Frame;
use reifydb_core::interface::{Tx, UnversionedStorage, VersionedStorage};
use reifydb_rql::plan::physical::CreateDeferredViewPlan;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn create_deferred_view(
        &mut self,
        _tx: &mut impl Tx<VS, US>,
        _plan: CreateDeferredViewPlan,
    ) -> crate::Result<Frame> {
        // if plan.if_not_exists {
        //     unimplemented!()
        // } else {
        //     tx.dep_schema_mut(&plan.schema)?.create(StoreToCreate::DeferredView {
        //         view: plan.view.clone(),
        //         columns: plan.columns,
        //     })?;
        // }
        //
        // Ok(ExecutionResult::CreateDeferredView { schema: plan.schema, view: plan.view.clone() })
        unimplemented!()
    }
}
