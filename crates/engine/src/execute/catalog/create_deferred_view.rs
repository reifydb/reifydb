// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ExecutionResult;
use crate::execute::Executor;
use reifydb_rql::plan::CreateDeferredViewPlan;
use reifydb_transaction::Tx;

impl Executor {
    pub(crate) fn create_deferred_view(
        &mut self,
        tx: &mut impl Tx,
        plan: CreateDeferredViewPlan,
    ) -> crate::Result<ExecutionResult> {
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
