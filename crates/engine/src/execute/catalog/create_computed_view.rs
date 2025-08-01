// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::columns::Columns;
use crate::execute::Executor;
use reifydb_core::interface::{
    ActiveWriteTransaction, UnversionedTransaction, VersionedTransaction,
};
use reifydb_rql::plan::physical::CreateComputedViewPlan;

impl<VT: VersionedTransaction, UT: UnversionedTransaction> Executor<VT, UT> {
    pub(crate) fn create_computed_view(
        &mut self,
        _atx: &mut ActiveWriteTransaction<VT, UT>,
        _plan: CreateComputedViewPlan,
    ) -> crate::Result<Columns> {
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
