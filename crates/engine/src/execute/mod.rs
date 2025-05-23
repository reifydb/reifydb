// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod query;

use crate::function::{FunctionRegistry, math};
use crate::old_execute::ExecutionResult;
use dataframe::DataFrame;
use rql::plan::QueryPlan;
use transaction::Rx;

pub(crate) struct Executor {
    functions: FunctionRegistry,
    frame: DataFrame,
}

pub fn execute(plan: QueryPlan, rx: &impl Rx) -> crate::Result<ExecutionResult> {
    let mut executor = Executor {
        functions: FunctionRegistry::new(), // FIXME receive functions from RX
        frame: DataFrame::new(vec![]),
    };

    executor.functions.register(math::AbsFunction {});
    executor.functions.register(math::AvgFunction {});

    executor.execute(plan, rx)
}

impl Executor {
    pub(crate) fn execute(
        mut self,
        plan: QueryPlan,
        rx: &impl Rx,
    ) -> crate::Result<ExecutionResult> {
        let next = match plan {
            QueryPlan::Aggregate { group_by, project, next } => {
                self.aggregate(&group_by, &project)?;
                next
            }
            QueryPlan::Scan { schema, store, next } => {
                self.scan(rx, &schema, &store)?;
                next
            }
            QueryPlan::Project { expressions, next } => {
                self.project(expressions)?;
                next
            }
            QueryPlan::Sort { keys, next } => {
                self.sort(&keys)?;
                next
            }
            QueryPlan::Limit { limit, next } => {
                self.limit(limit)?;
                next
            }
        };

        if let Some(next) = next { self.execute(*next, rx) } else { Ok(self.frame.into()) }
    }
}
