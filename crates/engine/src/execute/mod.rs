// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod query;

use crate::function::math;
use base::function::FunctionRegistry;
use base::{RowMeta, Row, RowIter};
use rql::plan::QueryPlan;
use transaction::Rx;
use crate::old_execute::ExecutionResult;
// #[derive(Debug)]
// pub enum ExecutionResult {
//     CreateSchema { schema: String },
//     CreateTable { schema: String, table: String },
//     InsertIntoTable { schema: String, table: String, inserted: usize },
//     Query { labels: Vec<Label>, rows: Vec<Row> },
// }

pub(crate) struct Executor {
    functions: FunctionRegistry,
    stream: Option<RowIter>,
}

pub fn execute(plan: QueryPlan, rx: &impl Rx) -> crate::Result<ExecutionResult> {
    let mut executor = Executor {
        functions: FunctionRegistry::new(), // FIXME receive functions from RX
        stream: None,
    };

    executor.functions.register(math::AbsFunction {});
    executor.functions.register(math::AvgFunction {});

    executor.execute(plan, rx)
}

impl Executor {
    pub(crate) fn execute<'a>(
        &mut self,
        plan: QueryPlan,
        rx: &'a impl Rx,
    ) -> crate::Result<ExecutionResult> {
        let next = match plan {
            QueryPlan::Aggregate { .. } => unimplemented!(),
            QueryPlan::Scan { .. } => unimplemented!(),
            QueryPlan::Project { expressions, next } => {
                self.project(expressions)?;
                next
            }
            QueryPlan::Sort { .. } => unimplemented!(),
            QueryPlan::Limit { .. } => unimplemented!(),
        };

        if let Some(next) = next {
            // crate::old_execute::execute_node(*next_node, rx, labels, schema, store, Some(result_iter))
            self.execute(*next, rx)
        } else {
            // Ok((labels, result_iter))
            Ok(ExecutionResult::Query { labels: vec![], rows: vec![] })
        }
    }
}
