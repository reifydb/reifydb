// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::lazy::Source;
use crate::frame::{Frame, LazyFrame};
use reifydb_rql::plan::QueryPlan;

impl LazyFrame {
    pub fn compile(plan: QueryPlan) -> Self {
        Self::compile_internal(plan)
    }

    fn compile_internal(plan: QueryPlan) -> LazyFrame {
        match plan {
            QueryPlan::ScanTable { schema, table, next } => {
                let frame = LazyFrame {
                    source: Source::Table { schema, table },
                    frame: Frame::empty(),
                    expressions: vec![],
                    filter: vec![],
                    limit: None,
                };
                if let Some(next) = next { Self::apply(next, frame) } else { frame }
            }

            QueryPlan::Project { expressions, next } => {
                let frame = LazyFrame {
                    source: Source::None,
                    frame: Frame::empty(),
                    expressions,
                    filter: vec![],
                    limit: None,
                };
                if let Some(next) = next { Self::apply(next, frame) } else { frame }
            }
            _ => unimplemented!(),
        }
    }

    fn apply(plan: Box<QueryPlan>, mut frame: LazyFrame) -> LazyFrame {
        match *plan {
            QueryPlan::Filter { expression, next } => {
                frame.filter.push(expression);
                if let Some(next) = next { Self::apply(next, frame) } else { frame }
            }

            QueryPlan::Project { expressions, next } => {
                frame.expressions = expressions;
                if let Some(next) = next { Self::apply(next, frame) } else { frame }
            }

            QueryPlan::Limit { limit, next } => {
                frame.limit = Some(limit);
                if let Some(next) = next { Self::apply(next, frame) } else { frame }
            }

            QueryPlan::Sort { keys, next } => {
                unimplemented!()
            }

            QueryPlan::Aggregate { group_by, project, next } => {
                unimplemented!()
            }

            QueryPlan::ScanTable { .. } => {
                unreachable!()
            }
        }
    }
}
