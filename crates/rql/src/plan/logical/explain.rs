// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::parse;
use crate::plan::logical::{
    AggregateNode, FilterNode, JoinLeftNode, LimitNode, LogicalQueryPlan, OrderNode, ProjectNode,
    TableScanNode, compile_logical,
};
use reifydb_core::Error;

pub(crate) fn explain_logical_plan(query: &str) -> Result<String, Error> {
    let statements = parse(query).unwrap();

    let mut plans = Vec::new();
    for statement in statements {
        plans.extend(compile_logical(statement).unwrap())
    }

    let mut result = String::new();
    for plan in plans {
        let mut output = String::new();
        render_logical_plan_inner(&plan, "", true, &mut output);
        result += output.as_str();
    }

    Ok(result)
}

fn render_logical_plan_inner(
    plan: &LogicalQueryPlan,
    prefix: &str,
    is_last: bool,
    output: &mut String,
) {
    let branch = if is_last { "└──" } else { "├──" };
    let child_prefix = format!("{}{}", prefix, if is_last { "    " } else { "│   " });

    match plan {
        LogicalQueryPlan::Limit(LimitNode { limit }) => {
            output.push_str(&format!("{}{} Limit {}\n", prefix, branch, limit));
        }
        LogicalQueryPlan::Filter(FilterNode { filter }) => {
            output.push_str(&format!("{}{} Filter\n", prefix, branch));
            for (i, cond) in filter.iter().enumerate() {
                let cond_last = i == filter.len() - 1;
                output.push_str(&format!(
                    "{}{} condition: {}\n",
                    child_prefix,
                    if cond_last { "└──" } else { "├──" },
                    cond.to_string()
                ));
            }
        }
        LogicalQueryPlan::Project(ProjectNode { project }) => {
            output.push_str(&format!("{}{} Project\n", prefix, branch));
            for (i, expr) in project.iter().enumerate() {
                let last = i == project.len() - 1;
                output.push_str(&format!(
                    "{}{} column: {}\n",
                    child_prefix,
                    if last { "└──" } else { "├──" },
                    expr.to_string()
                ));
            }
        }
        LogicalQueryPlan::Aggregate(AggregateNode { by, project }) => {
            output.push_str(&format!("{}{} Aggregate\n", prefix, branch));
            if !by.is_empty() {
                output.push_str(&format!(
                    "{}├── by: {}\n",
                    child_prefix,
                    by.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
                ));
            }
            if !project.is_empty() {
                output.push_str(&format!(
                    "{}└── project: {}\n",
                    child_prefix,
                    project.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
                ));
            }
        }
        LogicalQueryPlan::Order(OrderNode { by }) => {
            output.push_str(&format!("{}{} Order\n", prefix, branch));
            for (i, key) in by.iter().enumerate() {
                let last = i == by.len() - 1;
                output.push_str(&format!(
                    "{}{} by: {}\n",
                    child_prefix,
                    if last { "└──" } else { "├──" },
                    key.to_string()
                ));
            }
        }
        LogicalQueryPlan::JoinLeft(JoinLeftNode { on }) => {
            output.push_str(&format!("{}{} JoinLeft\n", prefix, branch));
            for (i, cond) in on.iter().enumerate() {
                let last = i == on.len() - 1;
                output.push_str(&format!(
                    "{}{} on: {}\n",
                    child_prefix,
                    if last { "└──" } else { "├──" },
                    cond.to_string()
                ));
            }
        }
        LogicalQueryPlan::TableScan(TableScanNode { schema, table }) => {
            let name = match schema {
                Some(s) => format!("{}.{}", s.fragment, table.fragment),
                None => table.fragment.to_string(),
            };
            output.push_str(&format!("{}{} TableScan {}\n", prefix, branch, name));
        }
    }
}
