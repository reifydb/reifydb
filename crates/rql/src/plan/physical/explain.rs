// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::parse;
use crate::plan::logical::compile_logical;
use crate::plan::physical::{
    AggregateNode, FilterNode, JoinLeftNode, LimitNode, OrderNode, PhysicalQueryPlan, SelectNode,
    TableScanNode, compile_physical,
};
use reifydb_core::Error;
use std::fmt::Write;

pub(crate) fn explain_physical_plan(query: &str) -> Result<String, Error> {
    let statements = parse(query).unwrap(); // FIXME

    let mut plans = Vec::new();
    for statement in statements {
        let logical = compile_logical(statement).unwrap(); // FIXME
        plans.extend(compile_physical(logical))
    }

    let mut result = String::new();
    for plan in plans {
        let mut output = String::new();
        render_physical_plan_inner(&plan, "", true, &mut output);
        result += output.as_str();
    }

    Ok(result)
}

fn render_with_child<F: FnOnce(&str)>(
    output: &mut String,
    prefix: &str,
    is_last: bool,
    label: &str,
    child_fn: F,
) {
    let branch = if is_last { "└──" } else { "├──" };
    writeln!(output, "{}{} {}", prefix, branch, label).unwrap();

    let child_prefix = format!("{}{}", prefix, if is_last { "    " } else { "│   " });
    child_fn(&child_prefix);
}

/// Write the current node line
fn write_node_header(output: &mut String, prefix: &str, is_last: bool, label: &str) {
    let branch = if is_last { "└──" } else { "├──" };
    writeln!(output, "{}{} {}", prefix, branch, label).unwrap();
}

/// Compute prefix for child nodes
fn with_child_prefix<F: FnOnce(&str)>(prefix: &str, is_last: bool, f: F) {
    let child_prefix = format!("{}{}", prefix, if is_last { "    " } else { "│   " });
    f(&child_prefix);
}


fn render_physical_plan_inner(
    plan: &PhysicalQueryPlan,
    prefix: &str,
    is_last: bool,
    output: &mut String,
) {
    match plan {
        PhysicalQueryPlan::Aggregate(AggregateNode { by, select, next }) => {
            let label = format!(
                "Aggregate by: [{}], select: [{}]",
                by.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", "),
                select.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
            );

            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                if let Some(child) = next.as_deref() {
                    render_physical_plan_inner(child, child_prefix, true, output);
                }
            });

        }

        PhysicalQueryPlan::Filter(FilterNode { condition, next }) => {
            let label = format!("Filter condition: {}", condition);

            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                if let Some(child) = next.as_deref() {
                    render_physical_plan_inner(child, child_prefix, true, output);
                }
            });
        }

        PhysicalQueryPlan::JoinLeft(JoinLeftNode { left, right, on, next }) => {
            let label = format!(
                "Join(Left) on: [{}]",
                on.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
            );

            write_node_header(output, prefix, is_last, &label);

            with_child_prefix(prefix, is_last, |child_prefix| {
                render_physical_plan_inner(left, child_prefix, false, output);
                render_physical_plan_inner(right, child_prefix, true, output);
            });

            if let Some(child) = next.as_deref() {
                render_physical_plan_inner(child, prefix, is_last, output);
            }
        }

        PhysicalQueryPlan::Limit(LimitNode { limit, next }) => {
            let label = format!("Limit {}", limit);
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                if let Some(child) = next.as_deref() {
                    render_physical_plan_inner(child, child_prefix, true, output);
                }
            });
        }

        PhysicalQueryPlan::Order(OrderNode { by, next }) => {
            let label = format!(
                "Order by: [{}]",
                by.iter().map(|o| o.to_string()).collect::<Vec<_>>().join(", ")
            );
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                if let Some(child) = next.as_deref() {
                    render_physical_plan_inner(child, child_prefix, true, output);
                }
            });
        }

        PhysicalQueryPlan::Select(SelectNode { select, next }) => {
            let label = format!(
                "Select [{}]",
                select.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
            );
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                if let Some(child) = next.as_deref() {
                    render_physical_plan_inner(child, child_prefix, true, output);
                }
            });
        }

        PhysicalQueryPlan::TableScan(TableScanNode { schema, table, next }) => {
            let label = match schema {
                Some(s) => format!("TableScan {}.{}", s.fragment, table.fragment),
                None => format!("TableScan {}", table.fragment),
            };
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                if let Some(child) = next.as_deref() {
                    render_physical_plan_inner(child, child_prefix, true, output);
                }
            });
        }
    }
}