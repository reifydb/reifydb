// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::parse;
use crate::plan::logical::compile_logical_query;
use crate::plan::physical;
use crate::plan::physical::PhysicalQueryPlan;
use reifydb_core::Error;
use std::fmt::Write;

pub(crate) fn explain_physical_plan(query: &str) -> Result<String, Error> {
    let statements = parse(query).unwrap(); // FIXME

    let mut plans = Vec::new();
    for statement in statements {
        let logical = compile_logical_query(statement).unwrap(); // FIXME
        // plans.extend(compile_physical_query(logical))
        todo!()
    }

    let mut result = String::new();
    for plan in plans {
        if let Some(plan) = plan {
            let mut output = String::new();
            render_physical_plan_inner(&plan, "", true, &mut output);
            result += output.as_str();
        }
    }

    Ok(result)
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
        PhysicalQueryPlan::Aggregate(physical::AggregateNode { by, select, input }) => {
            let label = format!(
                "Aggregate by: [{}], select: [{}]",
                by.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", "),
                select.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
            );
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                render_physical_plan_inner(input, child_prefix, true, output);
            });
        }

        PhysicalQueryPlan::Filter(physical::FilterNode { conditions, input }) => {
            let label = format!(
                "Filter [{}]",
                conditions.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
            );
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                render_physical_plan_inner(input, child_prefix, true, output);
            });
        }

        PhysicalQueryPlan::Limit(physical::LimitNode { limit, input }) => {
            let label = format!("Limit {}", limit);
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                render_physical_plan_inner(input, child_prefix, true, output);
            });
        }

        PhysicalQueryPlan::Order(physical::OrderNode { by, input }) => {
            let label = format!(
                "Order by: [{}]",
                by.iter().map(|o| o.to_string()).collect::<Vec<_>>().join(", ")
            );
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                render_physical_plan_inner(input, child_prefix, true, output);
            });
        }

        PhysicalQueryPlan::Select(physical::SelectNode { select, input }) => {
            let label = format!(
                "Select [{}]",
                select.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
            );
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                if let Some(input) = input {
                    render_physical_plan_inner(input, child_prefix, true, output);
                }
            });
        }

        PhysicalQueryPlan::JoinLeft(physical::JoinLeftNode { left, right, on }) => {
            let label = format!(
                "Join(Left) on: [{}]",
                on.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
            );
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                render_physical_plan_inner(left, child_prefix, false, output);
                render_physical_plan_inner(right, child_prefix, true, output);
            });
        }

        PhysicalQueryPlan::TableScan(physical::TableScanNode { schema, table }) => {
            let label = match schema {
                Some(s) => format!("TableScan {}.{}", s.fragment, table.fragment),
                None => format!("TableScan {}", table.fragment),
            };
            write_node_header(output, prefix, is_last, &label);
            // no input to recurse into
        }
    }
}
