// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::parse;
use crate::plan::logical::compile_logical;
use crate::plan::physical;
use crate::plan::physical::{PhysicalPlan, compile_physical};
use reifydb_core::JoinType;
use reifydb_core::interface::VersionedReadTransaction;
use std::fmt::Write;

pub fn explain_physical_plan(rx: &mut impl VersionedReadTransaction, query: &str) -> crate::Result<String> {
    let statements = parse(query)?;

    let mut plans = Vec::new();
    for statement in statements {
        let logical = compile_logical(statement)?;
        plans.extend(compile_physical(rx, logical))
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
    plan: &PhysicalPlan,
    prefix: &str,
    is_last: bool,
    output: &mut String,
) {
    match plan {
        PhysicalPlan::CreateComputedView(_) => unimplemented!(),
        PhysicalPlan::CreateSchema(_) => unimplemented!(),
        PhysicalPlan::CreateTable(_) => unimplemented!(),
        PhysicalPlan::Delete(_) => unimplemented!(),
        PhysicalPlan::Insert(_) => unimplemented!(),
        PhysicalPlan::Update(_) => unimplemented!(),
        PhysicalPlan::Aggregate(physical::AggregateNode { by, map, input }) => {
            let label = format!(
                "Aggregate by: [{}], map: [{}]",
                by.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", "),
                map.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
            );
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                render_physical_plan_inner(input, child_prefix, true, output);
            });
        }

        PhysicalPlan::Filter(physical::FilterNode { conditions, input }) => {
            let label = format!(
                "Filter [{}]",
                conditions.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
            );
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                render_physical_plan_inner(input, child_prefix, true, output);
            });
        }

        PhysicalPlan::Take(physical::TakeNode { take, input }) => {
            let label = format!("Take {}", take);
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                render_physical_plan_inner(input, child_prefix, true, output);
            });
        }

        PhysicalPlan::Sort(physical::SortNode { by, input }) => {
            let label = format!(
                "Sort: [{}]",
                by.iter().map(|o| o.to_string()).collect::<Vec<_>>().join(", ")
            );
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                render_physical_plan_inner(input, child_prefix, true, output);
            });
        }

        PhysicalPlan::Map(physical::MapNode { map, input }) => {
            let label = format!(
                "Map [{}]",
                map.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
            );
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                if let Some(input) = input {
                    render_physical_plan_inner(input, child_prefix, true, output);
                }
            });
        }

        PhysicalPlan::JoinInner(physical::JoinInnerNode { left, right, on }) => {
            let label = format!(
                "Join(Inner) on: [{}]",
                on.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
            );
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                render_physical_plan_inner(left, child_prefix, false, output);
                render_physical_plan_inner(right, child_prefix, true, output);
            });
        }

        PhysicalPlan::JoinLeft(physical::JoinLeftNode { left, right, on }) => {
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

        PhysicalPlan::JoinNatural(physical::JoinNaturalNode { left, right, join_type }) => {
            let join_type_str = match join_type {
                JoinType::Inner => "Inner",
                JoinType::Left => "Left",
            };
            let label = format!("Join(Natural {}) [using common columns]", join_type_str);
            write_node_header(output, prefix, is_last, &label);
            with_child_prefix(prefix, is_last, |child_prefix| {
                render_physical_plan_inner(left, child_prefix, false, output);
                render_physical_plan_inner(right, child_prefix, true, output);
            });
        }

        PhysicalPlan::TableScan(physical::TableScanNode { schema, table }) => {
            let label = match schema {
                Some(s) => format!("TableScan {}.{}", s.fragment, table.fragment),
                None => format!("TableScan {}", table.fragment),
            };
            write_node_header(output, prefix, is_last, &label);
        }

        PhysicalPlan::InlineData(physical::InlineDataNode { rows }) => {
            let total_fields: usize = rows.iter().map(|row| row.len()).sum();
            let label = format!("InlineData rows: {}, fields: {}", rows.len(), total_fields);
            write_node_header(output, prefix, is_last, &label);
        }
    }
}
