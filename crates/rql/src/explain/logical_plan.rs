// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::parse;
use crate::plan::logical::{
    AggregateNode, FilterNode, InlineDataNode, JoinInnerNode, JoinLeftNode, JoinNaturalNode,
    LogicalPlan, MapNode, OrderNode, TableScanNode, TakeNode, compile_logical,
};
use reifydb_core::JoinType;

pub fn explain_logical_plan(query: &str) -> crate::Result<String> {
    let statements = parse(query).unwrap(); // FIXME

    let mut plans = Vec::new();
    for statement in statements {
        plans.extend(compile_logical(statement).unwrap()) // FIXME
    }

    let mut result = String::new();
    for plan in plans {
        let mut output = String::new();
        render_logical_plan_inner(&plan, "", true, &mut output);
        result += output.as_str();
    }

    Ok(result)
}

fn render_logical_plan_inner(plan: &LogicalPlan, prefix: &str, is_last: bool, output: &mut String) {
    let branch = if is_last { "└──" } else { "├──" };
    let child_prefix = format!("{}{}", prefix, if is_last { "    " } else { "│   " });

    match plan {
        LogicalPlan::CreateComputedView(_) => unimplemented!(),
        LogicalPlan::CreateSchema(_) => unimplemented!(),
        LogicalPlan::CreateSequence(_) => unimplemented!(),
        LogicalPlan::CreateTable(_) => unimplemented!(),
        LogicalPlan::Delete(_) => unimplemented!(),
        LogicalPlan::Insert(_) => unimplemented!(),
        LogicalPlan::Update(_) => unimplemented!(),

        LogicalPlan::Take(TakeNode { take }) => {
            output.push_str(&format!("{}{} Take {}\n", prefix, branch, take));
        }
        LogicalPlan::Filter(FilterNode { condition }) => {
            output.push_str(&format!("{}{} Filter\n", prefix, branch));
            output.push_str(&format!(
                "{}{} condition: {}\n",
                child_prefix,
                "└──",
                condition.to_string()
            ));
        }
        LogicalPlan::Map(MapNode { map }) => {
            output.push_str(&format!("{}{} Map\n", prefix, branch));
            for (i, expr) in map.iter().enumerate() {
                let last = i == map.len() - 1;
                output.push_str(&format!(
                    "{}{} {}\n",
                    child_prefix,
                    if last { "└──" } else { "├──" },
                    expr.to_string()
                ));
            }
        }
        LogicalPlan::Aggregate(AggregateNode { by, map }) => {
            output.push_str(&format!("{}{} Aggregate\n", prefix, branch));
            if !by.is_empty() {
                output.push_str(&format!(
                    "{}├── by: {}\n",
                    child_prefix,
                    by.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
                ));
            }
            if !map.is_empty() {
                output.push_str(&format!(
                    "{}└── map: {}\n",
                    child_prefix,
                    map.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
                ));
            }
        }
        LogicalPlan::Order(OrderNode { by }) => {
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
        LogicalPlan::JoinInner(JoinInnerNode { with, on }) => {
            let on = on.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", ");
            output.push_str(&format!("{}{}Join(Inner) [{}]\n", prefix, branch, on));

            for (i, plan) in with.iter().enumerate() {
                let last = i == with.len() - 1;
                render_logical_plan_inner(plan, child_prefix.as_str(), last, output);
            }
        }
        LogicalPlan::JoinLeft(JoinLeftNode { with, on }) => {
            let on = on.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", ");
            output.push_str(&format!("{}{}Join(Left) [{}]\n", prefix, branch, on));

            for (i, plan) in with.iter().enumerate() {
                let last = i == with.len() - 1;
                render_logical_plan_inner(plan, child_prefix.as_str(), last, output);
            }
        }
        LogicalPlan::JoinNatural(JoinNaturalNode { with, join_type }) => {
            let join_type_str = match join_type {
                JoinType::Inner => "Inner",
                JoinType::Left => "Left",
            };
            output.push_str(&format!(
                "{}{}Join(Natural {}) [using common columns]\n",
                prefix, branch, join_type_str
            ));

            for (i, plan) in with.iter().enumerate() {
                let last = i == with.len() - 1;
                render_logical_plan_inner(plan, child_prefix.as_str(), last, output);
            }
        }
        LogicalPlan::TableScan(TableScanNode { schema, table }) => {
            let name = match schema {
                Some(s) => format!("{}.{}", s.fragment, table.fragment),
                None => table.fragment.to_string(),
            };
            output.push_str(&format!("{}{} TableScan {}\n", prefix, branch, name));
        }
        LogicalPlan::InlineData(InlineDataNode { rows }) => {
            output.push_str(&format!("{}{} InlineData\n", prefix, branch));
            let total_fields: usize = rows.iter().map(|row| row.len()).sum();
            output.push_str(&format!(
                "{}{} rows: {}, fields: {}\n",
                child_prefix,
                "└──",
                rows.len(),
                total_fields
            ));
        }
    }
}
