// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::Write;

use crate::{
	ast::parse,
	plan::{
		logical::compile_logical,
		physical,
		physical::{compile_physical, PhysicalPlan},
	},
};
use reifydb_core::interface::UnderlyingQueryTransaction;
use reifydb_core::JoinType;

pub fn explain_physical_plan(
	rx: &mut impl UnderlyingQueryTransaction,
	query: &str,
) -> crate::Result<String> {
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
			render_physical_plan_inner(
				&plan,
				"",
				true,
				&mut output,
			);
			result += output.as_str();
		}
	}

	Ok(result)
}

/// Write the current node line
fn write_node_header(
	output: &mut String,
	prefix: &str,
	is_last: bool,
	label: &str,
) {
	let branch = if is_last {
		"└──"
	} else {
		"├──"
	};
	writeln!(output, "{}{} {}", prefix, branch, label).unwrap();
}

/// Compute prefix for child nodes
fn with_child_prefix<F: FnOnce(&str)>(prefix: &str, is_last: bool, f: F) {
	let child_prefix = format!(
		"{}{}",
		prefix,
		if is_last {
			"    "
		} else {
			"│   "
		}
	);
	f(&child_prefix);
}

fn render_physical_plan_inner(
	plan: &PhysicalPlan,
	prefix: &str,
	is_last: bool,
	output: &mut String,
) {
	match plan {
		PhysicalPlan::CreateDeferredView(_) => unimplemented!(),
		PhysicalPlan::CreateTransactionalView(_) => unimplemented!(),
		PhysicalPlan::CreateSchema(_) => unimplemented!(),
		PhysicalPlan::CreateTable(_) => unimplemented!(),
		PhysicalPlan::AlterSequence(physical::AlterSequencePlan {
			schema,
			table,
			column,
			value,
		}) => {
			let schema_str = schema
				.as_ref()
				.map(|s| format!("{}.", s.fragment()))
				.unwrap_or_default();
			let label = format!(
				"AlterSequence {}{}.{} SET VALUE {}",
				schema_str,
				table.fragment(),
				column.fragment(),
				value
			);
			write_node_header(output, prefix, is_last, &label);
		}
		PhysicalPlan::Delete(_) => unimplemented!(),
		PhysicalPlan::Insert(_) => unimplemented!(),
		PhysicalPlan::Update(_) => unimplemented!(),
		PhysicalPlan::Aggregate(physical::AggregateNode {
			by,
			map,
			input,
		}) => {
			let label = format!(
				"Aggregate by: [{}], map: [{}]",
				by.iter()
					.map(|e| e.to_string())
					.collect::<Vec<_>>()
					.join(", "),
				map.iter()
					.map(|e| e.to_string())
					.collect::<Vec<_>>()
					.join(", ")
			);
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				render_physical_plan_inner(
					input,
					child_prefix,
					true,
					output,
				);
			});
		}

		PhysicalPlan::Filter(physical::FilterNode {
			conditions,
			input,
		}) => {
			let label = format!(
				"Filter [{}]",
				conditions
					.iter()
					.map(|e| e.to_string())
					.collect::<Vec<_>>()
					.join(", ")
			);
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				render_physical_plan_inner(
					input,
					child_prefix,
					true,
					output,
				);
			});
		}

		PhysicalPlan::Take(physical::TakeNode {
			take,
			input,
		}) => {
			let label = format!("Take {}", take);
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				render_physical_plan_inner(
					input,
					child_prefix,
					true,
					output,
				);
			});
		}

		PhysicalPlan::Sort(physical::SortNode {
			by,
			input,
		}) => {
			let label = format!(
				"Sort: [{}]",
				by.iter()
					.map(|o| o.to_string())
					.collect::<Vec<_>>()
					.join(", ")
			);
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				render_physical_plan_inner(
					input,
					child_prefix,
					true,
					output,
				);
			});
		}

		PhysicalPlan::Map(physical::MapNode {
			map,
			input,
		}) => {
			let label = format!(
				"Map [{}]",
				map.iter()
					.map(|e| e.to_string())
					.collect::<Vec<_>>()
					.join(", ")
			);
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				if let Some(input) = input {
					render_physical_plan_inner(
						input,
						child_prefix,
						true,
						output,
					);
				}
			});
		}

		PhysicalPlan::JoinInner(physical::JoinInnerNode {
			left,
			right,
			on,
		}) => {
			let label = format!(
				"Join(Inner) on: [{}]",
				on.iter()
					.map(|e| e.to_string())
					.collect::<Vec<_>>()
					.join(", ")
			);
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				render_physical_plan_inner(
					left,
					child_prefix,
					false,
					output,
				);
				render_physical_plan_inner(
					right,
					child_prefix,
					true,
					output,
				);
			});
		}

		PhysicalPlan::JoinLeft(physical::JoinLeftNode {
			left,
			right,
			on,
		}) => {
			let label = format!(
				"Join(Left) on: [{}]",
				on.iter()
					.map(|e| e.to_string())
					.collect::<Vec<_>>()
					.join(", ")
			);
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				render_physical_plan_inner(
					left,
					child_prefix,
					false,
					output,
				);
				render_physical_plan_inner(
					right,
					child_prefix,
					true,
					output,
				);
			});
		}

		PhysicalPlan::JoinNatural(physical::JoinNaturalNode {
			left,
			right,
			join_type,
		}) => {
			let join_type_str = match join_type {
				JoinType::Inner => "Inner",
				JoinType::Left => "Left",
			};
			let label = format!(
				"Join(Natural {}) [using common columns]",
				join_type_str
			);
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				render_physical_plan_inner(
					left,
					child_prefix,
					false,
					output,
				);
				render_physical_plan_inner(
					right,
					child_prefix,
					true,
					output,
				);
			});
		}

		PhysicalPlan::TableScan(physical::TableScanNode {
			schema,
			table,
		}) => {
			let label = format!(
				"TableScan {}.{}",
				schema.name, table.name
			);
			write_node_header(output, prefix, is_last, &label);
		}

		PhysicalPlan::ViewScan(physical::ViewScanNode {
			schema,
			view,
		}) => {
			let label = format!(
				"ViewScan {}.{}",
				schema.name, view.name
			);
			write_node_header(output, prefix, is_last, &label);
		}

		PhysicalPlan::InlineData(physical::InlineDataNode {
			rows,
		}) => {
			let total_fields: usize =
				rows.iter().map(|row| row.len()).sum();
			let label = format!(
				"InlineData rows: {}, fields: {}",
				rows.len(),
				total_fields
			);
			write_node_header(output, prefix, is_last, &label);
		}
	}
}
