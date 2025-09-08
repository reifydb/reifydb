// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::Write;

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::{JoinType, interface::QueryTransaction};

use crate::{
	ast::parse_str,
	plan::{
		logical::compile_logical,
		physical,
		physical::{DistinctNode, PhysicalPlan, compile_physical},
	},
};

pub fn explain_physical_plan<T>(
	rx: &mut T,
	query: &str,
) -> crate::Result<String>
where
	T: QueryTransaction + CatalogQueryTransaction,
{
	let statements = parse_str(query)?;

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
			write_node_header(output, prefix, is_last, "Aggregate");
			with_child_prefix(prefix, is_last, |child_prefix| {
				// Show Map branch
				if !map.is_empty() {
					writeln!(
						output,
						"{}├── Map",
						child_prefix
					)
					.unwrap();
					let map_prefix =
						format!("{}│   ", child_prefix);
					for (i, expr) in map.iter().enumerate()
					{
						let last = i == map.len() - 1;
						writeln!(
							output,
							"{}{} {}",
							map_prefix,
							if last {
								"└──"
							} else {
								"├──"
							},
							expr.to_string()
						)
						.unwrap();
					}
				}

				// Show By branch (even if empty for
				// consistency)
				if !by.is_empty() {
					writeln!(
						output,
						"{}├── By",
						child_prefix
					)
					.unwrap();
					let by_prefix =
						format!("{}│   ", child_prefix);
					for (i, expr) in by.iter().enumerate() {
						let last = i == by.len() - 1;
						writeln!(
							output,
							"{}{} {}",
							by_prefix,
							if last {
								"└──"
							} else {
								"├──"
							},
							expr.to_string()
						)
						.unwrap();
					}
				} else {
					// Show empty By for global aggregations
					writeln!(
						output,
						"{}├── By",
						child_prefix
					)
					.unwrap();
				}

				// Show Source branch
				writeln!(output, "{}└── Source", child_prefix)
					.unwrap();
				let source_prefix =
					format!("{}    ", child_prefix);
				render_physical_plan_inner(
					input,
					&source_prefix,
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

		PhysicalPlan::Extend(physical::ExtendNode {
			extend,
			input,
		}) => {
			let label = format!(
				"Extend [{}]",
				extend.iter()
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

		PhysicalPlan::IndexScan(physical::IndexScanNode {
			schema,
			table,
			index_name,
		}) => {
			let label = format!(
				"IndexScan {}.{}::{}",
				schema.name, table.name, index_name
			);
			write_node_header(output, prefix, is_last, &label);
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
		PhysicalPlan::Distinct(DistinctNode {
			input,
			columns,
		}) => {
			let label = if columns.is_empty() {
				"Distinct (primary key)".to_string()
			} else {
				let cols: Vec<String> = columns
					.iter()
					.map(|c| c.fragment().to_string())
					.collect();
				format!("Distinct {{{}}}", cols.join(", "))
			};
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				render_physical_plan_inner(
					input,
					&child_prefix,
					true,
					output,
				);
			});
		}
		PhysicalPlan::AlterTable(_) => {
			write_node_header(
				output,
				prefix,
				is_last,
				"AlterTable",
			);
		}
		PhysicalPlan::AlterView(_) => {
			write_node_header(output, prefix, is_last, "AlterView");
		}
		PhysicalPlan::TableVirtualScan(
			physical::TableVirtualScanNode {
				schema,
				table,
				..
			},
		) => {
			let label = format!(
				"VirtualScan: {}.{}",
				schema.name, table.name
			);
			write_node_header(output, prefix, is_last, &label);
		}
	}
}
