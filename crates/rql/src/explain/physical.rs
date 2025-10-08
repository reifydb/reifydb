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

pub fn explain_physical_plan<T>(rx: &mut T, query: &str) -> crate::Result<String>
where
	T: QueryTransaction + CatalogQueryTransaction,
{
	let statements = parse_str(query)?;

	let mut plans = Vec::new();
	for statement in statements {
		let logical = compile_logical(rx, statement)?;
		plans.extend(compile_physical(rx, logical));
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

/// Write the current operator line
fn write_node_header(output: &mut String, prefix: &str, is_last: bool, label: &str) {
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

fn render_physical_plan_inner(plan: &PhysicalPlan, prefix: &str, is_last: bool, output: &mut String) {
	match plan {
		PhysicalPlan::CreateDeferredView(_) => unimplemented!(),
		PhysicalPlan::CreateTransactionalView(_) => unimplemented!(),
		PhysicalPlan::CreateNamespace(_) => unimplemented!(),
		PhysicalPlan::CreateTable(_) => unimplemented!(),
		PhysicalPlan::CreateRingBuffer(_) => unimplemented!(),
		PhysicalPlan::AlterSequence(physical::AlterSequenceNode {
			sequence,
			column,
			value,
		}) => {
			let label =
				format!("AlterSequence {}.{} SET VALUE {}", sequence.def().name, column.name(), value);
			write_node_header(output, prefix, is_last, &label);
		}
		PhysicalPlan::Delete(_) => unimplemented!(),
		PhysicalPlan::DeleteRingBuffer(_) => unimplemented!(),
		PhysicalPlan::InsertTable(_) => unimplemented!(),
		PhysicalPlan::InsertRingBuffer(_) => unimplemented!(),
		PhysicalPlan::Update(_) => unimplemented!(),
		PhysicalPlan::UpdateRingBuffer(_) => unimplemented!(),
		PhysicalPlan::Aggregate(physical::AggregateNode {
			by,
			map,
			input,
		}) => {
			write_node_header(output, prefix, is_last, "Aggregate");
			with_child_prefix(prefix, is_last, |child_prefix| {
				// Show Map branch
				if !map.is_empty() {
					writeln!(output, "{}├── Map", child_prefix).unwrap();
					let map_prefix = format!("{}│   ", child_prefix);
					for (i, expr) in map.iter().enumerate() {
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
					writeln!(output, "{}├── By", child_prefix).unwrap();
					let by_prefix = format!("{}│   ", child_prefix);
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
					writeln!(output, "{}├── By", child_prefix).unwrap();
				}

				// Show Source branch
				writeln!(output, "{}└── Source", child_prefix).unwrap();
				let source_prefix = format!("{}    ", child_prefix);
				render_physical_plan_inner(input, &source_prefix, true, output);
			});
		}

		PhysicalPlan::Filter(physical::FilterNode {
			conditions,
			input,
		}) => {
			let label = format!(
				"Filter [{}]",
				conditions.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
			);
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				render_physical_plan_inner(input, child_prefix, true, output);
			});
		}

		PhysicalPlan::Take(physical::TakeNode {
			take,
			input,
		}) => {
			let label = format!("Take {}", take);
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				render_physical_plan_inner(input, child_prefix, true, output);
			});
		}

		PhysicalPlan::Sort(physical::SortNode {
			by,
			input,
		}) => {
			let label =
				format!("Sort: [{}]", by.iter().map(|o| o.to_string()).collect::<Vec<_>>().join(", "));
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				render_physical_plan_inner(input, child_prefix, true, output);
			});
		}

		PhysicalPlan::Map(physical::MapNode {
			map,
			input,
		}) => {
			let label =
				format!("Map [{}]", map.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", "));
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				if let Some(input) = input {
					render_physical_plan_inner(input, child_prefix, true, output);
				}
			});
		}

		PhysicalPlan::Extend(physical::ExtendNode {
			extend,
			input,
		}) => {
			let label = format!(
				"Extend [{}]",
				extend.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
			);
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				if let Some(input) = input {
					render_physical_plan_inner(input, child_prefix, true, output);
				}
			});
		}

		PhysicalPlan::JoinInner(physical::JoinInnerNode {
			left,
			right,
			on,
			alias: _,
			strategy: _,
			right_query: _,
		}) => {
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

		PhysicalPlan::JoinLeft(physical::JoinLeftNode {
			left,
			right,
			on,
			alias: _,
			strategy: _,
			right_query: _,
		}) => {
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

		PhysicalPlan::JoinNatural(physical::JoinNaturalNode {
			left,
			right,
			join_type,
			alias: _,
			strategy: _,
			right_query: _,
		}) => {
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

		PhysicalPlan::IndexScan(node) => {
			let label = format!(
				"IndexScan {}.{}::{}",
				node.source.namespace().name(),
				node.source.name(),
				node.index_name
			);
			write_node_header(output, prefix, is_last, &label);
		}

		PhysicalPlan::TableScan(node) => {
			let label = format!("TableScan {}.{}", node.source.namespace().name(), node.source.name());
			write_node_header(output, prefix, is_last, &label);
		}

		PhysicalPlan::ViewScan(node) => {
			let label = format!("ViewScan {}.{}", node.source.namespace().name(), node.source.name());
			write_node_header(output, prefix, is_last, &label);
		}

		PhysicalPlan::RingBufferScan(node) => {
			let label = format!("RingBufferScan {}.{}", node.source.namespace().name(), node.source.name());
			write_node_header(output, prefix, is_last, &label);
		}

		PhysicalPlan::Apply(physical::ApplyNode {
			operator: operator_name,
			expressions: arguments,
			input,
		}) => {
			let label = format!(
				"Apply {} [{}]",
				operator_name.text(),
				if arguments.is_empty() {
					"no args".to_string()
				} else {
					format!("{} args", arguments.len())
				}
			);
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				if let Some(input) = input {
					render_physical_plan_inner(input, child_prefix, true, output);
				}
			});
		}

		PhysicalPlan::InlineData(physical::InlineDataNode {
			rows,
		}) => {
			let total_fields: usize = rows.iter().map(|row| row.len()).sum();
			let label = format!("InlineData rows: {}, fields: {}", rows.len(), total_fields);
			write_node_header(output, prefix, is_last, &label);
		}
		PhysicalPlan::Distinct(DistinctNode {
			input,
			columns,
		}) => {
			let label = if columns.is_empty() {
				"Distinct (primary key)".to_string()
			} else {
				let cols: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();
				format!("Distinct {{{}}}", cols.join(", "))
			};
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				render_physical_plan_inner(input, &child_prefix, true, output);
			});
		}
		PhysicalPlan::AlterTable(_) => {
			write_node_header(output, prefix, is_last, "AlterTable");
		}
		PhysicalPlan::AlterView(_) => {
			write_node_header(output, prefix, is_last, "AlterView");
		}
		PhysicalPlan::TableVirtualScan(node) => {
			let label = format!("VirtualScan: {}.{}", node.source.namespace().name(), node.source.name());
			write_node_header(output, prefix, is_last, &label);
		}
		PhysicalPlan::Generator(node) => {
			let label = format!("Generator: {}", node.name.text());
			write_node_header(output, prefix, is_last, &label);
		}
		PhysicalPlan::Window(node) => {
			let label = format!("Window: {:?}, Size: {:?}", node.window_type, node.size);
			write_node_header(output, prefix, is_last, &label);

			if let Some(ref input) = node.input {
				let child_prefix = format!("{}    ", prefix);
				render_physical_plan_inner(input, &child_prefix, true, output);
			}
		}
		PhysicalPlan::Declare(declare_node) => {
			let label = format!(
				"Declare {} = {} (mutable: {})",
				declare_node.name.text(),
				declare_node.value,
				declare_node.mutable
			);
			write_node_header(output, prefix, is_last, &label);
		}

		PhysicalPlan::Assign(assign_node) => {
			let label = format!("Assign {} = {}", assign_node.name.text(), assign_node.value);
			write_node_header(output, prefix, is_last, &label);
		}

		PhysicalPlan::Variable(var_node) => {
			let label = format!("Variable: {}", var_node.variable_expr.fragment.text());
			write_node_header(output, prefix, is_last, &label);
		}
	}
}
