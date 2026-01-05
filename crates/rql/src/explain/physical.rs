// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fmt::Write;

use reifydb_catalog::Catalog;
use reifydb_core::JoinType;
use reifydb_transaction::IntoStandardTransaction;

use crate::{
	ast::parse_str,
	plan::{
		logical::compile_logical,
		physical,
		physical::{DistinctNode, PhysicalPlan, compile_physical},
	},
};

pub async fn explain_physical_plan<T: IntoStandardTransaction>(
	catalog: &Catalog,
	rx: &mut T,
	query: &str,
) -> crate::Result<String> {
	let statements = parse_str(query)?;

	let mut plans = Vec::new();
	for statement in statements {
		let logical = compile_logical(catalog, rx, statement).await?;
		plans.push(compile_physical(catalog, rx, logical).await?);
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
		PhysicalPlan::CreateDictionary(_) => unimplemented!(),
		PhysicalPlan::CreateSubscription(_) => unimplemented!(),
		PhysicalPlan::CreateFlow(create_flow) => {
			let mut label =
				format!("CreateFlow {}.{}", create_flow.namespace.name, create_flow.flow.text());

			if create_flow.if_not_exists {
				label.push_str(" (IF NOT EXISTS)");
			}

			write_node_header(output, prefix, is_last, &label);

			// Render the WITH query as a child
			with_child_prefix(prefix, is_last, |child_prefix| {
				render_physical_plan_inner(&create_flow.as_clause, child_prefix, true, output);
			});
		}
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
		PhysicalPlan::InsertDictionary(_) => unimplemented!(),
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

		PhysicalPlan::Merge(physical::MergeNode {
			left,
			right,
		}) => {
			write_node_header(output, prefix, is_last, "Merge");
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
		PhysicalPlan::FlowScan(node) => {
			let label = format!("FlowScan {}.{}", node.source.namespace().name(), node.source.name());
			write_node_header(output, prefix, is_last, &label);
		}

		PhysicalPlan::DictionaryScan(node) => {
			let label = format!("DictionaryScan {}.{}", node.source.namespace().name(), node.source.name());
			write_node_header(output, prefix, is_last, &label);
		}

		PhysicalPlan::Apply(physical::ApplyNode {
			operator,
			expressions: arguments,
			input,
		}) => {
			let label = format!(
				"Apply {} [{}]",
				operator.text(),
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
		PhysicalPlan::AlterFlow(alter_flow) => {
			use crate::plan::physical::AlterFlowAction;

			let flow_name = if let Some(ns) = &alter_flow.flow.namespace {
				format!("{}.{}", ns.text(), alter_flow.flow.name.text())
			} else {
				alter_flow.flow.name.text().to_string()
			};

			let action_str = match &alter_flow.action {
				AlterFlowAction::Rename {
					new_name,
				} => format!("RENAME TO {}", new_name.text()),
				AlterFlowAction::SetQuery {
					..
				} => "SET QUERY".to_string(),
				AlterFlowAction::Pause => "PAUSE".to_string(),
				AlterFlowAction::Resume => "RESUME".to_string(),
			};

			let label = format!("AlterFlow {} ({})", flow_name, action_str);
			write_node_header(output, prefix, is_last, &label);

			// Render the SetQuery child plan if present
			if let AlterFlowAction::SetQuery {
				query,
			} = &alter_flow.action
			{
				with_child_prefix(prefix, is_last, |child_prefix| {
					render_physical_plan_inner(query, child_prefix, true, output);
				});
			}
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
			let label = format!("Declare {} = {}", declare_node.name.text(), declare_node.value,);
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

		PhysicalPlan::Conditional(conditional_node) => {
			write_node_header(output, prefix, is_last, "Conditional");
			with_child_prefix(prefix, is_last, |child_prefix| {
				// Show condition
				let condition_label = format!("If: {}", conditional_node.condition);
				write_node_header(output, child_prefix, false, &condition_label);

				// Show then branch
				write_node_header(output, child_prefix, false, "Then:");
				with_child_prefix(child_prefix, false, |then_child_prefix| {
					render_physical_plan_inner(
						&conditional_node.then_branch,
						then_child_prefix,
						true,
						output,
					);
				});

				// Show else if branches
				for (i, else_if) in conditional_node.else_ifs.iter().enumerate() {
					let is_last_else_if = i == conditional_node.else_ifs.len() - 1
						&& conditional_node.else_branch.is_none();
					let else_if_label = format!("Else If: {}", else_if.condition);
					write_node_header(output, child_prefix, false, &else_if_label);

					write_node_header(output, child_prefix, is_last_else_if, "Then:");
					with_child_prefix(child_prefix, is_last_else_if, |else_if_child_prefix| {
						render_physical_plan_inner(
							&else_if.then_branch,
							else_if_child_prefix,
							true,
							output,
						);
					});
				}

				// Show else branch if present
				if let Some(else_branch) = &conditional_node.else_branch {
					write_node_header(output, child_prefix, false, "Else:");
					with_child_prefix(child_prefix, true, |else_child_prefix| {
						render_physical_plan_inner(
							else_branch,
							else_child_prefix,
							true,
							output,
						);
					});
				}
			});
		}

		PhysicalPlan::Scalarize(scalarize) => {
			write_node_header(output, prefix, is_last, "Scalarize (convert 1x1 frame to scalar)");
			with_child_prefix(prefix, is_last, |child_prefix| {
				render_physical_plan_inner(&scalarize.input, child_prefix, true, output);
			});
		}

		PhysicalPlan::Environment(_) => {
			write_node_header(output, prefix, is_last, "Environment");
		}

		PhysicalPlan::RowPointLookup(lookup) => {
			let source_name = match &lookup.source {
				reifydb_core::interface::resolved::ResolvedPrimitive::Table(t) => {
					t.identifier().text().to_string()
				}
				reifydb_core::interface::resolved::ResolvedPrimitive::View(v) => {
					v.identifier().text().to_string()
				}
				reifydb_core::interface::resolved::ResolvedPrimitive::RingBuffer(rb) => {
					rb.identifier().text().to_string()
				}
				reifydb_core::interface::resolved::ResolvedPrimitive::Flow(f) => {
					f.identifier().text().to_string()
				}
				_ => "unknown".to_string(),
			};
			write_node_header(
				output,
				prefix,
				is_last,
				&format!("RowPointLookup (source: {}, row: {})", source_name, lookup.row_number),
			);
		}

		PhysicalPlan::RowListLookup(lookup) => {
			let source_name = match &lookup.source {
				reifydb_core::interface::resolved::ResolvedPrimitive::Table(t) => {
					t.identifier().text().to_string()
				}
				reifydb_core::interface::resolved::ResolvedPrimitive::View(v) => {
					v.identifier().text().to_string()
				}
				reifydb_core::interface::resolved::ResolvedPrimitive::RingBuffer(rb) => {
					rb.identifier().text().to_string()
				}
				reifydb_core::interface::resolved::ResolvedPrimitive::Flow(f) => {
					f.identifier().text().to_string()
				}
				_ => "unknown".to_string(),
			};
			let rows_str = lookup.row_numbers.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(", ");
			write_node_header(
				output,
				prefix,
				is_last,
				&format!("RowListLookup (source: {}, rows: [{}])", source_name, rows_str),
			);
		}

		PhysicalPlan::RowRangeScan(scan) => {
			let source_name = match &scan.source {
				reifydb_core::interface::resolved::ResolvedPrimitive::Table(t) => {
					t.identifier().text().to_string()
				}
				reifydb_core::interface::resolved::ResolvedPrimitive::View(v) => {
					v.identifier().text().to_string()
				}
				reifydb_core::interface::resolved::ResolvedPrimitive::RingBuffer(rb) => {
					rb.identifier().text().to_string()
				}
				reifydb_core::interface::resolved::ResolvedPrimitive::Flow(f) => {
					f.identifier().text().to_string()
				}
				_ => "unknown".to_string(),
			};
			write_node_header(
				output,
				prefix,
				is_last,
				&format!(
					"RowRangeScan (source: {}, range: {}..={})",
					source_name, scan.start, scan.end
				),
			);
		}
	}
}
