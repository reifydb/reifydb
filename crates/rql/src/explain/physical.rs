// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fmt::Write;

use reifydb_catalog::catalog::Catalog;
use reifydb_core::common::JoinType;
use reifydb_transaction::transaction::Transaction;

use crate::{
	ast::parse_str,
	bump::Bump,
	nodes::AlterSequenceNode,
	plan::{
		logical::compile_logical,
		physical::{
			AggregateNode, AlterFlowAction, AppendPhysicalNode, ApplyNode, AssertNode, DistinctNode,
			ExtendNode, FilterNode, JoinInnerNode, JoinLeftNode, JoinNaturalNode, MapNode, PatchNode,
			PhysicalPlan, SortNode, TakeNode, compile_physical,
		},
	},
};

pub fn explain_physical_plan(catalog: &Catalog, rx: &mut Transaction<'_>, query: &str) -> crate::Result<String> {
	let bump = Bump::new();
	let statements = parse_str(&bump, query)?;

	let mut plans = Vec::new();
	for statement in statements {
		let logical = compile_logical(&bump, catalog, rx, statement)?;
		plans.push(compile_physical(&bump, catalog, rx, logical)?);
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

fn render_physical_plan_inner(plan: &PhysicalPlan<'_>, prefix: &str, is_last: bool, output: &mut String) {
	match plan {
		PhysicalPlan::Loop(_) => {
			output.push_str(&format!("{}Loop\n", prefix));
		}
		PhysicalPlan::While(_) => {
			output.push_str(&format!("{}While\n", prefix));
		}
		PhysicalPlan::For(_) => {
			output.push_str(&format!("{}For\n", prefix));
		}
		PhysicalPlan::Break => {
			output.push_str(&format!("{}Break\n", prefix));
		}
		PhysicalPlan::Continue => {
			output.push_str(&format!("{}Continue\n", prefix));
		}
		PhysicalPlan::CreateDeferredView(_) => unimplemented!(),
		PhysicalPlan::CreateTransactionalView(_) => unimplemented!(),
		PhysicalPlan::CreateNamespace(_) => unimplemented!(),
		PhysicalPlan::CreateTable(_) => unimplemented!(),
		PhysicalPlan::CreateRingBuffer(_) => unimplemented!(),
		PhysicalPlan::CreateDictionary(_) => unimplemented!(),
		PhysicalPlan::CreateSumType(_) => unimplemented!(),
		PhysicalPlan::CreateSubscription(_) => unimplemented!(),
		PhysicalPlan::DropNamespace(_) => unimplemented!(),
		PhysicalPlan::DropTable(_) => unimplemented!(),
		PhysicalPlan::DropView(_) => unimplemented!(),
		PhysicalPlan::DropRingBuffer(_) => unimplemented!(),
		PhysicalPlan::DropDictionary(_) => unimplemented!(),
		PhysicalPlan::DropSumType(_) => unimplemented!(),
		PhysicalPlan::DropFlow(_) => unimplemented!(),
		PhysicalPlan::DropSubscription(_) => unimplemented!(),
		PhysicalPlan::CreateFlow(create_flow) => {
			let mut label =
				format!("CreateFlow {}::{}", create_flow.namespace.name, create_flow.flow.text());

			if create_flow.if_not_exists {
				label.push_str(" (IF NOT EXISTS)");
			}

			write_node_header(output, prefix, is_last, &label);

			// Render the WITH query as a child
			with_child_prefix(prefix, is_last, |child_prefix| {
				render_physical_plan_inner(&create_flow.as_clause, child_prefix, true, output);
			});
		}
		PhysicalPlan::AlterSequence(AlterSequenceNode {
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
		PhysicalPlan::Aggregate(AggregateNode {
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

		PhysicalPlan::Filter(FilterNode {
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

		PhysicalPlan::Assert(AssertNode {
			conditions,
			input,
			message,
		}) => {
			let label = if let Some(msg) = message {
				format!(
					"Assert [{}] \"{}\"",
					conditions.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", "),
					msg
				)
			} else {
				format!(
					"Assert [{}]",
					conditions.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
				)
			};
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				if let Some(input) = input {
					render_physical_plan_inner(input, child_prefix, true, output);
				}
			});
		}

		PhysicalPlan::Take(TakeNode {
			take,
			input,
		}) => {
			let label = format!("Take {}", take);
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				render_physical_plan_inner(input, child_prefix, true, output);
			});
		}

		PhysicalPlan::Sort(SortNode {
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

		PhysicalPlan::Map(MapNode {
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

		PhysicalPlan::Extend(ExtendNode {
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

		PhysicalPlan::Patch(PatchNode {
			assignments,
			input,
		}) => {
			let label = format!(
				"Patch [{}]",
				assignments.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
			);
			write_node_header(output, prefix, is_last, &label);
			with_child_prefix(prefix, is_last, |child_prefix| {
				if let Some(input) = input {
					render_physical_plan_inner(input, child_prefix, true, output);
				}
			});
		}

		PhysicalPlan::JoinInner(JoinInnerNode {
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

		PhysicalPlan::JoinLeft(JoinLeftNode {
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

		PhysicalPlan::JoinNatural(JoinNaturalNode {
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

		PhysicalPlan::IndexScan(node) => {
			let label = format!(
				"IndexScan {}::{}::{}",
				node.source.namespace().name(),
				node.source.name(),
				node.index_name
			);
			write_node_header(output, prefix, is_last, &label);
		}

		PhysicalPlan::TableScan(node) => {
			let label = format!("TableScan {}::{}", node.source.namespace().name(), node.source.name());
			write_node_header(output, prefix, is_last, &label);
		}

		PhysicalPlan::ViewScan(node) => {
			let label = format!("ViewScan {}::{}", node.source.namespace().name(), node.source.name());
			write_node_header(output, prefix, is_last, &label);
		}

		PhysicalPlan::RingBufferScan(node) => {
			let label =
				format!("RingBufferScan {}::{}", node.source.namespace().name(), node.source.name());
			write_node_header(output, prefix, is_last, &label);
		}
		PhysicalPlan::FlowScan(node) => {
			let label = format!("FlowScan {}::{}", node.source.namespace().name(), node.source.name());
			write_node_header(output, prefix, is_last, &label);
		}

		PhysicalPlan::DictionaryScan(node) => {
			let label =
				format!("DictionaryScan {}::{}", node.source.namespace().name(), node.source.name());
			write_node_header(output, prefix, is_last, &label);
		}

		PhysicalPlan::Apply(ApplyNode {
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

		PhysicalPlan::InlineData(node) => {
			let total_fields: usize = node.rows.iter().map(|row| row.len()).sum();
			let label = format!("InlineData rows: {}, fields: {}", node.rows.len(), total_fields);
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
		PhysicalPlan::CreatePrimaryKey(_) => {
			write_node_header(output, prefix, is_last, "CreatePrimaryKey");
		}
		PhysicalPlan::CreatePolicy(_) => {
			write_node_header(output, prefix, is_last, "CreatePolicy");
		}
		PhysicalPlan::CreateProcedure(_) => {
			write_node_header(output, prefix, is_last, "CreateProcedure");
		}
		PhysicalPlan::CreateEvent(_) => {
			write_node_header(output, prefix, is_last, "CreateEvent");
		}
		PhysicalPlan::CreateHandler(_) => {
			write_node_header(output, prefix, is_last, "CreateHandler");
		}
		PhysicalPlan::Dispatch(_) => {
			write_node_header(output, prefix, is_last, "Dispatch");
		}
		PhysicalPlan::AlterFlow(alter_flow) => {
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
			let label = format!("VirtualScan: {}::{}", node.source.namespace().name(), node.source.name());
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

		PhysicalPlan::DefineFunction(def) => {
			let params: Vec<String> = def
				.parameters
				.iter()
				.map(|p| {
					if let Some(ref tc) = p.type_constraint {
						format!("${}: {:?}", p.name.text(), tc)
					} else {
						format!("${}", p.name.text())
					}
				})
				.collect();
			let return_str = if let Some(ref rt) = def.return_type {
				format!(" -> {:?}", rt)
			} else {
				String::new()
			};
			write_node_header(
				output,
				prefix,
				is_last,
				&format!("DefineFunction: {}[{}]{}", def.name.text(), params.join(", "), return_str),
			);

			// Render body
			let child_prefix = format!(
				"{}{}",
				prefix,
				if is_last {
					"    "
				} else {
					"│   "
				}
			);
			for (i, plan) in def.body.iter().enumerate() {
				let is_last_plan = i == def.body.len() - 1;
				render_physical_plan_inner(plan, &child_prefix, is_last_plan, output);
			}
		}

		PhysicalPlan::Return(ret) => {
			let value_str = if let Some(ref expr) = ret.value {
				format!(" {}", expr)
			} else {
				String::new()
			};
			write_node_header(output, prefix, is_last, &format!("Return{}", value_str));
		}

		PhysicalPlan::CallFunction(call) => {
			let args: Vec<String> = call.arguments.iter().map(|a| format!("{}", a)).collect();
			write_node_header(
				output,
				prefix,
				is_last,
				&format!("CallFunction: {}({})", call.name.text(), args.join(", ")),
			);
		}
		PhysicalPlan::Append(node) => match node {
			AppendPhysicalNode::IntoVariable {
				target,
				..
			} => {
				write_node_header(output, prefix, is_last, &format!("Append: ${}", target.text()));
			}
			AppendPhysicalNode::Query {
				left,
				right,
			} => {
				write_node_header(output, prefix, is_last, "Append");
				with_child_prefix(prefix, is_last, |child_prefix| {
					render_physical_plan_inner(left, child_prefix, false, output);
					render_physical_plan_inner(right, child_prefix, true, output);
				});
			}
		},
		PhysicalPlan::DefineClosure(closure_node) => {
			let params: Vec<String> = closure_node
				.parameters
				.iter()
				.map(|p| {
					if let Some(ref tc) = p.type_constraint {
						format!("${}: {:?}", p.name.text(), tc)
					} else {
						format!("${}", p.name.text())
					}
				})
				.collect();
			write_node_header(output, prefix, is_last, &format!("DefineClosure[{}]", params.join(", ")));
		}
	}
}
