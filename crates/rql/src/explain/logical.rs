// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::common::JoinType;
use reifydb_transaction::transaction::Transaction;

use crate::{
	ast::parse_str,
	bump::Bump,
	plan::logical::{
		AggregateNode, AlterSequenceNode, AppendNode, AssertNode, CreateIndexNode, CreatePolicyNode,
		CreatePrimaryKeyNode, DistinctNode, ExtendNode, FilterNode, GeneratorNode, InlineDataNode,
		JoinInnerNode, JoinLeftNode, JoinNaturalNode, LogicalPlan, MapNode, OrderNode, PatchNode,
		PrimitiveScanNode, TakeNode, VariableSourceNode, alter::flow::AlterFlowAction,
	},
};

pub fn explain_logical_plan(catalog: &Catalog, rx: &mut Transaction<'_>, query: &str) -> crate::Result<String> {
	let bump = Bump::new();
	let statements = parse_str(&bump, query)?;

	let mut plans = Vec::new();
	for statement in statements {
		let compiler = crate::plan::logical::Compiler {
			catalog: catalog.clone(),
			bump: &bump,
		};
		plans.extend(compiler.compile(statement, rx)?);
	}

	explain_logical_plans(&plans)
}

pub fn explain_logical_plans(plans: &[LogicalPlan<'_>]) -> crate::Result<String> {
	let mut result = String::new();
	for plan in plans {
		let mut output = String::new();
		render_logical_plan_inner(&plan, "", true, &mut output);
		result += output.as_str();
	}

	Ok(result)
}

fn render_logical_plan_inner(plan: &LogicalPlan<'_>, prefix: &str, is_last: bool, output: &mut String) {
	let branch = if is_last {
		"└──"
	} else {
		"├──"
	};
	let child_prefix = format!(
		"{}{}",
		prefix,
		if is_last {
			"    "
		} else {
			"│   "
		}
	);

	match plan {
		LogicalPlan::Loop(_) => {
			output.push_str(&format!("{}{} Loop\n", prefix, branch));
		}
		LogicalPlan::While(_) => {
			output.push_str(&format!("{}{} While\n", prefix, branch));
		}
		LogicalPlan::For(_) => {
			output.push_str(&format!("{}{} For\n", prefix, branch));
		}
		LogicalPlan::Break => {
			output.push_str(&format!("{}{} Break\n", prefix, branch));
		}
		LogicalPlan::Continue => {
			output.push_str(&format!("{}{} Continue\n", prefix, branch));
		}
		LogicalPlan::CreateDeferredView(_) => unimplemented!(),
		LogicalPlan::CreateTransactionalView(_) => unimplemented!(),
		LogicalPlan::CreateNamespace(_) => unimplemented!(),
		LogicalPlan::CreateSequence(_) => unimplemented!(),
		LogicalPlan::CreateTable(_) => unimplemented!(),
		LogicalPlan::CreateRingBuffer(_) => unimplemented!(),
		LogicalPlan::CreateDictionary(_) => unimplemented!(),
		LogicalPlan::CreateSumType(_) => unimplemented!(),
		LogicalPlan::CreateSubscription(_) => unimplemented!(),
		LogicalPlan::DropNamespace(_) => unimplemented!(),
		LogicalPlan::DropTable(_) => unimplemented!(),
		LogicalPlan::DropView(_) => unimplemented!(),
		LogicalPlan::DropRingBuffer(_) => unimplemented!(),
		LogicalPlan::DropDictionary(_) => unimplemented!(),
		LogicalPlan::DropSumType(_) => unimplemented!(),
		LogicalPlan::DropFlow(_) => unimplemented!(),
		LogicalPlan::DropSubscription(_) => unimplemented!(),
		LogicalPlan::DropSeries(_) => unimplemented!(),
		LogicalPlan::AlterSequence(AlterSequenceNode {
			sequence,
			column,
			value,
		}) => {
			output.push_str(&format!("{}{} AlterSequence\n", prefix, branch));
			let child_prefix = format!(
				"{}{}",
				prefix,
				if is_last {
					"    "
				} else {
					"│   "
				}
			);

			output.push_str(&format!("{}├── Namespace: {:?}\n", child_prefix, sequence.namespace));
			output.push_str(&format!("{}├── Sequence: {:?}\n", child_prefix, sequence.name));
			output.push_str(&format!("{}├── Column: {}\n", child_prefix, column.name.text()));
			output.push_str(&format!("{}└── Value: {}\n", child_prefix, value));
		}
		LogicalPlan::CreateIndex(CreateIndexNode {
			index_type,
			index,
			columns,
			filter,
			map,
		}) => {
			output.push_str(&format!("{}{} CreateIndex\n", prefix, branch));
			let child_prefix = format!(
				"{}{}",
				prefix,
				if is_last {
					"    "
				} else {
					"│   "
				}
			);

			output.push_str(&format!("{}├── Type: {:?}\n", child_prefix, index_type));
			output.push_str(&format!("{}├── Name: {}\n", child_prefix, index.name.text()));
			output.push_str(&format!(
				"{}├── Namespace: {}\n",
				child_prefix,
				index.namespace.first().map(|ns| ns.text()).unwrap_or("default")
			));
			output.push_str(&format!("{}├── Table: {}\n", child_prefix, index.table.text()));

			let columns_str = columns
				.iter()
				.map(|col| {
					if let Some(order) = &col.order {
						format!("{} {:?}", col.column.text(), order)
					} else {
						col.column.text().to_string()
					}
				})
				.collect::<Vec<_>>()
				.join(", ");

			if !filter.is_empty() {
				output.push_str(&format!("{}├── Columns: {}\n", child_prefix, columns_str));
				let filter_str = filter.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(", ");
				output.push_str(&format!("{}├── Filters: {}\n", child_prefix, filter_str));
			} else {
				output.push_str(&format!("{}└── Columns: {}\n", child_prefix, columns_str));
			}

			if let Some(map_expr) = map {
				output.push_str(&format!("{}└── Map: {}\n", child_prefix, map_expr.to_string()));
			}
		}
		LogicalPlan::DeleteTable(delete) => {
			output.push_str(&format!("{}{} DeleteTable\n", prefix, branch));

			// Show target table if specified
			if let Some(table) = &delete.target {
				let namespace = table.namespace.first().map(|n| n.text()).unwrap_or("default");
				output.push_str(&format!(
					"{}├── target table: {}.{}\n",
					child_prefix,
					namespace,
					table.name.text()
				));
			} else {
				output.push_str(&format!("{}├── target table: <inferred from input>\n", child_prefix));
			}

			// Explain the input pipeline if present
			if let Some(input) = &delete.input {
				output.push_str(&format!("{}└── Input Pipeline:\n", child_prefix));
				let pipeline_prefix = format!("{}    ", child_prefix);
				render_logical_plan_inner(input, &pipeline_prefix, true, output);
			}
		}
		LogicalPlan::DeleteRingBuffer(delete) => {
			output.push_str(&format!("{}{} DeleteRingBuffer\n", prefix, branch));

			// Show target ring buffer
			let namespace = delete.target.namespace.first().map(|n| n.text()).unwrap_or("default");
			output.push_str(&format!(
				"{}├── target ring buffer: {}.{}\n",
				child_prefix,
				namespace,
				delete.target.name.text()
			));

			// Explain the input pipeline if present
			if let Some(input) = &delete.input {
				output.push_str(&format!("{}└── Input Pipeline:\n", child_prefix));
				let pipeline_prefix = format!("{}    ", child_prefix);
				render_logical_plan_inner(input, &pipeline_prefix, true, output);
			}
		}
		LogicalPlan::InsertTable(_) => unimplemented!(),
		LogicalPlan::InsertRingBuffer(_) => unimplemented!(),
		LogicalPlan::InsertDictionary(_) => unimplemented!(),
		LogicalPlan::InsertSeries(_) => unimplemented!(),
		LogicalPlan::DeleteSeries(_) => unimplemented!(),
		LogicalPlan::Update(update) => {
			output.push_str(&format!("{}{} Update\n", prefix, branch));

			// Show target table if specified
			if let Some(target) = &update.target {
				let namespace = target.namespace.first().map(|n| n.text()).unwrap_or("default");
				output.push_str(&format!(
					"{}├── target table: {}.{}\n",
					child_prefix,
					namespace,
					target.name.text()
				));
			} else {
				output.push_str(&format!("{}├── target table: <inferred from input>\n", child_prefix));
			}

			// Explain the input pipeline if present
			if let Some(input) = &update.input {
				output.push_str(&format!("{}└── Input Pipeline:\n", child_prefix));
				let pipeline_prefix = format!("{}    ", child_prefix);
				render_logical_plan_inner(input, &pipeline_prefix, true, output);
			}
		}
		LogicalPlan::UpdateRingBuffer(update_rb) => {
			output.push_str(&format!("{}{} UpdateRingBuffer\n", prefix, branch));

			// Show target ring buffer
			let namespace = update_rb.target.namespace.first().map(|n| n.text()).unwrap_or("default");
			output.push_str(&format!(
				"{}├── target ring buffer: {}.{}\n",
				child_prefix,
				namespace,
				update_rb.target.name.text()
			));

			// Explain the input pipeline if present
			if let Some(input) = &update_rb.input {
				output.push_str(&format!("{}└── Input Pipeline:\n", child_prefix));
				let pipeline_prefix = format!("{}    ", child_prefix);
				render_logical_plan_inner(input, &pipeline_prefix, true, output);
			}
		}

		LogicalPlan::Take(TakeNode {
			take,
		}) => {
			output.push_str(&format!("{}{} Take {}\n", prefix, branch, take));
		}
		LogicalPlan::Assert(AssertNode {
			condition,
			message,
		}) => {
			let msg = message.as_deref().unwrap_or("assertion failed");
			output.push_str(&format!("{}{} Assert \"{}\"\n", prefix, branch, msg));
			output.push_str(&format!("{}{} condition: {}\n", child_prefix, "└──", condition.to_string()));
		}
		LogicalPlan::Filter(FilterNode {
			condition,
		}) => {
			output.push_str(&format!("{}{} Filter\n", prefix, branch));
			output.push_str(&format!("{}{} condition: {}\n", child_prefix, "└──", condition.to_string()));
		}
		LogicalPlan::Map(MapNode {
			map,
		}) => {
			output.push_str(&format!("{}{} Map\n", prefix, branch));
			for (i, expr) in map.iter().enumerate() {
				let last = i == map.len() - 1;
				output.push_str(&format!(
					"{}{} {}\n",
					child_prefix,
					if last {
						"└──"
					} else {
						"├──"
					},
					expr.to_string()
				));
			}
		}
		LogicalPlan::Extend(ExtendNode {
			extend,
		}) => {
			output.push_str(&format!("{}{} Extend\n", prefix, branch));
			for (i, expr) in extend.iter().enumerate() {
				let last = i == extend.len() - 1;
				output.push_str(&format!(
					"{}{} {}\n",
					child_prefix,
					if last {
						"└──"
					} else {
						"├──"
					},
					expr.to_string()
				));
			}
		}
		LogicalPlan::Patch(PatchNode {
			assignments,
		}) => {
			output.push_str(&format!("{}{} Patch\n", prefix, branch));
			for (i, expr) in assignments.iter().enumerate() {
				let last = i == assignments.len() - 1;
				output.push_str(&format!(
					"{}{} {}\n",
					child_prefix,
					if last {
						"└──"
					} else {
						"├──"
					},
					expr.to_string()
				));
			}
		}
		LogicalPlan::Aggregate(AggregateNode {
			by,
			map,
		}) => {
			output.push_str(&format!("{}{} Aggregate\n", prefix, branch));

			// Show Map branch
			if !map.is_empty() {
				output.push_str(&format!("{}├── Map\n", child_prefix));
				let map_prefix = format!("{}│   ", child_prefix);
				for (i, expr) in map.iter().enumerate() {
					let last = i == map.len() - 1;
					output.push_str(&format!(
						"{}{} {}\n",
						map_prefix,
						if last {
							"└──"
						} else {
							"├──"
						},
						expr.to_string()
					));
				}
			}

			// Show By branch (even if empty for consistency)
			if !by.is_empty() {
				output.push_str(&format!("{}└── By\n", child_prefix));
				let by_prefix = format!("{}    ", child_prefix);
				for (i, expr) in by.iter().enumerate() {
					let last = i == by.len() - 1;
					output.push_str(&format!(
						"{}{} {}\n",
						by_prefix,
						if last {
							"└──"
						} else {
							"├──"
						},
						expr.to_string()
					));
				}
			} else {
				// Show empty By for global aggregations
				output.push_str(&format!("{}└── By\n", child_prefix));
			}
		}
		LogicalPlan::Order(OrderNode {
			by,
		}) => {
			output.push_str(&format!("{}{} Order\n", prefix, branch));
			for (i, key) in by.iter().enumerate() {
				let last = i == by.len() - 1;
				output.push_str(&format!(
					"{}{} by: {}\n",
					child_prefix,
					if last {
						"└──"
					} else {
						"├──"
					},
					key.to_string()
				));
			}
		}
		LogicalPlan::JoinInner(JoinInnerNode {
			with,
			on,
			alias: _,
		}) => {
			let on = on.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", ");
			output.push_str(&format!("{}{}Join(Inner) [{}]\n", prefix, branch, on));

			for (i, plan) in with.iter().enumerate() {
				let last = i == with.len() - 1;
				render_logical_plan_inner(plan, child_prefix.as_str(), last, output);
			}
		}
		LogicalPlan::JoinLeft(JoinLeftNode {
			with,
			on,
			alias: _,
		}) => {
			let on = on.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", ");
			output.push_str(&format!("{}{}Join(Left) [{}]\n", prefix, branch, on));

			for (i, plan) in with.iter().enumerate() {
				let last = i == with.len() - 1;
				render_logical_plan_inner(plan, child_prefix.as_str(), last, output);
			}
		}
		LogicalPlan::JoinNatural(JoinNaturalNode {
			with,
			join_type,
			alias: _,
		}) => {
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
		LogicalPlan::PrimitiveScan(PrimitiveScanNode {
			source,
			columns: _,
			index,
		}) => {
			let name = if let Some(idx) = index {
				format!(
					"{}::{}",
					source.fully_qualified_name()
						.unwrap_or_else(|| source.identifier().text().to_string()),
					idx.identifier().text()
				)
			} else {
				source.fully_qualified_name().unwrap_or_else(|| source.identifier().text().to_string())
			};

			let display_name = name;
			let scan_type = if index.is_some() {
				"IndexScan"
			} else {
				"TableScan"
			};

			output.push_str(&format!("{}{} {} {}\n", prefix, branch, scan_type, display_name));
		}
		LogicalPlan::InlineData(InlineDataNode {
			rows,
		}) => {
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
		LogicalPlan::Generator(GeneratorNode {
			name,
			expressions,
		}) => {
			output.push_str(&format!("{}{} Generator {}\n", prefix, branch, name.text()));
			output.push_str(&format!("{}{} parameters: {}\n", child_prefix, "└──", expressions.len()));
		}
		LogicalPlan::VariableSource(VariableSourceNode {
			name: variable_name,
		}) => {
			output.push_str(&format!("{}{} VariableSource {}\n", prefix, branch, variable_name.text()));
		}

		LogicalPlan::Environment(_) => {
			output.push_str(&format!("{}{} Environment\n", prefix, branch));
		}
		LogicalPlan::Distinct(DistinctNode {
			columns,
		}) => {
			output.push_str(&format!("{}{} Distinct\n", prefix, branch));
			let child_prefix = format!(
				"{}{}",
				prefix,
				if is_last {
					"    "
				} else {
					"│   "
				}
			);

			if columns.is_empty() {
				output.push_str(&format!("{}└── Columns: (primary key)\n", child_prefix));
			} else {
				output.push_str(&format!("{}└── Columns: ", child_prefix));
				for (i, col) in columns.iter().enumerate() {
					if i > 0 {
						output.push_str(", ");
					}
					output.push_str(col.name.text());
				}
				output.push_str("\n");
			}
		}
		LogicalPlan::Apply(apply) => {
			output.push_str(&format!("{}Apply\n", prefix));
			let child_prefix = format!(
				"{}{}",
				prefix,
				if is_last {
					"   "
				} else {
					"│  "
				}
			);
			output.push_str(&format!("{}├──Operator: {}\n", child_prefix, apply.operator.text()));
			if !apply.arguments.is_empty() {
				output.push_str(&format!(
					"{}└──Arguments: {} expressions\n",
					child_prefix,
					apply.arguments.len()
				));
			}
		}
		LogicalPlan::Pipeline(pipeline) => {
			output.push_str(&format!("{}{} Pipeline\n", prefix, branch));
			for (i, step) in pipeline.steps.iter().enumerate() {
				let last = i == pipeline.steps.len() - 1;
				render_logical_plan_inner(step, child_prefix.as_str(), last, output);
			}
		}
		LogicalPlan::CreatePrimaryKey(CreatePrimaryKeyNode {
			..
		}) => {
			output.push_str(&format!("{}{} CreatePrimaryKey\n", prefix, branch));
		}
		LogicalPlan::CreatePolicy(CreatePolicyNode {
			..
		}) => {
			output.push_str(&format!("{}{} CreateColumnPolicy\n", prefix, branch));
		}
		LogicalPlan::CreateProcedure(_) => {
			output.push_str(&format!("{}{} CreateProcedure\n", prefix, branch));
		}
		LogicalPlan::CreateSeries(_) => {
			output.push_str(&format!("{}{} CreateSeries\n", prefix, branch));
		}
		LogicalPlan::CreateEvent(_) => {
			output.push_str(&format!("{}{} CreateEvent\n", prefix, branch));
		}
		LogicalPlan::CreateTag(_) => {
			output.push_str(&format!("{}{} CreateTag\n", prefix, branch));
		}
		LogicalPlan::CreateHandler(_) => {
			output.push_str(&format!("{}{} CreateHandler\n", prefix, branch));
		}
		LogicalPlan::Dispatch(_) => {
			output.push_str(&format!("{}{} Dispatch\n", prefix, branch));
		}
		LogicalPlan::Window(window) => {
			output.push_str(&format!("{}{} Window\n", prefix, branch));
			let child_prefix = format!(
				"{}{}",
				prefix,
				if is_last {
					"    "
				} else {
					"│   "
				}
			);
			output.push_str(&format!("{}├── Window Type: {:?}\n", child_prefix, window.window_type));
			output.push_str(&format!("{}├── Size: {:?}\n", child_prefix, window.size));
			if let Some(ref slide) = window.slide {
				output.push_str(&format!("{}├── Slide: {:?}\n", child_prefix, slide));
			}
			if !window.group_by.is_empty() {
				output.push_str(&format!(
					"{}├── Group By: {} expressions\n",
					child_prefix,
					window.group_by.len()
				));
			}
			output.push_str(&format!(
				"{}└── Aggregations: {} expressions\n",
				child_prefix,
				window.aggregations.len()
			));
		}
		LogicalPlan::Declare(declare_node) => {
			output.push_str(&format!(
				"{}{} Declare {} = {}\n",
				prefix,
				branch,
				declare_node.name.text(),
				declare_node.value,
			));
		}

		LogicalPlan::Assign(assign_node) => {
			output.push_str(&format!(
				"{}{} Assign {} = {}\n",
				prefix,
				branch,
				assign_node.name.text(),
				assign_node.value
			));
		}

		LogicalPlan::Conditional(conditional_node) => {
			output.push_str(&format!("{}{} Conditional\n", prefix, branch));

			// Show condition
			output.push_str(&format!(
				"{}{}   If: {}\n",
				child_prefix,
				if conditional_node.else_ifs.is_empty() && conditional_node.else_branch.is_none() {
					"└──"
				} else {
					"├──"
				},
				conditional_node.condition
			));

			// Show then branch
			output.push_str(&format!(
				"{}{}   Then:\n",
				child_prefix,
				if conditional_node.else_ifs.is_empty() && conditional_node.else_branch.is_none() {
					"    "
				} else {
					"│   "
				}
			));
			render_logical_plan_inner(
				&conditional_node.then_branch,
				&format!(
					"{}{}     ",
					child_prefix,
					if conditional_node.else_ifs.is_empty()
						&& conditional_node.else_branch.is_none()
					{
						"    "
					} else {
						"│   "
					}
				),
				true,
				output,
			);

			// Show else if branches
			for (i, else_if) in conditional_node.else_ifs.iter().enumerate() {
				let is_last_else_if = i == conditional_node.else_ifs.len() - 1
					&& conditional_node.else_branch.is_none();
				output.push_str(&format!(
					"{}{}   Else If: {}\n",
					child_prefix,
					if is_last_else_if {
						"└──"
					} else {
						"├──"
					},
					else_if.condition
				));

				output.push_str(&format!(
					"{}{}   Then:\n",
					child_prefix,
					if is_last_else_if {
						"    "
					} else {
						"│   "
					}
				));
				render_logical_plan_inner(
					&else_if.then_branch,
					&format!(
						"{}{}     ",
						child_prefix,
						if is_last_else_if {
							"    "
						} else {
							"│   "
						}
					),
					true,
					output,
				);
			}

			// Show else branch if present
			if let Some(else_branch) = &conditional_node.else_branch {
				output.push_str(&format!("{}└──   Else:\n", child_prefix));
				render_logical_plan_inner(
					else_branch,
					&format!("{}      ", child_prefix),
					true,
					output,
				);
			}
		}

		LogicalPlan::Scalarize(scalarize) => {
			output.push_str(&format!("{}{} Scalarize (convert 1x1 frame to scalar)\n", prefix, branch));

			// Render the input plan
			let child_prefix = format!(
				"{}{}",
				prefix,
				if is_last {
					"    "
				} else {
					"│   "
				}
			);
			render_logical_plan_inner(&scalarize.input, &child_prefix, true, output);
		}
		LogicalPlan::CreateFlow(create_flow) => {
			let flow_name = if let Some(ns) = create_flow.flow.namespace.first() {
				format!("{}::{}", ns.text(), create_flow.flow.name.text())
			} else {
				create_flow.flow.name.text().to_string()
			};

			output.push_str(&format!("{}{} CreateFlow: {}", prefix, branch, flow_name));

			if create_flow.if_not_exists {
				output.push_str(" (IF NOT EXISTS)");
			}

			output.push_str("\n");

			// Render the AS query as a child
			if !create_flow.as_clause.is_empty() {
				for (i, plan) in create_flow.as_clause.iter().enumerate() {
					let is_last = i == create_flow.as_clause.len() - 1;
					let new_prefix = format!(
						"{}{}",
						prefix,
						if is_last {
							"    "
						} else {
							"│   "
						}
					);
					render_logical_plan_inner(plan, &new_prefix, is_last, output);
				}
			}
		}
		LogicalPlan::AlterFlow(alter_flow) => {
			let flow_name = if let Some(ns) = alter_flow.flow.namespace.first() {
				format!("{}::{}", ns.text(), alter_flow.flow.name.text())
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

			output.push_str(&format!("{}{} AlterFlow: {} ({})\n", prefix, branch, flow_name, action_str));

			// Render the SetQuery child plan if present
			if let AlterFlowAction::SetQuery {
				query,
			} = &alter_flow.action
			{
				for (i, plan) in query.iter().enumerate() {
					let is_last = i == query.len() - 1;
					let new_prefix = format!(
						"{}{}",
						prefix,
						if is_last {
							"    "
						} else {
							"│   "
						}
					);
					render_logical_plan_inner(plan, &new_prefix, is_last, output);
				}
			}
		}
		LogicalPlan::DefineFunction(def) => {
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
			output.push_str(&format!(
				"{}{} DefineFunction: {}[{}]{}\n",
				prefix,
				branch,
				def.name.text(),
				params.join(", "),
				return_str
			));

			// Render body statements
			let child_prefix = format!(
				"{}{}",
				prefix,
				if is_last {
					"    "
				} else {
					"│   "
				}
			);
			for (i, stmt) in def.body.iter().enumerate() {
				for (j, plan) in stmt.iter().enumerate() {
					let is_last_stmt = i == def.body.len() - 1 && j == stmt.len() - 1;
					render_logical_plan_inner(plan, &child_prefix, is_last_stmt, output);
				}
			}
		}
		LogicalPlan::Return(ret) => {
			let value_str = if let Some(ref expr) = ret.value {
				format!(" {}", expr)
			} else {
				String::new()
			};
			output.push_str(&format!("{}{} Return{}\n", prefix, branch, value_str));
		}
		LogicalPlan::CallFunction(call) => {
			let args: Vec<String> = call.arguments.iter().map(|a| format!("{}", a)).collect();
			output.push_str(&format!(
				"{}{} CallFunction: {}({})\n",
				prefix,
				branch,
				call.name.text(),
				args.join(", ")
			));
		}
		LogicalPlan::Append(node) => match node {
			AppendNode::IntoVariable {
				target,
				..
			} => {
				output.push_str(&format!("{}{} Append: ${}\n", prefix, branch, target.text()));
			}
			AppendNode::Query {
				with,
			} => {
				output.push_str(&format!("{}{}Append\n", prefix, branch));
				for (i, plan) in with.iter().enumerate() {
					let last = i == with.len() - 1;
					render_logical_plan_inner(plan, child_prefix.as_str(), last, output);
				}
			}
		},
		LogicalPlan::DefineClosure(closure_node) => {
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
			output.push_str(&format!("{}{} DefineClosure[{}]\n", prefix, branch, params.join(", ")));
			let child_prefix = format!(
				"{}{}",
				prefix,
				if is_last {
					"    "
				} else {
					"│   "
				}
			);
			for (i, stmt) in closure_node.body.iter().enumerate() {
				for (j, plan) in stmt.iter().enumerate() {
					let is_last_stmt = i == closure_node.body.len() - 1 && j == stmt.len() - 1;
					render_logical_plan_inner(plan, &child_prefix, is_last_stmt, output);
				}
			}
		}
	}
}
