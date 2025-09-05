// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::JoinType;

use crate::{
	ast::{AstAlterTableOperation, AstAlterViewOperation, parse_str},
	plan::logical::{
		AggregateNode, AlterSequenceNode, CreateIndexNode,
		DistinctNode, ExtendNode, FilterNode, InlineDataNode,
		JoinInnerNode, JoinLeftNode, JoinNaturalNode, LogicalPlan,
		MapNode, OrderNode, SourceScanNode, TakeNode,
		alter::{AlterTableNode, AlterViewNode},
		compile_logical,
	},
};

pub fn explain_logical_plan(query: &str) -> crate::Result<String> {
	let statements = parse_str(query)?;

	let mut plans = Vec::new();
	for statement in statements {
		plans.extend(compile_logical(statement)?)
	}

	explain_logical_plans(&plans)
}

pub fn explain_logical_plans(plans: &[LogicalPlan]) -> crate::Result<String> {
	let mut result = String::new();
	for plan in plans {
		let mut output = String::new();
		render_logical_plan_inner(&plan, "", true, &mut output);
		result += output.as_str();
	}

	Ok(result)
}

fn render_logical_plan_inner(
	plan: &LogicalPlan,
	prefix: &str,
	is_last: bool,
	output: &mut String,
) {
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
		LogicalPlan::CreateDeferredView(_) => unimplemented!(),
		LogicalPlan::CreateTransactionalView(_) => unimplemented!(),
		LogicalPlan::CreateSchema(_) => unimplemented!(),
		LogicalPlan::CreateSequence(_) => unimplemented!(),
		LogicalPlan::CreateTable(_) => unimplemented!(),
		LogicalPlan::AlterSequence(AlterSequenceNode {
			schema,
			table,
			column,
			value,
		}) => {
			output.push_str(&format!(
				"{}{} AlterSequence\n",
				prefix, branch
			));
			let child_prefix = format!(
				"{}{}",
				prefix,
				if is_last {
					"    "
				} else {
					"│   "
				}
			);

			if let Some(schema_fragment) = schema {
				output.push_str(&format!(
					"{}├── Schema: {}\n",
					child_prefix,
					schema_fragment.fragment()
				));
				output.push_str(&format!(
					"{}├── Table: {}\n",
					child_prefix,
					table.fragment()
				));
			} else {
				output.push_str(&format!(
					"{}├── Table: {}\n",
					child_prefix,
					table.fragment()
				));
			}
			output.push_str(&format!(
				"{}├── Column: {}\n",
				child_prefix,
				column.fragment()
			));
			output.push_str(&format!(
				"{}└── Value: {}\n",
				child_prefix, value
			));
		}
		LogicalPlan::CreateIndex(CreateIndexNode {
			index_type,
			name,
			schema,
			table,
			columns,
			filter,
			map,
		}) => {
			output.push_str(&format!(
				"{}{} CreateIndex\n",
				prefix, branch
			));
			let child_prefix = format!(
				"{}{}",
				prefix,
				if is_last {
					"    "
				} else {
					"│   "
				}
			);

			output.push_str(&format!(
				"{}├── Type: {:?}\n",
				child_prefix, index_type
			));
			output.push_str(&format!(
				"{}├── Name: {}\n",
				child_prefix,
				name.fragment()
			));
			output.push_str(&format!(
				"{}├── Schema: {}\n",
				child_prefix,
				schema.fragment()
			));
			output.push_str(&format!(
				"{}├── Table: {}\n",
				child_prefix,
				table.fragment()
			));

			let columns_str = columns
				.iter()
				.map(|col| {
					if let Some(order) = &col.order {
						format!(
							"{} {:?}",
							col.column.fragment(),
							order
						)
					} else {
						col.column
							.fragment()
							.to_string()
					}
				})
				.collect::<Vec<_>>()
				.join(", ");

			if !filter.is_empty() {
				output.push_str(&format!(
					"{}├── Columns: {}\n",
					child_prefix, columns_str
				));
				let filter_str = filter
					.iter()
					.map(|f| f.to_string())
					.collect::<Vec<_>>()
					.join(", ");
				output.push_str(&format!(
					"{}├── Filters: {}\n",
					child_prefix, filter_str
				));
			} else {
				output.push_str(&format!(
					"{}└── Columns: {}\n",
					child_prefix, columns_str
				));
			}

			if let Some(map_expr) = map {
				output.push_str(&format!(
					"{}└── Map: {}\n",
					child_prefix,
					map_expr.to_string()
				));
			}
		}
		LogicalPlan::Delete(delete) => {
			output.push_str(&format!(
				"{}{} Delete\n",
				prefix, branch
			));

			// Show target table if specified
			if let Some(table) = &delete.table {
				output.push_str(&format!(
					"{}├── target table: {}\n",
					child_prefix,
					if let Some(schema) = &delete.schema {
						format!(
							"{}.{}",
							schema.fragment(),
							table.fragment()
						)
					} else {
						table.fragment().to_string()
					}
				));
			} else {
				output.push_str(&format!(
					"{}├── target table: <inferred from input>\n",
					child_prefix
				));
			}

			// Explain the input pipeline if present
			if let Some(input) = &delete.input {
				output.push_str(&format!(
					"{}└── Input Pipeline:\n",
					child_prefix
				));
				let pipeline_prefix =
					format!("{}    ", child_prefix);
				render_logical_plan_inner(
					input,
					&pipeline_prefix,
					true,
					output,
				);
			}
		}
		LogicalPlan::Insert(_) => unimplemented!(),
		LogicalPlan::Update(update) => {
			output.push_str(&format!(
				"{}{} Update\n",
				prefix, branch
			));

			// Show target table if specified
			if let Some(table) = &update.table {
				output.push_str(&format!(
					"{}├── target table: {}\n",
					child_prefix,
					if let Some(schema) = &update.schema {
						format!(
							"{}.{}",
							schema.fragment(),
							table.fragment()
						)
					} else {
						table.fragment().to_string()
					}
				));
			} else {
				output.push_str(&format!(
					"{}├── target table: <inferred from input>\n",
					child_prefix
				));
			}

			// Explain the input pipeline if present
			if let Some(input) = &update.input {
				output.push_str(&format!(
					"{}└── Input Pipeline:\n",
					child_prefix
				));
				let pipeline_prefix =
					format!("{}    ", child_prefix);
				render_logical_plan_inner(
					input,
					&pipeline_prefix,
					true,
					output,
				);
			}
		}

		LogicalPlan::Take(TakeNode {
			take,
		}) => {
			output.push_str(&format!(
				"{}{} Take {}\n",
				prefix, branch, take
			));
		}
		LogicalPlan::Filter(FilterNode {
			condition,
		}) => {
			output.push_str(&format!(
				"{}{} Filter\n",
				prefix, branch
			));
			output.push_str(&format!(
				"{}{} condition: {}\n",
				child_prefix,
				"└──",
				condition.to_string()
			));
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
			output.push_str(&format!(
				"{}{} Extend\n",
				prefix, branch
			));
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
		LogicalPlan::Aggregate(AggregateNode {
			by,
			map,
		}) => {
			output.push_str(&format!(
				"{}{} Aggregate\n",
				prefix, branch
			));

			// Show Map branch
			if !map.is_empty() {
				output.push_str(&format!(
					"{}├── Map\n",
					child_prefix
				));
				let map_prefix =
					format!("{}│   ", child_prefix);
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
				output.push_str(&format!(
					"{}└── By\n",
					child_prefix
				));
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
				output.push_str(&format!(
					"{}└── By\n",
					child_prefix
				));
			}
		}
		LogicalPlan::Order(OrderNode {
			by,
		}) => {
			output.push_str(&format!(
				"{}{} Order\n",
				prefix, branch
			));
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
		}) => {
			let on = on
				.iter()
				.map(|c| c.to_string())
				.collect::<Vec<_>>()
				.join(", ");
			output.push_str(&format!(
				"{}{}Join(Inner) [{}]\n",
				prefix, branch, on
			));

			for (i, plan) in with.iter().enumerate() {
				let last = i == with.len() - 1;
				render_logical_plan_inner(
					plan,
					child_prefix.as_str(),
					last,
					output,
				);
			}
		}
		LogicalPlan::JoinLeft(JoinLeftNode {
			with,
			on,
		}) => {
			let on = on
				.iter()
				.map(|c| c.to_string())
				.collect::<Vec<_>>()
				.join(", ");
			output.push_str(&format!(
				"{}{}Join(Left) [{}]\n",
				prefix, branch, on
			));

			for (i, plan) in with.iter().enumerate() {
				let last = i == with.len() - 1;
				render_logical_plan_inner(
					plan,
					child_prefix.as_str(),
					last,
					output,
				);
			}
		}
		LogicalPlan::JoinNatural(JoinNaturalNode {
			with,
			join_type,
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
				render_logical_plan_inner(
					plan,
					child_prefix.as_str(),
					last,
					output,
				);
			}
		}
		LogicalPlan::SourceScan(SourceScanNode {
			schema,
			source: table,
			index_name,
		}) => {
			let name = if let Some(idx) = index_name {
				format!(
					"{}.{}::{}",
					schema.fragment(),
					table.fragment(),
					idx.fragment()
				)
			} else {
				format!(
					"{}.{}",
					schema.fragment(),
					table.fragment()
				)
			};

			let scan_type = if index_name.is_some() {
				"IndexScan"
			} else {
				"TableScan"
			};

			output.push_str(&format!(
				"{}{} {} {}\n",
				prefix, branch, scan_type, name
			));
		}
		LogicalPlan::InlineData(InlineDataNode {
			rows,
		}) => {
			output.push_str(&format!(
				"{}{} InlineData\n",
				prefix, branch
			));
			let total_fields: usize =
				rows.iter().map(|row| row.len()).sum();
			output.push_str(&format!(
				"{}{} rows: {}, fields: {}\n",
				child_prefix,
				"└──",
				rows.len(),
				total_fields
			));
		}
		LogicalPlan::Distinct(DistinctNode {
			columns,
		}) => {
			output.push_str(&format!(
				"{}{} Distinct\n",
				prefix, branch
			));
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
				output.push_str(&format!(
					"{}└── Columns: (primary key)\n",
					child_prefix
				));
			} else {
				output.push_str(&format!(
					"{}└── Columns: ",
					child_prefix
				));
				for (i, col) in columns.iter().enumerate() {
					if i > 0 {
						output.push_str(", ");
					}
					output.push_str(col.fragment());
				}
				output.push_str("\n");
			}
		}
		LogicalPlan::Pipeline(pipeline) => {
			output.push_str(&format!(
				"{}{} Pipeline\n",
				prefix, branch
			));
			for (i, step) in pipeline.steps.iter().enumerate() {
				let last = i == pipeline.steps.len() - 1;
				render_logical_plan_inner(
					step,
					child_prefix.as_str(),
					last,
					output,
				);
			}
		}
		LogicalPlan::AlterTable(AlterTableNode {
			table,
		}) => {
			output.push_str(&format!(
				"{}{} AlterTable\n",
				prefix, branch
			));

			// Show schema and table
			output.push_str(&format!(
				"{}├── Schema: {}\n",
				child_prefix,
				table.schema.value()
			));
			output.push_str(&format!(
				"{}├── Table: {}\n",
				child_prefix,
				table.table.value()
			));

			// Show operations
			let ops_count = table.operations.len();
			for (i, op) in table.operations.iter().enumerate() {
				let is_last_op = i == ops_count - 1;
				let op_branch = if is_last_op {
					"└──"
				} else {
					"├──"
				};

				match op {
					AstAlterTableOperation::CreatePrimaryKey { name, columns } => {
						let pk_name = name.as_ref()
							.map(|n| format!(" {}", n.value()))
							.unwrap_or_default();
						output.push_str(&format!(
							"{}{}Operation: CREATE PRIMARY KEY{}\n",
							child_prefix, op_branch, pk_name
						));

						// Show columns
						let cols_prefix = format!("{}{}    ",
							child_prefix,
							if is_last_op { " " } else { "│" }
						);
						for (j, col) in columns.iter().enumerate() {
							let col_last = j == columns.len() - 1;
							let col_branch = if col_last { "└──" } else { "├──" };
							output.push_str(&format!(
								"{}{}Column: {}\n",
								cols_prefix, col_branch, col.column.value()
							));
						}
					}
					AstAlterTableOperation::DropPrimaryKey => {
						output.push_str(&format!(
							"{}{}Operation: DROP PRIMARY KEY\n",
							child_prefix, op_branch
						));
					}
				}
			}
		}
		LogicalPlan::AlterView(AlterViewNode {
			view,
		}) => {
			output.push_str(&format!(
				"{}{} AlterView\n",
				prefix, branch
			));

			// Show schema and view
			output.push_str(&format!(
				"{}├── Schema: {}\n",
				child_prefix,
				view.schema.value()
			));
			output.push_str(&format!(
				"{}├── View: {}\n",
				child_prefix,
				view.view.value()
			));

			// Show operations
			let ops_count = view.operations.len();
			for (i, op) in view.operations.iter().enumerate() {
				let is_last_op = i == ops_count - 1;
				let op_branch = if is_last_op {
					"└──"
				} else {
					"├──"
				};

				match op {
					AstAlterViewOperation::CreatePrimaryKey { name, columns } => {
						let pk_name = name.as_ref()
							.map(|n| format!(" {}", n.value()))
							.unwrap_or_default();
						output.push_str(&format!(
							"{}{}Operation: CREATE PRIMARY KEY{}\n",
							child_prefix, op_branch, pk_name
						));

						// Show columns
						let cols_prefix = format!("{}{}    ",
							child_prefix,
							if is_last_op { " " } else { "│" }
						);
						for (j, col) in columns.iter().enumerate() {
							let col_last = j == columns.len() - 1;
							let col_branch = if col_last { "└──" } else { "├──" };
							output.push_str(&format!(
								"{}{}Column: {}\n",
								cols_prefix, col_branch, col.column.value()
							));
						}
					}
					AstAlterViewOperation::DropPrimaryKey => {
						output.push_str(&format!(
							"{}{}Operation: DROP PRIMARY KEY\n",
							child_prefix, op_branch
						));
					}
				}
			}
		}
	}
}
