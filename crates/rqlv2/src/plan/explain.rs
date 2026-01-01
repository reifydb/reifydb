// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Human-readable plan explanation in SQL-like EXPLAIN format.

use std::fmt::Write;

use crate::plan::{
	CatalogColumn, Plan, Primitive,
	node::{
		control::DeclareValue,
		ddl::{AlterNode, AlterTableAction, CreateNode, DropNode, DropTarget},
		expr::{BinaryPlanOp, PlanExpr, UnaryPlanOp},
		mutate::{DeleteTarget, InsertTarget, UpdateTarget},
		query::{JoinType, Projection, SortDirection, WindowSize, WindowSlide, WindowType},
	},
};

/// Explain a plan in human-readable SQL-like format.
pub fn explain_plan(plan: &Plan<'_>) -> String {
	let mut output = String::new();
	let mut explainer = PlanExplainer {
		output: &mut output,
		indent: 0,
	};
	explainer.format_plan(plan);
	output
}

/// Explain multiple plans (a program).
pub fn explain_plans(plans: &[Plan<'_>]) -> String {
	let mut output = String::new();
	let mut explainer = PlanExplainer {
		output: &mut output,
		indent: 0,
	};
	for (i, plan) in plans.iter().enumerate() {
		if i > 0 {
			explainer.output.push('\n');
		}
		explainer.format_plan(plan);
	}
	output
}

/// Plan explainer with indentation state.
struct PlanExplainer<'a> {
	output: &'a mut String,
	indent: usize,
}

impl<'a> PlanExplainer<'a> {
	/// Write a line with current indentation.
	fn write_line(&mut self, text: &str) {
		for _ in 0..self.indent {
			self.output.push_str("  ");
		}
		writeln!(self.output, "{}", text).unwrap();
	}

	/// Write an indented detail line (label: value).
	fn write_detail(&mut self, label: &str, value: &str) {
		for _ in 0..self.indent {
			self.output.push_str("  ");
		}
		writeln!(self.output, "  {}: {}", label, value).unwrap();
	}

	/// Execute a closure with increased indentation.
	fn with_indent<F>(&mut self, f: F)
	where
		F: FnOnce(&mut Self),
	{
		self.indent += 1;
		f(self);
		self.indent -= 1;
	}

	/// Write schema section with catalog columns.
	fn write_schema_columns(&mut self, columns: &[CatalogColumn<'_>]) {
		self.write_line("schema:");
		for col in columns {
			for _ in 0..self.indent {
				self.output.push_str("  ");
			}
			writeln!(self.output, "    {}: {}", col.name, col.column_type).unwrap();
		}
	}

	/// Write schema section with computed columns from projections.
	fn write_schema_projections(&mut self, projections: &[Projection<'_>]) {
		self.write_line("schema:");
		for proj in projections {
			let name = proj.alias.unwrap_or_else(|| self.extract_expr_name(proj.expr));
			for _ in 0..self.indent {
				self.output.push_str("  ");
			}
			writeln!(self.output, "    {}: Any", name).unwrap();
		}
	}

	/// Extract a column name from a plan expression.
	fn extract_expr_name<'b>(&self, expr: &PlanExpr<'b>) -> &'b str {
		match expr {
			PlanExpr::Column(col) => col.name(),
			PlanExpr::Variable(var) => var.name,
			_ => "<expr>",
		}
	}

	/// Collect schema info from a plan as formatted strings.
	fn collect_schema_info(&self, plan: &Plan<'_>) -> Vec<String> {
		match plan {
			Plan::Scan(n) => {
				if let Some(columns) = n.primitive.columns() {
					columns.iter().map(|c| format!("{}: {}", c.name, c.column_type)).collect()
				} else {
					vec![]
				}
			}
			Plan::Filter(n) => self.collect_schema_info(n.input),
			Plan::Sort(n) => self.collect_schema_info(n.input),
			Plan::Take(n) => self.collect_schema_info(n.input),
			Plan::Distinct(n) => self.collect_schema_info(n.input),
			Plan::Project(n) => n
				.projections
				.iter()
				.map(|p| {
					let name = p.alias.unwrap_or_else(|| self.extract_expr_name(p.expr));
					format!("{}: Any", name)
				})
				.collect(),
			Plan::Extend(n) => {
				let mut result = if let Some(input) = n.input {
					self.collect_schema_info(input)
				} else {
					vec![]
				};
				for ext in n.extensions.iter() {
					let name = ext.alias.unwrap_or_else(|| self.extract_expr_name(ext.expr));
					result.push(format!("{}: Any", name));
				}
				result
			}
			Plan::Aggregate(n) => {
				let mut result = Vec::new();
				for expr in n.group_by.iter() {
					let name = self.extract_expr_name(expr);
					result.push(format!("{}: Any", name));
				}
				for proj in n.aggregations.iter() {
					let name = proj.alias.unwrap_or_else(|| self.extract_expr_name(proj.expr));
					result.push(format!("{}: Any", name));
				}
				result
			}
			Plan::JoinInner(n) => {
				let mut result = self.collect_schema_info(n.left);
				result.extend(self.collect_schema_info_with_prefix(n.right, n.alias));
				result
			}
			Plan::JoinLeft(n) => {
				let mut result = self.collect_schema_info(n.left);
				result.extend(self.collect_schema_info_with_prefix(n.right, n.alias));
				result
			}
			Plan::JoinNatural(n) => {
				let mut result = self.collect_schema_info(n.left);
				result.extend(self.collect_schema_info_with_prefix(n.right, n.alias));
				result
			}
			_ => vec![],
		}
	}

	/// Collect schema info with optional prefix for column names.
	fn collect_schema_info_with_prefix(&self, plan: &Plan<'_>, prefix: Option<&str>) -> Vec<String> {
		let base = self.collect_schema_info(plan);
		if let Some(p) = prefix {
			base.into_iter()
				.map(|s| {
					// s is like "name: Type", split and prefix the name
					if let Some((name, rest)) = s.split_once(':') {
						format!("{}_{}: {}", p, name, rest.trim())
					} else {
						s
					}
				})
				.collect()
		} else {
			base
		}
	}

	/// Write schema section from collected schema info.
	fn write_schema_info(&mut self, info: &[String]) {
		if !info.is_empty() {
			self.write_line("schema:");
			for item in info {
				for _ in 0..self.indent {
					self.output.push_str("  ");
				}
				writeln!(self.output, "    {}", item).unwrap();
			}
		}
	}

	/// Format a plan node.
	fn format_plan(&mut self, plan: &Plan<'_>) {
		match plan {
			// Query Operations
			Plan::Scan(n) => {
				let name = self.format_primitive(&n.primitive);
				if let Some(alias) = n.alias {
					self.write_line(&format!("SCAN {} AS {}", name, alias));
				} else {
					self.write_line(&format!("SCAN {}", name));
				}
				if let Some(columns) = n.primitive.columns() {
					self.write_schema_columns(columns);
				}
			}
			Plan::IndexScan(n) => {
				let table_name = format!("{}.{}", n.primitive.namespace.name, n.primitive.name);
				if let Some(alias) = n.alias {
					self.write_line(&format!(
						"INDEX_SCAN {} USING {} AS {}",
						table_name, n.index.name, alias
					));
				} else {
					self.write_line(&format!("INDEX_SCAN {} USING {}", table_name, n.index.name));
				}
			}
			Plan::Filter(n) => {
				self.write_line("FILTER");
				self.write_detail("condition", &self.format_expr(n.predicate));
				self.with_indent(|e| e.format_plan(n.input));
			}
			Plan::Project(n) => {
				self.write_line("PROJECT");
				let projs: Vec<String> = n
					.projections
					.iter()
					.map(|p| {
						if let Some(alias) = p.alias {
							format!("{} AS {}", self.format_expr(p.expr), alias)
						} else {
							self.format_expr(p.expr)
						}
					})
					.collect();
				self.write_detail("output", &format!("[{}]", projs.join(", ")));
				self.write_schema_projections(n.projections);
				if let Some(input) = n.input {
					self.with_indent(|e| e.format_plan(input));
				}
			}
			Plan::Extend(n) => {
				self.write_line("EXTEND");
				let exts: Vec<String> = n
					.extensions
					.iter()
					.map(|p| {
						if let Some(alias) = p.alias {
							format!("{} AS {}", self.format_expr(p.expr), alias)
						} else {
							self.format_expr(p.expr)
						}
					})
					.collect();
				self.write_detail("add", &format!("[{}]", exts.join(", ")));
				let schema_info = self.collect_schema_info(plan);
				self.write_schema_info(&schema_info);
				if let Some(input) = n.input {
					self.with_indent(|e| e.format_plan(input));
				}
			}
			Plan::Aggregate(n) => {
				self.write_line("AGGREGATE");
				if !n.group_by.is_empty() {
					let groups: Vec<String> =
						n.group_by.iter().map(|e| self.format_expr(e)).collect();
					self.write_detail("group_by", &format!("[{}]", groups.join(", ")));
				}
				let aggs: Vec<String> = n
					.aggregations
					.iter()
					.map(|p| {
						if let Some(alias) = p.alias {
							format!("{} AS {}", self.format_expr(p.expr), alias)
						} else {
							self.format_expr(p.expr)
						}
					})
					.collect();
				self.write_detail("aggregations", &format!("[{}]", aggs.join(", ")));
				let schema_info = self.collect_schema_info(plan);
				self.write_schema_info(&schema_info);
				self.with_indent(|e| e.format_plan(n.input));
			}
			Plan::Sort(n) => {
				self.write_line("SORT");
				let keys: Vec<String> =
					n.keys.iter()
						.map(|k| {
							let dir = match k.direction {
								SortDirection::Asc => "ASC",
								SortDirection::Desc => "DESC",
							};
							format!("{} {}", self.format_expr(k.expr), dir)
						})
						.collect();
				self.write_detail("by", &format!("[{}]", keys.join(", ")));
				self.with_indent(|e| e.format_plan(n.input));
			}
			Plan::Take(n) => {
				self.write_line(&format!("TAKE {}", n.count));
				self.with_indent(|e| e.format_plan(n.input));
			}
			Plan::Distinct(n) => {
				if n.columns.is_empty() {
					self.write_line("DISTINCT");
				} else {
					let cols: Vec<&str> = n.columns.iter().map(|c| c.name).collect();
					self.write_line(&format!("DISTINCT ON [{}]", cols.join(", ")));
				}
				self.with_indent(|e| e.format_plan(n.input));
			}
			Plan::JoinInner(n) => {
				if let Some(alias) = n.alias {
					self.write_line(&format!("JOIN INNER AS {}", alias));
				} else {
					self.write_line("JOIN INNER");
				}
				let conditions: Vec<String> = n
					.on
					.iter()
					.map(|c| {
						format!("{} = {}", self.format_expr(c.left), self.format_expr(c.right))
					})
					.collect();
				if !conditions.is_empty() {
					self.write_detail("on", &conditions.join(" AND "));
				}
				let schema_info = self.collect_schema_info(plan);
				self.write_schema_info(&schema_info);
				self.with_indent(|e| {
					e.write_line("left:");
					e.with_indent(|e| e.format_plan(n.left));
					e.write_line("right:");
					e.with_indent(|e| e.format_plan(n.right));
				});
			}
			Plan::JoinLeft(n) => {
				if let Some(alias) = n.alias {
					self.write_line(&format!("JOIN LEFT AS {}", alias));
				} else {
					self.write_line("JOIN LEFT");
				}
				let conditions: Vec<String> = n
					.on
					.iter()
					.map(|c| {
						format!("{} = {}", self.format_expr(c.left), self.format_expr(c.right))
					})
					.collect();
				if !conditions.is_empty() {
					self.write_detail("on", &conditions.join(" AND "));
				}
				let schema_info = self.collect_schema_info(plan);
				self.write_schema_info(&schema_info);
				self.with_indent(|e| {
					e.write_line("left:");
					e.with_indent(|e| e.format_plan(n.left));
					e.write_line("right:");
					e.with_indent(|e| e.format_plan(n.right));
				});
			}
			Plan::JoinNatural(n) => {
				let join_type = match n.join_type {
					JoinType::Inner => "INNER",
					JoinType::Left => "LEFT",
					JoinType::Right => "RIGHT",
					JoinType::Full => "FULL",
				};
				if let Some(alias) = n.alias {
					self.write_line(&format!("JOIN NATURAL {} AS {}", join_type, alias));
				} else {
					self.write_line(&format!("JOIN NATURAL {}", join_type));
				}
				let schema_info = self.collect_schema_info(plan);
				self.write_schema_info(&schema_info);
				self.with_indent(|e| {
					e.write_line("left:");
					e.with_indent(|e| e.format_plan(n.left));
					e.write_line("right:");
					e.with_indent(|e| e.format_plan(n.right));
				});
			}
			Plan::Merge(n) => {
				self.write_line("MERGE");
				self.with_indent(|e| {
					e.write_line("left:");
					e.with_indent(|e| e.format_plan(n.left));
					e.write_line("right:");
					e.with_indent(|e| e.format_plan(n.right));
				});
			}
			Plan::Window(n) => {
				let win_type = match n.window_type {
					WindowType::Tumbling => "TUMBLING",
					WindowType::Sliding => "SLIDING",
					WindowType::Session => "SESSION",
				};
				let size = match n.size {
					WindowSize::Rows(r) => format!("{} rows", r),
					WindowSize::Duration(d) => format!("{:?}", d),
				};
				self.write_line(&format!("WINDOW {} size={}", win_type, size));
				if let Some(slide) = &n.slide {
					let slide_str = match slide {
						WindowSlide::Rows(r) => format!("{} rows", r),
						WindowSlide::Duration(d) => format!("{:?}", d),
					};
					self.write_detail("slide", &slide_str);
				}
				if !n.group_by.is_empty() {
					let groups: Vec<String> =
						n.group_by.iter().map(|e| self.format_expr(e)).collect();
					self.write_detail("group_by", &format!("[{}]", groups.join(", ")));
				}
				let aggs: Vec<String> = n
					.aggregations
					.iter()
					.map(|p| {
						if let Some(alias) = p.alias {
							format!("{} AS {}", self.format_expr(p.expr), alias)
						} else {
							self.format_expr(p.expr)
						}
					})
					.collect();
				self.write_detail("aggregations", &format!("[{}]", aggs.join(", ")));
				if let Some(input) = n.input {
					self.with_indent(|e| e.format_plan(input));
				}
			}
			Plan::Apply(n) => {
				let args: Vec<String> = n.arguments.iter().map(|a| self.format_expr(a)).collect();
				self.write_line(&format!("APPLY {}({})", n.operator, args.join(", ")));
				if let Some(input) = n.input {
					self.with_indent(|e| e.format_plan(input));
				}
			}

			// Optimized Row Access
			Plan::RowPointLookup(n) => {
				let name = self.format_primitive(&n.source);
				self.write_line(&format!("ROW_POINT_LOOKUP {} row={}", name, n.row_number));
			}
			Plan::RowListLookup(n) => {
				let name = self.format_primitive(&n.source);
				let rows: Vec<String> = n.row_numbers.iter().map(|r| r.to_string()).collect();
				self.write_line(&format!("ROW_LIST_LOOKUP {} rows=[{}]", name, rows.join(", ")));
			}
			Plan::RowRangeScan(n) => {
				let name = self.format_primitive(&n.source);
				self.write_line(&format!("ROW_RANGE_SCAN {} range={}..{}", name, n.start, n.end));
			}

			// DML Operations
			Plan::Insert(n) => {
				let target = match &n.target {
					InsertTarget::Table(t) => format!("{}.{}", t.namespace.name, t.name),
					InsertTarget::RingBuffer(r) => format!("{}.{}", r.namespace.name, r.name),
					InsertTarget::Dictionary(d) => format!("{}.{}", d.namespace.name, d.name),
				};
				self.write_line(&format!("INSERT INTO {}", target));
				if let Some(cols) = n.columns {
					let col_names: Vec<&str> = cols.iter().map(|c| c.name).collect();
					self.write_detail("columns", &format!("[{}]", col_names.join(", ")));
				}
				self.with_indent(|e| e.format_plan(n.input));
			}
			Plan::Update(n) => {
				let target = match &n.target {
					UpdateTarget::Table(t) => format!("{}.{}", t.namespace.name, t.name),
					UpdateTarget::RingBuffer(r) => format!("{}.{}", r.namespace.name, r.name),
				};
				self.write_line(&format!("UPDATE {}", target));
				if let Some(input) = n.input {
					self.with_indent(|e| e.format_plan(input));
				}
			}
			Plan::Delete(n) => {
				let target = match &n.target {
					DeleteTarget::Table(t) => format!("{}.{}", t.namespace.name, t.name),
					DeleteTarget::RingBuffer(r) => format!("{}.{}", r.namespace.name, r.name),
				};
				self.write_line(&format!("DELETE FROM {}", target));
				if let Some(input) = n.input {
					self.with_indent(|e| e.format_plan(input));
				}
			}

			// DDL Operations
			Plan::Create(n) => self.format_create(n),
			Plan::Alter(n) => self.format_alter(n),
			Plan::Drop(n) => self.format_drop(n),

			// Control Flow
			Plan::Conditional(n) => {
				self.write_line(&format!("IF {}", self.format_expr(n.condition)));
				self.write_line("THEN");
				self.with_indent(|e| {
					for stmt in n.then_branch.iter() {
						e.format_plan(stmt);
					}
				});
				for else_if in n.else_ifs.iter() {
					self.write_line(&format!("ELSE IF {}", self.format_expr(else_if.condition)));
					self.with_indent(|e| {
						for stmt in else_if.body.iter() {
							e.format_plan(stmt);
						}
					});
				}
				if let Some(else_branch) = n.else_branch {
					self.write_line("ELSE");
					self.with_indent(|e| {
						for stmt in else_branch.iter() {
							e.format_plan(stmt);
						}
					});
				}
			}
			Plan::Loop(n) => {
				self.write_line("LOOP");
				self.with_indent(|e| {
					for stmt in n.body.iter() {
						e.format_plan(stmt);
					}
				});
			}
			Plan::For(n) => {
				self.write_line(&format!(
					"FOR ${} IN {}",
					n.variable.name,
					self.format_expr(n.iterable)
				));
				self.with_indent(|e| {
					for stmt in n.body.iter() {
						e.format_plan(stmt);
					}
				});
			}
			Plan::Declare(n) => {
				let value = match &n.value {
					DeclareValue::Expression(expr) => self.format_expr(expr),
					DeclareValue::Plan(plans) => format!("<{} statements>", plans.len()),
				};
				self.write_line(&format!("LET ${} = {}", n.variable.name, value));
			}
			Plan::Assign(n) => {
				let value = match &n.value {
					DeclareValue::Expression(expr) => self.format_expr(expr),
					DeclareValue::Plan(plans) => format!("<{} statements>", plans.len()),
				};
				self.write_line(&format!("${} = {}", n.variable.name, value));
			}
			Plan::Return(n) => {
				if let Some(value) = n.value {
					self.write_line(&format!("RETURN {}", self.format_expr(value)));
				} else {
					self.write_line("RETURN");
				}
			}
			Plan::Break(_) => {
				self.write_line("BREAK");
			}
			Plan::Continue(_) => {
				self.write_line("CONTINUE");
			}

			// Other
			Plan::InlineData(n) => {
				self.write_line(&format!("INLINE_DATA ({} rows)", n.rows.len()));
			}
			Plan::Generator(n) => {
				let args: Vec<String> = n.arguments.iter().map(|a| self.format_expr(a)).collect();
				self.write_line(&format!("GENERATOR {}({})", n.name, args.join(", ")));
			}
			Plan::VariableSource(n) => {
				self.write_line(&format!("FROM ${}", n.variable.name));
			}
			Plan::Environment(_) => {
				self.write_line("FROM $env");
			}
			Plan::Scalarize(n) => {
				self.write_line("SCALARIZE");
				self.with_indent(|e| e.format_plan(n.input));
			}
		}
	}

	/// Format a primitive reference.
	fn format_primitive(&self, primitive: &Primitive<'_>) -> String {
		format!("{}.{}", primitive.namespace().name, primitive.name())
	}

	/// Format a CREATE node.
	fn format_create(&mut self, node: &CreateNode<'_>) {
		match node {
			CreateNode::Namespace(n) => {
				let if_not_exists = if n.if_not_exists {
					" IF NOT EXISTS"
				} else {
					""
				};
				self.write_line(&format!("CREATE NAMESPACE{} {}", if_not_exists, n.name));
			}
			CreateNode::Table(n) => {
				let if_not_exists = if n.if_not_exists {
					" IF NOT EXISTS"
				} else {
					""
				};
				self.write_line(&format!(
					"CREATE TABLE{} {}.{}",
					if_not_exists, n.namespace.name, n.name
				));
				let cols: Vec<String> = n
					.columns
					.iter()
					.map(|c| {
						let nullable = if c.nullable {
							""
						} else {
							" NOT NULL"
						};
						format!("{} {}{}", c.name, c.column_type, nullable)
					})
					.collect();
				self.write_detail("columns", &format!("[{}]", cols.join(", ")));
				if let Some(pk) = n.primary_key {
					self.write_detail("primary_key", &format!("[{}]", pk.join(", ")));
				}
			}
			CreateNode::View(n) => {
				let if_not_exists = if n.if_not_exists {
					" IF NOT EXISTS"
				} else {
					""
				};
				self.write_line(&format!(
					"CREATE VIEW{} {}.{}",
					if_not_exists, n.namespace.name, n.name
				));
				self.write_line("  AS");
				self.with_indent(|e| e.format_plan(n.query));
			}
			CreateNode::Index(n) => {
				let unique = if n.unique {
					"UNIQUE "
				} else {
					""
				};
				self.write_line(&format!(
					"CREATE {}INDEX {} ON {}.{}",
					unique, n.name, n.table.namespace.name, n.table.name
				));
				let cols: Vec<String> = n
					.columns
					.iter()
					.map(|c| {
						let dir = match c.direction {
							SortDirection::Asc => "",
							SortDirection::Desc => " DESC",
						};
						format!("{}{}", c.column.name, dir)
					})
					.collect();
				self.write_detail("columns", &format!("[{}]", cols.join(", ")));
				if let Some(filter) = n.filter {
					self.write_detail("where", &self.format_expr(filter));
				}
			}
			CreateNode::Sequence(n) => {
				let if_not_exists = if n.if_not_exists {
					" IF NOT EXISTS"
				} else {
					""
				};
				self.write_line(&format!(
					"CREATE SEQUENCE{} {}.{}",
					if_not_exists, n.namespace.name, n.name
				));
				if let Some(start) = n.start {
					self.write_detail("start", &start.to_string());
				}
				if let Some(inc) = n.increment {
					self.write_detail("increment", &inc.to_string());
				}
			}
			CreateNode::RingBuffer(n) => {
				let if_not_exists = if n.if_not_exists {
					" IF NOT EXISTS"
				} else {
					""
				};
				self.write_line(&format!(
					"CREATE RINGBUFFER{} {}.{}",
					if_not_exists, n.namespace.name, n.name
				));
				self.write_detail("capacity", &n.capacity.to_string());
				let cols: Vec<String> =
					n.columns.iter().map(|c| format!("{} {}", c.name, c.column_type)).collect();
				self.write_detail("columns", &format!("[{}]", cols.join(", ")));
			}
			CreateNode::Dictionary(n) => {
				let if_not_exists = if n.if_not_exists {
					" IF NOT EXISTS"
				} else {
					""
				};
				self.write_line(&format!(
					"CREATE DICTIONARY{} {}.{}",
					if_not_exists, n.namespace.name, n.name
				));
				self.write_detail("key_type", &n.key_type.to_string());
				self.write_detail("value_type", &n.value_type.to_string());
			}
		}
	}

	/// Format an ALTER node.
	fn format_alter(&mut self, node: &AlterNode<'_>) {
		match node {
			AlterNode::Table(n) => {
				self.write_line(&format!("ALTER TABLE {}.{}", n.table.namespace.name, n.table.name));
				match &n.action {
					AlterTableAction::AddColumn(col) => {
						let nullable = if col.nullable {
							""
						} else {
							" NOT NULL"
						};
						self.write_detail(
							"ADD COLUMN",
							&format!("{} {}{}", col.name, col.column_type, nullable),
						);
					}
					AlterTableAction::DropColumn(name) => {
						self.write_detail("DROP COLUMN", name);
					}
					AlterTableAction::RenameColumn {
						from,
						to,
					} => {
						self.write_detail("RENAME COLUMN", &format!("{} TO {}", from, to));
					}
				}
			}
			AlterNode::Sequence(n) => {
				self.write_line(&format!(
					"ALTER SEQUENCE {}.{}",
					n.sequence.namespace.name, n.sequence.name
				));
				if let Some(restart) = n.restart {
					self.write_detail("RESTART WITH", &restart.to_string());
				}
			}
		}
	}

	/// Format a DROP node.
	fn format_drop(&mut self, node: &DropNode<'_>) {
		let if_exists = if node.if_exists {
			" IF EXISTS"
		} else {
			""
		};
		match &node.target {
			DropTarget::Namespace(name) => {
				self.write_line(&format!("DROP NAMESPACE{} {}", if_exists, name));
			}
			DropTarget::Table(t) => {
				self.write_line(&format!("DROP TABLE{} {}.{}", if_exists, t.namespace.name, t.name));
			}
			DropTarget::View(v) => {
				self.write_line(&format!("DROP VIEW{} {}.{}", if_exists, v.namespace.name, v.name));
			}
			DropTarget::Index(i) => {
				self.write_line(&format!(
					"DROP INDEX{} {} ON {}.{}",
					if_exists, i.name, i.table.namespace.name, i.table.name
				));
			}
			DropTarget::Sequence(s) => {
				self.write_line(&format!("DROP SEQUENCE{} {}.{}", if_exists, s.namespace.name, s.name));
			}
			DropTarget::RingBuffer(r) => {
				self.write_line(&format!(
					"DROP RINGBUFFER{} {}.{}",
					if_exists, r.namespace.name, r.name
				));
			}
			DropTarget::Dictionary(d) => {
				self.write_line(&format!(
					"DROP DICTIONARY{} {}.{}",
					if_exists, d.namespace.name, d.name
				));
			}
		}
	}

	/// Format an expression to a string.
	fn format_expr(&self, expr: &PlanExpr<'_>) -> String {
		match expr {
			// Literals
			PlanExpr::LiteralNull(_) => "NULL".to_string(),
			PlanExpr::LiteralBool(b, _) => if *b {
				"true"
			} else {
				"false"
			}
			.to_string(),
			PlanExpr::LiteralInt(i, _) => i.to_string(),
			PlanExpr::LiteralFloat(f, _) => f.to_string(),
			PlanExpr::LiteralString(s, _) => format!("'{}'", s),
			PlanExpr::LiteralBytes(b, _) => format!("x'{}'", hex::encode(b)),

			// References
			PlanExpr::Column(c) => c.name().to_string(),
			PlanExpr::Variable(v) => format!("${}", v.name),
			PlanExpr::Rownum(_) => "rownum".to_string(),
			PlanExpr::Wildcard(_) => "*".to_string(),

			// Operators
			PlanExpr::Binary {
				op,
				left,
				right,
				..
			} => {
				let op_str = match op {
					BinaryPlanOp::Add => "+",
					BinaryPlanOp::Sub => "-",
					BinaryPlanOp::Mul => "*",
					BinaryPlanOp::Div => "/",
					BinaryPlanOp::Rem => "%",
					BinaryPlanOp::Eq => "=",
					BinaryPlanOp::Ne => "!=",
					BinaryPlanOp::Lt => "<",
					BinaryPlanOp::Le => "<=",
					BinaryPlanOp::Gt => ">",
					BinaryPlanOp::Ge => ">=",
					BinaryPlanOp::And => "AND",
					BinaryPlanOp::Or => "OR",
					BinaryPlanOp::Xor => "XOR",
					BinaryPlanOp::Concat => "||",
				};
				format!("({} {} {})", self.format_expr(left), op_str, self.format_expr(right))
			}
			PlanExpr::Unary {
				op,
				operand,
				..
			} => {
				let op_str = match op {
					UnaryPlanOp::Neg => "-",
					UnaryPlanOp::Not => "NOT ",
					UnaryPlanOp::Plus => "+",
				};
				format!("{}{}", op_str, self.format_expr(operand))
			}

			// Special
			PlanExpr::Between {
				expr,
				low,
				high,
				negated,
				..
			} => {
				let not = if *negated {
					"NOT "
				} else {
					""
				};
				format!(
					"{} {}BETWEEN {} AND {}",
					self.format_expr(expr),
					not,
					self.format_expr(low),
					self.format_expr(high)
				)
			}
			PlanExpr::In {
				expr,
				list,
				negated,
				..
			} => {
				let not = if *negated {
					"NOT "
				} else {
					""
				};
				let items: Vec<String> = list.iter().map(|e| self.format_expr(e)).collect();
				format!("{} {}IN ({})", self.format_expr(expr), not, items.join(", "))
			}
			PlanExpr::Cast {
				expr,
				target_type,
				..
			} => {
				format!("CAST({} AS {})", self.format_expr(expr), target_type)
			}
			PlanExpr::Call {
				function,
				arguments,
				..
			} => {
				let args: Vec<String> = arguments.iter().map(|a| self.format_expr(a)).collect();
				format!("{}({})", function.name, args.join(", "))
			}
			PlanExpr::Aggregate {
				function,
				arguments,
				distinct,
				..
			} => {
				let distinct_str = if *distinct {
					"DISTINCT "
				} else {
					""
				};
				let args: Vec<String> = arguments.iter().map(|a| self.format_expr(a)).collect();
				format!("{}({}{})", function.name, distinct_str, args.join(", "))
			}
			PlanExpr::Conditional {
				condition,
				then_expr,
				else_expr,
				..
			} => {
				format!(
					"IF {} THEN {} ELSE {}",
					self.format_expr(condition),
					self.format_expr(then_expr),
					self.format_expr(else_expr)
				)
			}
			PlanExpr::Subquery(plan) => {
				format!("({})", explain_plan(plan).trim())
			}
			PlanExpr::List(items, _) => {
				let elements: Vec<String> = items.iter().map(|e| self.format_expr(e)).collect();
				format!("[{}]", elements.join(", "))
			}
			PlanExpr::Tuple(items, _) => {
				let elements: Vec<String> = items.iter().map(|e| self.format_expr(e)).collect();
				format!("({})", elements.join(", "))
			}
			PlanExpr::Record(fields, _) => {
				let pairs: Vec<String> =
					fields.iter().map(|(k, v)| format!("{}: {}", k, self.format_expr(v))).collect();
				format!("{{{}}}", pairs.join(", "))
			}
			PlanExpr::Alias {
				expr,
				alias,
				..
			} => {
				format!("{} AS {}", self.format_expr(expr), alias)
			}
		}
	}
}

// Simple hex encoding for bytes
mod hex {
	pub fn encode(bytes: &[u8]) -> String {
		bytes.iter().map(|b| format!("{:02x}", b)).collect()
	}
}
