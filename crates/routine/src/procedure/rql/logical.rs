// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Display, sync::LazyLock};

use bumpalo::Bump;
use reifydb_core::{
	common::JoinType,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_rql::{
	ast::{ast::AstAlterPolicyAction, parse_str},
	plan::logical::{
		AggregateNode, AlterSequenceNode, AppendNode, AssertNode, CreateColumnPropertyNode, CreateIndexNode,
		CreatePrimaryKeyNode, DistinctNode, ExtendNode, FilterNode, GateNode, GeneratorNode, InlineDataNode,
		JoinInnerNode, JoinLeftNode, JoinNaturalNode, LogicalPlan, MapNode, OrderNode, PatchNode,
		RemoteScanNode, ShapeScanNode, TakeNode, VariableSourceNode, compile_logical,
	},
};
use reifydb_value::value::value_type::ValueType;

use crate::{
	procedure::rql::extract_query,
	routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError},
};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("rql::logical"));

pub struct RqlLogical;

impl Default for RqlLogical {
	fn default() -> Self {
		Self::new()
	}
}

impl RqlLogical {
	pub fn new() -> Self {
		Self
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for RqlLogical {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	fn attaches_row_metadata(&self) -> bool {
		false
	}

	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let query = extract_query(ctx.params, "rql::logical")?;

		let bump = Bump::new();
		let statements = parse_str(&bump, query.as_str())?;

		let mut walker = LogicalWalker::default();
		for statement in statements {
			let plans = compile_logical(&bump, ctx.catalog, ctx.tx, statement)?;
			for plan in plans.iter() {
				walker.walk(plan, 0, None);
			}
		}

		Ok(walker.into_columns())
	}
}

#[derive(Default)]
struct LogicalWalker {
	idx: Vec<i32>,
	depth: Vec<i32>,
	parent: Vec<Option<i32>>,
	kind: Vec<String>,
	detail: Vec<String>,
}

impl LogicalWalker {
	fn emit(&mut self, depth: i32, parent: Option<i32>, kind: &str, detail: String) -> i32 {
		let next = self.idx.len() as i32;
		self.idx.push(next);
		self.depth.push(depth);
		self.parent.push(parent);
		self.kind.push(kind.to_string());
		self.detail.push(detail);
		next
	}

	fn into_columns(self) -> Columns {
		Columns::new(vec![
			ColumnWithName::int4("idx", self.idx),
			ColumnWithName::int4("depth", self.depth),
			ColumnWithName::new("parent", ColumnBuffer::int4_optional(self.parent)),
			ColumnWithName::utf8("kind", self.kind),
			ColumnWithName::utf8("detail", self.detail),
		])
	}

	fn walk(&mut self, plan: &LogicalPlan<'_>, depth: i32, parent: Option<i32>) {
		let (kind, detail) = describe(plan);
		let me = self.emit(depth, parent, kind, detail);
		self.recurse_children(plan, depth + 1, Some(me));
	}

	fn recurse_children(&mut self, plan: &LogicalPlan<'_>, depth: i32, parent: Option<i32>) {
		match plan {
			LogicalPlan::Pipeline(pipeline) => {
				for step in pipeline.steps.iter() {
					self.walk(step, depth, parent);
				}
			}
			LogicalPlan::JoinInner(JoinInnerNode {
				with,
				..
			})
			| LogicalPlan::JoinLeft(JoinLeftNode {
				with,
				..
			})
			| LogicalPlan::JoinNatural(JoinNaturalNode {
				with,
				..
			}) => {
				for child in with.iter() {
					self.walk(child, depth, parent);
				}
			}
			LogicalPlan::Append(AppendNode::Query {
				with,
				..
			}) => {
				for child in with.iter() {
					self.walk(child, depth, parent);
				}
			}
			LogicalPlan::Scalarize(scalarize) => {
				self.walk(&scalarize.input, depth, parent);
			}
			LogicalPlan::DeleteTable(node) => {
				if let Some(input) = &node.input {
					self.walk(input, depth, parent);
				}
			}
			LogicalPlan::DeleteRingBuffer(node) => {
				if let Some(input) = &node.input {
					self.walk(input, depth, parent);
				}
			}
			LogicalPlan::Update(node) => {
				if let Some(input) = &node.input {
					self.walk(input, depth, parent);
				}
			}
			LogicalPlan::UpdateRingBuffer(node) => {
				if let Some(input) = &node.input {
					self.walk(input, depth, parent);
				}
			}
			LogicalPlan::UpdateSeries(node) => {
				if let Some(input) = &node.input {
					self.walk(input, depth, parent);
				}
			}
			LogicalPlan::Conditional(node) => {
				self.walk(&node.then_branch, depth, parent);
				for else_if in node.else_ifs.iter() {
					self.walk(&else_if.then_branch, depth, parent);
				}
				if let Some(else_branch) = &node.else_branch {
					self.walk(else_branch, depth, parent);
				}
			}
			LogicalPlan::DefineFunction(def) => {
				for stmt in def.body.iter() {
					for child in stmt.iter() {
						self.walk(child, depth, parent);
					}
				}
			}
			LogicalPlan::DefineClosure(def) => {
				for stmt in def.body.iter() {
					for child in stmt.iter() {
						self.walk(child, depth, parent);
					}
				}
			}
			_ => {}
		}
	}
}

fn describe(plan: &LogicalPlan<'_>) -> (&'static str, String) {
	match plan {
		LogicalPlan::Loop(_) => ("Loop", String::new()),
		LogicalPlan::While(_) => ("While", String::new()),
		LogicalPlan::For(_) => ("For", String::new()),
		LogicalPlan::Break => ("Break", String::new()),
		LogicalPlan::Continue => ("Continue", String::new()),
		LogicalPlan::CreateDeferredView(_) => ("CreateDeferredView", String::new()),
		LogicalPlan::CreateTransactionalView(_) => ("CreateTransactionalView", String::new()),
		LogicalPlan::CreateNamespace(_) => ("CreateNamespace", String::new()),
		LogicalPlan::CreateRemoteNamespace(_) => ("CreateRemoteNamespace", String::new()),
		LogicalPlan::CreateSequence(_) => ("CreateSequence", String::new()),
		LogicalPlan::CreateTable(_) => ("CreateTable", String::new()),
		LogicalPlan::CreateRingBuffer(_) => ("CreateRingBuffer", String::new()),
		LogicalPlan::CreateDictionary(_) => ("CreateDictionary", String::new()),
		LogicalPlan::CreateSumType(_) => ("CreateSumType", String::new()),
		LogicalPlan::CreateSubscription(_) => ("CreateSubscription", String::new()),
		LogicalPlan::DropNamespace(_) => ("DropNamespace", String::new()),
		LogicalPlan::DropTable(_) => ("DropTable", String::new()),
		LogicalPlan::DropView(_) => ("DropView", String::new()),
		LogicalPlan::DropRingBuffer(_) => ("DropRingBuffer", String::new()),
		LogicalPlan::DropDictionary(_) => ("DropDictionary", String::new()),
		LogicalPlan::DropSumType(_) => ("DropSumType", String::new()),
		LogicalPlan::DropSubscription(_) => ("DropSubscription", String::new()),
		LogicalPlan::DropSeries(_) => ("DropSeries", String::new()),
		LogicalPlan::DropProcedure(_) => ("DropProcedure", String::new()),
		LogicalPlan::DropHandler(_) => ("DropHandler", String::new()),
		LogicalPlan::DropTest(_) => ("DropTest", String::new()),
		LogicalPlan::CreateSource(_) => ("CreateSource", String::new()),
		LogicalPlan::CreateSink(_) => ("CreateSink", String::new()),
		LogicalPlan::CreateBinding(_) => ("CreateBinding", String::new()),
		LogicalPlan::DropSource(_) => ("DropSource", String::new()),
		LogicalPlan::DropSink(_) => ("DropSink", String::new()),
		LogicalPlan::DropBinding(_) => ("DropBinding", String::new()),
		LogicalPlan::CreateIdentity(n) => ("CreateIdentity", format!("name={}", n.name.text())),
		LogicalPlan::AlterIdentity(n) => ("AlterIdentity", format!("name={}", n.name.text())),
		LogicalPlan::CreateIdentityAttribute(n) => {
			("CreateIdentityAttribute", format!("name={}", n.name.text()))
		}
		LogicalPlan::CreateRole(n) => ("CreateRole", format!("name={}", n.name.text())),
		LogicalPlan::Grant(n) => ("Grant", format!("role={} user={}", n.role.text(), n.user.text())),
		LogicalPlan::Revoke(n) => ("Revoke", format!("role={} user={}", n.role.text(), n.user.text())),
		LogicalPlan::DropIdentity(n) => {
			("DropIdentity", format!("name={} if_exists={}", n.name.text(), n.if_exists))
		}
		LogicalPlan::DropIdentityAttribute(n) => {
			("DropIdentityAttribute", format!("name={} if_exists={}", n.name.text(), n.if_exists))
		}
		LogicalPlan::DropRole(n) => ("DropRole", format!("name={} if_exists={}", n.name.text(), n.if_exists)),
		LogicalPlan::CreateAuthentication(n) => ("CreateAuthentication", format!("user={}", n.user.text())),
		LogicalPlan::DropAuthentication(n) => {
			("DropAuthentication", format!("user={} if_exists={}", n.user.text(), n.if_exists))
		}
		LogicalPlan::CreatePolicy(n) => {
			let name = n.name.as_ref().map(|f| f.text()).unwrap_or("<unnamed>");
			("CreatePolicy", format!("name={} type={:?}", name, n.target_type))
		}
		LogicalPlan::AlterPolicy(n) => {
			let enabled = n.action == AstAlterPolicyAction::Enable;
			("AlterPolicy", format!("name={} enabled={}", n.name.text(), enabled))
		}
		LogicalPlan::DropPolicy(n) => {
			("DropPolicy", format!("name={} if_exists={}", n.name.text(), n.if_exists))
		}
		LogicalPlan::AlterSequence(AlterSequenceNode {
			sequence,
			column,
			value,
		}) => {
			let namespace =
				sequence.namespace.first().map(|s| format!("{}.", s.text())).unwrap_or_default();
			(
				"AlterSequence",
				format!("{}{}.{} = {}", namespace, sequence.name.text(), column.name.text(), value),
			)
		}
		LogicalPlan::CreateIndex(CreateIndexNode {
			index_type,
			index,
			columns,
			..
		}) => {
			let cols = columns
				.iter()
				.map(|c| {
					if let Some(order) = &c.order {
						format!("{} {:?}", c.column.text(), order)
					} else {
						c.column.text().to_string()
					}
				})
				.collect::<Vec<_>>()
				.join(", ");
			(
				"CreateIndex",
				format!(
					"type={:?} name={} table={} columns=[{}]",
					index_type,
					index.name.text(),
					index.table.text(),
					cols
				),
			)
		}
		LogicalPlan::DeleteTable(node) => {
			let target = node
				.target
				.as_ref()
				.map(|t| {
					let ns = t.namespace.first().map(|n| n.text()).unwrap_or("default");
					format!("{}::{}", ns, t.name.text())
				})
				.unwrap_or_else(|| "<inferred>".to_string());
			("DeleteTable", format!("target={}", target))
		}
		LogicalPlan::DeleteRingBuffer(node) => {
			let ns = node.target.namespace.first().map(|n| n.text()).unwrap_or("default");
			("DeleteRingBuffer", format!("target={}::{}", ns, node.target.name.text()))
		}
		LogicalPlan::InsertTable(_) => ("InsertTable", String::new()),
		LogicalPlan::InsertRingBuffer(_) => ("InsertRingBuffer", String::new()),
		LogicalPlan::InsertDictionary(_) => ("InsertDictionary", String::new()),
		LogicalPlan::InsertSeries(_) => ("InsertSeries", String::new()),
		LogicalPlan::DeleteSeries(_) => ("DeleteSeries", String::new()),
		LogicalPlan::UpdateSeries(node) => {
			let ns = node.target.namespace.first().map(|n| n.text()).unwrap_or("default");
			("UpdateSeries", format!("target={}::{}", ns, node.target.name.text()))
		}
		LogicalPlan::Update(node) => {
			let target = node
				.target
				.as_ref()
				.map(|t| {
					let ns = t.namespace.first().map(|n| n.text()).unwrap_or("default");
					format!("{}::{}", ns, t.name.text())
				})
				.unwrap_or_else(|| "<inferred>".to_string());
			("Update", format!("target={}", target))
		}
		LogicalPlan::UpdateRingBuffer(node) => {
			let ns = node.target.namespace.first().map(|n| n.text()).unwrap_or("default");
			("UpdateRingBuffer", format!("target={}::{}", ns, node.target.name.text()))
		}
		LogicalPlan::Take(TakeNode {
			take,
		}) => ("Take", take.to_string()),
		LogicalPlan::Assert(AssertNode {
			condition,
			message,
			..
		}) => {
			let msg = message.as_deref().unwrap_or("assertion failed");
			("Assert", format!("\"{}\" condition: {}", msg, condition))
		}
		LogicalPlan::AssertBlock(node) => {
			let kind = if node.expect_error {
				"AssertError"
			} else {
				"AssertBlock"
			};
			let msg = node.message.as_deref().unwrap_or("");
			(kind, format!("\"{}\"", msg))
		}
		LogicalPlan::Filter(FilterNode {
			condition,
			..
		}) => ("Filter", format!("condition: {}", condition)),
		LogicalPlan::Gate(GateNode {
			condition,
			..
		}) => ("Gate", format!("condition: {}", condition)),
		LogicalPlan::Map(MapNode {
			map,
			..
		}) => ("Map", expressions_inline(map)),
		LogicalPlan::Extend(ExtendNode {
			extend,
			..
		}) => ("Extend", expressions_inline(extend)),
		LogicalPlan::Patch(PatchNode {
			assignments,
			..
		}) => ("Patch", expressions_inline(assignments)),
		LogicalPlan::Aggregate(AggregateNode {
			by,
			map,
			..
		}) => {
			let by_str = expressions_inline(by);
			let map_str = expressions_inline(map);
			("Aggregate", format!("by=[{}] map=[{}]", by_str, map_str))
		}
		LogicalPlan::Order(OrderNode {
			by,
			..
		}) => ("Order", expressions_inline(by)),
		LogicalPlan::JoinInner(JoinInnerNode {
			on,
			..
		}) => ("JoinInner", expressions_inline(on)),
		LogicalPlan::JoinLeft(JoinLeftNode {
			on,
			..
		}) => ("JoinLeft", expressions_inline(on)),
		LogicalPlan::JoinNatural(JoinNaturalNode {
			join_type,
			..
		}) => {
			let kind = match join_type {
				JoinType::Inner => "Inner",
				JoinType::Left => "Left",
			};
			("JoinNatural", format!("type={}", kind))
		}
		LogicalPlan::PrimitiveScan(ShapeScanNode {
			source,
			index,
			..
		}) => {
			let name =
				source.fully_qualified_name().unwrap_or_else(|| source.identifier().text().to_string());
			if let Some(idx) = index {
				("IndexScan", format!("{}::{}", name, idx.identifier().text()))
			} else {
				("TableScan", name)
			}
		}
		LogicalPlan::RemoteScan(RemoteScanNode {
			address,
			local_namespace,
			remote_name,
			..
		}) => ("RemoteScan", format!("{}::{} @ {}", local_namespace, remote_name, address)),
		LogicalPlan::InlineData(InlineDataNode {
			rows,
		}) => {
			let total_fields: usize = rows.iter().map(|row| row.len()).sum();
			("InlineData", format!("rows={} fields={}", rows.len(), total_fields))
		}
		LogicalPlan::Generator(GeneratorNode {
			name,
			expressions,
		}) => ("Generator", format!("name={} parameters={}", name.text(), expressions.len())),
		LogicalPlan::VariableSource(VariableSourceNode {
			name,
		}) => ("VariableSource", name.text().to_string()),
		LogicalPlan::Environment(_) => ("Environment", String::new()),
		LogicalPlan::Distinct(DistinctNode {
			columns,
			..
		}) => {
			let detail = if columns.is_empty() {
				"primary key".to_string()
			} else {
				columns.iter().map(|c| c.name.text().to_string()).collect::<Vec<_>>().join(", ")
			};
			("Distinct", detail)
		}
		LogicalPlan::Apply(apply) => {
			("Apply", format!("operator={} arguments={}", apply.operator.text(), apply.arguments.len()))
		}
		LogicalPlan::Pipeline(_) => ("Pipeline", String::new()),
		LogicalPlan::CreatePrimaryKey(CreatePrimaryKeyNode {
			..
		}) => ("CreatePrimaryKey", String::new()),
		LogicalPlan::CreateColumnProperty(CreateColumnPropertyNode {
			..
		}) => ("CreateColumnProperty", String::new()),
		LogicalPlan::CreateProcedure(_) => ("CreateProcedure", String::new()),
		LogicalPlan::CreateSeries(_) => ("CreateSeries", String::new()),
		LogicalPlan::CreateEvent(_) => ("CreateEvent", String::new()),
		LogicalPlan::CreateTag(_) => ("CreateTag", String::new()),
		LogicalPlan::CreateTest(_) => ("CreateTest", String::new()),
		LogicalPlan::RunTests(_) => ("RunTests", String::new()),
		LogicalPlan::CreateMigration(_) => ("CreateMigration", String::new()),
		LogicalPlan::Migrate(_) => ("Migrate", String::new()),
		LogicalPlan::RollbackMigration(_) => ("RollbackMigration", String::new()),
		LogicalPlan::Dispatch(_) => ("Dispatch", String::new()),
		LogicalPlan::Window(window) => {
			let group = window.group_by.len();
			let agg = window.aggregations.len();
			("Window", format!("kind={:?} group_by={} aggregations={}", window.kind, group, agg))
		}
		LogicalPlan::Declare(node) => ("Declare", format!("{} = {}", node.name.text(), node.value)),
		LogicalPlan::Assign(node) => ("Assign", format!("{} = {}", node.name.text(), node.value)),
		LogicalPlan::Conditional(node) => ("Conditional", format!("if {}", node.condition)),
		LogicalPlan::Scalarize(_) => ("Scalarize", String::new()),
		LogicalPlan::AlterTable(node) => {
			let name = if let Some(ns) = node.table.namespace.first() {
				format!("{}::{}", ns.text(), node.table.name.text())
			} else {
				node.table.name.text().to_string()
			};
			("AlterTable", name)
		}
		LogicalPlan::AlterRemoteNamespace(_) => ("AlterRemoteNamespace", String::new()),
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
			let ret = if let Some(ref rt) = def.return_type {
				format!(" -> {:?}", rt)
			} else {
				String::new()
			};
			("DefineFunction", format!("{}[{}]{}", def.name.text(), params.join(", "), ret))
		}
		LogicalPlan::Return(ret) => {
			let value = ret.value.as_ref().map(|expr| expr.to_string()).unwrap_or_default();
			("Return", value)
		}
		LogicalPlan::CallFunction(call) => {
			let args: Vec<String> = call.arguments.iter().map(|a| format!("{}", a)).collect();
			("CallFunction", format!("{}({})", call.name.text(), args.join(", ")))
		}
		LogicalPlan::Append(node) => match node {
			AppendNode::IntoVariable {
				target,
				..
			} => ("Append", format!("${}", target.text())),
			AppendNode::Query {
				..
			} => ("Append", String::new()),
		},
		LogicalPlan::DefineClosure(node) => {
			let params: Vec<String> = node
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
			("DefineClosure", format!("[{}]", params.join(", ")))
		}
	}
}

fn expressions_inline<E: Display>(items: &[E]) -> String {
	items.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
}
