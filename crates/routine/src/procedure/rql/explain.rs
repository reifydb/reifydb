// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Display, sync::LazyLock};

use bumpalo::Bump;
use reifydb_core::{
	common::JoinType,
	interface::resolved::ResolvedShape,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_rql::{
	ast::parse_str,
	nodes::AlterSequenceNode,
	optimize::optimize_physical,
	plan::{
		logical::compile_logical,
		physical::{
			AggregateNode, AppendPhysicalNode, ApplyNode, AssertNode, DistinctNode, ExtendNode, FilterNode,
			GateNode, JoinInnerNode, JoinLeftNode, JoinNaturalNode, MapNode, PatchNode, PhysicalPlan,
			SortNode, TakeNode, compile_physical,
		},
	},
};
use reifydb_value::value::value_type::ValueType;

use crate::{
	procedure::rql::extract_query,
	routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError},
};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("rql::explain"));

pub struct RqlExplain;

impl Default for RqlExplain {
	fn default() -> Self {
		Self::new()
	}
}

impl RqlExplain {
	pub fn new() -> Self {
		Self
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for RqlExplain {
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
		let query = extract_query(ctx.params, "rql::explain")?;

		let bump = Bump::new();
		let statements = parse_str(&bump, query.as_str())?;

		let mut walker = PhysicalWalker::default();
		for statement in statements {
			let logical = compile_logical(&bump, ctx.catalog, ctx.tx, statement)?;
			if let Some(mut plan) = compile_physical(&bump, ctx.catalog, ctx.tx, logical)? {
				optimize_physical(&mut plan);
				walker.walk(&plan, 0, None);
			}
		}

		Ok(walker.into_columns())
	}
}

#[derive(Default)]
struct PhysicalWalker {
	idx: Vec<i32>,
	depth: Vec<i32>,
	parent: Vec<Option<i32>>,
	kind: Vec<String>,
	detail: Vec<String>,
}

impl PhysicalWalker {
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

	fn walk(&mut self, plan: &PhysicalPlan<'_>, depth: i32, parent: Option<i32>) {
		let (kind, detail) = describe(plan);
		let me = self.emit(depth, parent, kind, detail);
		self.recurse_children(plan, depth + 1, Some(me));
	}

	fn recurse_children(&mut self, plan: &PhysicalPlan<'_>, depth: i32, parent: Option<i32>) {
		match plan {
			PhysicalPlan::Aggregate(AggregateNode {
				input,
				..
			}) => self.walk(input, depth, parent),
			PhysicalPlan::Filter(FilterNode {
				input,
				..
			}) => self.walk(input, depth, parent),
			PhysicalPlan::Gate(GateNode {
				input,
				..
			}) => self.walk(input, depth, parent),
			PhysicalPlan::Assert(AssertNode {
				input: Some(input),
				..
			}) => {
				self.walk(input, depth, parent);
			}
			PhysicalPlan::Take(TakeNode {
				input,
				..
			}) => self.walk(input, depth, parent),
			PhysicalPlan::Sort(SortNode {
				input,
				..
			}) => self.walk(input, depth, parent),
			PhysicalPlan::Map(MapNode {
				input: Some(input),
				..
			}) => {
				self.walk(input, depth, parent);
			}
			PhysicalPlan::Extend(ExtendNode {
				input: Some(input),
				..
			}) => {
				self.walk(input, depth, parent);
			}
			PhysicalPlan::Patch(PatchNode {
				input: Some(input),
				..
			}) => {
				self.walk(input, depth, parent);
			}
			PhysicalPlan::JoinInner(JoinInnerNode {
				left,
				right,
				..
			}) => {
				self.walk(left, depth, parent);
				self.walk(right, depth, parent);
			}
			PhysicalPlan::JoinLeft(JoinLeftNode {
				left,
				right,
				..
			}) => {
				self.walk(left, depth, parent);
				self.walk(right, depth, parent);
			}
			PhysicalPlan::JoinNatural(JoinNaturalNode {
				left,
				right,
				..
			}) => {
				self.walk(left, depth, parent);
				self.walk(right, depth, parent);
			}
			PhysicalPlan::Apply(ApplyNode {
				input: Some(input),
				..
			}) => {
				self.walk(input, depth, parent);
			}
			PhysicalPlan::Distinct(DistinctNode {
				input,
				..
			}) => self.walk(input, depth, parent),
			PhysicalPlan::Window(node) => {
				if let Some(input) = &node.input {
					self.walk(input, depth, parent);
				}
			}
			PhysicalPlan::Conditional(node) => {
				self.walk(&node.then_branch, depth, parent);
				for else_if in node.else_ifs.iter() {
					self.walk(&else_if.then_branch, depth, parent);
				}
				if let Some(else_branch) = &node.else_branch {
					self.walk(else_branch, depth, parent);
				}
			}
			PhysicalPlan::Scalarize(scalarize) => self.walk(&scalarize.input, depth, parent),
			PhysicalPlan::DefineFunction(def) => {
				for child in def.body.iter() {
					self.walk(child, depth, parent);
				}
			}
			PhysicalPlan::Append(AppendPhysicalNode::Query {
				left,
				right,
				..
			}) => {
				self.walk(left, depth, parent);
				self.walk(right, depth, parent);
			}
			_ => {}
		}
	}
}

fn describe(plan: &PhysicalPlan<'_>) -> (&'static str, String) {
	match plan {
		PhysicalPlan::Loop(_) => ("Loop", String::new()),
		PhysicalPlan::While(_) => ("While", String::new()),
		PhysicalPlan::For(_) => ("For", String::new()),
		PhysicalPlan::Break => ("Break", String::new()),
		PhysicalPlan::Continue => ("Continue", String::new()),
		PhysicalPlan::CreateDeferredView(_) => ("CreateDeferredView", String::new()),
		PhysicalPlan::CreateTransactionalView(_) => ("CreateTransactionalView", String::new()),
		PhysicalPlan::CreateNamespace(_) => ("CreateNamespace", String::new()),
		PhysicalPlan::CreateRemoteNamespace(_) => ("CreateRemoteNamespace", String::new()),
		PhysicalPlan::CreateTable(_) => ("CreateTable", String::new()),
		PhysicalPlan::CreateRingBuffer(_) => ("CreateRingBuffer", String::new()),
		PhysicalPlan::CreateDictionary(_) => ("CreateDictionary", String::new()),
		PhysicalPlan::CreateSumType(_) => ("CreateSumType", String::new()),
		PhysicalPlan::CreateSubscription(_) => ("CreateSubscription", String::new()),
		PhysicalPlan::DropNamespace(_) => ("DropNamespace", String::new()),
		PhysicalPlan::DropTable(_) => ("DropTable", String::new()),
		PhysicalPlan::DropView(_) => ("DropView", String::new()),
		PhysicalPlan::DropRingBuffer(_) => ("DropRingBuffer", String::new()),
		PhysicalPlan::DropDictionary(_) => ("DropDictionary", String::new()),
		PhysicalPlan::DropSumType(_) => ("DropSumType", String::new()),
		PhysicalPlan::DropSubscription(_) => ("DropSubscription", String::new()),
		PhysicalPlan::DropSeries(_) => ("DropSeries", String::new()),
		PhysicalPlan::DropSegmentTree(_) => ("DropSegmentTree", String::new()),
		PhysicalPlan::DropProcedure(_) => ("DropProcedure", String::new()),
		PhysicalPlan::DropHandler(_) => ("DropHandler", String::new()),
		PhysicalPlan::DropTest(_) => ("DropTest", String::new()),
		PhysicalPlan::CreateSource(_) => ("CreateSource", String::new()),
		PhysicalPlan::CreateSink(_) => ("CreateSink", String::new()),
		PhysicalPlan::CreateBinding(_) => ("CreateBinding", String::new()),
		PhysicalPlan::DropSource(_) => ("DropSource", String::new()),
		PhysicalPlan::DropSink(_) => ("DropSink", String::new()),
		PhysicalPlan::DropBinding(_) => ("DropBinding", String::new()),
		PhysicalPlan::CreateIdentity(n) => ("CreateIdentity", format!("name={}", n.name.text())),
		PhysicalPlan::AlterIdentity(n) => ("AlterIdentity", format!("name={}", n.name.text())),
		PhysicalPlan::CreateIdentityAttribute(n) => {
			("CreateIdentityAttribute", format!("name={}", n.name.text()))
		}
		PhysicalPlan::CreateRole(n) => ("CreateRole", format!("name={}", n.name.text())),
		PhysicalPlan::Grant(n) => ("Grant", format!("role={} user={}", n.role.text(), n.user.text())),
		PhysicalPlan::Revoke(n) => ("Revoke", format!("role={} user={}", n.role.text(), n.user.text())),
		PhysicalPlan::DropIdentity(n) => {
			("DropIdentity", format!("name={} if_exists={}", n.name.text(), n.if_exists))
		}
		PhysicalPlan::DropIdentityAttribute(n) => {
			("DropIdentityAttribute", format!("name={} if_exists={}", n.name.text(), n.if_exists))
		}
		PhysicalPlan::DropRole(n) => ("DropRole", format!("name={} if_exists={}", n.name.text(), n.if_exists)),
		PhysicalPlan::CreateAuthentication(n) => {
			("CreateAuthentication", format!("user={} method={}", n.user.text(), n.method.text()))
		}
		PhysicalPlan::DropAuthentication(n) => (
			"DropAuthentication",
			format!("user={} method={} if_exists={}", n.user.text(), n.method.text(), n.if_exists),
		),
		PhysicalPlan::CreatePolicy(n) => {
			let name = n.name.as_ref().map(|f| f.text()).unwrap_or("<unnamed>");
			("CreatePolicy", format!("name={} type={}", name, n.target_type))
		}
		PhysicalPlan::AlterPolicy(n) => ("AlterPolicy", format!("name={} enabled={}", n.name.text(), n.enable)),
		PhysicalPlan::DropPolicy(n) => {
			("DropPolicy", format!("name={} if_exists={}", n.name.text(), n.if_exists))
		}
		PhysicalPlan::AlterSequence(AlterSequenceNode {
			sequence,
			column,
			value,
		}) => ("AlterSequence", format!("{}.{} = {}", sequence.def().name, column.name(), value)),
		PhysicalPlan::Delete(_) => ("Delete", String::new()),
		PhysicalPlan::DeleteRingBuffer(_) => ("DeleteRingBuffer", String::new()),
		PhysicalPlan::InsertTable(_) => ("InsertTable", String::new()),
		PhysicalPlan::InsertRingBuffer(_) => ("InsertRingBuffer", String::new()),
		PhysicalPlan::InsertDictionary(_) => ("InsertDictionary", String::new()),
		PhysicalPlan::DeleteSeries(_) => ("DeleteSeries", String::new()),
		PhysicalPlan::InsertSeries(_) => ("InsertSeries", String::new()),
		PhysicalPlan::Update(_) => ("Update", String::new()),
		PhysicalPlan::UpdateRingBuffer(_) => ("UpdateRingBuffer", String::new()),
		PhysicalPlan::UpdateSeries(_) => ("UpdateSeries", String::new()),
		PhysicalPlan::Aggregate(AggregateNode {
			by,
			map,
			..
		}) => {
			let by_str = by.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ");
			let map_str = map.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ");
			("Aggregate", format!("by=[{}] map=[{}]", by_str, map_str))
		}
		PhysicalPlan::Filter(FilterNode {
			conditions,
			..
		}) => ("Filter", expressions_inline(conditions)),
		PhysicalPlan::Gate(GateNode {
			conditions,
			..
		}) => ("Gate", expressions_inline(conditions)),
		PhysicalPlan::Assert(AssertNode {
			conditions,
			message,
			..
		}) => {
			let cond = expressions_inline(conditions);
			match message {
				Some(msg) => ("Assert", format!("\"{}\" [{}]", msg, cond)),
				None => ("Assert", format!("[{}]", cond)),
			}
		}
		PhysicalPlan::AssertBlock(node) => {
			let kind = if node.expect_error {
				"AssertError"
			} else {
				"AssertBlock"
			};
			let msg = node.message.as_deref().unwrap_or("");
			(kind, format!("\"{}\"", msg))
		}
		PhysicalPlan::Take(TakeNode {
			take,
			..
		}) => ("Take", take.to_string()),
		PhysicalPlan::Sort(SortNode {
			by,
			..
		}) => ("Sort", expressions_inline(by)),
		PhysicalPlan::Map(MapNode {
			map,
			..
		}) => ("Map", expressions_inline(map)),
		PhysicalPlan::Extend(ExtendNode {
			extend,
			..
		}) => ("Extend", expressions_inline(extend)),
		PhysicalPlan::Patch(PatchNode {
			assignments,
			..
		}) => ("Patch", expressions_inline(assignments)),
		PhysicalPlan::JoinInner(JoinInnerNode {
			on,
			..
		}) => ("JoinInner", format!("on=[{}]", expressions_inline(on))),
		PhysicalPlan::JoinLeft(JoinLeftNode {
			on,
			..
		}) => ("JoinLeft", format!("on=[{}]", expressions_inline(on))),
		PhysicalPlan::JoinNatural(JoinNaturalNode {
			join_type,
			..
		}) => {
			let kind = match join_type {
				JoinType::Inner => "Inner",
				JoinType::Left => "Left",
			};
			("JoinNatural", format!("type={}", kind))
		}
		PhysicalPlan::IndexScan(node) => (
			"IndexScan",
			format!("{}::{}::{}", node.source.namespace().name(), node.source.name(), node.index_name),
		),
		PhysicalPlan::TableScan(node) => {
			("TableScan", format!("{}::{}", node.source.namespace().name(), node.source.name()))
		}
		PhysicalPlan::ViewScan(node) => {
			("ViewScan", format!("{}::{}", node.source.namespace().name(), node.source.name()))
		}
		PhysicalPlan::RingBufferScan(node) => {
			("RingBufferScan", format!("{}::{}", node.source.namespace().name(), node.source.name()))
		}
		PhysicalPlan::DictionaryScan(node) => {
			("DictionaryScan", format!("{}::{}", node.source.namespace().name(), node.source.name()))
		}
		PhysicalPlan::SeriesScan(node) => {
			("SeriesScan", format!("{}.{}", node.source.namespace().name(), node.source.name()))
		}
		PhysicalPlan::Apply(ApplyNode {
			operator,
			expressions,
			..
		}) => {
			let summary = if expressions.is_empty() {
				"no args".to_string()
			} else {
				format!("{} args", expressions.len())
			};
			("Apply", format!("operator={} {}", operator.text(), summary))
		}
		PhysicalPlan::RemoteScan(node) => (
			"RemoteScan",
			format!(
				"{}::{} @ {} rql=\"{}\"",
				node.local_namespace, node.remote_name, node.address, node.remote_rql
			),
		),
		PhysicalPlan::InlineData(node) => {
			let total_fields: usize = node.rows.iter().map(|row| row.len()).sum();
			("InlineData", format!("rows={} fields={}", node.rows.len(), total_fields))
		}
		PhysicalPlan::Distinct(DistinctNode {
			columns,
			..
		}) => {
			let detail = if columns.is_empty() {
				"primary key".to_string()
			} else {
				columns.iter().map(|c| c.name().to_string()).collect::<Vec<_>>().join(", ")
			};
			("Distinct", detail)
		}
		PhysicalPlan::CreatePrimaryKey(_) => ("CreatePrimaryKey", String::new()),
		PhysicalPlan::CreateColumnProperty(_) => ("CreateColumnProperty", String::new()),
		PhysicalPlan::CreateProcedure(_) => ("CreateProcedure", String::new()),
		PhysicalPlan::CreateSeries(_) => ("CreateSeries", String::new()),
		PhysicalPlan::CreateSegmentTree(_) => ("CreateSegmentTree", String::new()),
		PhysicalPlan::CreateEvent(_) => ("CreateEvent", String::new()),
		PhysicalPlan::CreateTag(_) => ("CreateTag", String::new()),
		PhysicalPlan::CreateTest(_) => ("CreateTest", String::new()),
		PhysicalPlan::RunTests(_) => ("RunTests", String::new()),
		PhysicalPlan::CreateMigration(_) => ("CreateMigration", String::new()),
		PhysicalPlan::Migrate(_) => ("Migrate", String::new()),
		PhysicalPlan::RollbackMigration(_) => ("RollbackMigration", String::new()),
		PhysicalPlan::Dispatch(_) => ("Dispatch", String::new()),
		PhysicalPlan::AlterTable(node) => {
			("AlterTable", format!("{}.{}", node.namespace.name(), node.table.text()))
		}
		PhysicalPlan::AlterRemoteNamespace(_) => ("AlterRemoteNamespace", String::new()),
		PhysicalPlan::TableVirtualScan(node) => {
			("TableVirtualScan", format!("{}::{}", node.source.namespace().name(), node.source.name()))
		}
		PhysicalPlan::Generator(node) => ("Generator", node.name.text().to_string()),
		PhysicalPlan::Window(node) => ("Window", format!("kind={:?}", node.kind)),
		PhysicalPlan::Declare(node) => ("Declare", format!("{} = {}", node.name.text(), node.value)),
		PhysicalPlan::Assign(node) => ("Assign", format!("{} = {}", node.name.text(), node.value)),
		PhysicalPlan::Variable(node) => ("Variable", node.variable_expr.fragment.text().to_string()),
		PhysicalPlan::Conditional(node) => ("Conditional", format!("if {}", node.condition)),
		PhysicalPlan::Scalarize(_) => ("Scalarize", String::new()),
		PhysicalPlan::Environment(_) => ("Environment", String::new()),
		PhysicalPlan::RowPointLookup(lookup) => {
			let source_name = source_name_of(&lookup.source);
			("RowPointLookup", format!("source={} row={}", source_name, lookup.row_number))
		}
		PhysicalPlan::RowListLookup(lookup) => {
			let source_name = source_name_of(&lookup.source);
			let rows = lookup.row_numbers.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(", ");
			("RowListLookup", format!("source={} rows=[{}]", source_name, rows))
		}
		PhysicalPlan::RowRangeScan(scan) => {
			let source_name = source_name_of(&scan.source);
			("RowRangeScan", format!("source={} range={}..={}", source_name, scan.start, scan.end))
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
			let ret = if let Some(ref rt) = def.return_type {
				format!(" -> {:?}", rt)
			} else {
				String::new()
			};
			("DefineFunction", format!("{}[{}]{}", def.name.text(), params.join(", "), ret))
		}
		PhysicalPlan::Return(ret) => {
			let value = ret.value.as_ref().map(|expr| expr.to_string()).unwrap_or_default();
			("Return", value)
		}
		PhysicalPlan::CallFunction(call) => {
			let args: Vec<String> = call.arguments.iter().map(|a| format!("{}", a)).collect();
			("CallFunction", format!("{}({})", call.name.text(), args.join(", ")))
		}
		PhysicalPlan::Append(node) => match node {
			AppendPhysicalNode::IntoVariable {
				target,
				..
			} => ("Append", format!("${}", target.text())),
			AppendPhysicalNode::Query {
				..
			} => ("Append", String::new()),
		},
		PhysicalPlan::DefineClosure(node) => {
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

fn source_name_of(source: &ResolvedShape) -> String {
	match source {
		ResolvedShape::Table(t) => t.identifier().text().to_string(),
		ResolvedShape::View(v) => v.identifier().text().to_string(),
		ResolvedShape::RingBuffer(rb) => rb.identifier().text().to_string(),
		_ => "unknown".to_string(),
	}
}

fn expressions_inline<E: Display>(items: &[E]) -> String {
	items.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
}
