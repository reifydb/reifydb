// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::{IndexId, NamespaceId};
use reifydb_rql::plan::{physical, physical::PhysicalPlan};

use crate::{
	StandardTransaction,
	execute::{
		ExecutionContext, ExecutionPlan,
		query::{
			aggregate::AggregateNode,
			assign::AssignNode,
			conditional::ConditionalNode,
			declare::DeclareNode,
			extend::{ExtendNode, ExtendWithoutInputNode},
			filter::FilterNode,
			generator::GeneratorNode,
			index_scan::IndexScanNode,
			inline::InlineDataNode,
			join::{InnerJoinNode, LeftJoinNode, NaturalJoinNode},
			map::{MapNode, MapWithoutInputNode},
			ring_buffer_scan::RingBufferScan,
			scalarize::ScalarizeNode,
			sort::SortNode,
			table_scan::TableScanNode,
			table_virtual_scan::VirtualScanNode,
			take::TakeNode,
			view_scan::ViewScanNode,
		},
	},
	table_virtual::{
		TableVirtual, TableVirtualContext,
		system::{
			ColumnPolicies, ColumnsTable, Namespaces, PrimaryKeyColumns, PrimaryKeys, Sequences, Tables,
			Versions, Views,
		},
	},
};

pub(crate) fn compile<'a>(
	plan: PhysicalPlan<'a>,
	rx: &mut StandardTransaction<'a>,
	context: Arc<ExecutionContext<'a>>,
) -> ExecutionPlan<'a> {
	match plan {
		PhysicalPlan::Aggregate(physical::AggregateNode {
			by,
			map,
			input,
		}) => {
			let input_node = Box::new(compile(*input, rx, context.clone()));
			ExecutionPlan::Aggregate(AggregateNode::new(input_node, by, map, context))
		}

		PhysicalPlan::Filter(physical::FilterNode {
			conditions,
			input,
		}) => {
			let input_node = Box::new(compile(*input, rx, context));
			ExecutionPlan::Filter(FilterNode::new(input_node, conditions))
		}

		PhysicalPlan::Take(physical::TakeNode {
			take,
			input,
		}) => {
			let input_node = Box::new(compile(*input, rx, context));
			ExecutionPlan::Take(TakeNode::new(input_node, take))
		}

		PhysicalPlan::Sort(physical::SortNode {
			by,
			input,
		}) => {
			let input_node = Box::new(compile(*input, rx, context));
			ExecutionPlan::Sort(SortNode::new(input_node, by))
		}

		PhysicalPlan::Map(physical::MapNode {
			map,
			input,
		}) => {
			if let Some(input) = input {
				let input_node = Box::new(compile(*input, rx, context));
				ExecutionPlan::Map(MapNode::new(input_node, map))
			} else {
				ExecutionPlan::MapWithoutInput(MapWithoutInputNode::new(map))
			}
		}

		PhysicalPlan::Extend(physical::ExtendNode {
			extend,
			input,
		}) => {
			if let Some(input) = input {
				let input_node = Box::new(compile(*input, rx, context));
				ExecutionPlan::Extend(ExtendNode::new(input_node, extend))
			} else {
				ExecutionPlan::ExtendWithoutInput(ExtendWithoutInputNode::new(extend))
			}
		}

		PhysicalPlan::JoinInner(physical::JoinInnerNode {
			left,
			right,
			on,
			alias,
			strategy: _,
			right_query: _,
		}) => {
			let left_node = Box::new(compile(*left, rx, context.clone()));
			let right_node = Box::new(compile(*right, rx, context.clone()));
			ExecutionPlan::InnerJoin(InnerJoinNode::new(left_node, right_node, on, alias))
		}

		PhysicalPlan::JoinLeft(physical::JoinLeftNode {
			left,
			right,
			on,
			alias,
			strategy: _,
			right_query: _,
		}) => {
			let left_node = Box::new(compile(*left, rx, context.clone()));
			let right_node = Box::new(compile(*right, rx, context.clone()));
			ExecutionPlan::LeftJoin(LeftJoinNode::new(left_node, right_node, on, alias))
		}

		PhysicalPlan::JoinNatural(physical::JoinNaturalNode {
			left,
			right,
			join_type,
			alias,
			strategy: _,
			right_query: _,
		}) => {
			let left_node = Box::new(compile(*left, rx, context.clone()));
			let right_node = Box::new(compile(*right, rx, context.clone()));
			ExecutionPlan::NaturalJoin(NaturalJoinNode::new(left_node, right_node, join_type, alias))
		}

		PhysicalPlan::InlineData(physical::InlineDataNode {
			rows,
		}) => ExecutionPlan::InlineData(InlineDataNode::new(rows, context)),

		PhysicalPlan::Generator(physical::GeneratorNode {
			name,
			expressions,
		}) => ExecutionPlan::Generator(GeneratorNode::new(name, expressions)),

		PhysicalPlan::IndexScan(node) => {
			let table = node.source.def().clone();
			let Some(pk) = table.primary_key.clone() else {
				unimplemented!()
			};

			ExecutionPlan::IndexScan(IndexScanNode::new(table, IndexId::primary(pk.id), context).unwrap())
		}

		PhysicalPlan::TableScan(node) => {
			ExecutionPlan::TableScan(TableScanNode::new(node.source.clone(), context).unwrap())
		}

		PhysicalPlan::ViewScan(node) => {
			ExecutionPlan::ViewScan(ViewScanNode::new(node.source.clone(), context).unwrap())
		}

		PhysicalPlan::RingBufferScan(node) => {
			ExecutionPlan::RingBufferScan(RingBufferScan::new(node.source.clone(), context).unwrap())
		}

		PhysicalPlan::TableVirtualScan(node) => {
			// Create the appropriate virtual table implementation
			let namespace = node.source.namespace().def();
			let table = node.source.def();
			let virtual_table_impl: Box<dyn TableVirtual> = if namespace.id == NamespaceId(1) {
				match table.name.as_str() {
					"sequences" => Box::new(Sequences::new()),
					"namespaces" => Box::new(Namespaces::new()),
					"tables" => Box::new(Tables::new()),
					"views" => Box::new(Views::new()),
					"columns" => Box::new(ColumnsTable::new()),
					"primary_keys" => Box::new(PrimaryKeys::new()),
					"primary_key_columns" => Box::new(PrimaryKeyColumns::new()),
					"column_policies" => Box::new(ColumnPolicies::new()),
					"versions" => Box::new(Versions::new()),
					_ => panic!("Unknown virtual table type: {}", table.name),
				}
			} else {
				panic!("Unknown virtual table type: {}", table.name)
			};

			let virtual_context = node
				.pushdown_context
				.map(|ctx| TableVirtualContext::PushDown {
					filters: ctx.filters,
					projections: ctx.projections,
					order_by: ctx.order_by,
					limit: ctx.limit,
					params: context.params.clone(),
				})
				.unwrap_or(TableVirtualContext::Basic {
					params: context.params.clone(),
				});

			ExecutionPlan::VirtualScan(
				VirtualScanNode::new(virtual_table_impl, context, virtual_context).unwrap(),
			)
		}

		PhysicalPlan::Declare(declare_node) => ExecutionPlan::Declare(DeclareNode::new(declare_node)),

		PhysicalPlan::Assign(assign_node) => ExecutionPlan::Assign(AssignNode::new(assign_node)),

		PhysicalPlan::Conditional(conditional_node) => {
			ExecutionPlan::Conditional(ConditionalNode::new(conditional_node))
		}

		PhysicalPlan::Variable(var_node) => ExecutionPlan::Variable(
			crate::execute::query::variable::VariableNode::new(var_node.variable_expr),
		),

		PhysicalPlan::Scalarize(scalarize_node) => {
			let input = compile(*scalarize_node.input, rx, context.clone());
			ExecutionPlan::Scalarize(ScalarizeNode::new(Box::new(input)))
		}

		PhysicalPlan::AlterSequence(_)
		| PhysicalPlan::AlterTable(_)
		| PhysicalPlan::AlterView(_)
		| PhysicalPlan::CreateDeferredView(_)
		| PhysicalPlan::CreateTransactionalView(_)
		| PhysicalPlan::CreateNamespace(_)
		| PhysicalPlan::CreateTable(_)
		| PhysicalPlan::CreateRingBuffer(_)
		| PhysicalPlan::Delete(_)
		| PhysicalPlan::DeleteRingBuffer(_)
		| PhysicalPlan::InsertTable(_)
		| PhysicalPlan::InsertRingBuffer(_)
		| PhysicalPlan::Update(_)
		| PhysicalPlan::UpdateRingBuffer(_)
		| PhysicalPlan::Distinct(_) => unreachable!(),
		PhysicalPlan::Apply(_) => {
			unimplemented!(
				"Apply operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
			)
		}
		PhysicalPlan::Window(_) => {
			unimplemented!(
				"Window operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
			)
		}
	}
}
