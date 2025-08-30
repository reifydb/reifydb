// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::{QueryTransaction, SchemaId, Transaction};
use reifydb_rql::plan::{physical, physical::PhysicalPlan};

use crate::{
	execute::{
		ExecutionContext, ExecutionPlan,
		query::{
			aggregate::AggregateNode,
			extend::{ExtendNode, ExtendWithoutInputNode},
			filter::FilterNode,
			inline::InlineDataNode,
			join_inner::InnerJoinNode,
			join_left::LeftJoinNode,
			join_natural::NaturalJoinNode,
			map::{MapNode, MapWithoutInputNode},
			sort::SortNode,
			table_scan::TableScanNode,
			take::TakeNode,
			view_scan::ViewScanNode,
			virtual_table_scan::VirtualScanNode,
		},
	},
	virtual_table::{VirtualTable, system::Sequences},
};

pub(crate) fn compile(
	plan: PhysicalPlan,
	rx: &mut impl QueryTransaction,
	context: Arc<ExecutionContext>,
) -> ExecutionPlan {
	match plan {
		PhysicalPlan::Aggregate(physical::AggregateNode {
			by,
			map,
			input,
		}) => {
			let input_node =
				Box::new(compile(*input, rx, context.clone()));
			ExecutionPlan::Aggregate(AggregateNode::new(
				input_node, by, map, context,
			))
		}

		PhysicalPlan::Filter(physical::FilterNode {
			conditions,
			input,
		}) => {
			let input_node = Box::new(compile(*input, rx, context));
			ExecutionPlan::Filter(FilterNode::new(
				input_node, conditions,
			))
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
				let input_node =
					Box::new(compile(*input, rx, context));
				ExecutionPlan::Map(MapNode::new(
					input_node, map,
				))
			} else {
				ExecutionPlan::MapWithoutInput(
					MapWithoutInputNode::new(map),
				)
			}
		}

		PhysicalPlan::Extend(physical::ExtendNode {
			extend,
			input,
		}) => {
			if let Some(input) = input {
				let input_node =
					Box::new(compile(*input, rx, context));
				ExecutionPlan::Extend(ExtendNode::new(
					input_node, extend,
				))
			} else {
				ExecutionPlan::ExtendWithoutInput(
					ExtendWithoutInputNode::new(extend),
				)
			}
		}

		PhysicalPlan::JoinInner(physical::JoinInnerNode {
			left,
			right,
			on,
		}) => {
			let left_node =
				Box::new(compile(*left, rx, context.clone()));
			let right_node =
				Box::new(compile(*right, rx, context.clone()));
			ExecutionPlan::InnerJoin(InnerJoinNode::new(
				left_node, right_node, on,
			))
		}

		PhysicalPlan::JoinLeft(physical::JoinLeftNode {
			left,
			right,
			on,
		}) => {
			let left_node =
				Box::new(compile(*left, rx, context.clone()));
			let right_node =
				Box::new(compile(*right, rx, context.clone()));
			ExecutionPlan::LeftJoin(LeftJoinNode::new(
				left_node, right_node, on,
			))
		}

		PhysicalPlan::JoinNatural(physical::JoinNaturalNode {
			left,
			right,
			join_type,
		}) => {
			let left_node =
				Box::new(compile(*left, rx, context.clone()));
			let right_node =
				Box::new(compile(*right, rx, context.clone()));
			ExecutionPlan::NaturalJoin(NaturalJoinNode::new(
				left_node, right_node, join_type,
			))
		}

		PhysicalPlan::InlineData(physical::InlineDataNode {
			rows,
		}) => ExecutionPlan::InlineData(InlineDataNode::new(
			rows, context,
		)),

		PhysicalPlan::TableScan(physical::TableScanNode {
			schema: _,
			table,
		}) => ExecutionPlan::TableScan(
			TableScanNode::new(table, context).unwrap(),
		),

		PhysicalPlan::ViewScan(physical::ViewScanNode {
			schema: _,
			view,
		}) => ExecutionPlan::ViewScan(
			ViewScanNode::new(view, context).unwrap(),
		),

		PhysicalPlan::VirtualScan(physical::VirtualScanNode {
			schema,
			virtual_table,
		}) => {
			// Create the appropriate virtual table implementation
			let virtual_table_impl: Box<dyn VirtualTable> =
				if schema.id == SchemaId(1)
					&& virtual_table.name == "sequences"
				{
					Box::new(Sequences::new(virtual_table))
				} else {
					panic!(
						"Unknown virtual table type: {}",
						virtual_table.name
					)
				};

			ExecutionPlan::VirtualScan(
				VirtualScanNode::new(
					virtual_table_impl,
					context,
				)
				.unwrap(),
			)
		}

		PhysicalPlan::AlterSequence(_)
		| PhysicalPlan::CreateDeferredView(_)
		| PhysicalPlan::CreateTransactionalView(_)
		| PhysicalPlan::CreateSchema(_)
		| PhysicalPlan::CreateTable(_)
		| PhysicalPlan::Delete(_)
		| PhysicalPlan::Insert(_)
		| PhysicalPlan::Update(_)
		| PhysicalPlan::Distinct(_) => unreachable!(),
	}
}
