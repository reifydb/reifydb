// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::Catalog;
use reifydb_core::interface::VersionedQueryTransaction;
use reifydb_rql::plan::{physical, physical::PhysicalPlan};

use crate::execute::{
	ExecutionContext, ExecutionPlan,
	query::{
		aggregate::AggregateNode,
		filter::FilterNode,
		inline::InlineDataNode,
		join_inner::InnerJoinNode,
		join_left::LeftJoinNode,
		join_natural::NaturalJoinNode,
		map::{MapNode, MapWithoutInputNode},
		scan::ScanColumnsNode,
		sort::SortNode,
		take::TakeNode,
	},
};

pub(crate) fn compile(
	plan: PhysicalPlan,
	rx: &mut impl VersionedQueryTransaction,
	context: Arc<ExecutionContext>,
) -> Box<dyn ExecutionPlan> {
	match plan {
		PhysicalPlan::Aggregate(physical::AggregateNode {
			by,
			map,
			input,
		}) => {
			let input_node = compile(*input, rx, context.clone());
			Box::new(AggregateNode::new(
				input_node, by, map, context,
			))
		}

		PhysicalPlan::Filter(physical::FilterNode {
			conditions,
			input,
		}) => {
			let input_node = compile(*input, rx, context);
			Box::new(FilterNode::new(input_node, conditions))
		}

		PhysicalPlan::Take(physical::TakeNode {
			take,
			input,
		}) => {
			let input_node = compile(*input, rx, context);
			Box::new(TakeNode::new(input_node, take))
		}

		PhysicalPlan::Sort(physical::SortNode {
			by,
			input,
		}) => {
			let input_node = compile(*input, rx, context);
			Box::new(SortNode::new(input_node, by))
		}

		PhysicalPlan::Map(physical::MapNode {
			map,
			input,
		}) => {
			if let Some(input) = input {
				let input_node = compile(*input, rx, context);
				Box::new(MapNode::new(input_node, map))
			} else {
				Box::new(MapWithoutInputNode::new(map))
			}
		}

		PhysicalPlan::JoinInner(physical::JoinInnerNode {
			left,
			right,
			on,
		}) => {
			let left_node = compile(*left, rx, context.clone());
			let right_node = compile(*right, rx, context.clone());
			Box::new(InnerJoinNode::new(left_node, right_node, on))
		}

		PhysicalPlan::JoinLeft(physical::JoinLeftNode {
			left,
			right,
			on,
		}) => {
			let left_node = compile(*left, rx, context.clone());
			let right_node = compile(*right, rx, context.clone());
			Box::new(LeftJoinNode::new(left_node, right_node, on))
		}

		PhysicalPlan::JoinNatural(physical::JoinNaturalNode {
			left,
			right,
			join_type,
		}) => {
			let left_node = compile(*left, rx, context.clone());
			let right_node = compile(*right, rx, context.clone());
			Box::new(NaturalJoinNode::new(
				left_node, right_node, join_type,
			))
		}

		PhysicalPlan::InlineData(physical::InlineDataNode {
			rows,
		}) => Box::new(InlineDataNode::new(rows, context)),

		PhysicalPlan::TableScan(physical::TableScanNode {
			schema,
			table,
		}) => {
			// FIXME If schema is NONE resolve table directly by
			// name
			let schema = Catalog::get_schema_by_name(
				rx,
				&schema.as_ref().unwrap().fragment.as_str(),
			)
			.unwrap()
			.unwrap();

			let table = Catalog::get_table_by_name(
				rx,
				schema.id,
				&table.fragment.as_str(),
			)
			.unwrap()
			.unwrap();

			Box::new(ScanColumnsNode::new(table, context).unwrap())
		}
		PhysicalPlan::AlterSequence(_)
		| PhysicalPlan::CreateComputedView(_)
		| PhysicalPlan::CreateSchema(_)
		| PhysicalPlan::CreateTable(_)
		| PhysicalPlan::Delete(_)
		| PhysicalPlan::Insert(_)
		| PhysicalPlan::Update(_) => unreachable!(),
	}
}
