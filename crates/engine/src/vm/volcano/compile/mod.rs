// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod join;
mod transform;
mod vtable;

use std::sync::Arc;

use reifydb_core::interface::catalog::id::IndexId;
use reifydb_evaluate::stack::Variable;
use reifydb_rql::{
	nodes::{
		AggregateNode as RqlAggregateNode, AssertNode as RqlAssertNode, GeneratorNode as RqlGeneratorNode,
		InlineDataNode as RqlInlineDataNode, RowListLookupNode as RqlRowListLookupNode,
		RowPointLookupNode as RqlRowPointLookupNode, RowRangeScanNode as RqlRowRangeScanNode,
		SortNode as RqlSortNode, TakeLimit, TakeNode as RqlTakeNode,
	},
	query::QueryPlan as RqlQueryPlan,
};
use reifydb_transaction::transaction::{ScanLayout, Transaction};
use reifydb_value::fragment::Fragment;
use tracing::instrument;

use super::{
	apply_transform::{ApplyTransformNode, UnknownTransformNode},
	run_tests::RunTestsQueryNode,
};
use crate::vm::volcano::{
	aggregate::AggregateNode,
	append::UnsupportedAppendNode,
	assert::{AssertNode, AssertWithoutInputNode},
	distinct::DistinctNode,
	environment::EnvironmentNode,
	filter::FilterNode,
	generator::GeneratorNode,
	inline::InlineDataNode,
	query::{QueryContext, QueryNode},
	row_lookup::{RowListLookupNode, RowPointLookupNode, RowRangeScanNode},
	scalarize::ScalarizeNode,
	scan::{
		column_series::ColumnSeriesScanNode, column_table::ColumnTableScanNode,
		column_unsupported::UnsupportedColumnScanNode,
		dictionary::DictionaryScanNode, index::IndexScanNode, queue::QueueScan, remote::RemoteFetchNode,
		ringbuffer::RingBufferScan, series::SeriesScanNode as VolcanoSeriesScanNode, table::TableScanNode,
		view::ViewScanNode,
	},
	sort::SortNode,
	take::TakeNode,
	top_k::TopKNode,
	variable::VariableNode,
	window::UnsupportedWindowNode,
};

fn extract_source_name_from_query(plan: &RqlQueryPlan) -> Option<Fragment> {
	match plan {
		RqlQueryPlan::TableScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		RqlQueryPlan::ViewScan(node) => Some(Fragment::internal(node.source.def().name())),
		RqlQueryPlan::RingBufferScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		RqlQueryPlan::DictionaryScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		RqlQueryPlan::SeriesScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		RqlQueryPlan::QueueScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		RqlQueryPlan::RemoteScan(_) => None,
		RqlQueryPlan::Assert(node) => node.input.as_ref().and_then(|p| extract_source_name_from_query(p)),
		RqlQueryPlan::Filter(node) => extract_source_name_from_query(&node.input),
		RqlQueryPlan::Map(node) => node.input.as_ref().and_then(|p| extract_source_name_from_query(p)),
		RqlQueryPlan::Take(node) => extract_source_name_from_query(&node.input),
		_ => None,
	}
}

fn row_store_scan(plan: &RqlQueryPlan) -> Option<String> {
	match plan {
		RqlQueryPlan::ViewScan(node) => Some(format!("view '{}'", node.source.def().name())),
		RqlQueryPlan::RingBufferScan(node) => Some(format!("ring buffer '{}'", node.source.def().name)),
		RqlQueryPlan::QueueScan(node) => Some(format!("queue '{}'", node.source.def().name)),
		RqlQueryPlan::DictionaryScan(node) => Some(format!("dictionary '{}'", node.source.def().name)),
		RqlQueryPlan::IndexScan(node) => Some(format!("index scan of table '{}'", node.source.def().name)),
		RqlQueryPlan::RowPointLookup(node) => Some(format!("row lookup on '{}'", node.source.name())),
		RqlQueryPlan::RowListLookup(node) => Some(format!("row lookup on '{}'", node.source.name())),
		RqlQueryPlan::RowRangeScan(node) => Some(format!("row range scan on '{}'", node.source.name())),
		_ => None,
	}
}

#[instrument(name = "volcano::compile", level = "debug", skip_all)]
pub(crate) fn compile<'a>(
	plan: RqlQueryPlan,
	rx: &mut Transaction<'a>,
	context: Arc<QueryContext>,
) -> Box<dyn QueryNode> {
	if rx.layout() == ScanLayout::Column
		&& let Some(what) = row_store_scan(&plan)
	{
		return Box::new(UnsupportedColumnScanNode::new(what));
	}
	match plan {
		RqlQueryPlan::Aggregate(RqlAggregateNode {
			by,
			map,
			input,
			..
		}) => {
			let input_node = compile(*input, rx, context.clone());
			Box::new(AggregateNode::new(input_node, by, map, context))
		}
		RqlQueryPlan::Distinct(node) => {
			let input = compile(*node.input, rx, context);
			Box::new(DistinctNode::new(input, node.columns))
		}

		RqlQueryPlan::Filter(node) => transform::compile_filter(node, rx, context),
		RqlQueryPlan::Gate(node) => {
			let input_node = compile(*node.input, rx, context);
			Box::new(FilterNode::new(input_node, node.conditions))
		}
		RqlQueryPlan::Map(node) => transform::compile_map(node, rx, context),
		RqlQueryPlan::Extend(node) => transform::compile_extend(node, rx, context),
		RqlQueryPlan::Patch(node) => transform::compile_patch(node, rx, context),

		RqlQueryPlan::Sort(RqlSortNode {
			by,
			input,
		}) => {
			let input_node = compile(*input, rx, context);
			Box::new(SortNode::new(input_node, by))
		}
		RqlQueryPlan::Take(RqlTakeNode {
			take,
			input,
		}) => {
			let limit = match take {
				TakeLimit::Literal(n) => n,
				TakeLimit::Variable(ref name) => context
					.symbols
					.get(name)
					.and_then(|var| match var {
						Variable::Columns {
							columns: cols,
							..
						} => cols.scalar_value().to_usize(),
						_ => None,
					})
					.unwrap_or_else(|| panic!("TAKE variable ${} must be a numeric value", name)),
			};

			if let RqlQueryPlan::Sort(sort_node) = *input {
				let input_node = compile(*sort_node.input, rx, context);
				return Box::new(TopKNode::new(input_node, sort_node.by, limit));
			}
			let input_node = compile(*input, rx, context);
			Box::new(TakeNode::new(input_node, limit))
		}

		RqlQueryPlan::JoinInner(node) => join::compile_inner_join(node, rx, context),
		RqlQueryPlan::JoinLeft(node) => join::compile_left_join(node, rx, context),
		RqlQueryPlan::JoinNatural(node) => join::compile_natural_join(node, rx, context),

		RqlQueryPlan::Assert(RqlAssertNode {
			conditions,
			input,
			message,
		}) => {
			if let Some(input) = input {
				let input_node = compile(*input, rx, context);
				Box::new(AssertNode::new(input_node, conditions, message))
			} else {
				Box::new(AssertWithoutInputNode::new(conditions, message))
			}
		}

		RqlQueryPlan::TableScan(node) => match rx.layout() {
			ScanLayout::Row => {
				Box::new(TableScanNode::new(node.source.clone(), node.partition, context, rx).unwrap())
			}
			ScanLayout::Column if node.partition.is_some() => Box::new(UnsupportedColumnScanNode::new(format!(
				"a partition scan of table '{}'",
				node.source.fully_qualified_name()
			))),
			ScanLayout::Column => Box::new(ColumnTableScanNode::new(node.source.clone(), context)),
		},
		RqlQueryPlan::ViewScan(node) => {
			Box::new(ViewScanNode::new(node.source.clone(), node.partition, context, rx).unwrap())
		}
		RqlQueryPlan::RingBufferScan(node) => {
			Box::new(RingBufferScan::new(node.source.clone(), context, rx).unwrap())
		}
		RqlQueryPlan::QueueScan(node) => Box::new(QueueScan::new(node.source.clone(), context, rx).unwrap()),
		RqlQueryPlan::DictionaryScan(node) => {
			Box::new(DictionaryScanNode::new(node.source.clone(), context).unwrap())
		}
		RqlQueryPlan::SeriesScan(node) => match rx.layout() {
			ScanLayout::Row => Box::new(
				VolcanoSeriesScanNode::new(
					node.source.clone(),
					node.key_range_start,
					node.key_range_end,
					node.variant_tag,
					node.partition,
					context,
				)
				.unwrap(),
			),
			ScanLayout::Column => Box::new(ColumnSeriesScanNode::new(
				node.source.clone(),
				node.key_range_start,
				node.key_range_end,
				node.variant_tag,
				node.partition,
				context,
			)),
		},
		RqlQueryPlan::IndexScan(node) => {
			let table = node.source.def().clone();
			let Some(pk) = table.primary_key.clone() else {
				unimplemented!()
			};
			Box::new(IndexScanNode::new(table, IndexId::primary(pk.id), context).unwrap())
		}
		RqlQueryPlan::RemoteScan(node) => {
			Box::new(RemoteFetchNode::new(node.address, node.token, node.remote_rql, node.variables))
		}
		RqlQueryPlan::TableVirtualScan(node) => vtable::compile_virtual_scan(node, context),

		RqlQueryPlan::RowPointLookup(RqlRowPointLookupNode {
			source,
			row_number,
		}) => {
			let resolved_source = source;
			Box::new(
				RowPointLookupNode::new(resolved_source, row_number, context)
					.expect("Failed to create RowPointLookupNode"),
			)
		}
		RqlQueryPlan::RowListLookup(RqlRowListLookupNode {
			source,
			row_numbers,
		}) => {
			let resolved_source = source;
			Box::new(
				RowListLookupNode::new(resolved_source, row_numbers, context)
					.expect("Failed to create RowListLookupNode"),
			)
		}
		RqlQueryPlan::RowRangeScan(RqlRowRangeScanNode {
			source,
			start,
			end,
		}) => {
			let resolved_source = source;
			Box::new(
				RowRangeScanNode::new(resolved_source, start, end, context)
					.expect("Failed to create RowRangeScanNode"),
			)
		}

		RqlQueryPlan::InlineData(RqlInlineDataNode {
			rows,
		}) => Box::new(InlineDataNode::new(rows, context)),
		RqlQueryPlan::Generator(RqlGeneratorNode {
			name,
			expressions,
		}) => Box::new(GeneratorNode::new(name, expressions)),
		RqlQueryPlan::Variable(node) => Box::new(VariableNode::new(node.variable_expr)),
		RqlQueryPlan::Environment(_) => Box::new(EnvironmentNode::new()),
		RqlQueryPlan::Scalarize(node) => {
			let input = compile(*node.input, rx, context.clone());
			Box::new(ScalarizeNode::new(input))
		}
		RqlQueryPlan::Apply(node) => {
			let Some(transform) = context.services.transforms.get_transform(node.operator.text()) else {
				return Box::new(UnknownTransformNode::new(node.operator));
			};
			let input = node.input.expect("Apply requires input");
			let input_node = compile(*input, rx, context);
			Box::new(ApplyTransformNode::new(input_node, transform))
		}
		RqlQueryPlan::RunTests(node) => Box::new(RunTestsQueryNode::new(node, context.clone())),
		RqlQueryPlan::CallFunction(node) => Box::new(GeneratorNode::new(node.name, node.arguments)),

		RqlQueryPlan::Window(node) => Box::new(UnsupportedWindowNode::new(node.fragment)),
		RqlQueryPlan::Append(node) => Box::new(UnsupportedAppendNode::new(node.fragment)),
	}
}
