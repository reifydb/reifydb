// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

mod join;
mod transform;
mod vtable;

use std::sync::Arc;

use reifydb_core::interface::{catalog::id::IndexId, resolved::ResolvedShape};
use reifydb_rql::{
	nodes::{
		AggregateNode as RqlAggregateNode, AssertNode as RqlAssertNode, GeneratorNode as RqlGeneratorNode,
		InlineDataNode as RqlInlineDataNode, RowListLookupNode as RqlRowListLookupNode,
		RowPointLookupNode as RqlRowPointLookupNode, RowRangeScanNode as RqlRowRangeScanNode,
		SortNode as RqlSortNode, TakeLimit, TakeNode as RqlTakeNode,
	},
	query::QueryPlan as RqlQueryPlan,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;
use tracing::instrument;

use super::{apply_transform::ApplyTransformNode, run_tests::RunTestsQueryNode};
use crate::vm::{
	stack::Variable,
	volcano::{
		aggregate::AggregateNode,
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
			dictionary::DictionaryScanNode, index::IndexScanNode, remote::RemoteFetchNode,
			ringbuffer::RingBufferScan, series::SeriesScanNode as VolcanoSeriesScanNode,
			table::TableScanNode, view::ViewScanNode,
		},
		sort::SortNode,
		take::TakeNode,
		top_k::TopKNode,
		variable::VariableNode,
	},
};

// Helpers

/// Extract the source name from a query plan if it's a scan node.
fn extract_source_name_from_query(plan: &RqlQueryPlan) -> Option<Fragment> {
	match plan {
		RqlQueryPlan::TableScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		RqlQueryPlan::ViewScan(node) => Some(Fragment::internal(node.source.def().name().to_string())),
		RqlQueryPlan::RingBufferScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		RqlQueryPlan::DictionaryScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		RqlQueryPlan::SeriesScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		RqlQueryPlan::RemoteScan(_) => None,
		RqlQueryPlan::Assert(node) => node.input.as_ref().and_then(|p| extract_source_name_from_query(p)),
		RqlQueryPlan::Filter(node) => extract_source_name_from_query(&node.input),
		RqlQueryPlan::Map(node) => node.input.as_ref().and_then(|p| extract_source_name_from_query(p)),
		RqlQueryPlan::Take(node) => extract_source_name_from_query(&node.input),
		_ => None,
	}
}

pub(crate) fn extract_resolved_source(plan: &RqlQueryPlan) -> Option<ResolvedShape> {
	match plan {
		RqlQueryPlan::TableScan(node) => Some(ResolvedShape::Table(node.source.clone())),
		RqlQueryPlan::ViewScan(node) => Some(ResolvedShape::View(node.source.clone())),
		RqlQueryPlan::RingBufferScan(node) => Some(ResolvedShape::RingBuffer(node.source.clone())),
		RqlQueryPlan::DictionaryScan(node) => Some(ResolvedShape::Dictionary(node.source.clone())),
		RqlQueryPlan::SeriesScan(node) => Some(ResolvedShape::Series(node.source.clone())),
		RqlQueryPlan::RemoteScan(_) => None,
		RqlQueryPlan::Filter(node) => extract_resolved_source(&node.input),
		RqlQueryPlan::Assert(node) => node.input.as_ref().and_then(|p| extract_resolved_source(p)),
		RqlQueryPlan::Map(node) => node.input.as_ref().and_then(|p| extract_resolved_source(p)),
		RqlQueryPlan::Take(node) => extract_resolved_source(&node.input),
		RqlQueryPlan::Sort(node) => extract_resolved_source(&node.input),
		_ => None,
	}
}

// Main compile function

#[instrument(name = "volcano::compile", level = "debug", skip_all)]
pub(crate) fn compile<'a>(
	plan: RqlQueryPlan,
	rx: &mut Transaction<'a>,
	context: Arc<QueryContext>,
) -> Box<dyn QueryNode> {
	match plan {
		RqlQueryPlan::Aggregate(RqlAggregateNode {
			by,
			map,
			input,
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
						Variable::Scalar(cols) | Variable::Columns(cols) => {
							cols.scalar_value().to_usize()
						}
						_ => None,
					})
					.unwrap_or_else(|| panic!("TAKE variable ${} must be a numeric value", name)),
			};
			// Optimize: TAKE over SORT becomes TopK
			if let RqlQueryPlan::Sort(sort_node) = *input {
				let input_node = compile(*sort_node.input, rx, context);
				return Box::new(TopKNode::new(input_node, sort_node.by, limit));
			}
			let mut input_node = compile(*input, rx, context);
			// Push limit hint into scan operators so they read fewer rows
			input_node.set_scan_limit(limit);
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

		RqlQueryPlan::TableScan(node) => {
			Box::new(TableScanNode::new(node.source.clone(), context, rx).unwrap())
		}
		RqlQueryPlan::ViewScan(node) => Box::new(ViewScanNode::new(node.source.clone(), context).unwrap()),
		RqlQueryPlan::RingBufferScan(node) => {
			Box::new(RingBufferScan::new(node.source.clone(), context, rx).unwrap())
		}
		RqlQueryPlan::DictionaryScan(node) => {
			Box::new(DictionaryScanNode::new(node.source.clone(), context).unwrap())
		}
		RqlQueryPlan::SeriesScan(node) => Box::new(
			VolcanoSeriesScanNode::new(
				node.source.clone(),
				node.key_range_start,
				node.key_range_end,
				node.variant_tag,
				context,
			)
			.unwrap(),
		),
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
			let operator_name = node.operator.text().to_string();
			let transform = context
				.services
				.transforms
				.get_transform(&operator_name)
				.unwrap_or_else(|| panic!("Unknown transform: {}", operator_name));
			let input = node.input.expect("Apply requires input");
			let input_node = compile(*input, rx, context);
			Box::new(ApplyTransformNode::new(input_node, transform))
		}
		RqlQueryPlan::RunTests(node) => Box::new(RunTestsQueryNode::new(node, context.clone())),
		RqlQueryPlan::CallFunction(node) => Box::new(GeneratorNode::new(node.name, node.arguments)),

		RqlQueryPlan::Window(_) => {
			unimplemented!(
				"Window operator is only supported in deferred views and requires the flow engine."
			)
		}
		RqlQueryPlan::Append(_) => {
			unimplemented!(
				"Append operator is only supported in deferred views and requires the flow engine."
			)
		}
	}
}
