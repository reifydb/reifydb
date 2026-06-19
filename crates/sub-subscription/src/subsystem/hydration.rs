// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::result::Result as StdResult;

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{interface::catalog::shape::ShapeId, metric::StatementMetric, value::column::columns::Columns};
use reifydb_engine::{engine::StandardEngine, subscription::HydrateError};
use reifydb_rql::flow::{flow::FlowDag, node::FlowNodeType};
use reifydb_transaction::transaction::{Transaction, query::QueryTransaction};
use reifydb_value::params::Params;

use super::pushdown::{append_pushdown, walk_for_source_pushdown};

pub(crate) type SourceFrames = Vec<(ShapeId, Vec<Columns>)>;

pub(crate) fn run_source_queries(
	engine: &StandardEngine,
	outer: &mut QueryTransaction,
	sources: Vec<(ShapeId, String)>,
	max_rows: u64,
) -> StdResult<(SourceFrames, Vec<StatementMetric>), HydrateError> {
	let mut total_rows: u64 = 0;
	let mut source_frames: SourceFrames = Vec::with_capacity(sources.len());
	let mut statements: Vec<StatementMetric> = Vec::new();
	for (shape, query_string) in sources {
		let result = engine.query_in_txn(outer, &query_string, Params::None);
		if let Some(err) = result.error {
			return Err(err.into());
		}
		statements.extend(result.metrics.statements);
		let mut shape_columns: Vec<Columns> = Vec::new();
		for frame in result.frames {
			let columns = Columns::from(frame);
			let row_count = columns.row_count() as u64;
			total_rows = total_rows.saturating_add(row_count);
			if total_rows > max_rows {
				return Err(HydrateError::RowCapExceeded {
					cap: max_rows,
				});
			}
			shape_columns.push(columns);
		}
		source_frames.push((shape, shape_columns));
	}
	Ok((source_frames, statements))
}

pub(crate) fn collect_source_descriptors(
	flow: &FlowDag,
	catalog: &Catalog,
	outer: &mut QueryTransaction,
) -> StdResult<Vec<(ShapeId, String)>, HydrateError> {
	let mut txn = Transaction::Query(outer);

	let mut out: Vec<(ShapeId, String)> = Vec::new();
	for node_id in flow.topological_order()? {
		let node = match flow.get_node(&node_id) {
			Some(n) => n,
			None => continue,
		};
		match &node.ty {
			FlowNodeType::SourceTable {
				table,
			} => {
				let t = catalog.get_table(&mut txn, *table)?;
				let ns = catalog.get_namespace(&mut txn, t.namespace)?;
				let mut q = format!("from {}::{}", ns.name(), t.name);
				append_pushdown(&mut q, walk_for_source_pushdown(flow, &node_id));
				out.push((ShapeId::Table(*table), q));
			}
			FlowNodeType::SourceView {
				view,
			} => {
				let v = catalog.get_view(&mut txn, *view)?;
				let ns = catalog.get_namespace(&mut txn, v.namespace())?;
				let mut q = format!("from {}::{}", ns.name(), v.name());
				append_pushdown(&mut q, walk_for_source_pushdown(flow, &node_id));
				out.push((ShapeId::View(*view), q));
			}
			FlowNodeType::SourceRingBuffer {
				ringbuffer,
			} => {
				let r = catalog.get_ringbuffer(&mut txn, *ringbuffer)?;
				let ns = catalog.get_namespace(&mut txn, r.namespace)?;
				let mut q = format!("from {}::{}", ns.name(), r.name);
				append_pushdown(&mut q, walk_for_source_pushdown(flow, &node_id));
				out.push((ShapeId::RingBuffer(*ringbuffer), q));
			}
			_ => {
				if matches!(
					&node.ty,
					FlowNodeType::SourceInlineData { .. }
						| FlowNodeType::SourceFlow { .. } | FlowNodeType::SourceSeries { .. }
				) {
					return Err(HydrateError::UnsupportedSourceType);
				}
			}
		}
	}
	Ok(out)
}
