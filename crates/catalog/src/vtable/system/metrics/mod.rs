// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// TODO(index-metrics): per-index storage/cdc stats require `KeyKind::Index` /
// `KeyKind::IndexEntry` to carry `IndexId` in their key layout. Add
// `system::metrics::storage::index::current` and `system::metrics::cdc::index::current`
// when that lands.

pub mod cdc;
pub mod storage;

use reifydb_core::interface::catalog::{id::NamespaceId, shape::ShapeId};
use reifydb_metrics::MetricsId;
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, vtable::VTableRegistry};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsPrimitive {
	Table,
	View,
	TableVirtual,
	RingBuffer,
	Dictionary,
	Series,
	Flow,
	FlowNode,
	System,
}

pub(crate) struct StatsRow {
	pub id: u64,
	pub namespace_id: u64,
}

impl MetricsPrimitive {
	pub(crate) fn match_metric_id(
		self,
		txn: &mut Transaction<'_>,
		metric_id: MetricsId,
	) -> Result<Option<StatsRow>> {
		match (self, metric_id) {
			(MetricsPrimitive::Table, MetricsId::Shape(ShapeId::Table(id))) => {
				let namespace_id = CatalogStore::find_table(txn, id)?.map_or(0, |t| t.namespace.0);
				Ok(Some(StatsRow {
					id: id.0,
					namespace_id,
				}))
			}
			(MetricsPrimitive::View, MetricsId::Shape(ShapeId::View(id))) => {
				let namespace_id = CatalogStore::find_view(txn, id)?.map_or(0, |v| v.namespace().0);
				Ok(Some(StatsRow {
					id: id.0,
					namespace_id,
				}))
			}
			(MetricsPrimitive::TableVirtual, MetricsId::Shape(ShapeId::TableVirtual(id))) => {
				let namespace_id = VTableRegistry::find_vtable(txn, id)?.map_or(0, |vt| vt.namespace.0);
				Ok(Some(StatsRow {
					id: id.0,
					namespace_id,
				}))
			}
			(MetricsPrimitive::RingBuffer, MetricsId::Shape(ShapeId::RingBuffer(id))) => {
				let namespace_id = CatalogStore::find_ringbuffer(txn, id)?.map_or(0, |r| r.namespace.0);
				Ok(Some(StatsRow {
					id: id.0,
					namespace_id,
				}))
			}
			(MetricsPrimitive::Dictionary, MetricsId::Shape(ShapeId::Dictionary(id))) => {
				let namespace_id = CatalogStore::find_dictionary(txn, id)?.map_or(0, |d| d.namespace.0);
				Ok(Some(StatsRow {
					id: id.0,
					namespace_id,
				}))
			}
			(MetricsPrimitive::Series, MetricsId::Shape(ShapeId::Series(id))) => {
				let namespace_id = CatalogStore::find_series(txn, id)?.map_or(0, |s| s.namespace.0);
				Ok(Some(StatsRow {
					id: id.0,
					namespace_id,
				}))
			}
			(MetricsPrimitive::Flow, MetricsId::FlowNode(flow_node_id)) => {
				let Some(node) = CatalogStore::find_flow_node(txn, flow_node_id)? else {
					return Ok(None);
				};
				let flow_id = node.flow;
				let namespace_id = CatalogStore::find_flow(txn, flow_id)?.map_or(0, |f| f.namespace.0);
				Ok(Some(StatsRow {
					id: flow_id.0,
					namespace_id,
				}))
			}
			(MetricsPrimitive::FlowNode, MetricsId::FlowNode(flow_node_id)) => {
				let Some(node) = CatalogStore::find_flow_node(txn, flow_node_id)? else {
					return Ok(None);
				};
				let namespace_id =
					CatalogStore::find_flow(txn, node.flow)?.map_or(0, |f| f.namespace.0);
				Ok(Some(StatsRow {
					id: flow_node_id.0,
					namespace_id,
				}))
			}
			(MetricsPrimitive::System, MetricsId::System) => Ok(Some(StatsRow {
				id: 0,
				namespace_id: NamespaceId::SYSTEM.0,
			})),
			_ => Ok(None),
		}
	}
}
