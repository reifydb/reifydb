// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// TODO(index-metrics): per-index storage/cdc stats require `KeyKind::Index` /
// `KeyKind::IndexEntry` to carry `IndexId` in their key layout. Add
// `system::metrics::storage::index` and `system::metrics::cdc::index` when
// that lands.

pub mod cdc;
pub mod storage;

use reifydb_core::interface::catalog::{id::NamespaceId, shape::ShapeId};
use reifydb_metric::MetricId;
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, vtable::VTableRegistry};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsPrimitive {
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

impl StatsPrimitive {
	pub(crate) fn match_metric_id(
		self,
		txn: &mut Transaction<'_>,
		metric_id: MetricId,
	) -> Result<Option<StatsRow>> {
		match (self, metric_id) {
			(StatsPrimitive::Table, MetricId::Shape(ShapeId::Table(id))) => {
				let namespace_id = CatalogStore::find_table(txn, id)?.map_or(0, |t| t.namespace.0);
				Ok(Some(StatsRow {
					id: id.0,
					namespace_id,
				}))
			}
			(StatsPrimitive::View, MetricId::Shape(ShapeId::View(id))) => {
				let namespace_id = CatalogStore::find_view(txn, id)?.map_or(0, |v| v.namespace().0);
				Ok(Some(StatsRow {
					id: id.0,
					namespace_id,
				}))
			}
			(StatsPrimitive::TableVirtual, MetricId::Shape(ShapeId::TableVirtual(id))) => {
				let namespace_id = VTableRegistry::find_vtable(txn, id)?.map_or(0, |vt| vt.namespace.0);
				Ok(Some(StatsRow {
					id: id.0,
					namespace_id,
				}))
			}
			(StatsPrimitive::RingBuffer, MetricId::Shape(ShapeId::RingBuffer(id))) => {
				let namespace_id = CatalogStore::find_ringbuffer(txn, id)?.map_or(0, |r| r.namespace.0);
				Ok(Some(StatsRow {
					id: id.0,
					namespace_id,
				}))
			}
			(StatsPrimitive::Dictionary, MetricId::Shape(ShapeId::Dictionary(id))) => {
				let namespace_id = CatalogStore::find_dictionary(txn, id)?.map_or(0, |d| d.namespace.0);
				Ok(Some(StatsRow {
					id: id.0,
					namespace_id,
				}))
			}
			(StatsPrimitive::Series, MetricId::Shape(ShapeId::Series(id))) => {
				let namespace_id = CatalogStore::find_series(txn, id)?.map_or(0, |s| s.namespace.0);
				Ok(Some(StatsRow {
					id: id.0,
					namespace_id,
				}))
			}
			(StatsPrimitive::Flow, MetricId::FlowNode(flow_node_id)) => {
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
			(StatsPrimitive::FlowNode, MetricId::FlowNode(flow_node_id)) => {
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
			(StatsPrimitive::System, MetricId::System) => Ok(Some(StatsRow {
				id: 0,
				namespace_id: NamespaceId::SYSTEM.0,
			})),
			_ => Ok(None),
		}
	}
}
