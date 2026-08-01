// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{id::NamespaceId, metrics::MetricsId, object::ObjectId};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, vtable::VTableRegistry};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsObject {
	Table,
	View,
	TableVirtual,
	RingBuffer,
	Dictionary,
	Series,
	Flow,
	Operator,
	System,
}

impl MetricsObject {
	pub fn name(self) -> &'static str {
		match self {
			MetricsObject::Table => "table",
			MetricsObject::View => "view",
			MetricsObject::TableVirtual => "table_virtual",
			MetricsObject::RingBuffer => "ringbuffer",
			MetricsObject::Dictionary => "dictionary",
			MetricsObject::Series => "series",
			MetricsObject::Flow => "flow",
			MetricsObject::Operator => "operator",
			MetricsObject::System => "system",
		}
	}

	pub fn resolve(txn: &mut Transaction<'_>, metric_id: MetricsId) -> Result<Option<ResolvedMetric>> {
		match metric_id {
			MetricsId::Object(ObjectId::Table(id)) => {
				let namespace_id = CatalogStore::find_table(txn, id)?.map_or(0, |t| t.namespace.0);
				Ok(Some(ResolvedMetric::plain(MetricsObject::Table, id.0, namespace_id)))
			}
			MetricsId::Object(ObjectId::View(id)) => {
				let namespace_id = CatalogStore::find_view(txn, id)?.map_or(0, |v| v.namespace().0);
				Ok(Some(ResolvedMetric::plain(MetricsObject::View, id.0, namespace_id)))
			}
			MetricsId::Object(ObjectId::TableVirtual(id)) => {
				let namespace_id = VTableRegistry::find_vtable(txn, id)?.map_or(0, |vt| vt.namespace.0);
				Ok(Some(ResolvedMetric::plain(MetricsObject::TableVirtual, id.0, namespace_id)))
			}
			MetricsId::Object(ObjectId::RingBuffer(id)) => {
				let namespace_id = CatalogStore::find_ringbuffer(txn, id)?.map_or(0, |r| r.namespace.0);
				Ok(Some(ResolvedMetric::plain(MetricsObject::RingBuffer, id.0, namespace_id)))
			}
			MetricsId::Object(ObjectId::Dictionary(id)) => {
				let namespace_id = CatalogStore::find_dictionary(txn, id)?.map_or(0, |d| d.namespace.0);
				Ok(Some(ResolvedMetric::plain(MetricsObject::Dictionary, id.0, namespace_id)))
			}
			MetricsId::Object(ObjectId::Series(id)) => {
				let namespace_id = CatalogStore::find_series(txn, id)?.map_or(0, |s| s.namespace.0);
				Ok(Some(ResolvedMetric::plain(MetricsObject::Series, id.0, namespace_id)))
			}
			MetricsId::Operator(operator_id) => {
				let Some(operator) = CatalogStore::find_operator(txn, operator_id)? else {
					return Ok(None);
				};
				let flow_id = operator.flow;
				let namespace_id = CatalogStore::find_flow(txn, flow_id)?.map_or(0, |f| f.namespace.0);
				Ok(Some(ResolvedMetric {
					object: MetricsObject::Operator,
					id: operator_id.0,
					namespace_id,
					flow: Some((flow_id.0, namespace_id)),
				}))
			}
			MetricsId::System => {
				Ok(Some(ResolvedMetric::plain(MetricsObject::System, 0, NamespaceId::SYSTEM.0)))
			}
			_ => Ok(None),
		}
	}
}

#[derive(Debug, Clone, Copy)]
pub struct ResolvedMetric {
	pub object: MetricsObject,
	pub id: u64,
	pub namespace_id: u64,
	pub flow: Option<(u64, u64)>,
}

impl ResolvedMetric {
	fn plain(object: MetricsObject, id: u64, namespace_id: u64) -> Self {
		Self {
			object,
			id,
			namespace_id,
			flow: None,
		}
	}
}
