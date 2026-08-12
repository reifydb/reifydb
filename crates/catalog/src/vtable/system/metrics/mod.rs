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
			MetricsObject::System => "system",
		}
	}

	pub fn resolve(txn: &mut Transaction<'_>, metric_id: MetricsId) -> Result<Option<ResolvedMetric>> {
		match metric_id {
			MetricsId::Object(ObjectId::Table(id)) => Ok(CatalogStore::find_table(txn, id)?
				.map(|t| ResolvedMetric::plain(MetricsObject::Table, id.0, t.namespace.0))),
			MetricsId::Object(ObjectId::View(id)) => Ok(CatalogStore::find_view(txn, id)?
				.map(|v| ResolvedMetric::plain(MetricsObject::View, id.0, v.namespace().0))),
			MetricsId::Object(ObjectId::TableVirtual(id)) => Ok(VTableRegistry::find_vtable(txn, id)?
				.map(|vt| ResolvedMetric::plain(MetricsObject::TableVirtual, id.0, vt.namespace.0))),
			MetricsId::Object(ObjectId::RingBuffer(id)) => Ok(CatalogStore::find_ringbuffer(txn, id)?
				.map(|r| ResolvedMetric::plain(MetricsObject::RingBuffer, id.0, r.namespace.0))),
			MetricsId::Object(ObjectId::Dictionary(id)) => Ok(CatalogStore::find_dictionary(txn, id)?
				.map(|d| ResolvedMetric::plain(MetricsObject::Dictionary, id.0, d.namespace.0))),
			MetricsId::Object(ObjectId::Series(id)) => Ok(CatalogStore::find_series(txn, id)?
				.map(|s| ResolvedMetric::plain(MetricsObject::Series, id.0, s.namespace.0))),
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
}

impl ResolvedMetric {
	fn plain(object: MetricsObject, id: u64, namespace_id: u64) -> Self {
		Self {
			object,
			id,
			namespace_id,
		}
	}
}
