// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Catalog entity resolution methods.
//!
//! These methods resolve catalog entity IDs to their fully resolved
//! counterparts, including namespace resolution and identifier creation.

use reifydb_core::interface::{
	catalog::{
		flow::FlowId,
		id::{NamespaceId, RingBufferId, TableId, ViewId},
	},
	resolved::{ResolvedFlow, ResolvedNamespace, ResolvedRingBuffer, ResolvedTable, ResolvedView},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;
use tracing::instrument;

use super::Catalog;

impl Catalog {
	/// Resolve a namespace ID to a fully resolved namespace with identifier
	#[instrument(name = "catalog::resolve::namespace", level = "trace", skip(self, txn))]
	pub fn resolve_namespace(
		&self,
		txn: &mut Transaction<'_>,
		namespace_id: NamespaceId,
	) -> crate::Result<ResolvedNamespace> {
		let def = self.get_namespace(txn, namespace_id)?;
		let ident = Fragment::internal(def.name.clone());
		Ok(ResolvedNamespace::new(ident, def))
	}

	/// Resolve a table ID to a fully resolved table with namespace and identifiers
	#[instrument(name = "catalog::resolve::table", level = "trace", skip(self, txn))]
	pub fn resolve_table(&self, txn: &mut Transaction<'_>, table_id: TableId) -> crate::Result<ResolvedTable> {
		let table_def = self.get_table(txn, table_id)?;
		let resolved_namespace = self.resolve_namespace(txn, table_def.namespace)?;
		let table_ident = Fragment::internal(table_def.name.clone());

		Ok(ResolvedTable::new(table_ident, resolved_namespace, table_def))
	}

	/// Resolve a view ID to a fully resolved view with namespace and identifiers
	#[instrument(name = "catalog::resolve::view", level = "trace", skip(self, txn))]
	pub fn resolve_view(&self, txn: &mut Transaction<'_>, view_id: ViewId) -> crate::Result<ResolvedView> {
		let view_def = self.get_view(txn, view_id)?;
		let resolved_namespace = self.resolve_namespace(txn, view_def.namespace)?;
		let view_ident = Fragment::internal(view_def.name.clone());

		Ok(ResolvedView::new(view_ident, resolved_namespace, view_def))
	}

	/// Resolve a flow ID to a fully resolved flow with namespace and identifiers
	#[instrument(name = "catalog::resolve::flow", level = "trace", skip(self, txn))]
	pub fn resolve_flow(&self, txn: &mut Transaction<'_>, flow_id: FlowId) -> crate::Result<ResolvedFlow> {
		let flow_def = self.get_flow(txn, flow_id)?;
		let resolved_namespace = self.resolve_namespace(txn, flow_def.namespace)?;
		let flow_ident = Fragment::internal(flow_def.name.clone());

		Ok(ResolvedFlow::new(flow_ident, resolved_namespace, flow_def))
	}

	/// Resolve a ring buffer ID to a fully resolved ring buffer with namespace and identifiers
	#[instrument(name = "catalog::resolve::ringbuffer", level = "trace", skip(self, txn))]
	pub fn resolve_ringbuffer(
		&self,
		txn: &mut Transaction<'_>,
		ringbuffer_id: RingBufferId,
	) -> crate::Result<ResolvedRingBuffer> {
		let ringbuffer_def = self.get_ringbuffer(txn, ringbuffer_id)?;
		let resolved_namespace = self.resolve_namespace(txn, ringbuffer_def.namespace)?;
		let ringbuffer_ident = Fragment::internal(ringbuffer_def.name.clone());

		Ok(ResolvedRingBuffer::new(ringbuffer_ident, resolved_namespace, ringbuffer_def))
	}

	/// Resolve column names for a target entity (table, ring buffer, or dictionary) by name.
	pub fn resolve_column_names(
		&self,
		txn: &mut Transaction<'_>,
		namespace_name: &str,
		target_name: &str,
	) -> crate::Result<Vec<String>> {
		let namespace_id = if let Some(ns) = self.find_namespace_by_name(txn, namespace_name)? {
			ns.id
		} else {
			return Ok(vec![]);
		};

		if let Some(table_def) = self.find_table_by_name(txn, namespace_id, target_name)? {
			return Ok(table_def.columns.iter().map(|c| c.name.clone()).collect());
		}

		if let Some(rb_def) = self.find_ringbuffer_by_name(txn, namespace_id, target_name)? {
			return Ok(rb_def.columns.iter().map(|c| c.name.clone()).collect());
		}

		if self.find_dictionary_by_name(txn, namespace_id, target_name)?.is_some() {
			return Ok(vec!["id".to_string(), "value".to_string()]);
		}

		Ok(vec![])
	}
}
