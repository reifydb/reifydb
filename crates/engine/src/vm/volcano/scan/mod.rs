// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, sync::Arc};

use reifydb_core::interface::{
	catalog::{
		id::{NamespaceId, ViewId},
		object::ObjectId,
	},
	resolved::ResolvedView,
};
use reifydb_rql::flow::{analyzer::FlowGraphAnalyzer, loader::load_flow_dag};
use reifydb_transaction::{error::TransactionError, transaction::Transaction};

use crate::{Result, vm::services::Services};

pub mod dictionary;
pub mod index;
pub mod queue;
pub mod remote;
pub mod ringbuffer;
pub mod series;
pub mod table;
pub mod view;
pub mod vtable;

/// Reading a view while the transaction holds unprocessed changes upstream of it would return the
/// view's pre-request contents, since view maintenance runs after commit. Fails closed: an unknown
/// view is resolved from the catalog, not waved through.
pub(crate) fn guard_view_read(view: &ResolvedView, rx: &mut Transaction<'_>, services: &Services) -> Result<()> {
	if matches!(rx, Transaction::Test(_)) {
		unimplemented!("RUN TESTS view reads; see plan-operator.md follow-up");
	}
	if !rx.has_unprocessed_flow_changes() {
		return Ok(());
	}
	let upstream = match services.view_lineage.upstream_of(view.def().id()) {
		Some(upstream) => upstream,
		None => match upstream_from_catalog(services, rx, view.def().id())? {
			Some(upstream) => Arc::new(upstream),
			None => return Ok(()),
		},
	};
	let offending: Vec<ObjectId> =
		rx.unprocessed_flow_change_objects().into_iter().filter(|object| upstream.contains(object)).collect();
	if offending.is_empty() {
		return Ok(());
	}
	Err(TransactionError::ViewPendingUpstreamChanges {
		view: view.fully_qualified_name(),
		kind: view.def().kind(),
		upstream: resolve_object_names(services, rx, &offending),
		fragment: view.identifier().clone(),
	}
	.into())
}

/// The published snapshot only learns of a flow at post-commit, so a view this transaction just
/// created is missing from it while the catalog already holds the uncommitted CREATE VIEW.
fn upstream_from_catalog(
	services: &Services,
	rx: &mut Transaction<'_>,
	view: ViewId,
) -> Result<Option<BTreeSet<ObjectId>>> {
	let mut dags = Vec::new();
	for flow in services.catalog.list_flows_all(rx)? {
		dags.push(load_flow_dag(rx, flow.id)?);
	}
	let mut analyzer = FlowGraphAnalyzer::new();
	analyzer.add_all(dags);
	Ok(analyzer.get_dependency_graph().upstream_closure().remove(&view))
}

fn resolve_object_names(services: &Services, rx: &mut Transaction<'_>, objects: &[ObjectId]) -> Vec<String> {
	let catalog = &services.catalog;
	objects.iter()
		.map(|object| {
			let named = match object {
				ObjectId::Table(id) => catalog
					.find_table(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("table", def.namespace, def.name)),
				ObjectId::View(id) => catalog
					.find_view(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("view", def.namespace(), def.name().to_string())),
				ObjectId::RingBuffer(id) => catalog
					.find_ringbuffer(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("ring buffer", def.namespace, def.name)),
				ObjectId::Series(id) => catalog
					.find_series(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("series", def.namespace, def.name)),
				ObjectId::Dictionary(id) => catalog
					.find_dictionary(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("dictionary", def.namespace, def.name)),
				ObjectId::Queue(id) => catalog
					.find_queue(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("queue", def.namespace, def.name)),
				ObjectId::TableVirtual(_) => None,
			};
			match named {
				Some((kind, namespace, name)) => {
					format!("{} '{}'", kind, qualify(services, rx, namespace, &name))
				}
				None => format!("object {}", object),
			}
		})
		.collect()
}

/// Without the namespace, `alpha::orders` and `beta::orders` both print as `orders` and the
/// diagnostic cannot be acted on. Degrades to the bare name rather than masking the real error.
fn qualify(services: &Services, rx: &mut Transaction<'_>, namespace: NamespaceId, name: &str) -> String {
	match services.catalog.find_namespace(rx, namespace) {
		Ok(Some(namespace)) => format!("{}::{}", namespace.name(), name),
		_ => name.to_string(),
	}
}
