// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, sync::Arc};

use reifydb_core::interface::{
	catalog::{
		id::{NamespaceId, ViewId},
		shape::ShapeId,
	},
	resolved::ResolvedView,
};
use reifydb_rql::flow::{analyzer::FlowGraphAnalyzer, loader::load_flow_dag};
use reifydb_transaction::{error::TransactionError, transaction::Transaction};

use crate::{Result, vm::services::Services};

pub mod dictionary;
pub mod index;
pub mod remote;
pub mod ringbuffer;
pub mod series;
pub mod table;
pub mod view;
pub mod vtable;

/// Reject reading ANY view - transactional or deferred - while the current
/// transaction holds unprocessed changes to shapes upstream of it, walking
/// the flow DAG through every view kind: you never read your own uncommitted
/// writes through a view. Transactional views are maintained in the
/// pre-commit interceptor and deferred views asynchronously after commit, so
/// such a read would silently return the view's pre-request contents; failing
/// the transaction (TXN_015) is the contract instead. Query, Test, and
/// Replica transactions never accumulate changes and pass through
/// unconditionally - the Test exemption is load-bearing for RUN TESTS, which
/// maintains views inline. A view this transaction created is absent from the
/// published lineage snapshot, which only learns of a flow at post-commit, so
/// a snapshot miss falls back to the catalog rather than waving the read
/// through: the guard fails closed on an unknown view.
pub(crate) fn guard_view_read(view: &ResolvedView, rx: &mut Transaction<'_>, services: &Services) -> Result<()> {
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
	let offending: Vec<ShapeId> =
		rx.unprocessed_flow_change_shapes().into_iter().filter(|shape| upstream.contains(shape)).collect();
	if offending.is_empty() {
		return Ok(());
	}
	Err(TransactionError::ViewPendingUpstreamChanges {
		view: view.fully_qualified_name(),
		kind: view.def().kind(),
		upstream: resolve_shape_names(services, rx, &offending),
		fragment: view.identifier().clone(),
	}
	.into())
}

/// Recompute the upstream closure straight from the catalog the current
/// transaction can see. The published snapshot only learns of a flow at
/// post-commit, so a view this very transaction created is absent from it;
/// treating that absence as "no upstreams" would let the read through. The
/// catalog already holds the uncommitted CREATE VIEW, so it is the truth
/// here. Returns None only when no flow produces the view at all.
fn upstream_from_catalog(
	services: &Services,
	rx: &mut Transaction<'_>,
	view: ViewId,
) -> Result<Option<BTreeSet<ShapeId>>> {
	let mut dags = Vec::new();
	for flow in services.catalog.list_flows_all(rx)? {
		dags.push(load_flow_dag(rx, flow.id)?);
	}
	let mut analyzer = FlowGraphAnalyzer::new();
	analyzer.add_all(dags);
	Ok(analyzer.get_dependency_graph().upstream_closure().remove(&view))
}

fn resolve_shape_names(services: &Services, rx: &mut Transaction<'_>, shapes: &[ShapeId]) -> Vec<String> {
	let catalog = &services.catalog;
	shapes.iter()
		.map(|shape| {
			let named = match shape {
				ShapeId::Table(id) => catalog
					.find_table(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("table", def.namespace, def.name)),
				ShapeId::View(id) => catalog
					.find_view(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("view", def.namespace(), def.name().to_string())),
				ShapeId::RingBuffer(id) => catalog
					.find_ringbuffer(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("ring buffer", def.namespace, def.name)),
				ShapeId::Series(id) => catalog
					.find_series(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("series", def.namespace, def.name)),
				ShapeId::Dictionary(id) => catalog
					.find_dictionary(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("dictionary", def.namespace, def.name)),
				ShapeId::TableVirtual(_) => None,
				ShapeId::SegmentTree(_) => None,
			};
			match named {
				Some((kind, namespace, name)) => {
					format!("{} '{}'", kind, qualify(services, rx, namespace, &name))
				}
				None => format!("shape {}", shape),
			}
		})
		.collect()
}

/// Render a shape's name namespace-qualified, matching how the offending view
/// itself is rendered. Without the namespace, `alpha::orders` and
/// `beta::orders` both print as `orders` and the diagnostic cannot be acted on.
/// Degrades to the bare name if the namespace is unreadable: this is already
/// the error path, and a slightly vaguer message beats masking the real error.
fn qualify(services: &Services, rx: &mut Transaction<'_>, namespace: NamespaceId, name: &str) -> String {
	match services.catalog.find_namespace(rx, namespace) {
		Ok(Some(namespace)) => format!("{}::{}", namespace.name(), name),
		_ => name.to_string(),
	}
}
