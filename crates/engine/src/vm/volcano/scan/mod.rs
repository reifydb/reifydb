// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, sync::Arc};

use reifydb_core::interface::{
	catalog::{
		id::{NamespaceId, ViewId},
		object::ObjectId,
		view::ViewKind,
	},
	resolved::ResolvedView,
};
use reifydb_rql::flow::{analyzer::FlowGraphAnalyzer, loader::load_flow_dag};
use reifydb_transaction::{error::TransactionError, transaction::Transaction};
use reifydb_value::reifydb_assertions;

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

/// Materialization barrier: a transactional view read must reflect the writes this transaction
/// already made to the view's upstream closure, so any unprocessed change to that closure is fed
/// through the flow engine before the read proceeds. A deferred view is still maintained after
/// commit and cannot be made current here, so it keeps failing closed, as does a transactional view
/// whose upstream closure holds a deferred view that this transaction has invalidated.
pub(crate) fn materialize_view_read(view: &ResolvedView, rx: &mut Transaction<'_>, services: &Services) -> Result<()> {
	if !rx.has_unprocessed_flow_changes() || rx.is_flushing_flow_changes() {
		return Ok(());
	}
	let Some(upstream) = upstream_closure(services, rx, view.def().id())? else {
		return Ok(());
	};
	let dirty = dirty_upstream(rx, &upstream);
	if dirty.is_empty() {
		return Ok(());
	}
	if !matches!(view.def().kind(), ViewKind::Transactional)
		|| has_stale_deferred_upstream(services, rx, &upstream, &dirty)?
	{
		return Err(TransactionError::ViewPendingUpstreamChanges {
			view: view.fully_qualified_name(),
			kind: view.def().kind(),
			upstream: resolve_object_names(services, rx, &dirty),
			fragment: view.identifier().clone(),
		}
		.into());
	}

	let mut rounds = 0usize;
	let mut dirty = dirty;
	while !dirty.is_empty() {
		rx.flush_flow_changes(Some(&upstream))?;
		rounds += 1;
		assert!(
			rounds <= upstream.len() + 1,
			"view materialization reached round {} over {} upstream object(s) without a fixpoint; every \
			 round consumes the dirty upstream changes it feeds, so exceeding that bound means the flush \
			 re-dirties an upstream object and the barrier never terminates",
			rounds,
			upstream.len()
		);
		let remaining = dirty_upstream(rx, &upstream);
		reifydb_assertions! {
			assert!(
				remaining.is_empty(),
				"flush left {:?} unprocessed upstream of the view being read, so the next round would \
				 feed the same changes again and apply operator state twice",
				remaining
			);
		}
		dirty = remaining;
	}

	Ok(())
}

fn dirty_upstream(rx: &Transaction<'_>, upstream: &BTreeSet<ObjectId>) -> Vec<ObjectId> {
	rx.unprocessed_flow_change_objects().into_iter().filter(|object| upstream.contains(object)).collect()
}

/// Flow maintains a deferred view only after commit, so one that this transaction has invalidated
/// stays stale no matter how often the barrier flushes, and every transactional view derived from it
/// would materialize from that stale input.
fn has_stale_deferred_upstream(
	services: &Services,
	rx: &mut Transaction<'_>,
	upstream: &BTreeSet<ObjectId>,
	dirty: &[ObjectId],
) -> Result<bool> {
	for object in upstream {
		let ObjectId::View(id) = object else {
			continue;
		};
		let Some(def) = services.catalog.find_view(rx, *id)? else {
			return Ok(true);
		};
		if matches!(def.kind(), ViewKind::Transactional) {
			continue;
		}
		if dirty.contains(object) {
			return Ok(true);
		}
		let Some(closure) = upstream_closure(services, rx, *id)? else {
			return Ok(true);
		};
		if dirty.iter().any(|object| closure.contains(object)) {
			return Ok(true);
		}
	}
	Ok(false)
}

fn upstream_closure(
	services: &Services,
	rx: &mut Transaction<'_>,
	view: ViewId,
) -> Result<Option<Arc<BTreeSet<ObjectId>>>> {
	match services.view_lineage.upstream_of(view) {
		Some(upstream) => Ok(Some(upstream)),
		None => Ok(upstream_from_catalog(services, rx, view)?.map(Arc::new)),
	}
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
