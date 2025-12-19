// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! CDC event dispatcher for independent flow processing.

use std::{collections::HashSet, sync::Arc, time::Duration};

use crossbeam_channel::{Receiver, RecvTimeoutError};
use reifydb_core::{
	Result,
	interface::{Cdc, KeyKind, catalog::FlowId},
	key::{FlowEdgeByFlowKey, FlowKey, FlowNodeByFlowKey, Key, NamespaceFlowKey},
	util::encoding::keycode::deserialize,
};
use reifydb_engine::StandardEngine;
use reifydb_rql::flow::load_flow;
use tokio::task::spawn_blocking;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace};

use crate::{registry::FlowRegistry, routing::route_to_flows};

/// Main dispatcher task that routes CDC events to flows.
///
/// This runs on the tokio runtime and handles:
/// 1. Receiving CDC batches from the upstream channel
/// 2. Detecting flow lifecycle changes (create/delete)
/// 3. Routing data events to interested flows
#[instrument(name = "dispatcher", level = "info", skip(cdc_rx, registry, engine, shutdown))]
pub async fn dispatcher(
	cdc_rx: Receiver<Cdc>,
	registry: Arc<FlowRegistry>,
	engine: StandardEngine,
	shutdown: CancellationToken,
) {
	info!("dispatcher started");

	loop {
		// Check shutdown signal
		if shutdown.is_cancelled() {
			info!("dispatcher received shutdown signal");
			break;
		}

		// Receive CDC from channel (blocking in spawn_blocking to not block async runtime)
		let cdc = {
			let rx = cdc_rx.clone();
			let shutdown = shutdown.clone();

			spawn_blocking(move || {
				// Use recv_timeout to periodically check shutdown
				loop {
					match rx.recv_timeout(Duration::from_millis(1)) {
						Ok(cdc) => return Some(cdc),
						Err(RecvTimeoutError::Timeout) => {
							if shutdown.is_cancelled() {
								return None;
							}
							// Continue waiting
						}
						Err(RecvTimeoutError::Disconnected) => {
							return None;
						}
					}
				}
			})
			.await
			.expect("blocking recv panicked")
		};

		let Some(cdc) = cdc else {
			// Channel closed or shutdown
			break;
		};

		let version = cdc.version;
		trace!(version = version.0, changes = cdc.changes.len(), "processing cdc batch");

		// Step 1: Handle flow lifecycle changes
		if let Err(e) = handle_flow_changes(&registry, &engine, &cdc).await {
			error!(version = version.0, error = %e, "failed to handle flow changes");
			panic!("failed to handle flow changes: {}", e);
		}

		// Step 2: Route data events to flows
		if let Err(e) = route_to_flows(&registry, &engine, &cdc).await {
			error!(version = version.0, error = %e, "failed to route events");
			panic!("failed to route events: {}", e);
		}

		trace!(version = version.0, "CDC batch dispatched");
	}

	info!("dispatcher exiting");
}

/// Detect and handle flow lifecycle changes from CDC.
async fn handle_flow_changes(registry: &FlowRegistry, engine: &StandardEngine, cdc: &Cdc) -> Result<()> {
	// Collect all flow-related changes
	let mut affected_flows: HashSet<FlowId> = HashSet::new();

	for change in &cdc.changes {
		let Some(kind) = Key::kind(change.key()) else {
			continue;
		};

		// These key kinds indicate flow definition changes
		match kind {
			KeyKind::Flow
			| KeyKind::FlowNode
			| KeyKind::FlowNodeByFlow
			| KeyKind::FlowEdge
			| KeyKind::FlowEdgeByFlow
			| KeyKind::NamespaceFlow => {
				if let Some(flow_id) = extract_flow_id_from_key(change.key()) {
					affected_flows.insert(flow_id);
				}
			}
			_ => {}
		}
	}

	if affected_flows.is_empty() {
		return Ok(());
	}

	debug!(count = affected_flows.len(), version = cdc.version.0, "detected flow changes");

	// Process each affected flow
	for flow_id in affected_flows {
		let existed_before = registry.contains(flow_id).await;
		let exists_now = flow_exists_in_catalog(engine, flow_id)?;

		match (existed_before, exists_now) {
			// Flow created
			(false, true) => {
				info!(
					flow_id = flow_id.0,
					version = cdc.version.0,
					"flow created, registering with backfill"
				);

				// Load flow definition from catalog
				let flow = load_flow_from_catalog(engine, flow_id)?;
				let sources = get_flow_sources(&flow);

				// Register with backfill at this version
				registry.register_with_backfill(flow, sources, cdc.version).await?;
			}

			// Flow deleted
			(true, false) => {
				info!(flow_id = flow_id.0, "flow deleted, deregistering");

				// Deregister (drops sender, task will exit)
				if let Some(task) = registry.deregister(flow_id).await {
					// Optionally wait for task to finish
					let _ = task.await;
				}
			}

			// No state change
			(true, true) | (false, false) => {
				trace!(flow_id = flow_id.0, existed_before, exists_now, "no flow state change");
			}
		}
	}

	Ok(())
}

/// Extract FlowId from a flow-related catalog key.
fn extract_flow_id_from_key(key: &[u8]) -> Option<FlowId> {
	use reifydb_core::{CowVec, EncodedKey, key::EncodableKey as _};

	if key.len() < 2 {
		return None;
	}

	let kind: KeyKind = deserialize(&key[1..2]).ok()?;

	let encoded = EncodedKey(CowVec::new(key.to_vec()));
	match kind {
		KeyKind::Flow => {
			let flow_key = FlowKey::decode(&encoded)?;
			Some(flow_key.flow)
		}
		KeyKind::FlowNodeByFlow => {
			let flow_node_key = FlowNodeByFlowKey::decode(&encoded)?;
			Some(flow_node_key.flow)
		}
		KeyKind::FlowEdgeByFlow => {
			let flow_edge_key = FlowEdgeByFlowKey::decode(&encoded)?;
			Some(flow_edge_key.flow)
		}

		KeyKind::NamespaceFlow => {
			let namespace_flow_key = NamespaceFlowKey::decode(&encoded)?;
			Some(namespace_flow_key.flow)
		}
		KeyKind::FlowNode | KeyKind::FlowEdge => None,
		_ => None,
	}
}

/// Check if flow exists in catalog.
fn flow_exists_in_catalog(engine: &StandardEngine, flow_id: FlowId) -> Result<bool> {
	use reifydb_core::interface::{Engine, MultiVersionQueryTransaction};

	let mut txn = engine.begin_query()?;
	let key = FlowKey::encoded(flow_id);
	Ok(txn.get(&key)?.is_some())
}

/// Load flow definition from catalog.
fn load_flow_from_catalog(engine: &StandardEngine, flow_id: FlowId) -> Result<reifydb_rql::flow::Flow> {
	use reifydb_core::interface::Engine;

	let mut txn = engine.begin_query()?;
	load_flow(&mut txn, flow_id)
}

/// Get the source tables/views this flow subscribes to.
fn get_flow_sources(flow: &reifydb_rql::flow::Flow) -> HashSet<reifydb_core::interface::SourceId> {
	use reifydb_core::interface::SourceId;
	use reifydb_rql::flow::FlowNodeType;

	let mut sources = HashSet::new();

	for (_node_id, node) in flow.graph.nodes() {
		match &node.ty {
			FlowNodeType::SourceTable {
				table,
			} => {
				sources.insert(SourceId::Table(*table));
			}
			FlowNodeType::SourceView {
				view,
			} => {
				sources.insert(SourceId::View(*view));
			}
			FlowNodeType::SourceFlow {
				flow,
			} => {
				sources.insert(SourceId::Flow(*flow));
			}
			_ => {}
		}
	}

	sources
}
