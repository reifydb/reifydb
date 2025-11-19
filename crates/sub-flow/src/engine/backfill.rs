// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogSourceQueryOperations;
use reifydb_core::{
	CommitVersion, Error, Row,
	interface::{EncodableKey, FlowNodeId, MultiVersionQueryTransaction, RowKey, RowKeyRange, SourceDef, SourceId},
	log_info, log_trace,
	value::encoded::EncodedValuesNamedLayout,
};
use reifydb_engine::StandardCommandTransaction;
use reifydb_flow_operator_sdk::{FlowChange, FlowChangeOrigin, FlowDiff};
use reifydb_rql::flow::{Flow, FlowNodeType};
use reifydb_type::internal;

use crate::{engine::FlowEngine, transaction::FlowTransaction};

impl FlowEngine {
	pub(crate) fn load_initial_data(
		&self,
		txn: &mut StandardCommandTransaction,
		flow: &Flow,
		backfill_version: CommitVersion,
	) -> crate::Result<()> {
		log_trace!("[Backfill] Starting initial data load for flow {:?}", flow.id);

		// Collect all source nodes in topological order
		let mut source_nodes = Vec::new();
		for node_id in flow.topological_order()? {
			if let Some(node) = flow.get_node(&node_id) {
				match &node.ty {
					FlowNodeType::SourceTable {
						..
					}
					| FlowNodeType::SourceView {
						..
					} => {
						source_nodes.push(node.clone());
					}
					_ => {}
				}
			}
		}

		log_trace!(
			"[Backfill] Found {} source nodes: {:?}",
			source_nodes.len(),
			source_nodes.iter().map(|n| n.id).collect::<Vec<_>>()
		);

		// Phase 1: Load all source data and apply through source operators
		// This populates JOIN state for all sides before any propagation
		// Read data at the backfill version to ensure we only see data that existed
		// when the flow was created, not data inserted afterward
		log_trace!("[Backfill] Using snapshot_version={:?}", backfill_version);
		let mut flow_txn = FlowTransaction::new(txn, backfill_version);
		let mut source_changes: Vec<(FlowNodeId, FlowChange)> = Vec::new();

		for source_node in &source_nodes {
			let source_id = match &source_node.ty {
				FlowNodeType::SourceTable {
					table,
				} => SourceId::table(*table),
				FlowNodeType::SourceView {
					view,
				} => SourceId::view(*view),
				_ => continue,
			};

			let source_def = txn.get_source(source_id)?;
			let rows = self.scan_all_rows(txn, &source_def)?;
			if rows.is_empty() {
				continue;
			}

			let (namespace, name) = match &source_def {
				SourceDef::Table(t) => (&t.namespace, &t.name),
				SourceDef::View(v) => (&v.namespace, &v.name),
				_ => unreachable!("Only Table and View sources are supported for backfill"),
			};

			log_info!("[INITIAL_LOAD] Processing {} rows from source {}.{}", rows.len(), namespace, name);

			let diffs: Vec<FlowDiff> = rows
				.into_iter()
				.map(|row| FlowDiff::Insert {
					post: row,
				})
				.collect();

			let change = FlowChange {
				origin: FlowChangeOrigin::Internal(source_node.id),
				version: backfill_version,
				diffs,
			};

			// Apply through source operator to get transformed change
			let operators = self.inner.operators.read();
			let source_operator = operators
				.get(&source_node.id)
				.ok_or_else(|| Error(internal!("Source operator not found")))?
				.clone();
			drop(operators);

			let result_change = source_operator.apply(&mut flow_txn, change, &self.inner.evaluator)?;
			if !result_change.diffs.is_empty() {
				source_changes.push((source_node.id, result_change));
			}
		}

		// Phase 2: Propagate all source changes through downstream operators
		// Now all JOIN sides have their data in state
		for (source_node_id, change) in source_changes {
			log_trace!(
				"[Backfill] Propagating {} diffs from source {:?}",
				change.diffs.len(),
				source_node_id
			);
			self.propagate_initial_change(&mut flow_txn, flow, source_node_id, change)?;
		}

		flow_txn.commit(txn)?;

		log_trace!("[Backfill] Initial data load complete for flow {:?}", flow.id);
		Ok(())
	}

	// FIXME this can be streamed without loading everything into memory first
	fn scan_all_rows(&self, txn: &mut StandardCommandTransaction, source: &SourceDef) -> crate::Result<Vec<Row>> {
		let mut rows = Vec::new();

		let layout: EncodedValuesNamedLayout = match source {
			SourceDef::Table(t) => t.into(),
			SourceDef::View(v) => v.into(),
			_ => unreachable!("Only Table and View sources are supported for backfill"),
		};
		let range = RowKeyRange::scan_range(source.id(), None);

		const BATCH_SIZE: u64 = 10000;
		let multi_rows: Vec<_> = txn.range_batched(range, BATCH_SIZE)?.into_iter().collect();

		for multi in multi_rows {
			if let Some(key) = RowKey::decode(&multi.key) {
				rows.push(Row {
					number: key.row,
					encoded: multi.values,
					layout: layout.clone(),
				});
			}
		}

		Ok(rows)
	}

	fn propagate_initial_change(
		&self,
		flow_txn: &mut FlowTransaction,
		flow: &Flow,
		from_node_id: FlowNodeId,
		change: FlowChange,
	) -> crate::Result<()> {
		let downstream_nodes = flow
			.graph
			.nodes()
			.filter(|(_, node)| node.inputs.contains(&from_node_id))
			.map(|(id, _)| *id)
			.collect::<Vec<_>>();

		log_trace!(
			"[Backfill] Propagating from {:?} to {} downstream nodes: {:?}",
			from_node_id,
			downstream_nodes.len(),
			downstream_nodes
		);

		for downstream_node_id in downstream_nodes {
			let operators = self.inner.operators.read();
			if let Some(operator) = operators.get(&downstream_node_id) {
				let operator = operator.clone();
				drop(operators);

				log_trace!(
					"[Backfill] Applying change to downstream node {:?} (from {:?}), diffs={}",
					downstream_node_id,
					from_node_id,
					change.diffs.len()
				);

				let result = operator.apply(flow_txn, change.clone(), &self.inner.evaluator)?;
				log_trace!(
					"[Backfill] Downstream node {:?} produced {} result diffs",
					downstream_node_id,
					result.diffs.len()
				);
				if !result.diffs.is_empty() {
					self.propagate_initial_change(flow_txn, flow, downstream_node_id, result)?;
				}
			}
		}

		Ok(())
	}
}
