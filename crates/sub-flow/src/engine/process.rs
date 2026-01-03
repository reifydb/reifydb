// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{future::Future, pin::Pin};

use reifydb_core::interface::FlowId;
use reifydb_rql::flow::{Flow, FlowNode, FlowNodeType::SourceInlineData};
use reifydb_sdk::{FlowChange, FlowChangeOrigin};
use tracing::{instrument, trace_span};

use crate::{engine::FlowEngine, transaction::FlowTransaction};

impl FlowEngine {
	#[instrument(name = "flow::process", level = "debug", skip(self, txn), fields(flow_id = ?flow_id, origin = ?change.origin, version = change.version.0, diff_count = change.diffs.len()))]
	pub async fn process(
		&self,
		txn: &mut FlowTransaction<'_>,
		change: FlowChange,
		flow_id: FlowId,
	) -> crate::Result<()> {
		match change.origin {
			FlowChangeOrigin::External(source) => {
				let node_registrations = {
					let sources = self.inner.sources.read().await;
					sources.get(&source).cloned()
				};

				if let Some(node_registrations) = node_registrations {
					for (registered_flow_id, node_id) in node_registrations {
						// Only process the flow that was passed as parameter
						if registered_flow_id != flow_id {
							continue;
						}

						let flow_and_node = {
							let flows = self.inner.flows.read().await;
							flows.get(&registered_flow_id).and_then(|flow| {
								flow.get_node(&node_id)
									.map(|node| (flow.clone(), node.clone()))
							})
						};

						if let Some((flow, node)) = flow_and_node {
							self.process_change(
								txn,
								&flow,
								&node,
								FlowChange::internal(
									node_id,
									change.version,
									change.diffs.clone(),
								),
							)
							.await?;
						}
					}
				}
			}
			FlowChangeOrigin::Internal(node_id) => {
				// Internal changes are already scoped to a specific node
				// This path is used by the partition logic to directly process a node's changes
				// Use the flow_id parameter for direct lookup instead of iterating all flows
				let flow_and_node = {
					let flows = self.inner.flows.read().await;
					flows.get(&flow_id).and_then(|flow| {
						flow.get_node(&node_id).map(|node| (flow.clone(), node.clone()))
					})
				};

				if let Some((flow, node)) = flow_and_node {
					self.process_change(txn, &flow, &node, change).await?;
				}
			}
		}
		Ok(())
	}

	#[instrument(name = "flow::apply", level = "trace", skip(self, txn), fields(node_id = ?node.id, input_diffs = change.diffs.len(), output_diffs))]
	async fn apply(
		&self,
		txn: &mut FlowTransaction<'_>,
		node: &FlowNode,
		change: FlowChange,
	) -> crate::Result<FlowChange> {
		let operator = self.inner.operators.read().await.get(&node.id).unwrap().clone();
		{
			let _span = trace_span!("flow::operator_apply", node_id = ?node.id, operator_type = ?node.ty)
				.entered();
		}
		let result = operator.apply(txn, change, &self.inner.evaluator).await?;
		tracing::Span::current().record("output_diffs", result.diffs.len());
		Ok(result)
	}

	fn process_change<'a>(
		&'a self,
		txn: &'a mut FlowTransaction<'_>,
		flow: &'a Flow,
		node: &'a FlowNode,
		change: FlowChange,
	) -> Pin<Box<dyn Future<Output = crate::Result<()>> + Send + 'a>> {
		Box::pin(self.process_change_inner(txn, flow, node, change))
	}

	async fn process_change_inner(
		&self,
		txn: &mut FlowTransaction<'_>,
		flow: &Flow,
		node: &FlowNode,
		change: FlowChange,
	) -> crate::Result<()> {
		let node_type = &node.ty;
		let changes = &node.outputs;

		let change = match &node_type {
			SourceInlineData {} => unimplemented!(),
			_ => {
				let result = self.apply(txn, node, change).await?;
				tracing::Span::current().record("output_diffs", result.diffs.len());
				result
			}
		};

		if changes.is_empty() {
		} else if changes.len() == 1 {
			let output_id = changes[0];
			Box::pin(self.process_change_inner(txn, flow, flow.get_node(&output_id).unwrap(), change))
				.await?;
		} else {
			let (last, rest) = changes.split_last().unwrap();
			for output_id in rest {
				Box::pin(self.process_change_inner(
					txn,
					flow,
					flow.get_node(output_id).unwrap(),
					change.clone(),
				))
				.await?;
			}
			Box::pin(self.process_change_inner(txn, flow, flow.get_node(last).unwrap(), change)).await?;
		}

		Ok(())
	}
}
