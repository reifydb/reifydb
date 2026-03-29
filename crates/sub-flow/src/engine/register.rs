// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{mem, sync::Arc};

use postcard::from_bytes;
use reifydb_core::{
	interface::catalog::{
		flow::{FlowId, FlowNodeId},
		shape::ShapeId,
		view::ViewKind,
	},
	internal,
};
use reifydb_rql::flow::{
	flow::FlowDag,
	node::{
		FlowNode,
		FlowNodeType::{
			self, Aggregate, Append, Apply, Distinct, Extend, Filter, Gate, Join, Map, SinkRingBufferView,
			SinkSeriesView, SinkSubscription, SinkTableView, Sort, SourceFlow, SourceInlineData,
			SourceRingBuffer, SourceSeries, SourceTable, SourceView, Take, Window,
		},
	},
};
use reifydb_transaction::transaction::{Transaction, command::CommandTransaction};
use reifydb_type::{Result, error::Error};
use tracing::instrument;

use super::eval::evaluate_operator_config;
#[cfg(reifydb_target = "native")]
use crate::operator::apply::ApplyOperator;
use crate::{
	engine::FlowEngine,
	operator::{
		Operators,
		append::AppendOperator,
		distinct::DistinctOperator,
		extend::ExtendOperator,
		filter::FilterOperator,
		gate::GateOperator,
		join::operator::JoinOperator,
		map::MapOperator,
		scan::{
			flow::PrimitiveFlowOperator, ringbuffer::PrimitiveRingBufferOperator,
			series::PrimitiveSeriesOperator, table::PrimitiveTableOperator, view::PrimitiveViewOperator,
		},
		sink::{
			ringbuffer_view::SinkRingBufferViewOperator, series_view::SinkSeriesViewOperator,
			subscription::SinkSubscriptionOperator, view::SinkTableViewOperator,
		},
		sort::SortOperator,
		take::TakeOperator,
		window::WindowOperator,
	},
};

impl FlowEngine {
	#[instrument(name = "flow::register", level = "debug", skip(self, txn), fields(flow_id = ?flow.id))]
	pub fn register(&mut self, txn: &mut CommandTransaction, flow: FlowDag) -> Result<()> {
		self.register_with_transaction(&mut Transaction::Command(txn), flow)
	}

	#[instrument(name = "flow::register_with_transaction", level = "debug", skip(self, txn), fields(flow_id = ?flow.id))]
	pub fn register_with_transaction(&mut self, txn: &mut Transaction<'_>, flow: FlowDag) -> Result<()> {
		debug_assert!(!self.flows.contains_key(&flow.id), "Flow already registered");

		for node_id in flow.topological_order()? {
			let node = flow.get_node(&node_id).unwrap();
			self.add(txn, &flow, node)?;
		}

		self.analyzer.add(flow.clone());
		self.flows.insert(flow.id, flow);

		Ok(())
	}

	#[instrument(name = "flow::add", level = "debug", skip(self, txn, flow), fields(flow_id = ?flow.id, node_id = ?node.id, node_type = ?mem::discriminant(&node.ty)))]
	fn add(&mut self, txn: &mut Transaction<'_>, flow: &FlowDag, node: &FlowNode) -> Result<()> {
		debug_assert!(!self.operators.contains_key(&node.id), "Operator already registered");
		let node = node.clone();

		match node.ty {
			SourceInlineData {
				..
			} => {
				unimplemented!()
			}
			SourceTable {
				table,
			} => {
				let table = self.catalog.get_table(&mut txn.reborrow(), table)?;

				self.add_source(flow.id, node.id, ShapeId::table(table.id));
				self.operators.insert(
					node.id,
					Arc::new(Operators::SourceTable(PrimitiveTableOperator::new(node.id, table))),
				);
			}
			SourceView {
				view,
			} => {
				let view = self.catalog.get_view(&mut txn.reborrow(), view)?;
				self.add_source(flow.id, node.id, ShapeId::view(view.id()));

				// For deferred views, also register the underlying primitive as a source.
				// Deferred view sinks write to the underlying primitive's key space,
				// so CDC from upstream deferred view commits uses the underlying ShapeId.
				if view.kind() == ViewKind::Deferred {
					self.add_source(flow.id, node.id, view.underlying_id());
				}

				// For transactional views, also register the underlying table/ringbuffer
				// sources so the deferred coordinator routes changes correctly. A transactional
				// view is computed on-the-fly; its changes are never published to CDC. By
				// registering the view's upstream primitives, the deferred flow is triggered
				// when the underlying data changes.
				if view.kind() == ViewKind::Transactional {
					let mut additional_sources = Vec::new();
					if let Some(view_flow) = self.catalog.find_flow_by_name(
						&mut txn.reborrow(),
						view.namespace(),
						view.name(),
					)? {
						let flow_nodes = self
							.catalog
							.list_flow_nodes_by_flow(&mut txn.reborrow(), view_flow.id)?;
						for flow_node in &flow_nodes {
							// SourceTable = 1, SourceRingBuffer = 17, SourceSeries = 18
							if flow_node.node_type == 1
								|| flow_node.node_type == 17 || flow_node.node_type == 18
							{
								if let Ok(nt) =
									from_bytes::<FlowNodeType>(&flow_node.data)
								{
									match nt {
										SourceTable {
											table: t,
										} => {
											additional_sources.push(
												ShapeId::table(t),
											);
										}
										SourceRingBuffer {
											ringbuffer: rb,
										} => {
											additional_sources.push(
												ShapeId::ringbuffer(rb),
											);
										}
										SourceSeries {
											series: s,
										} => {
											additional_sources.push(
												ShapeId::series(s),
											);
										}
										_ => {}
									}
								}
							}
						}
					}
					for source in additional_sources {
						self.add_source(flow.id, node.id, source);
					}
				}

				self.operators.insert(
					node.id,
					Arc::new(Operators::SourceView(PrimitiveViewOperator::new(node.id, view))),
				);
			}
			SourceFlow {
				flow: source_flow,
			} => {
				let source_flow = self.catalog.get_flow(&mut txn.reborrow(), source_flow)?;
				self.operators.insert(
					node.id,
					Arc::new(Operators::SourceFlow(PrimitiveFlowOperator::new(
						node.id,
						source_flow,
					))),
				);
			}
			SourceRingBuffer {
				ringbuffer,
			} => {
				let rb = self.catalog.get_ringbuffer(&mut txn.reborrow(), ringbuffer)?;
				self.add_source(flow.id, node.id, ShapeId::ringbuffer(rb.id));
				self.operators.insert(
					node.id,
					Arc::new(Operators::SourceRingBuffer(PrimitiveRingBufferOperator::new(
						node.id, rb,
					))),
				);
			}
			SourceSeries {
				series,
			} => {
				let s = self.catalog.get_series(&mut txn.reborrow(), series)?;
				self.add_source(flow.id, node.id, ShapeId::series(s.id));
				self.operators.insert(
					node.id,
					Arc::new(Operators::SourceSeries(PrimitiveSeriesOperator::new(node.id, s))),
				);
			}
			SinkTableView {
				view,
				table,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();

				self.add_sink(flow.id, node.id, ShapeId::view(*view));
				let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
				self.operators.insert(
					node.id,
					Arc::new(Operators::SinkTableView(SinkTableViewOperator::new(
						parent, node.id, resolved, table,
					))),
				);
			}
			SinkRingBufferView {
				view,
				ringbuffer,
				capacity,
				propagate_evictions,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.add_sink(flow.id, node.id, ShapeId::view(*view));
				let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
				self.operators.insert(
					node.id,
					Arc::new(Operators::SinkRingBufferView(SinkRingBufferViewOperator::new(
						parent,
						node.id,
						resolved,
						ringbuffer,
						capacity,
						propagate_evictions,
					))),
				);
			}
			SinkSeriesView {
				view,
				series,
				key,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.add_sink(flow.id, node.id, ShapeId::view(*view));
				let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
				self.operators.insert(
					node.id,
					Arc::new(Operators::SinkSeriesView(SinkSeriesViewOperator::new(
						parent,
						node.id,
						resolved,
						series,
						key.clone(),
					))),
				);
			}
			SinkSubscription {
				subscription,
			} => {
				// Guard against race condition: flow may have been deleted during loading
				if node.inputs.is_empty() {
					return Err(Error(internal!(
						"SinkSubscription node has no inputs - flow may have been deleted during loading"
					)));
				}
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();

				// Note: Subscriptions use UUID-based IDs and are not added to the sinks map
				// which uses ShapeId (u64-based). Subscriptions are ephemeral 1:1 mapped.
				let resolved = self.catalog.resolve_subscription(&mut txn.reborrow(), subscription)?;
				self.operators.insert(
					node.id,
					Arc::new(Operators::SinkSubscription(SinkSubscriptionOperator::new(
						parent, node.id, resolved,
					))),
				);
			}
			Filter {
				conditions,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.operators.insert(
					node.id,
					Arc::new(Operators::Filter(FilterOperator::new(
						parent,
						node.id,
						conditions,
						self.executor.functions.clone(),
						self.runtime_context.clone(),
					))),
				);
			}
			Gate {
				conditions,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.operators.insert(
					node.id,
					Arc::new(Operators::Gate(GateOperator::new(
						parent,
						node.id,
						conditions,
						self.executor.functions.clone(),
						self.runtime_context.clone(),
					))),
				);
			}
			Map {
				expressions,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.operators.insert(
					node.id,
					Arc::new(Operators::Map(MapOperator::new(
						parent,
						node.id,
						expressions,
						self.executor.functions.clone(),
						self.runtime_context.clone(),
					))),
				);
			}
			Extend {
				expressions,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.operators.insert(
					node.id,
					Arc::new(Operators::Extend(ExtendOperator::new(parent, node.id, expressions))),
				);
			}
			Sort {
				by: _,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.operators.insert(
					node.id,
					Arc::new(Operators::Sort(SortOperator::new(parent, node.id, Vec::new()))),
				);
			}
			Take {
				limit,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.operators.insert(
					node.id,
					Arc::new(Operators::Take(TakeOperator::new(parent, node.id, limit))),
				);
			}
			Join {
				join_type,
				left,
				right,
				alias,
			} => {
				// The join node should have exactly 2 inputs
				if node.inputs.len() != 2 {
					return Err(Error(internal!("Join node must have exactly 2 inputs")));
				}

				let left_node = node.inputs[0];
				let right_node = node.inputs[1];

				let left_parent = self
					.operators
					.get(&left_node)
					.ok_or_else(|| Error(internal!("Left parent operator not found")))?
					.clone();

				let right_parent = self
					.operators
					.get(&right_node)
					.ok_or_else(|| Error(internal!("Right parent operator not found")))?
					.clone();

				self.operators.insert(
					node.id,
					Arc::new(Operators::Join(JoinOperator::new(
						left_parent,
						right_parent,
						node.id,
						join_type,
						left_node,
						right_node,
						left,
						right,
						alias,
						self.executor.clone(),
					))),
				);
			}
			Distinct {
				expressions,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.operators.insert(
					node.id,
					Arc::new(Operators::Distinct(DistinctOperator::new(
						parent,
						node.id,
						expressions,
						self.executor.functions.clone(),
						self.runtime_context.clone(),
					))),
				);
			}
			Append {} => {
				// Append requires at least 2 inputs
				if node.inputs.len() < 2 {
					return Err(Error(internal!("Append node must have at least 2 inputs")));
				}

				let mut parents = Vec::with_capacity(node.inputs.len());

				for input_node_id in &node.inputs {
					let parent = self
						.operators
						.get(input_node_id)
						.ok_or_else(|| {
							Error(internal!(
								"Parent operator not found for input {:?}",
								input_node_id
							))
						})?
						.clone();
					parents.push(parent);
				}

				self.operators.insert(
					node.id,
					Arc::new(Operators::Append(AppendOperator::new(
						node.id,
						parents,
						node.inputs.clone(),
					))),
				);
			}
			Apply {
				operator,
				expressions,
			} => {
				let config = evaluate_operator_config(
					expressions.as_slice(),
					&self.executor.functions,
					&self.runtime_context,
				)?;

				if let Some(factory) = self.custom_operators.get(operator.as_str()) {
					let op = factory(node.id, &config)?;
					self.operators.insert(node.id, Arc::new(Operators::Custom(op)));
				} else {
					#[cfg(reifydb_target = "native")]
					{
						let parent = self
							.operators
							.get(&node.inputs[0])
							.ok_or_else(|| Error(internal!("Parent operator not found")))?
							.clone();

						if !self.is_ffi_operator(operator.as_str()) {
							return Err(Error(internal!("Unknown operator: {}", operator)));
						}

						let ffi_op =
							self.create_ffi_operator(operator.as_str(), node.id, &config)?;

						self.operators.insert(
							node.id,
							Arc::new(Operators::Apply(ApplyOperator::new(
								parent, node.id, ffi_op,
							))),
						);
					}
					#[cfg(not(reifydb_target = "native"))]
					{
						let _ = operator;
						return Err(Error(internal!(
							"FFI operators are not supported in WASM"
						)));
					}
				}
			}
			Aggregate {
				..
			} => unimplemented!(),
			Window {
				kind,
				group_by,
				aggregations,
				ts,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				let operator = WindowOperator::new(
					parent,
					node.id,
					kind.clone(),
					group_by.clone(),
					aggregations.clone(),
					ts.clone(),
					self.runtime_context.clone(),
					self.executor.functions.clone(),
				);
				self.operators.insert(node.id, Arc::new(Operators::Window(operator)));
			}
		}

		Ok(())
	}

	fn add_source(&mut self, flow: FlowId, node: FlowNodeId, shape: ShapeId) {
		let nodes = self.sources.entry(shape).or_insert_with(Vec::new);

		let entry = (flow, node);
		if !nodes.contains(&entry) {
			nodes.push(entry);
		}
	}

	fn add_sink(&mut self, flow: FlowId, node: FlowNodeId, sink: ShapeId) {
		let nodes = self.sinks.entry(sink).or_insert_with(Vec::new);

		let entry = (flow, node);
		if !nodes.contains(&entry) {
			nodes.push(entry);
		}
	}
}
