// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use reifydb_catalog::resolve::{resolve_ring_buffer, resolve_table, resolve_view};
use reifydb_cdc::CdcConsume;
use reifydb_core::{
	CommitVersion, Result, Row,
	interface::{Cdc, CdcChange, Engine, GetEncodedRowNamedLayout, Identity, Key, Params, SourceId},
	util::CowVec,
	value::encoded::EncodedValues,
};
use reifydb_engine::{StandardCommandTransaction, StandardEngine, StandardRowEvaluator};
use reifydb_rql::flow::Flow;
use reifydb_type::{RowNumber, Value};

use crate::{
	builder::OperatorFactory,
	engine::FlowEngine,
	flow::FlowDiff,
	operator::TransformOperatorRegistry,
	subsystem::intercept::Change,
	worker::{SameThreadedWorker, WorkerPool},
};

// The table ID for reifydb.flows table
// This is where flow definitions are stored
const FLOWS_TABLE_ID: u64 = 1025;

/// Consumer that processes CDC events for Flow subsystem
pub struct FlowConsumer {
	engine: StandardEngine,
	flow_engine: FlowEngine,
}

impl FlowConsumer {
	pub fn new(engine: StandardEngine, operators: Vec<(String, OperatorFactory)>) -> Self {
		let mut registry = TransformOperatorRegistry::new();
		for (name, factory) in operators.iter() {
			let factory = factory.clone();
			let name = name.clone();
			registry.register(name, move |node, exprs| factory(node, exprs));
		}

		let flow_engine = FlowEngine::new(StandardRowEvaluator::default(), engine.executor(), registry);

		let result = Self {
			engine: engine.clone(),
			flow_engine,
		};

		if let Ok(mut txn) = engine.begin_command() {
			if let Ok(flows) = result.load_flows() {
				for flow in flows {
					result.flow_engine.register(&mut txn, flow).unwrap();
				}
			}
		}

		result
	}

	/// Helper method to convert encoded bytes to Row format
	fn create_row(
		txn: &mut StandardCommandTransaction,
		source: SourceId,
		row_number: RowNumber,
		row_bytes: Vec<u8>,
	) -> Result<Row> {
		// Get source metadata from catalog
		let layout = match source {
			SourceId::Table(table_id) => {
				let resolved_table = resolve_table(txn, table_id)?;
				resolved_table.def().get_named_layout()
			}
			SourceId::View(view_id) => {
				let resolved_view = resolve_view(txn, view_id)?;
				resolved_view.def().get_named_layout()
			}
			SourceId::TableVirtual(_) => {
				// Virtual tables not supported in flows yet
				unimplemented!("Virtual table sources not supported in flows")
			}
			SourceId::RingBuffer(ring_buffer_id) => {
				let resolved_ring_buffer = resolve_ring_buffer(txn, ring_buffer_id)?;
				resolved_ring_buffer.def().get_named_layout()
			}
		};

		let encoded = EncodedValues(CowVec::new(row_bytes));
		Ok(Row {
			number: row_number,
			encoded,
			layout,
		})
	}

	/// Load flows from the catalog
	fn load_flows(&self) -> Result<Vec<Flow>> {
		let mut flows = Vec::new();

		// Query the reifydb.flows table
		let frames = self.engine.query_as(
			&Identity::root(),
			"FROM reifydb.flows map { cast(data, utf8) }",
			Params::None,
		)?;

		for frame in frames {
			// Process all rows in the frame
			if !frame.is_empty() {
				let column = &frame[0];
				for row_idx in 0..column.data.len() {
					let value = column.get_value(row_idx);
					if !matches!(value, Value::Undefined) {
						if let Ok(flow) = serde_json::from_str::<Flow>(&value.to_string()) {
							flows.push(flow);
						}
					}
				}
			}
		}

		Ok(flows)
	}
}

impl CdcConsume for FlowConsumer {
	fn consume(&self, txn: &mut StandardCommandTransaction, cdcs: Vec<Cdc>) -> Result<()> {
		if cdcs.is_empty() {
			return Ok(());
		}

		// Collect all changes grouped by version
		let mut changes_by_version: HashMap<CommitVersion, Vec<(SourceId, Change)>> = HashMap::new();
		let mut flows_changed_at_version: Option<CommitVersion> = None;

		for cdc in cdcs {
			let version = cdc.version;

			for sequenced_change in cdc.changes {
				if let Some(decoded_key) = Key::decode(sequenced_change.key()) {
					if let Key::Row(table_row) = decoded_key {
						let source_id = table_row.source;

						// Detect flow table changes - trigger reload but don't process as data
						if source_id.as_u64() == FLOWS_TABLE_ID {
							if flows_changed_at_version.is_none() {
								flows_changed_at_version = Some(version);
							}
							continue;
						}

						// CDC now returns resolved values directly
						let change = match &sequenced_change.change {
							CdcChange::Insert {
								key: _,
								post,
							} => Change::Insert {
								_source_id: source_id,
								row_number: table_row.row,
								post: post.to_vec(),
							},
							CdcChange::Update {
								key: _,
								pre,
								post,
							} => Change::Update {
								_source_id: source_id,
								row_number: table_row.row,
								pre: pre.to_vec(),
								post: post.to_vec(),
							},
							CdcChange::Delete {
								key: _,
								pre,
							} => Change::Delete {
								_source_id: source_id,
								row_number: table_row.row,
								pre: pre.as_ref()
									.map(|v| v.to_vec())
									.unwrap_or_default(),
							},
						};
						changes_by_version
							.entry(version)
							.or_insert_with(Vec::new)
							.push((source_id, change));
					}
				}
			}
		}

		// Reload flows if needed (before processing any changes)
		if flows_changed_at_version.is_some() {
			self.flow_engine.clear();
			let flows = self.load_flows()?;
			for flow in flows {
				self.flow_engine.register(txn, flow)?;
			}
		}

		// If no changes to process, we're done
		if changes_by_version.is_empty() {
			return Ok(());
		}

		// Convert raw changes to FlowDiff format
		let mut diffs_by_version: HashMap<CommitVersion, Vec<(SourceId, Vec<FlowDiff>)>> = HashMap::new();

		for (version, changes) in changes_by_version {
			// Group changes by source for this version
			let mut changes_by_source: HashMap<SourceId, Vec<FlowDiff>> = HashMap::new();

			for (source_id, change) in changes {
				let diff = match change {
					Change::Insert {
						row_number,
						post,
						..
					} => {
						let row = Self::create_row(txn, source_id, row_number, post)?;
						FlowDiff::Insert {
							post: row,
						}
					}
					Change::Update {
						row_number,
						pre,
						post,
						..
					} => {
						let pre_row = Self::create_row(txn, source_id, row_number, pre)?;
						let post_row = Self::create_row(txn, source_id, row_number, post)?;
						FlowDiff::Update {
							pre: pre_row,
							post: post_row,
						}
					}
					Change::Delete {
						row_number,
						pre,
						..
					} => {
						let row = Self::create_row(txn, source_id, row_number, pre)?;
						FlowDiff::Remove {
							pre: row,
						}
					}
				};
				changes_by_source.entry(source_id).or_insert_with(Vec::new).push(diff);
			}

			// Convert to Vec format expected by partition_multi_version
			let source_diffs: Vec<(SourceId, Vec<FlowDiff>)> = changes_by_source.into_iter().collect();
			diffs_by_version.insert(version, source_diffs);
		}

		// Partition all changes across all versions into units of work
		let units = self.flow_engine.partition_multi_version(diffs_by_version);
		if units.is_empty() {
			return Ok(());
		}

		// Process all flow units through the worker
		let worker = SameThreadedWorker::new();
		worker.process(txn, units, &self.flow_engine)
	}
}
