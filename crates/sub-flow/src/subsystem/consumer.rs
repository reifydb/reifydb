// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogStore;
use reifydb_cdc::CdcConsume;
use reifydb_core::{
	Result,
	flow::{Flow, FlowChange, FlowDiff},
	interface::{
		CdcChange, CdcEvent, CommandTransaction, Engine, GetEncodedRowLayout, Identity, Key, Params,
		QueryTransaction, SourceId, Transaction,
	},
	row::EncodedRow,
	util::CowVec,
	value::columnar::Columns,
};
use reifydb_engine::{StandardCommandTransaction, StandardEngine, StandardEvaluator};
use reifydb_type::Value;

use super::intercept::Change;
use crate::{builder::OperatorFactory, engine::FlowEngine, operator::transform::registry::TransformOperatorRegistry};

// The table ID for reifydb.flows table
// This is where flow definitions are stored
const FLOWS_TABLE_ID: u64 = 1025;

/// Consumer that processes CDC events for Flow subsystem
pub struct FlowConsumer<T: Transaction> {
	engine: StandardEngine<T>,
	operators: Vec<(String, OperatorFactory<T>)>,
}

impl<T: Transaction> FlowConsumer<T> {
	pub fn new(engine: StandardEngine<T>, operators: Vec<(String, OperatorFactory<T>)>) -> Self {
		Self {
			engine,
			operators,
		}
	}

	/// Helper method to convert row bytes to Columns format
	fn to_columns<CT: CommandTransaction>(txn: &mut CT, source: SourceId, row_bytes: &[u8]) -> Result<Columns> {
		// Get source metadata from catalog
		let (columns, layout) = match source {
			SourceId::Table(table_id) => {
				let table = CatalogStore::get_table(txn, table_id)?;
				let namespace = CatalogStore::get_namespace(txn, table.namespace)?;
				let layout = table.get_layout();
				let columns = Columns::from_table_def_fully_qualified(&namespace, &table);
				(columns, layout)
			}
			SourceId::View(view_id) => {
				let view = CatalogStore::get_view(txn, view_id)?;
				let namespace = CatalogStore::get_namespace(txn, view.namespace)?;
				let layout = view.get_layout();
				let columns = Columns::from_view_def_fully_qualified(&namespace, &view);
				(columns, layout)
			}
			SourceId::TableVirtual(_) => {
				// Virtual tables not supported in flows yet
				unimplemented!("Virtual table sources not supported in flows")
			}
			SourceId::RingBuffer(ring_buffer_id) => {
				let ring_buffer = CatalogStore::get_ring_buffer(txn, ring_buffer_id)?;
				let namespace = CatalogStore::get_namespace(txn, ring_buffer.namespace)?;
				let layout = ring_buffer.get_layout();
				let columns = Columns::from_ring_buffer_def_fully_qualified(&namespace, &ring_buffer);
				(columns, layout)
			}
			SourceId::FlowNode(_flow_node_id) => {
				// Flow nodes don't have catalog entries; they're intermediate results
				// Return empty columns - the actual schema will come from the flow operators
				// TODO: Consider storing flow node schemas in the flow graph context
				return Ok(Columns::empty());
			}
		};

		// Convert row bytes to EncodedRow
		if row_bytes.is_empty() {
			// Return empty columns for deleted rows
			// The row was already deleted from storage, so we don't have the actual data
			return Ok(columns);
		}

		let encoded_row = EncodedRow(CowVec::new(row_bytes.to_vec()));

		// Append the row data to columns
		let mut columns = columns;
		columns.append_rows(&layout, [encoded_row])?;

		Ok(columns)
	}

	/// Load flows from the catalog
	fn load_flows(&self, _txn: &impl QueryTransaction) -> Result<Vec<Flow>> {
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

	fn process_changes(&self, txn: &mut StandardCommandTransaction<T>, changes: Vec<Change>) -> Result<()> {
		// Create a new FlowEngine for this processing batch with custom
		// operators
		let mut registry = TransformOperatorRegistry::with_builtins();

		// Register custom operators
		for (name, factory) in self.operators.iter() {
			let factory = factory.clone();
			let name = name.clone();
			registry.register(name, move |node, exprs| factory(node, exprs));
		}

		let mut flow_engine = FlowEngine::with_registry(StandardEvaluator::default(), registry);

		let flows = self.load_flows(txn)?;

		for flow in flows {
			flow_engine.register(txn, flow)?;
		}

		// Convert FlowChange events to flow engine Change format
		let mut diffs = Vec::new();

		for change in changes {
			match change {
				Change::Insert {
					source_id,
					row_number,
					post,
				} => {
					// Convert row bytes to Columns format
					let columns = match Self::to_columns(txn, source_id, &post) {
						Ok(cols) => cols,
						Err(e) => {
							return Err(e);
						}
					};

					let diff = FlowDiff::Insert {
						source: source_id,
						row_ids: vec![row_number],
						post: columns,
					};
					diffs.push(diff);
				}
				Change::Update {
					source_id,
					row_number,
					pre,
					post,
				} => {
					// Convert row bytes to Columns format
					let before_columns = Self::to_columns(txn, source_id, &pre)?;
					let after_columns = Self::to_columns(txn, source_id, &post)?;

					let diff = FlowDiff::Update {
						source: source_id,
						row_ids: vec![row_number],
						pre: before_columns,
						post: after_columns,
					};
					diffs.push(diff);
				}
				Change::Delete {
					source_id,
					row_number,
					pre,
				} => {
					// Convert row bytes to Columns format
					let columns = Self::to_columns(txn, source_id, &pre)?;

					let diff = FlowDiff::Remove {
						source: source_id,
						row_ids: vec![row_number],
						pre: columns,
					};
					diffs.push(diff);
				}
			}
		}

		if !diffs.is_empty() {
			let change = FlowChange::new(diffs);
			flow_engine.process(txn, change)?;
		}

		Ok(())
	}
}

impl<T: Transaction> CdcConsume<T> for FlowConsumer<T> {
	fn consume(&self, txn: &mut StandardCommandTransaction<T>, events: Vec<CdcEvent>) -> Result<()> {
		let mut changes = Vec::new();

		for event in events {
			if let Some(decoded_key) = Key::decode(event.key()) {
				if let Key::Row(table_row) = decoded_key {
					let source_id = table_row.source;

					// Skip flow table changes - we don't
					// need to process them as data changes
					if source_id.as_u64() == FLOWS_TABLE_ID {
						continue;
					}

					let change = match &event.change {
						CdcChange::Insert {
							post,
							..
						} => Change::Insert {
							source_id,
							row_number: table_row.row,
							post: post.to_vec(),
						},
						CdcChange::Update {
							pre,
							post,
							..
						} => Change::Update {
							source_id,
							row_number: table_row.row,
							pre: pre.to_vec(),
							post: post.to_vec(),
						},
						CdcChange::Delete {
							pre,
							..
						} => Change::Delete {
							source_id,
							row_number: table_row.row,
							pre: pre.as_ref()
								.map(|row| row.to_vec())
								.unwrap_or_else(Vec::new),
						},
					};
					changes.push(change);
				}
			}
		}

		if !changes.is_empty() {
			self.process_changes(txn, changes)?;
		}

		Ok(())
	}
}
