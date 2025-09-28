// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::resolve::{resolve_ring_buffer, resolve_table, resolve_view};
use reifydb_cdc::CdcConsume;
use reifydb_core::{
	Result,
	interface::{
		Cdc, CdcChange, Engine, GetEncodedRowNamedLayout, Identity, Key, MultiVersionCommandTransaction,
		Params, QueryTransaction, SourceId, Transaction,
	},
	util::CowVec,
	value::row::{EncodedRow, Row},
};
use reifydb_engine::{StandardCommandTransaction, StandardEngine, StandardRowEvaluator};
use reifydb_rql::flow::Flow;
use reifydb_type::{RowNumber, Value};

use super::intercept::Change;
use crate::{
	builder::OperatorFactory,
	engine::FlowEngine,
	flow::{FlowChange, FlowDiff},
	operator::TransformOperatorRegistry,
};

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

	/// Helper method to convert row bytes to Row format
	fn to_row(
		txn: &mut StandardCommandTransaction<T>,
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

		let encoded = EncodedRow(CowVec::new(row_bytes));
		Ok(Row {
			number: row_number,
			encoded,
			layout,
		})
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
		let mut registry = TransformOperatorRegistry::new();

		// Register custom operators
		for (name, factory) in self.operators.iter() {
			let factory = factory.clone();
			let name = name.clone();
			registry.register(name, move |node, exprs| factory(node, exprs));
		}

		let mut flow_engine =
			FlowEngine::new(StandardRowEvaluator::default(), self.engine.executor(), registry);

		let flows = self.load_flows(txn)?;

		for flow in flows {
			flow_engine.register(txn, flow)?;
		}

		// Process each change immediately without grouping
		for change in changes {
			let (source_id, diff) = match change {
				Change::Insert {
					source_id,
					row_number,
					post,
				} => {
					let row = Self::to_row(txn, source_id, row_number, post)?;
					(
						source_id,
						FlowDiff::Insert {
							post: row,
						},
					)
				}
				Change::Update {
					source_id,
					row_number,
					pre,
					post,
				} => {
					let pre_row = Self::to_row(txn, source_id, row_number, pre)?;
					let post_row = Self::to_row(txn, source_id, row_number, post)?;
					(
						source_id,
						FlowDiff::Update {
							pre: pre_row,
							post: post_row,
						},
					)
				}
				Change::Delete {
					source_id,
					row_number,
					pre,
				} => {
					let row = Self::to_row(txn, source_id, row_number, pre)?;
					(
						source_id,
						FlowDiff::Remove {
							pre: row,
						},
					)
				}
			};

			// Process immediately
			let change = FlowChange::external(source_id, vec![diff]);
			flow_engine.process(txn, change)?;
		}

		Ok(())
	}
}

impl<T: Transaction> CdcConsume<T> for FlowConsumer<T> {
	fn consume(&self, txn: &mut StandardCommandTransaction<T>, cdcs: Vec<Cdc>) -> Result<()> {
		for cdc in cdcs {
			txn.read_as_of_version_inclusive(cdc.version)?;

			for sequenced_change in cdc.changes {
				let mut to_process = Vec::new();

				if let Some(decoded_key) = Key::decode(sequenced_change.key()) {
					if let Key::Row(table_row) = decoded_key {
						let source_id = table_row.source;

						// Skip flow table changes - we don't
						// need to process them as data changes
						if source_id.as_u64() == FLOWS_TABLE_ID {
							continue;
						}

						let change = match &sequenced_change.change {
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
						to_process.push(change);
					}
				}

				if !to_process.is_empty() {
					self.process_changes(txn, to_process)?;
				}
			}
		}

		Ok(())
	}
}
