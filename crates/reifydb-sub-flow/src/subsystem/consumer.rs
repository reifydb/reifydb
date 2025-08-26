// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	Result, Value,
	flow::{Flow, FlowChange, FlowDiff},
	interface::{
		CdcChange, CdcConsume, CdcEvent, CommandTransaction, Engine,
		GetEncodedRowLayout, Identity, Key, Params, QueryTransaction,
		TableId, Transaction,
	},
	log_debug,
	row::EncodedRow,
	util::CowVec,
	value::columnar::Columns,
};
use reifydb_engine::{StandardEngine, StandardEvaluator};

use super::intercept::Change;
use crate::engine::FlowEngine;

/// Consumer that processes CDC events for Flow subsystem
pub struct FlowConsumer<T: Transaction> {
	engine: StandardEngine<T>,
}

impl<T: Transaction> FlowConsumer<T> {
	pub fn new(engine: StandardEngine<T>) -> Self {
		Self {
			engine,
		}
	}

	/// Helper method to convert row bytes to Columns format
	fn to_columns<TC: CommandTransaction>(
		txn: &mut TC,
		table: TableId,
		row_bytes: &[u8],
	) -> Result<Columns> {
		// Get table metadata from catalog
		let table = CatalogStore::get_table(txn, table)?;
		let layout = table.get_layout();

		// Create columns structure based on table definition
		let mut columns = Columns::from_table_def(&table);

		// Convert row bytes to EncodedRow
		let encoded_row = EncodedRow(CowVec::new(row_bytes.to_vec()));

		// Append the row data to columns
		columns.append_rows(&layout, [encoded_row])?;

		Ok(columns)
	}

	/// Load flows from the catalog
	fn load_flows(
		&self,
		_txn: &impl QueryTransaction,
	) -> Result<Vec<Flow>> {
		let mut flows = Vec::new();

		// Query the reifydb.flows table
		let frames = self.engine.query_as(
			&Identity::root(),
			"FROM reifydb.flows map { cast(data, utf8) }",
			Params::None,
		)?;

		for frame in frames {
			// Access the first row, first column
			let value = frame[0].get_value(0);
			if !matches!(value, Value::Undefined) {
				if let Ok(flow) = serde_json::from_str::<Flow>(
					&value.to_string(),
				) {
					flows.push(flow);
				}
			}
		}

		Ok(flows)
	}

	fn process_changes<TC: CommandTransaction>(
		&self,
		txn: &mut TC,
		changes: Vec<Change>,
	) -> Result<()> {
		use reifydb_core::interface::SourceId;

		// Create a new FlowEngine for this processing batch
		let mut flow_engine =
			FlowEngine::new(StandardEvaluator::default());

		// Load and register flows
		let flows = self.load_flows(txn)?;
		for flow in flows {
			flow_engine.register(flow)?;
		}

		// Convert FlowChange events to flow engine Change format
		let mut diffs = Vec::new();

		for change in changes {
			match change {
				Change::Insert {
					table_id,
					row_number,
					row,
				} => {
					// Convert row bytes to Columns format
					let columns = Self::to_columns(
						txn, table_id, &row,
					)?;

					let diff = FlowDiff::Insert {
						source: SourceId::Table(
							table_id,
						),
						row_ids: vec![row_number],
						after: columns,
					};
					diffs.push(diff);
					log_debug!(
						"Processing insert: table={:?}, row={:?}",
						table_id,
						row_number
					);
				}
				Change::Update {
					table_id,
					row_number,
					before,
					after,
				} => {
					// Convert row bytes to Columns format
					let before_columns = Self::to_columns(
						txn, table_id, &before,
					)?;
					let after_columns = Self::to_columns(
						txn, table_id, &after,
					)?;

					let diff = FlowDiff::Update {
						source: SourceId::Table(
							table_id,
						),
						row_ids: vec![row_number],
						before: before_columns,
						after: after_columns,
					};
					diffs.push(diff);
					log_debug!(
						"Processing update: table={:?}, row={:?}",
						table_id,
						row_number
					);
				}
				Change::Delete {
					table_id,
					row_number,
					row,
				} => {
					// Convert row bytes to Columns format
					let columns = Self::to_columns(
						txn, table_id, &row,
					)?;

					let diff = FlowDiff::Remove {
						source: SourceId::Table(
							table_id,
						),
						row_ids: vec![row_number],
						before: columns,
					};
					diffs.push(diff);
					log_debug!(
						"Processing delete: table={:?}, row={:?}",
						table_id,
						row_number
					);
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
	fn consume(
		&self,
		txn: &mut impl CommandTransaction,
		events: Vec<CdcEvent>,
	) -> Result<()> {
		let mut changes = Vec::new();

		for event in events {
			if let Some(Key::TableRow(table_row)) =
				Key::decode(event.key())
			{
				// Convert CDC events to FlowChange events
				let flowchange = match &event.change {
					CdcChange::Insert {
						after,
						..
					} => Change::Insert {
						table_id: table_row.table,
						row_number: table_row.row,
						row: after.to_vec(),
					},
					CdcChange::Update {
						before,
						after,
						..
					} => Change::Update {
						table_id: table_row.table,
						row_number: table_row.row,
						before: before.to_vec(),
						after: after.to_vec(),
					},
					CdcChange::Delete {
						before,
						..
					} => Change::Delete {
						table_id: table_row.table,
						row_number: table_row.row,
						row: before.to_vec(),
					},
				};

				changes.push(flowchange);
			}
		}

		if !changes.is_empty() {
			log_debug!(
				"Flow consumer processing {} CDC events",
				changes.len()
			);
			self.process_changes(txn, changes)?;
		}

		Ok(())
	}
}
