// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::transaction::StandardCommandTransaction;
use reifydb_core::{
	interface::{
		CdcChange, CdcConsume, CdcEvent, Key,
		Transaction,
	},
	log_debug,
	Result,
};
use reifydb_engine::StandardEvaluator;

use super::intercept::FlowChange;
use crate::engine::FlowEngine;

/// Consumer that processes CDC events for Flow subsystem
pub struct FlowConsumer {
	flow_engine: Arc<FlowEngine<StandardEvaluator>>,
}

impl FlowConsumer {
	pub fn new(flow_engine: Arc<FlowEngine<StandardEvaluator>>) -> Self {
		Self {
			flow_engine,
		}
	}

	fn process_changes<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		changes: Vec<FlowChange>,
	) -> Result<()> {
		use reifydb_core::{
			interface::SourceId, value::columnar::Columns,
		};

		use crate::{Change, Diff};

		// Convert FlowChange events to flow engine Change format
		let mut diffs = Vec::new();

		for change in changes {
			match change {
				FlowChange::Insert {
					table_id,
					row_number,
					row: _,
				} => {
					// For now, create a simple columnar
					// representation This will need
					// proper column extraction from the row
					// data
					let diff = Diff::Insert {
						source: SourceId::Table(
							table_id,
						),
						row_ids: vec![row_number],
						after: Columns::empty(), /* TODO: Convert row bytes to Columns */
					};
					diffs.push(diff);
					log_debug!(
						"Processing insert: table={:?}, row={:?}",
						table_id,
						row_number
					);
				}
				FlowChange::Update {
					table_id,
					row_number,
					before: _,
					after: _,
				} => {
					let diff = Diff::Update {
						source: SourceId::Table(
							table_id,
						),
						row_ids: vec![row_number],
						before: Columns::empty(), /* TODO: Convert before bytes to Columns */
						after: Columns::empty(),  /* TODO: Convert after bytes to Columns */
					};
					diffs.push(diff);
					log_debug!(
						"Processing update: table={:?}, row={:?}",
						table_id,
						row_number
					);
				}
				FlowChange::Delete {
					table_id,
					row_number,
					row: _,
				} => {
					let diff = Diff::Remove {
						source: SourceId::Table(
							table_id,
						),
						row_ids: vec![row_number],
						before: Columns::empty(), /* TODO: Convert row bytes to Columns */
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
			let change = Change::new(diffs);
			self.flow_engine.process(txn, change)?;
		}

		Ok(())
	}
}

impl<T: Transaction> CdcConsume<T> for FlowConsumer {
	fn consume(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		events: Vec<CdcEvent>,
	) -> Result<()> {
		let mut changes = Vec::new();

		for event in events {
			if let Some(Key::TableRow(table_row)) =
				Key::decode(event.key())
			{
				// Convert CDC events to FlowChange events
				let flow_change = match &event.change {
					CdcChange::Insert {
						after,
						..
					} => FlowChange::Insert {
						table_id: table_row.table,
						row_number: table_row.row,
						row: after.to_vec(),
					},
					CdcChange::Update {
						before,
						after,
						..
					} => FlowChange::Update {
						table_id: table_row.table,
						row_number: table_row.row,
						before: before.to_vec(),
						after: after.to_vec(),
					},
					CdcChange::Delete {
						before,
						..
					} => FlowChange::Delete {
						table_id: table_row.table,
						row_number: table_row.row,
						row: before.to_vec(),
					},
				};

				changes.push(flow_change);
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
