// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod factory;
mod intercept;

use std::{any::Any, time::Duration};

pub use factory::FlowSubsystemFactory;
use reifydb_catalog::Catalog;
use reifydb_core::{
	Result,
	interface::{
		CdcChange, CdcConsume, CdcConsumer, CdcEvent,
		CommandTransaction, ConsumerId, Engine, GetEncodedRowLayout,
		Identity, Key, Params, SourceId, TableRowKey, Transaction,
	},
	value::columnar::Columns,
};
use reifydb_engine::{StandardEngine, StandardEvaluator};
use reifydb_flow::{Change, Diff, Flow, FlowEngine};

use super::{
	Subsystem,
	cdc::{PollConsumer, PollConsumerConfig},
};
use crate::health::HealthStatus;

#[derive(Clone)]
struct FlowConsumer<T: Transaction> {
	engine: StandardEngine<T>,
}

impl<T: Transaction> CdcConsume<T> for FlowConsumer<T> {
	fn consume(
		&self,
		txn: &mut CommandTransaction<T>,
		events: Vec<CdcEvent>,
	) -> Result<()> {
		eprintln!(
			"[FlowConsumer] Starting consume with {} events",
			events.len()
		);

		let frame = self
			.engine
			.query_as(
				&Identity::root(),
				"FROM reifydb.flows filter { id == 1 } map {
			cast(data, utf8) }",
				Params::None,
			)
			.unwrap()
			.pop()
			.unwrap();

		let value = frame[0].get_value(0);

		let flow: Flow =
			serde_json::from_str(value.to_string().as_str())
				.unwrap();

		eprintln!("[FlowConsumer] Loaded flow from database");

		let mut changes = Vec::new();

		for event in events {
			let key = Key::decode(event.key()).unwrap();

			let (table_id, row_id) = match key {
				Key::TableRow(TableRowKey {
					table,
					row,
				}) => (table, row),
				Key::ViewRow(_) => {
					// Skip view row events - they shouldn't
					// trigger flow processing
					eprintln!(
						"[FlowConsumer] Skipping view row event"
					);
					continue;
				}
				_ => continue,
			};

			// Get table to check its type
			let table = match Catalog::get_table(txn, table_id) {
				Ok(t) => t,
				Err(_) => {
					// Not a table, might be a view - skip
					// it
					eprintln!(
						"[FlowConsumer] Skipping non-table event for id {:?}",
						table_id
					);
					continue;
				}
			};

			eprintln!(
				"[FlowConsumer] Processing event for table '{}' (id={:?}, schema={:?})",
				table.name, table_id, table.schema
			);

			// Check if this is actually a view (views might be
			// stored as tables internally) Skip if the table
			// name matches a view name TODO: Better way to
			// detect views vs tables
			if table.name == "basic" {
				eprintln!(
					"[FlowConsumer] Skipping view table event"
				);
				continue;
			}

			let layout = table.get_layout();

			eprintln!(
				"[FlowConsumer] Event type: {:?}",
				std::mem::discriminant(&event.change)
			);

			match event.change {
				CdcChange::Insert {
					after,
					..
				} => {
					eprintln!(
						"[FlowConsumer] Processing Insert event"
					);
					let mut columns =
						Columns::from_table_def(&table);
					columns.append_rows(&layout, [after])
						.unwrap();

					let row_ids = vec![row_id];
					let change =
						Change::new(
							vec![Diff::Insert {
						source: SourceId::Table(table.id),
						row_ids,
						after: columns,
					}],
						);

					eprintln!(
						"[FlowConsumer] Created Insert diff with source: {:?}",
						SourceId::Table(table.id)
					);
					changes.push(change)
				}
				CdcChange::Update {
					..
				} => {
					// Skip Update events on user tables -
					// these are likely view updates
					// being incorrectly reported as table
					// updates
					eprintln!(
						"[FlowConsumer] Skipping Update event on table '{}' - likely a view update",
						table.name
					);
					continue;
				}
				CdcChange::Delete {
					before,
					..
				} => {
					let mut columns =
						Columns::from_table_def(&table);
					columns.append_rows(&layout, [before])
						.unwrap();

					let row_ids = vec![row_id];
					let change =
						Change::new(vec![Diff::Remove{
								source: SourceId::Table(table.id),
								row_ids,
								before: columns,
							}]);

					changes.push(change)
				}
			};
		}

		let mut engine = FlowEngine::new(StandardEvaluator::default());

		eprintln!("[FlowConsumer] Registering flow in engine");
		engine.register(flow.clone()).unwrap();

		eprintln!(
			"[FlowConsumer] Processing {} changes",
			changes.len()
		);
		for (i, change) in changes.iter().enumerate() {
			eprintln!(
				"[FlowConsumer] Processing change {}/{}",
				i + 1,
				changes.len()
			);
			engine.process(txn, change.clone()).unwrap();
		}
		eprintln!("[FlowConsumer] Completed processing all changes");
		Ok(())
	}
}

pub struct FlowSubsystem<T: Transaction> {
	consumer: PollConsumer<T, FlowConsumer<T>>,
}

impl<T: Transaction> FlowSubsystem<T> {
	pub fn new(engine: StandardEngine<T>) -> Self {
		let config = PollConsumerConfig::new(
			ConsumerId::flow_consumer(),
			Duration::from_millis(1),
		);
		Self {
			consumer: PollConsumer::new(
				config,
				engine.clone(),
				FlowConsumer {
					engine: engine.clone(),
				},
			),
		}
	}
}

impl<T: Transaction> Drop for FlowSubsystem<T> {
	fn drop(&mut self) {
		let _ = self.consumer.stop();
	}
}

impl<T: Transaction> Subsystem for FlowSubsystem<T> {
	fn name(&self) -> &'static str {
		"Flow"
	}

	fn start(&mut self) -> Result<()> {
		self.consumer.start()
		// println!("FLOW SUBSYSTEM DISABLED FOR NOW");
		// Ok(())
	}

	fn stop(&mut self) -> Result<()> {
		self.consumer.stop()
	}

	fn is_running(&self) -> bool {
		self.consumer.is_running()
	}

	fn health_status(&self) -> HealthStatus {
		if self.is_running() {
			HealthStatus::Healthy
		} else {
			HealthStatus::Unknown
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}
}
