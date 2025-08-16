// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{any::Any, time::Duration};

use reifydb_core::{
	Result, Type, Value,
	interface::{
		ActiveCommandTransaction, CdcChange, CdcConsume, CdcConsumer,
		CdcEvent, ColumnIndex, ConsumerId, Engine, FlowNodeId,
		Identity, Key, Params, SchemaId, SourceId, TableColumnDef,
		TableColumnId, TableDef, TableId, TableRowKey, Transaction,
	},
	row::EncodedRowLayout,
	value::columnar::Columns,
};
use reifydb_engine::{StandardEngine, StandardEvaluator};
use reifydb_flow::{Change, Diff, Flow, FlowEngine};

use super::{Subsystem, cdc::PollConsumer};
use crate::health::HealthStatus;

#[derive(Clone)]
struct FlowConsumer<T: Transaction> {
	engine: StandardEngine<T>,
}

impl<T: Transaction> CdcConsume<T> for FlowConsumer<T> {
	fn consume(
		&self,
		txn: &mut ActiveCommandTransaction<T>,
		events: Vec<CdcEvent>,
	) -> Result<()> {
		for event in events {
			let key = Key::decode(event.key()).unwrap();

			match key {
				Key::TableRow(TableRowKey {
					table,
					row,
				}) => {
					if table != 1026 {
						println!("skip {table:?}");
						continue;
					}
				}
				_ => continue,
			}

			// find all flows which needs to be updated with that
			// event

			let frame = self
				.engine
				.query_as(
					&Identity::root(),
					"FROM reifydb.flows filter { id == 1 } map {
			cast(data, utf8) }", Params::None,
				)
				.unwrap()
				.pop()
				.unwrap();

			let value = frame[0].get_value(0);

			if matches!(value, Value::Undefined) {
				continue;
			}

			// dbg!(&value.to_string());

			let flow: Flow = serde_json::from_str(
				value.to_string().as_str(),
			)
			.unwrap();

			let node_id = FlowNodeId(1026);

			let layout = EncodedRowLayout::new(&[
				Type::Utf8,
				Type::Int1,
			]);

			let table = TableDef {
				id: TableId(node_id.0),
				schema: SchemaId(0),
				name: "table".to_string(), // FIXME
				columns: vec![
					TableColumnDef {
						id: TableColumnId(0),
						name: "name".to_string(),
						ty: Type::Utf8,
						policies: vec![],
						index: ColumnIndex(0),
						auto_increment: false,
					},
					TableColumnDef {
						id: TableColumnId(1),
						name: "age".to_string(),
						ty: Type::Int1,
						policies: vec![],
						index: ColumnIndex(1),
						auto_increment: false,
					},
				],
			};

			let mut columns = Columns::from_table_def(&table);

			let row = match event.change {
				CdcChange::Insert {
					after,
					..
				} => after,
				_ => unimplemented!(),
			};

			columns.append_rows(&layout, [row]).unwrap();

			let change = Change::new(vec![Diff::Insert {
				source: SourceId::Table(TableId(1026)),
				after: columns,
			}]);

			let mut engine =
				FlowEngine::new(StandardEvaluator::default());
			engine.register(flow.clone()).unwrap();

			engine.process(txn, change.clone()).unwrap();
		}
		Ok(())
	}
}

pub struct FlowSubsystem<T: Transaction> {
	consumer: PollConsumer<T, FlowConsumer<T>>,
}

impl<T: Transaction> FlowSubsystem<T> {
	pub fn new(engine: StandardEngine<T>) -> Self {
		Self {
			consumer: PollConsumer::new(
				ConsumerId::flow_consumer(),
				Duration::from_millis(1),
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
