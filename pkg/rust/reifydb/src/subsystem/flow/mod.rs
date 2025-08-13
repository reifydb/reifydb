// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{any::Any, time::Duration};

use reifydb_core::{
	Result,
	interface::{
		ActiveCommandTransaction, CdcConsume, CdcConsumer, CdcEvent,
		Change, ConsumerId, Transaction,
	},
};
use reifydb_engine::Engine;

use super::{Subsystem, cdc::PollConsumer};
use crate::health::HealthStatus;

#[derive(Clone)]
struct FlowConsumer;

impl<T: Transaction> CdcConsume<T> for FlowConsumer {
	fn consume(
		&self,
		_txn: &mut ActiveCommandTransaction<T>,
		events: Vec<CdcEvent>,
	) -> Result<()> {
		for event in events {
			let change_description = match &event.change {
				Change::Insert {
					key,
					after,
				} => {
					format!(
						"INSERT key={:?} value={:?}",
						String::from_utf8_lossy(&key.0),
						String::from_utf8_lossy(
							&after.0
						)
					)
				}
				Change::Update {
					key,
					before,
					after,
				} => {
					let before_str =
						if before.is_deleted() {
							"<deleted>".to_string()
						} else {
							format!("{:?}", String::from_utf8_lossy(&before.0))
						};
					format!(
						"UPDATE key={:?} before={} after={:?}",
						String::from_utf8_lossy(&key.0),
						before_str,
						String::from_utf8_lossy(
							&after.0
						)
					)
				}
				Change::Delete {
					key,
					before,
				} => {
					let before_str =
						if before.is_deleted() {
							"<deleted>".to_string()
						} else {
							format!("{:?}", String::from_utf8_lossy(&before.0))
						};
					format!(
						"DELETE key={:?} before={}",
						String::from_utf8_lossy(&key.0),
						before_str
					)
				}
			};

			println!(
				"[CDC] v{} seq{} ts{} | {}",
				event.version,
				event.sequence,
				event.timestamp,
				change_description
			);
		}
		Ok(())
	}
}

pub struct FlowSubsystem<T: Transaction> {
	consumer: PollConsumer<T, FlowConsumer>,
}

impl<T: Transaction> FlowSubsystem<T> {
	pub fn new(engine: Engine<T>) -> Self {
		Self {
			consumer: PollConsumer::new(
				ConsumerId::FLOW_CONSUMER,
				Duration::from_millis(1),
				engine,
				FlowConsumer,
			),
		}
	}
}

impl<T: Transaction> Drop for FlowSubsystem<T> {
	fn drop(&mut self) {
		let _ = self.consumer.stop();
	}
}

impl<T: Transaction + Send + Sync> Subsystem for FlowSubsystem<T> {
	fn name(&self) -> &'static str {
		"Flow"
	}

	fn start(&mut self) -> Result<()> {
		self.consumer.start()
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
