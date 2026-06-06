// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	common::{CommitVersion, TimeDomain, WindowKind, WindowSize},
	encoded::shape::RowShape,
	error::diagnostic::flow::{flow_window_timestamp_column_not_found, flow_window_timestamp_column_type_mismatch},
	interface::{catalog::flow::FlowNodeId, change::Change},
	value::column::columns::Columns,
	window::engine::LatePolicy,
};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_sdk::operator::Tick;
use reifydb_value::{
	Result,
	error::Error,
	value::{Value, datetime::DateTime, duration::Duration},
};

use super::{
	aggregation::Aggregation,
	rolling::{
		apply_rolling_engine, apply_rolling_processing_engine, tick_expire_rolling_engine,
		tick_expire_rolling_processing_engine,
	},
	tumbling::{
		apply_session_engine, apply_sliding_engine, apply_tumbling_engine, tick_expire_engine_windows,
		tick_expire_session_engine,
	},
};
use crate::{
	operator::{
		Operator, OperatorCell,
		stateful::{raw::RawStatefulOperator, row::RowNumberProvider, window::WindowStateful},
	},
	transaction::FlowTransaction,
};

pub struct WindowConfig {
	pub parent: OperatorCell,
	pub node: FlowNodeId,
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub ts: Option<String>,
	pub runtime_context: RuntimeContext,
	pub routines: Routines,
	pub late_policy: LatePolicy,
}

pub struct WindowOperator {
	pub core: Aggregation,
	pub kind: WindowKind,
	pub ts: Option<String>,

	pub late_policy: LatePolicy,
	pub layout: RowShape,
	pub row_number_provider: RowNumberProvider,
}

impl WindowOperator {
	pub fn new(config: WindowConfig) -> Self {
		let core = Aggregation::new(
			config.node,
			config.parent,
			config.group_by,
			config.aggregations,
			config.routines,
			config.runtime_context,
		);
		Self {
			core,
			kind: config.kind,
			ts: config.ts,
			late_policy: config.late_policy,
			layout: RowShape::operator_state(),
			row_number_provider: RowNumberProvider::new(config.node),
		}
	}

	pub fn is_count_based(&self) -> bool {
		self.kind.size().is_some_and(|m| m.is_count())
	}

	pub fn is_rolling(&self) -> bool {
		matches!(self.kind, WindowKind::Rolling { .. })
	}

	pub fn size_duration(&self) -> Option<Duration> {
		self.kind.size().and_then(|m| m.as_duration())
	}

	pub fn size_count(&self) -> Option<u64> {
		self.kind.size().and_then(|m| m.as_count())
	}

	pub fn resolve_event_timestamps(&self, columns: &Columns, row_count: usize) -> Result<Vec<u64>> {
		if row_count == 0 {
			return Ok(Vec::new());
		}
		match (self.kind.time(), &self.ts) {
			(TimeDomain::Event, Some(ts_col)) => {
				let col = columns.column(ts_col).ok_or_else(|| {
					Error(Box::new(flow_window_timestamp_column_not_found(ts_col)))
				})?;
				let mut timestamps = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match col.data().get_value(i) {
						Value::DateTime(dt) => timestamps.push(dt.timestamp_millis() as u64),
						other => {
							return Err(Error(Box::new(
								flow_window_timestamp_column_type_mismatch(
									ts_col,
									other.get_type(),
								),
							)));
						}
					}
				}
				Ok(timestamps)
			}
			_ => {
				let now = self.core.current_timestamp();
				Ok(vec![now; row_count])
			}
		}
	}
}

impl RawStatefulOperator for WindowOperator {}

impl WindowStateful for WindowOperator {
	fn layout(&self) -> RowShape {
		self.layout.clone()
	}
}

impl Operator for WindowOperator {
	fn id(&self) -> FlowNodeId {
		self.core.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD_WITH_TICK
	}

	fn ticks(&self) -> Option<Duration> {
		match &self.kind {
			WindowKind::Tumbling {
				..
			}
			| WindowKind::Sliding {
				..
			}
			| WindowKind::Session {
				..
			}
			| WindowKind::Rolling {
				size: WindowSize::Duration(_),
				..
			} => Some(Duration::from_seconds(1).unwrap()),
			WindowKind::Rolling {
				size: WindowSize::Count(_),
				..
			} => None,
		}
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		match &self.kind {
			WindowKind::Tumbling {
				..
			} => apply_tumbling_engine(self, txn, change),
			WindowKind::Sliding {
				..
			} => apply_sliding_engine(self, txn, change),
			WindowKind::Rolling {
				..
			} => {
				if self.is_rolling_processing() {
					apply_rolling_processing_engine(self, txn, change)
				} else {
					apply_rolling_engine(self, txn, change)
				}
			}
			WindowKind::Session {
				..
			} => apply_session_engine(self, txn, change),
		}
	}

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		let current_timestamp = tick.now.to_nanos() / 1_000_000;
		let diffs = match &self.kind {
			WindowKind::Tumbling {
				..
			}
			| WindowKind::Sliding {
				..
			} => tick_expire_engine_windows(self, txn, current_timestamp)?,
			WindowKind::Rolling {
				size: WindowSize::Duration(_),
				..
			} if self.is_rolling_processing() => tick_expire_rolling_processing_engine(self, txn, current_timestamp)?,
			WindowKind::Rolling {
				size: WindowSize::Duration(_),
				..
			} => tick_expire_rolling_engine(self, txn, current_timestamp)?,
			WindowKind::Session {
				..
			} => tick_expire_session_engine(self, txn, current_timestamp)?,
			_ => vec![],
		};

		if diffs.is_empty() {
			Ok(None)
		} else {
			Ok(Some(Change::from_flow(
				self.core.node,
				CommitVersion(0),
				diffs,
				DateTime::from_nanos(self.core.runtime_context.clock.now_nanos()),
			)))
		}
	}
}
