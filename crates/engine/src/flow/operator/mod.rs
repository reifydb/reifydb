// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Deref, sync::Arc};

use reifydb_core::interface::flow::OperatorCapability;
use reifydb_core::{interface::catalog::flow::OperatorId, value::column::columns::Columns};
use reifydb_sdk::flow::operator::timer::Timer as Tick;
use reifydb_value::{Result, value::duration::Duration};

use crate::flow::transaction::FlowTransaction;

pub mod append;
pub mod apply;
pub mod distinct;
pub mod extend;
pub mod filter;
pub mod gate;
pub mod guard;
pub mod join;
pub mod map;
pub mod scan;
pub mod sink;
pub mod sort;
pub mod stateful;
pub mod take;

use append::AppendOperator;
use apply::ApplyOperator;
use distinct::operator::DistinctOperator;
use extend::ExtendOperator;
use filter::FilterOperator;
use gate::GateOperator;
use guard::enforce_apply_capabilities;
use join::operator::JoinOperator;
use map::MapOperator;
use reifydb_core::interface::change::Change;
use scan::{
	ringbuffer::PrimitiveRingBufferOperator, series::PrimitiveSeriesOperator, table::PrimitiveTableOperator,
	view::PrimitiveViewOperator,
};
use sink::{
	ringbuffer_view::SinkRingBufferViewOperator, series_view::SinkSeriesViewOperator, view::SinkTableViewOperator,
};
use sort::SortOperator;
use take::TakeOperator;

pub trait Operator: Send + Sync {
	fn id(&self) -> OperatorId;

	fn capabilities(&self) -> &[OperatorCapability];

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change>;

	fn tick(&self, _txn: &mut FlowTransaction, _tick: Tick) -> Result<Option<Change>> {
		Ok(None)
	}

	fn ticks(&self) -> Option<Duration> {
		None
	}
}

pub type BoxedOperator = Box<dyn Operator + Send + Sync>;

#[derive(Clone)]
pub struct OperatorCell(Arc<Operators>);

impl OperatorCell {
	pub fn new(operators: Operators) -> Self {
		Self(Arc::new(operators))
	}
}

impl Deref for OperatorCell {
	type Target = Operators;

	fn deref(&self) -> &Operators {
		&self.0
	}
}

pub enum Operators {
	SourceTable(PrimitiveTableOperator),
	SourceView(PrimitiveViewOperator),
	SourceRingBuffer(PrimitiveRingBufferOperator),
	SourceSeries(PrimitiveSeriesOperator),
	Filter(FilterOperator),
	Gate(GateOperator),
	Map(MapOperator),
	Extend(ExtendOperator),
	Join(JoinOperator),
	Sort(SortOperator),
	Take(TakeOperator),
	Distinct(DistinctOperator),
	Append(AppendOperator),
	Apply(ApplyOperator),
	SinkTableView(SinkTableViewOperator),
	SinkRingBufferView(SinkRingBufferViewOperator),
	SinkSeriesView(SinkSeriesViewOperator),
	Custom(BoxedOperator),
}

impl Operators {
	pub fn id(&self) -> OperatorId {
		match self {
			Operators::Filter(op) => op.id(),
			Operators::Gate(op) => op.id(),
			Operators::Map(op) => op.id(),
			Operators::Extend(op) => op.id(),
			Operators::Join(op) => op.id(),
			Operators::Sort(op) => op.id(),
			Operators::Take(op) => op.id(),
			Operators::Distinct(op) => op.id(),
			Operators::Append(op) => op.id(),
			Operators::Apply(op) => op.id(),
			Operators::SinkTableView(op) => op.id(),
			Operators::SinkRingBufferView(op) => op.id(),
			Operators::SinkSeriesView(op) => op.id(),
			Operators::SourceTable(op) => op.id(),
			Operators::SourceView(op) => op.id(),
			Operators::SourceRingBuffer(op) => op.id(),
			Operators::SourceSeries(op) => op.id(),
			Operators::Custom(op) => op.id(),
		}
	}

	pub fn capabilities(&self) -> &[OperatorCapability] {
		match self {
			Operators::Filter(op) => op.capabilities(),
			Operators::Gate(op) => op.capabilities(),
			Operators::Map(op) => op.capabilities(),
			Operators::Extend(op) => op.capabilities(),
			Operators::Join(op) => op.capabilities(),
			Operators::Sort(op) => op.capabilities(),
			Operators::Take(op) => op.capabilities(),
			Operators::Distinct(op) => op.capabilities(),
			Operators::Append(op) => op.capabilities(),
			Operators::Apply(op) => op.capabilities(),
			Operators::SinkTableView(op) => op.capabilities(),
			Operators::SinkRingBufferView(op) => op.capabilities(),
			Operators::SinkSeriesView(op) => op.capabilities(),
			Operators::SourceTable(op) => op.capabilities(),
			Operators::SourceView(op) => op.capabilities(),
			Operators::SourceRingBuffer(op) => op.capabilities(),
			Operators::SourceSeries(op) => op.capabilities(),
			Operators::Custom(op) => op.capabilities(),
		}
	}

	pub fn ticks(&self) -> Option<Duration> {
		match self {
			Operators::Filter(op) => op.ticks(),
			Operators::Gate(op) => op.ticks(),
			Operators::Map(op) => op.ticks(),
			Operators::Extend(op) => op.ticks(),
			Operators::Join(op) => op.ticks(),
			Operators::Sort(op) => op.ticks(),
			Operators::Take(op) => op.ticks(),
			Operators::Distinct(op) => op.ticks(),
			Operators::Append(op) => op.ticks(),
			Operators::Apply(op) => op.ticks(),
			Operators::SinkTableView(op) => op.ticks(),
			Operators::SinkRingBufferView(op) => op.ticks(),
			Operators::SinkSeriesView(op) => op.ticks(),
			Operators::SourceTable(op) => op.ticks(),
			Operators::SourceView(op) => op.ticks(),
			Operators::SourceRingBuffer(op) => op.ticks(),
			Operators::SourceSeries(op) => op.ticks(),
			Operators::Custom(op) => op.ticks(),
		}
	}

	pub fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		enforce_apply_capabilities(self.id(), self.capabilities(), &change);
		match self {
			Operators::Filter(op) => op.apply(txn, change),
			Operators::Gate(op) => op.apply(txn, change),
			Operators::Map(op) => op.apply(txn, change),
			Operators::Extend(op) => op.apply(txn, change),
			Operators::Join(op) => op.apply(txn, change),
			Operators::Sort(op) => op.apply(txn, change),
			Operators::Take(op) => op.apply(txn, change),
			Operators::Distinct(op) => op.apply(txn, change),
			Operators::Append(op) => op.apply(txn, change),
			Operators::Apply(op) => op.apply(txn, change),
			Operators::SinkTableView(op) => op.apply(txn, change),
			Operators::SinkRingBufferView(op) => op.apply(txn, change),
			Operators::SinkSeriesView(op) => op.apply(txn, change),
			Operators::SourceTable(op) => op.apply(txn, change),
			Operators::SourceView(op) => op.apply(txn, change),
			Operators::SourceRingBuffer(op) => op.apply(txn, change),
			Operators::SourceSeries(op) => op.apply(txn, change),
			Operators::Custom(op) => op.apply(txn, change),
		}
	}

	pub fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		match self {
			Operators::Custom(op) => {
				op.tick(txn, tick)
			}
			Operators::Apply(op) => {
				op.tick(txn, tick)
			}
			Operators::Distinct(op) => {
				op.tick(txn, tick)
			}
			Operators::Join(op) => {
				op.tick(txn, tick)
			}
			Operators::Append(op) => {
				op.tick(txn, tick)
			}
			_ => Ok(None),
		}
	}

	pub fn output_schema(&self) -> Option<Columns> {
		match self {
			Operators::SourceTable(op) => Some(op.output_schema()),
			Operators::SourceView(op) => Some(op.output_schema()),
			Operators::SourceRingBuffer(op) => Some(op.output_schema()),
			Operators::SourceSeries(_) => Some(Columns::empty()),
			Operators::Filter(op) => op.output_schema(),
			Operators::Gate(op) => op.output_schema(),
			Operators::Map(op) => op.output_schema(),
			Operators::Extend(op) => op.output_schema(),
			Operators::Sort(op) => op.output_schema(),
			Operators::Take(op) => op.output_schema(),
			Operators::Distinct(op) => op.output_schema(),
			Operators::Append(op) => op.output_schema(),
			Operators::Apply(op) => op.output_schema(),
			Operators::Join(_) => None,
			Operators::SinkTableView(_) => None,
			Operators::SinkRingBufferView(_) => None,
			Operators::SinkSeriesView(_) => None,
			Operators::Custom(_) => None,
		}
	}
}
