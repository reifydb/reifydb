// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	flow::OperatorId,
	id::{RingBufferId, SeriesId, TableId, ViewId},
	object::ObjectId,
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::{
	engine::FlowEngineInner,
	operator::scan::{
		ringbuffer::SourceRingBufferOperator, series::SourceSeriesOperator, table::SourceTableOperator,
		view::SourceViewOperator,
	},
};

impl FlowEngineInner {
	#[inline]
	pub(super) fn add_source_table(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		table: TableId,
	) -> Result<()> {
		let table = self.catalog.get_table(&mut txn.reborrow(), table)?;

		self.add_source(flow.id, operator_id, ObjectId::table(table.id));
		self.operators.insert(operator_id, Box::new(SourceTableOperator::new(operator_id, table)));
		Ok(())
	}

	#[inline]
	pub(super) fn add_source_ringbuffer(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		ringbuffer: RingBufferId,
	) -> Result<()> {
		let rb = self.catalog.get_ringbuffer(&mut txn.reborrow(), ringbuffer)?;
		self.add_source(flow.id, operator_id, ObjectId::ringbuffer(rb.id));
		self.operators.insert(operator_id, Box::new(SourceRingBufferOperator::new(operator_id, rb)));
		Ok(())
	}

	#[inline]
	pub(super) fn add_source_series(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		series: SeriesId,
	) -> Result<()> {
		let s = self.catalog.get_series(&mut txn.reborrow(), series)?;
		self.add_source(flow.id, operator_id, ObjectId::series(s.id));
		self.operators.insert(operator_id, Box::new(SourceSeriesOperator::new(operator_id)));
		Ok(())
	}

	#[inline]
	pub(super) fn add_source_view(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		view: ViewId,
	) -> Result<()> {
		let view = self.catalog.get_view(&mut txn.reborrow(), view)?;
		self.add_source(flow.id, operator_id, ObjectId::view(view.id()));

		self.add_source(flow.id, operator_id, view.storage_id().into());

		self.operators.insert(operator_id, Box::new(SourceViewOperator::new(operator_id, view)));
		Ok(())
	}
}
