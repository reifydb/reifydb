// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	flow::OperatorId,
	id::{RingBufferId, SeriesId, TableId, ViewId},
	object::ObjectId,
	series::SeriesKey,
	storage::StorageId,
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::{
	engine::{FlowEngineInner, register::first_input},
	operator::sink::{
		ringbuffer_view::SinkRingBufferViewOperator, series_view::SinkSeriesViewOperator,
		view::SinkTableViewOperator,
	},
};

impl FlowEngineInner {
	#[inline]
	pub(super) fn add_sink_table_view(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		view: ViewId,
		table: TableId,
	) -> Result<()> {
		self.require_parent(first_input(inputs)?)?;
		self.add_sink(flow.id, operator_id, ObjectId::view(*view));
		let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
		let partition_by = self.catalog.get_table(&mut txn.reborrow(), table)?.partition_by;
		self.durable_sinks.insert(
			operator_id,
			Box::new(SinkTableViewOperator::new(operator_id, resolved, table, partition_by)),
		);
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	pub(super) fn add_sink_ringbuffer_view(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		view: ViewId,
		ringbuffer: RingBufferId,
		capacity: u64,
	) -> Result<()> {
		self.require_parent(first_input(inputs)?)?;
		self.add_sink(flow.id, operator_id, ObjectId::view(*view));
		let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
		let partition_by = self.catalog.get_ringbuffer(&mut txn.reborrow(), ringbuffer)?.partition_by;
		let ttl = self
			.catalog
			.find_row_settings(&mut txn.reborrow(), StorageId::ringbuffer(ringbuffer))?
			.and_then(|settings| settings.ttl);
		let row_ttl = ttl.as_ref().map(|t| t.duration);
		self.durable_sinks.insert(
			operator_id,
			Box::new(SinkRingBufferViewOperator::new(
				operator_id,
				resolved,
				ringbuffer,
				capacity,
				row_ttl,
				partition_by,
			)),
		);
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	pub(super) fn add_sink_series_view(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		view: ViewId,
		series: SeriesId,
		key: SeriesKey,
	) -> Result<()> {
		self.require_parent(first_input(inputs)?)?;
		self.add_sink(flow.id, operator_id, ObjectId::view(*view));
		let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
		let partition_by = self.catalog.get_series(&mut txn.reborrow(), series)?.partition_by;
		self.durable_sinks.insert(
			operator_id,
			Box::new(SinkSeriesViewOperator::new(operator_id, resolved, series, key.clone(), partition_by)),
		);
		Ok(())
	}
}
