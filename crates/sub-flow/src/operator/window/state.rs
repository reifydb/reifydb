// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::operator_state::GroupId;
use reifydb_flow::transaction::FlowTransaction;
use reifydb_value::{Result, util::hash::Hash128, value::row_number::RowNumber};

use super::{operator::WindowOperator, tumbling::partition_group_key};
use crate::operator::store::OperatorStateStore;

impl WindowOperator {
	pub(super) fn partition_group(&self, txn: &mut FlowTransaction, partition: Hash128) -> Result<GroupId> {
		let (group, _) = txn.intern_group(self.core.node, &partition_group_key(partition))?;
		Ok(group)
	}

	pub fn store_row_index(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
		window_id: u64,
	) -> Result<()> {
		let group = self.partition_group(txn, group_hash)?;
		let mut store = OperatorStateStore::new(txn, self.core.node);
		self.aux_slot().store_row_index(&mut store, group, row_number, window_id)
	}

	pub(super) fn lookup_row_index(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
	) -> Result<Vec<u64>> {
		let group = self.partition_group(txn, group_hash)?;
		let mut store = OperatorStateStore::new(txn, self.core.node);
		self.aux_slot().lookup_row_index(&mut store, group, row_number)
	}

	pub fn get_and_increment_global_count(&self, txn: &mut FlowTransaction, group_hash: Hash128) -> Result<u64> {
		let group = self.partition_group(txn, group_hash)?;
		let mut store = OperatorStateStore::new(txn, self.core.node);
		self.aux_slot().get_and_increment_count(&mut store, group)
	}

	pub(super) fn load_event_watermark(&self, txn: &mut FlowTransaction) -> Result<u64> {
		let mut store = OperatorStateStore::new(txn, self.core.node);
		self.aux_slot().event_watermark(&mut store)
	}

	pub(super) fn advance_event_watermark(&self, txn: &mut FlowTransaction, coord: u64) -> Result<()> {
		let mut store = OperatorStateStore::new(txn, self.core.node);
		self.aux_slot().advance_event_watermark(&mut store, coord)
	}

	pub(super) fn event_time_cutoff(&self, txn: &mut FlowTransaction, span_ms: u64) -> Result<u64> {
		Ok(self.load_event_watermark(txn)?.saturating_sub(span_ms))
	}

	pub(super) fn load_expiry_watermark(&self, txn: &mut FlowTransaction) -> Result<u64> {
		let mut store = OperatorStateStore::new(txn, self.core.node);
		self.aux_slot().expiry_watermark(&mut store)
	}

	pub(super) fn advance_expiry_watermark(&self, txn: &mut FlowTransaction, coord: u64) -> Result<()> {
		let mut store = OperatorStateStore::new(txn, self.core.node);
		self.aux_slot().advance_expiry_watermark(&mut store, coord)
	}
}
