// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{key::operator_state::GroupId, window::span::WindowCoord};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{datetime::DateTime, row_number::RowNumber},
};

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

	pub(super) fn seal_ledger(&self, txn: &mut FlowTransaction) -> Result<DateTime> {
		let mut store = OperatorStateStore::new(txn, self.core.node);
		Ok(<DateTime as WindowCoord>::from_order(self.aux_slot().seal_ledger(&mut store)?))
	}

	pub(super) fn advance_seal_ledger(&self, txn: &mut FlowTransaction, at: DateTime) -> Result<()> {
		let mut store = OperatorStateStore::new(txn, self.core.node);
		self.aux_slot().advance_seal_ledger(&mut store, at.to_order())
	}

	pub(super) fn seal_frontier(&self, txn: &mut FlowTransaction) -> Result<DateTime> {
		let watermark = txn.flow_watermark();
		let ledger = self.seal_ledger(txn)?;
		Ok(watermark.map_or(ledger, |watermark| ledger.max(watermark)))
	}
}
