// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	state::StateBytes,
};
use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	key::{
		EncodableKey,
		flow_node_state::FlowNodeStateKey,
		operator_state::{GroupId, StateKey},
	},
	state::{
		store::StateStore,
	},
};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};

pub struct OperatorStateStore<'a> {
	txn: &'a mut FlowTransaction,
	node: FlowNodeId,
	now: DateTime,
}

impl<'a> OperatorStateStore<'a> {
	pub fn new(txn: &'a mut FlowTransaction, node: FlowNodeId) -> Self {
		let now = txn.clock().now();
		Self {
			txn,
			node,
			now,
		}
	}
}

impl StateStore for OperatorStateStore<'_> {
	fn state_get(&mut self, key: &StateKey) -> Result<Option<StateBytes>> {
		match self.txn.state_get(self.node, key)? {
			Some(row) => Ok(Some(StateBytes::from_row(row)?)),
			None => Ok(None),
		}
	}

	fn state_get_many_visit(
		&mut self,
		keys: &[StateKey],
		visit: &mut dyn FnMut(StateKey, StateBytes) -> Result<()>,
	) -> Result<()> {
		let batch = self.txn.state_get_many(self.node, keys)?;
		for r in batch.items {
			let Some(decoded) = FlowNodeStateKey::decode(&r.key) else {
				continue;
			};
			let Some(inner) = StateKey::from_framed(EncodedKey::new(decoded.key)) else {
				continue;
			};
			visit(inner, StateBytes::from_row(r.row)?)?;
		}
		Ok(())
	}

	fn state_set(&mut self, key: &StateKey, payload: StateBytes) -> Result<()> {
		self.txn.state_set(self.node, key, payload.into_row())
	}

	fn state_remove(&mut self, key: &StateKey) -> Result<()> {
		self.txn.state_remove(self.node, key)
	}

	fn state_range_visit(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(StateKey, StateBytes) -> Result<()>,
	) -> Result<()> {
		let batch = self.txn.state_range(self.node, range, limit)?;
		for r in batch.items {
			if let Some(decoded) = FlowNodeStateKey::decode(&r.key)
				&& let Some(inner) = StateKey::from_framed(EncodedKey::new(decoded.key))
			{
				visit(inner, StateBytes::from_row(r.row)?)?;
			}
		}
		Ok(())
	}

	fn intern_group(&mut self, group: &EncodedKey) -> Result<GroupId> {
		Ok(self.txn.intern_group(self.node, group)?.0)
	}

	fn lookup_group(&mut self, group: &EncodedKey) -> Result<Option<GroupId>> {
		self.txn.lookup_group(self.node, group)
	}

	fn get_or_create_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		self.txn.get_or_create_row_number(self.node, group, key)
	}

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		self.txn.get_or_create_row_numbers(self.node, group, keys)
	}

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		self.txn.remove_row_number(self.node, group, key).map(|_| ())
	}

	fn clock_now(&self) -> DateTime {
		self.now
	}
}
