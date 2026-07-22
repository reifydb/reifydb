// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	state::StateBytes,
};
use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	key::{EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey},
	state::store::StateStore,
};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::operator::stateful::utils::{internal_state_drop, state_drop};

pub struct FlowWindowStore<'a> {
	txn: &'a mut FlowTransaction,
	node: FlowNodeId,
	now_nanos: u64,
}

impl<'a> FlowWindowStore<'a> {
	pub fn new(txn: &'a mut FlowTransaction, node: FlowNodeId) -> Self {
		let now_nanos = txn.clock().now_nanos();
		Self {
			txn,
			node,
			now_nanos,
		}
	}
}

impl StateStore for FlowWindowStore<'_> {
	fn state_get(&mut self, key: &EncodedKey) -> Result<Option<StateBytes>> {
		match self.txn.state_get(self.node, key)? {
			Some(row) => Ok(Some(StateBytes::from_row(row)?)),
			None => Ok(None),
		}
	}

	fn state_get_many_visit(
		&mut self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
	) -> Result<()> {
		let batch = self.txn.state_get_many(self.node, keys)?;
		for r in batch.items {
			visit(r.key, StateBytes::from_row(r.row)?)?;
		}
		Ok(())
	}

	fn state_set(&mut self, key: &EncodedKey, payload: StateBytes) -> Result<()> {
		self.txn.state_set(self.node, key, payload.into_row())
	}

	fn state_remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.txn.state_remove(self.node, key)
	}

	fn state_drop(&mut self, key: &EncodedKey) -> Result<()> {
		state_drop(self.node, self.txn, key)
	}

	fn internal_get(&mut self, key: &EncodedKey) -> Result<Option<StateBytes>> {
		match self.txn.internal_state_get(self.node, key)? {
			Some(row) => Ok(Some(StateBytes::from_row(row)?)),
			None => Ok(None),
		}
	}

	fn internal_get_many_visit(
		&mut self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
	) -> Result<()> {
		let batch = self.txn.internal_state_get_many(self.node, keys)?;
		for r in batch.items {
			visit(r.key, StateBytes::from_row(r.row)?)?;
		}
		Ok(())
	}

	fn internal_set(&mut self, key: &EncodedKey, payload: StateBytes) -> Result<()> {
		self.txn.internal_state_set(self.node, key, payload.into_row())
	}

	fn internal_remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.txn.internal_state_remove(self.node, key)
	}

	fn internal_drop(&mut self, key: &EncodedKey) -> Result<()> {
		internal_state_drop(self.node, self.txn, key)
	}

	fn internal_range_visit(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
	) -> Result<()> {
		let batch = self.txn.internal_state_range(self.node, range, limit)?;
		for r in batch.items {
			if let Some(decoded) = FlowNodeInternalStateKey::decode(&r.key) {
				visit(EncodedKey::new(decoded.key), StateBytes::from_row(r.row)?)?;
			}
		}
		Ok(())
	}

	fn get_or_create_row_number(&mut self, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		self.txn.get_or_create_row_number(self.node, key)
	}

	fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		self.txn.get_or_create_row_numbers(self.node, keys)
	}

	fn drop_row_number(&mut self, key: &EncodedKey) -> Result<()> {
		self.txn.drop_row_number(self.node, key).map(|_| ())
	}

	fn clock_now_nanos(&self) -> u64 {
		self.now_nanos
	}
}
