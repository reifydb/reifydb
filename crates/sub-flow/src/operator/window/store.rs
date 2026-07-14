// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	key::{EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey},
	window::store::WindowStore,
};
use reifydb_sdk::state::{decode_payload, encode_payload};
use reifydb_value::{Result, value::row_number::RowNumber};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
	operator::stateful::{
		row::{RowNumberProvider, allocate_row_numbers},
		utils::{internal_state_drop, state_drop},
	},
	transaction::FlowTransaction,
};

pub struct FlowWindowStore<'a> {
	txn: &'a mut FlowTransaction,
	node: FlowNodeId,
	now_nanos: u64,
	row_numbers: &'a RowNumberProvider,
}

impl<'a> FlowWindowStore<'a> {
	pub fn new(txn: &'a mut FlowTransaction, node: FlowNodeId, row_numbers: &'a RowNumberProvider) -> Self {
		let now_nanos = txn.clock().now_nanos();
		Self {
			txn,
			node,
			now_nanos,
			row_numbers,
		}
	}
}

impl WindowStore for FlowWindowStore<'_> {
	fn state_get<V: DeserializeOwned>(&mut self, key: &EncodedKey) -> Result<Option<V>> {
		match self.txn.state_get(self.node, key)? {
			Some(row) => Ok(Some(decode_payload::<V>(&row)?)),
			None => Ok(None),
		}
	}

	fn state_get_many_visit<V: DeserializeOwned>(
		&mut self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, V) -> Result<()>,
	) -> Result<()> {
		let batch = self.txn.state_get_many(self.node, keys)?;
		for r in batch.items {
			let value = decode_payload::<V>(&r.row)?;
			visit(r.key, value)?;
		}
		Ok(())
	}

	fn state_set<V: Serialize>(&mut self, key: &EncodedKey, value: &V) -> Result<()> {
		self.txn.state_set(self.node, key, encode_payload(value, self.now_nanos)?)
	}

	fn state_remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.txn.state_remove(self.node, key)
	}

	fn state_drop(&mut self, key: &EncodedKey) -> Result<()> {
		state_drop(self.node, self.txn, key)
	}

	fn internal_get<V: DeserializeOwned>(&mut self, key: &EncodedKey) -> Result<Option<V>> {
		match self.txn.internal_state_get(self.node, key)? {
			Some(row) => Ok(Some(decode_payload::<V>(&row)?)),
			None => Ok(None),
		}
	}

	fn internal_get_many_visit<V: DeserializeOwned>(
		&mut self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, V) -> Result<()>,
	) -> Result<()> {
		let batch = self.txn.internal_state_get_many(self.node, keys)?;
		for r in batch.items {
			let value = decode_payload::<V>(&r.row)?;
			visit(r.key, value)?;
		}
		Ok(())
	}

	fn internal_set<V: Serialize>(&mut self, key: &EncodedKey, value: &V) -> Result<()> {
		self.txn.internal_state_set(self.node, key, encode_payload(value, self.now_nanos)?)
	}

	fn internal_remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.txn.internal_state_remove(self.node, key)
	}

	fn internal_drop(&mut self, key: &EncodedKey) -> Result<()> {
		internal_state_drop(self.node, self.txn, key)
	}

	fn internal_range_visit<V: DeserializeOwned>(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(EncodedKey, V) -> Result<()>,
	) -> Result<()> {
		let batch = self.txn.internal_state_range(self.node, range, limit)?;
		for r in batch.items {
			if let Some(decoded) = FlowNodeInternalStateKey::decode(&r.key) {
				let value = decode_payload::<V>(&r.row)?;
				visit(EncodedKey::new(decoded.key), value)?;
			}
		}
		Ok(())
	}

	fn get_or_create_row_number(&mut self, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		self.row_numbers.get_or_create_row_number(self.txn, key)
	}

	fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		self.row_numbers.get_or_create_row_numbers(self.txn, keys.iter())
	}

	fn drop_row_number(&mut self, key: &EncodedKey) -> Result<()> {
		self.row_numbers.remove_for_key(self.txn, key)?;
		Ok(())
	}

	fn allocate_row_numbers(&mut self, count: u64) -> Result<RowNumber> {
		allocate_row_numbers(self.txn, self.node, count).map(RowNumber)
	}

	fn clock_now_nanos(&self) -> u64 {
		self.now_nanos
	}
}
