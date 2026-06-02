// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use postcard::{from_bytes, to_stdvec};
use reifydb_core::{
	encoded::{
		key::EncodedKey,
		row::EncodedRow,
		shape::{RowShape, RowShapeField},
	},
	internal,
	key::{EncodableKey, flow_node_state::FlowNodeStateKey},
	row::Row,
	util::encoding::keycode::serializer::KeySerializer,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_runtime::hash::Hash128;
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	util::cowvec::CowVec,
	value::{Value, blob::Blob, row_number::RowNumber, value_type::ValueType},
};
use serde::{Deserialize, Serialize};

use super::operator::WindowOperator;
use crate::{operator::stateful::window::WindowStateful, transaction::FlowTransaction};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowLayout {
	pub names: Vec<String>,
	pub types: Vec<ValueType>,
}

impl WindowLayout {
	pub fn from_row(row: &Row) -> Self {
		Self {
			names: row.shape.field_names().map(|s| s.to_string()).collect(),
			types: row.shape.fields().iter().map(|f| f.constraint.get_type()).collect(),
		}
	}

	pub fn to_shape(&self) -> RowShape {
		let fields: Vec<RowShapeField> = self
			.names
			.iter()
			.zip(self.types.iter())
			.map(|(name, ty)| RowShapeField::unconstrained(name.clone(), ty.clone()))
			.collect();
		RowShape::new(fields)
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowEvent {
	pub row_number: RowNumber,
	pub timestamp: u64,
	#[serde(with = "serde_bytes")]
	pub encoded_bytes: Vec<u8>,
}

impl WindowEvent {
	pub fn from_row(row: &Row, timestamp: u64) -> Self {
		Self {
			row_number: row.number,
			timestamp,
			encoded_bytes: row.encoded.as_slice().to_vec(),
		}
	}

	pub fn to_row(&self, layout: &WindowLayout) -> Row {
		let shape = layout.to_shape();
		let encoded = EncodedRow(CowVec::new(self.encoded_bytes.clone()));
		Row {
			number: self.row_number,
			encoded,
			shape,
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WindowState {
	pub events: Vec<WindowEvent>,

	pub window_layout: Option<WindowLayout>,

	pub window_start: u64,

	pub event_count: u64,

	pub last_event_time: u64,

	pub running_totals: Vec<Value>,
}

impl WindowState {
	pub fn layout(&self) -> &WindowLayout {
		self.window_layout.as_ref().expect("WindowState layout must be set before accessing")
	}
}

impl WindowOperator {
	pub fn create_window_key(&self, group_hash: Hash128, window_id: u64) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"win:");
		serializer.extend_u128(group_hash);
		serializer.extend_u64(window_id);
		serializer.finish()
	}

	pub(super) fn create_row_index_key(&self, group_hash: Hash128, row_number: RowNumber) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"idx:");
		serializer.extend_u128(group_hash);
		serializer.extend_u64(row_number.0);
		serializer.finish()
	}

	pub fn store_row_index(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
		window_id: u64,
	) -> Result<()> {
		if self.is_rolling() {
			return Ok(());
		}
		let index_key = self.create_row_index_key(group_hash, row_number);
		let mut window_ids = self.lookup_row_index(txn, group_hash, row_number)?;
		if !window_ids.contains(&window_id) {
			window_ids.push(window_id);
		}
		let serialized = to_stdvec(&window_ids)
			.map_err(|e| Error(Box::new(internal!("Failed to serialize row index: {}", e))))?;
		let mut state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);
		self.save_state(txn, &index_key, state_row)
	}

	pub(super) fn lookup_row_index(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
	) -> Result<Vec<u64>> {
		if self.is_rolling() {
			let window_key = self.create_window_key(group_hash, 0);
			let window_state = self.load_window_state(txn, &window_key)?;
			return Ok(if window_state.events.iter().any(|e| e.row_number == row_number) {
				vec![0]
			} else {
				Vec::new()
			});
		}
		let index_key = self.create_row_index_key(group_hash, row_number);
		let state_row = self.load_state(txn, &index_key)?;
		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(Vec::new());
		}
		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(Vec::new());
		}
		let window_ids: Vec<u64> = from_bytes(blob.as_ref())
			.map_err(|e| Error(Box::new(internal!("Failed to deserialize row index: {}", e))))?;
		Ok(window_ids)
	}

	pub fn events_to_columns(&self, window_layout: &WindowLayout, events: &[WindowEvent]) -> Result<Columns> {
		if events.is_empty() {
			return Ok(Columns::new(Vec::new()));
		}

		let mut builders: Vec<ColumnBuffer> = window_layout
			.types
			.iter()
			.map(|ty| ColumnBuffer::with_capacity(ty.clone(), events.len()))
			.collect();

		for event in events.iter() {
			let row = event.to_row(window_layout);
			for (idx, builder) in builders.iter_mut().enumerate() {
				let value = row.shape.get_value(&row.encoded, idx);
				builder.push_value(value);
			}
		}

		let columns = window_layout
			.names
			.iter()
			.zip(builders)
			.map(|(name, data)| ColumnWithName {
				name: Fragment::internal(name.clone()),
				data,
			})
			.collect();

		Ok(Columns::new(columns))
	}

	pub fn load_window_state(&self, txn: &mut FlowTransaction, window_key: &EncodedKey) -> Result<WindowState> {
		let state_row = self.load_state(txn, window_key)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(WindowState::default());
		}

		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(WindowState::default());
		}

		from_bytes(blob.as_ref())
			.map_err(|e| Error(Box::new(internal!("Failed to deserialize WindowState: {}", e))))
	}

	pub fn save_window_state(
		&self,
		txn: &mut FlowTransaction,
		window_key: &EncodedKey,
		state: &WindowState,
	) -> Result<()> {
		let serialized = to_stdvec(state)
			.map_err(|e| Error(Box::new(internal!("Failed to serialize WindowState: {}", e))))?;

		let mut state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);

		self.save_state(txn, window_key, state_row)
	}

	pub fn get_and_increment_global_count(&self, txn: &mut FlowTransaction, group_hash: Hash128) -> Result<u64> {
		let count_key = self.create_count_key(group_hash);
		let count_row = self.load_state(txn, &count_key)?;

		let current_count = if count_row.is_empty() || !count_row.is_defined(0) {
			0
		} else {
			let blob = self.layout.get_blob(&count_row, 0);
			if blob.is_empty() {
				0
			} else {
				from_bytes(blob.as_ref()).unwrap_or(0)
			}
		};

		let new_count = current_count + 1;

		let serialized = to_stdvec(&new_count)
			.map_err(|e| Error(Box::new(internal!("Failed to serialize count: {}", e))))?;

		let mut count_state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut count_state_row, 0, &blob);

		self.save_state(txn, &count_key, count_state_row)?;

		Ok(current_count)
	}

	pub fn create_count_key(&self, group_hash: Hash128) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"cnt:");
		serializer.extend_u128(group_hash);
		serializer.finish()
	}

	pub(super) fn create_group_registry_key(&self) -> EncodedKey {
		EncodedKey::new(b"grp:")
	}

	pub fn load_group_registry(&self, txn: &mut FlowTransaction) -> Result<Vec<Hash128>> {
		let key = self.create_group_registry_key();
		let state_row = self.load_state(txn, &key)?;
		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(Vec::new());
		}
		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(Vec::new());
		}
		let groups: Vec<u128> = from_bytes(blob.as_ref()).unwrap_or_default();
		Ok(groups.into_iter().map(Hash128::from).collect())
	}

	pub(super) fn save_group_registry(&self, txn: &mut FlowTransaction, groups: &[Hash128]) -> Result<()> {
		let key = self.create_group_registry_key();
		let raw: Vec<u128> = groups.iter().map(|h| (*h).into()).collect();
		let serialized = to_stdvec(&raw)
			.map_err(|e| Error(Box::new(internal!("Failed to serialize group registry: {}", e))))?;
		let mut state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);
		self.save_state(txn, &key, state_row)
	}

	pub fn register_group(&self, txn: &mut FlowTransaction, group_hash: Hash128) -> Result<()> {
		let mut groups = self.load_group_registry(txn)?;
		if !groups.contains(&group_hash) {
			groups.push(group_hash);
			self.save_group_registry(txn, &groups)?;
		}
		Ok(())
	}

	#[inline]
	pub(super) fn scan_window_keys(&self, txn: &mut FlowTransaction) -> Result<Vec<EncodedKey>> {
		let all_state = txn.state_scan_all(self.node)?;
		let prefix = FlowNodeStateKey::new(self.node, vec![]).encode();
		let win_marker = b"win:";

		let mut keys = Vec::new();
		for item in &all_state.items {
			let full_key = &item.key;
			if full_key.len() <= prefix.len() {
				continue;
			}
			let inner = &full_key[prefix.len()..];
			if !inner.starts_with(win_marker) {
				continue;
			}
			keys.push(EncodedKey::new(inner));
		}
		Ok(keys)
	}
}
