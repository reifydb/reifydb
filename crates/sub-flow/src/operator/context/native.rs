// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{marker::PhantomData, mem, ops::Bound};

use reifydb_core::{
	common::CommitVersion,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
		shape::{RowShape, fingerprint::RowShapeFingerprint},
	},
	interface::{
		catalog::{
			flow::FlowNodeId,
			id::{NamespaceId, TableId},
			namespace::Namespace,
			table::Table,
		},
		change::Diff,
	},
};
use reifydb_sdk::{
	error::{Result as SdkResult, SdkError},
	operator::{
		column::{row::Row, sink::native::NativeRowSink},
		context::{CatalogApi, InternalStateApi, OperatorContext, RowEmit, StateApi, StoreApi, UpdateEmit},
	},
	state::{StateEntry, decode_payload, encode_payload},
};
use reifydb_type::{Result, value::row_number::RowNumber};
use serde::{Serialize, de::DeserializeOwned};

use crate::{operator::stateful::row::RowNumberProvider, transaction::FlowTransaction};

fn to_sdk_err<E: ToString>(e: E) -> SdkError {
	SdkError::Other(e.to_string())
}

fn decode<T: DeserializeOwned>(row: &EncodedRow) -> SdkResult<T> {
	decode_payload(row)
}

fn encode<T: Serialize>(value: &T, now_nanos: u64) -> SdkResult<EncodedRow> {
	encode_payload(value, now_nanos)
}

pub struct NativeOperatorContext<'a> {
	txn: *mut FlowTransaction,
	node: FlowNodeId,
	now_nanos: u64,
	diffs: Vec<Diff>,
	_marker: PhantomData<&'a mut FlowTransaction>,
}

impl<'a> NativeOperatorContext<'a> {
	pub fn new(txn: &'a mut FlowTransaction, node: FlowNodeId) -> Self {
		let now_nanos = txn.clock().now_nanos();
		Self {
			txn: txn as *mut FlowTransaction,
			node,
			now_nanos,
			diffs: Vec::new(),
			_marker: PhantomData,
		}
	}

	pub fn take_diffs(&mut self) -> Vec<Diff> {
		mem::take(&mut self.diffs)
	}
}

enum EmitKind {
	Insert,
	Remove,
}

pub struct NativeRowEmit<'a> {
	sink: NativeRowSink,
	diffs: &'a mut Vec<Diff>,
	kind: EmitKind,
}

impl RowEmit for NativeRowEmit<'_> {
	type Sink = NativeRowSink;
	fn sink(&mut self) -> &mut NativeRowSink {
		&mut self.sink
	}
	fn finish(self, row_numbers: &[RowNumber]) -> SdkResult<()> {
		let columns = self.sink.finish(row_numbers.to_vec())?;
		match self.kind {
			EmitKind::Insert => self.diffs.push(Diff::insert(columns)),
			EmitKind::Remove => self.diffs.push(Diff::remove(columns)),
		}
		Ok(())
	}
}

pub struct NativeUpdateEmit<'a> {
	pre: NativeRowSink,
	post: NativeRowSink,
	diffs: &'a mut Vec<Diff>,
}

impl UpdateEmit for NativeUpdateEmit<'_> {
	type Sink = NativeRowSink;
	fn pre(&mut self) -> &mut NativeRowSink {
		&mut self.pre
	}
	fn post(&mut self) -> &mut NativeRowSink {
		&mut self.post
	}
	fn finish(self, row_numbers: &[RowNumber]) -> SdkResult<()> {
		let pre_columns = self.pre.finish(row_numbers.to_vec())?;
		let post_columns = self.post.finish(row_numbers.to_vec())?;
		self.diffs.push(Diff::update(pre_columns, post_columns));
		Ok(())
	}
}

pub struct NativeState {
	txn: *mut FlowTransaction,
	node: FlowNodeId,
	now_nanos: u64,
}

impl StateApi for NativeState {
	fn get<T: DeserializeOwned>(&self, key: &EncodedKey) -> SdkResult<Option<T>> {
		match unsafe { (*self.txn).state_get(self.node, key) }.map_err(to_sdk_err)? {
			Some(row) => Ok(Some(decode(&row)?)),
			None => Ok(None),
		}
	}
	fn set<T: Serialize>(&mut self, key: &EncodedKey, value: &T) -> SdkResult<()> {
		let now = self.now_nanos;
		unsafe { (*self.txn).state_set(self.node, key, encode(value, now)?) }.map_err(to_sdk_err)
	}
	fn remove(&mut self, key: &EncodedKey) -> SdkResult<()> {
		unsafe { (*self.txn).state_remove(self.node, key) }.map_err(to_sdk_err)
	}
	fn contains(&self, key: &EncodedKey) -> SdkResult<bool> {
		Ok(unsafe { (*self.txn).state_get(self.node, key) }.map_err(to_sdk_err)?.is_some())
	}
	fn clear(&mut self) -> SdkResult<()> {
		unsafe { (*self.txn).state_clear(self.node) }.map_err(to_sdk_err)
	}
	fn scan_prefix<T: DeserializeOwned>(&self, prefix: &EncodedKey) -> SdkResult<Vec<(EncodedKey, T)>> {
		let batch = unsafe { (*self.txn).state_range(self.node, EncodedKeyRange::prefix(prefix.as_ref())) }
			.map_err(to_sdk_err)?;
		batch.items.iter().map(|r| Ok((r.key.clone(), decode(&r.row)?))).collect()
	}
	fn get_many<T: DeserializeOwned>(&self, keys: &[EncodedKey]) -> SdkResult<Vec<(EncodedKey, T)>> {
		let batch = unsafe { (*self.txn).state_get_many(self.node, keys) }.map_err(to_sdk_err)?;
		batch.items.iter().map(|r| Ok((r.key.clone(), decode(&r.row)?))).collect()
	}
	fn keys_with_prefix(&self, prefix: &EncodedKey) -> SdkResult<Vec<EncodedKey>> {
		let batch = unsafe { (*self.txn).state_range(self.node, EncodedKeyRange::prefix(prefix.as_ref())) }
			.map_err(to_sdk_err)?;
		Ok(batch.items.iter().map(|r| r.key.clone()).collect())
	}
	fn range<T: DeserializeOwned>(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> SdkResult<Vec<(EncodedKey, T)>> {
		let range = EncodedKeyRange::new(start.map(|k| k.clone()), end.map(|k| k.clone()));
		let batch = unsafe { (*self.txn).state_range(self.node, range) }.map_err(to_sdk_err)?;
		batch.items.iter().map(|r| Ok((r.key.clone(), decode(&r.row)?))).collect()
	}
	fn get_with_anchors<T: DeserializeOwned>(&self, key: &EncodedKey) -> SdkResult<Option<StateEntry<T>>> {
		match unsafe { (*self.txn).state_get(self.node, key) }.map_err(to_sdk_err)? {
			Some(row) => Ok(Some(StateEntry {
				created_at_nanos: row.created_at_nanos(),
				updated_at_nanos: row.updated_at_nanos(),
				value: decode(&row)?,
			})),
			None => Ok(None),
		}
	}
}

pub struct NativeInternalState {
	txn: *mut FlowTransaction,
	node: FlowNodeId,
	now_nanos: u64,
}

impl InternalStateApi for NativeInternalState {
	fn get<T: DeserializeOwned>(&self, key: &EncodedKey) -> SdkResult<Option<T>> {
		match unsafe { (*self.txn).internal_state_get(self.node, key) }.map_err(to_sdk_err)? {
			Some(row) => Ok(Some(decode(&row)?)),
			None => Ok(None),
		}
	}
	fn get_many<T: DeserializeOwned>(&self, keys: &[EncodedKey]) -> SdkResult<Vec<(EncodedKey, T)>> {
		let batch = unsafe { (*self.txn).internal_state_get_many(self.node, keys) }.map_err(to_sdk_err)?;
		batch.items.iter().map(|r| Ok((r.key.clone(), decode(&r.row)?))).collect()
	}
	fn set<T: Serialize>(&mut self, key: &EncodedKey, value: &T) -> SdkResult<()> {
		let now = self.now_nanos;
		unsafe { (*self.txn).internal_state_set(self.node, key, encode(value, now)?) }.map_err(to_sdk_err)
	}
	fn remove(&mut self, key: &EncodedKey) -> SdkResult<()> {
		unsafe { (*self.txn).internal_state_remove(self.node, key) }.map_err(to_sdk_err)
	}
	fn contains(&self, key: &EncodedKey) -> SdkResult<bool> {
		Ok(unsafe { (*self.txn).internal_state_get(self.node, key) }.map_err(to_sdk_err)?.is_some())
	}
}

pub struct NativeStore {
	txn: *mut FlowTransaction,
}

impl StoreApi for NativeStore {
	fn get(&self, key: &EncodedKey) -> SdkResult<Option<EncodedRow>> {
		unsafe { (*self.txn).get(key) }.map_err(to_sdk_err)
	}
	fn contains(&self, key: &EncodedKey) -> SdkResult<bool> {
		unsafe { (*self.txn).contains_key(key) }.map_err(to_sdk_err)
	}
	fn prefix(&self, prefix: &EncodedKey) -> SdkResult<Vec<(EncodedKey, EncodedRow)>> {
		let batch = unsafe { (*self.txn).prefix(prefix) }.map_err(to_sdk_err)?;
		Ok(batch.items.into_iter().map(|r| (r.key, r.row)).collect())
	}
	fn range(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> SdkResult<Vec<(EncodedKey, EncodedRow)>> {
		let range = EncodedKeyRange::new(start.map(|k| k.clone()), end.map(|k| k.clone()));
		let rows = unsafe { (*self.txn).range(range, 1024) }.collect::<Result<Vec<_>>>().map_err(to_sdk_err)?;
		Ok(rows.into_iter().map(|r| (r.key, r.row)).collect())
	}
}

pub struct NativeCatalog {
	txn: *mut FlowTransaction,
}

impl CatalogApi for NativeCatalog {
	fn find_namespace(&self, namespace: NamespaceId, version: CommitVersion) -> SdkResult<Option<Namespace>> {
		Ok(unsafe { (*self.txn).host_catalog() }.find_namespace(namespace, version))
	}
	fn find_namespace_by_name(&self, namespace: &str, version: CommitVersion) -> SdkResult<Option<Namespace>> {
		Ok(unsafe { (*self.txn).host_catalog() }.find_namespace_by_name(namespace, version))
	}
	fn find_table(&self, table: TableId, version: CommitVersion) -> SdkResult<Option<Table>> {
		Ok(unsafe { (*self.txn).host_catalog() }.find_table(table, version))
	}
	fn find_table_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> SdkResult<Option<Table>> {
		Ok(unsafe { (*self.txn).host_catalog() }.find_table_by_name(namespace, name, version))
	}
	fn find_row_shape(&self, fingerprint: RowShapeFingerprint) -> SdkResult<Option<RowShape>> {
		Ok(unsafe { (*self.txn).host_catalog() }.find_row_shape(fingerprint))
	}
}

impl OperatorContext for NativeOperatorContext<'_> {
	type InsertEmit<'a>
		= NativeRowEmit<'a>
	where
		Self: 'a;
	type UpdateEmit<'a>
		= NativeUpdateEmit<'a>
	where
		Self: 'a;
	type RemoveEmit<'a>
		= NativeRowEmit<'a>
	where
		Self: 'a;

	fn operator_id(&self) -> FlowNodeId {
		self.node
	}
	fn clock_now_nanos(&self) -> u64 {
		self.now_nanos
	}
	fn state(&mut self) -> impl StateApi + '_ {
		NativeState {
			txn: self.txn,
			node: self.node,
			now_nanos: self.now_nanos,
		}
	}
	fn internal_state(&mut self) -> impl InternalStateApi + '_ {
		NativeInternalState {
			txn: self.txn,
			node: self.node,
			now_nanos: self.now_nanos,
		}
	}
	fn store(&mut self) -> impl StoreApi + '_ {
		NativeStore {
			txn: self.txn,
		}
	}
	fn catalog(&mut self) -> impl CatalogApi + '_ {
		NativeCatalog {
			txn: self.txn,
		}
	}
	fn get_or_create_row_number(&mut self, key: &EncodedKey) -> SdkResult<(RowNumber, bool)> {
		RowNumberProvider::new(self.node)
			.get_or_create_row_number(unsafe { &mut *self.txn }, key)
			.map_err(to_sdk_err)
	}
	fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> SdkResult<Vec<(RowNumber, bool)>> {
		RowNumberProvider::new(self.node)
			.get_or_create_row_numbers(unsafe { &mut *self.txn }, keys.iter())
			.map_err(to_sdk_err)
	}
	fn shape_for_row(&mut self, row: &EncodedRow) -> SdkResult<RowShape> {
		let fingerprint = row.fingerprint();
		match self.catalog().find_row_shape(fingerprint)? {
			Some(shape) => Ok(shape),
			None => Err(SdkError::Other(format!(
				"row shape with fingerprint {} not registered in catalog",
				fingerprint.as_u64()
			))),
		}
	}
	fn insert_emit<R: Row>(&mut self, _row_capacity: usize) -> SdkResult<NativeRowEmit<'_>> {
		Ok(NativeRowEmit {
			sink: NativeRowSink::new(R::COLUMNS)?,
			diffs: &mut self.diffs,
			kind: EmitKind::Insert,
		})
	}
	fn update_emit<R: Row>(&mut self, _row_capacity: usize) -> SdkResult<NativeUpdateEmit<'_>> {
		Ok(NativeUpdateEmit {
			pre: NativeRowSink::new(R::COLUMNS)?,
			post: NativeRowSink::new(R::COLUMNS)?,
			diffs: &mut self.diffs,
		})
	}
	fn remove_emit<R: Row>(&mut self, _row_capacity: usize) -> SdkResult<NativeRowEmit<'_>> {
		Ok(NativeRowEmit {
			sink: NativeRowSink::new(R::COLUMNS)?,
			diffs: &mut self.diffs,
			kind: EmitKind::Remove,
		})
	}
}
