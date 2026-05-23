// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{marker::PhantomData, mem, ops::Bound};

use postcard::{from_bytes, to_stdvec};
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
	value::column::columns::Columns,
};
use reifydb_sdk::{
	error::{FFIError, Result as SdkResult},
	operator::{
		column::{row::Row, sink::native::NativeRowSink},
		context::{CatalogApi, OperatorContext, StateApi, StoreApi},
	},
	state::StateEntry,
};
use reifydb_type::{Result, util::cowvec::CowVec, value::row_number::RowNumber};
use serde::{Serialize, de::DeserializeOwned};

use crate::{operator::stateful::row::RowNumberProvider, transaction::FlowTransaction};

fn to_ffi<E: ToString>(e: E) -> FFIError {
	FFIError::Other(e.to_string())
}

fn decode<T: DeserializeOwned>(row: &EncodedRow) -> SdkResult<T> {
	from_bytes(row.as_slice()).map_err(to_ffi)
}

fn encode<T: Serialize>(value: &T) -> SdkResult<EncodedRow> {
	Ok(EncodedRow(CowVec::new(to_stdvec(value).map_err(to_ffi)?)))
}

pub struct NativeOperatorContext<'a> {
	txn: *mut FlowTransaction,
	node: FlowNodeId,
	diffs: Vec<Diff>,
	_marker: PhantomData<&'a mut FlowTransaction>,
}

impl<'a> NativeOperatorContext<'a> {
	pub fn new(txn: &'a mut FlowTransaction, node: FlowNodeId) -> Self {
		Self {
			txn: txn as *mut FlowTransaction,
			node,
			diffs: Vec::new(),
			_marker: PhantomData,
		}
	}

	pub fn take_diffs(&mut self) -> Vec<Diff> {
		mem::take(&mut self.diffs)
	}
}

fn encode_rows<R: Row>(rows: &[R], row_numbers: &[RowNumber]) -> SdkResult<Columns> {
	let mut sink = NativeRowSink::new(R::COLUMNS)?;
	for row in rows {
		row.encode_into(&mut sink)?;
	}
	sink.finish(row_numbers.to_vec())
}

pub struct NativeState {
	txn: *mut FlowTransaction,
	node: FlowNodeId,
}

impl StateApi for NativeState {
	fn get<T: DeserializeOwned>(&self, key: &EncodedKey) -> SdkResult<Option<T>> {
		match unsafe { (*self.txn).state_get(self.node, key) }.map_err(to_ffi)? {
			Some(row) => Ok(Some(decode(&row)?)),
			None => Ok(None),
		}
	}
	fn set<T: Serialize>(&mut self, key: &EncodedKey, value: &T) -> SdkResult<()> {
		unsafe { (*self.txn).state_set(self.node, key, encode(value)?) }.map_err(to_ffi)
	}
	fn remove(&mut self, key: &EncodedKey) -> SdkResult<()> {
		unsafe { (*self.txn).state_remove(self.node, key) }.map_err(to_ffi)
	}
	fn contains(&self, key: &EncodedKey) -> SdkResult<bool> {
		Ok(unsafe { (*self.txn).state_get(self.node, key) }.map_err(to_ffi)?.is_some())
	}
	fn clear(&mut self) -> SdkResult<()> {
		unsafe { (*self.txn).state_clear(self.node) }.map_err(to_ffi)
	}
	fn scan_prefix<T: DeserializeOwned>(&self, prefix: &EncodedKey) -> SdkResult<Vec<(EncodedKey, T)>> {
		let batch = unsafe { (*self.txn).state_range(self.node, EncodedKeyRange::prefix(prefix.as_ref())) }
			.map_err(to_ffi)?;
		batch.items.iter().map(|r| Ok((r.key.clone(), decode(&r.row)?))).collect()
	}
	fn get_many<T: DeserializeOwned>(&self, keys: &[EncodedKey]) -> SdkResult<Vec<(EncodedKey, T)>> {
		let batch = unsafe { (*self.txn).state_get_many(self.node, keys) }.map_err(to_ffi)?;
		batch.items.iter().map(|r| Ok((r.key.clone(), decode(&r.row)?))).collect()
	}
	fn keys_with_prefix(&self, prefix: &EncodedKey) -> SdkResult<Vec<EncodedKey>> {
		let batch = unsafe { (*self.txn).state_range(self.node, EncodedKeyRange::prefix(prefix.as_ref())) }
			.map_err(to_ffi)?;
		Ok(batch.items.iter().map(|r| r.key.clone()).collect())
	}
	fn range<T: DeserializeOwned>(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> SdkResult<Vec<(EncodedKey, T)>> {
		let range = EncodedKeyRange::new(start.map(|k| k.clone()), end.map(|k| k.clone()));
		let batch = unsafe { (*self.txn).state_range(self.node, range) }.map_err(to_ffi)?;
		batch.items.iter().map(|r| Ok((r.key.clone(), decode(&r.row)?))).collect()
	}
	fn get_with_anchors<T: DeserializeOwned>(&self, key: &EncodedKey) -> SdkResult<Option<StateEntry<T>>> {
		match unsafe { (*self.txn).state_get(self.node, key) }.map_err(to_ffi)? {
			Some(row) => Ok(Some(StateEntry {
				created_at_nanos: 0,
				updated_at_nanos: 0,
				value: decode(&row)?,
			})),
			None => Ok(None),
		}
	}
}

pub struct NativeStore {
	txn: *mut FlowTransaction,
}

impl StoreApi for NativeStore {
	fn get(&self, key: &EncodedKey) -> SdkResult<Option<EncodedRow>> {
		unsafe { (*self.txn).get(key) }.map_err(to_ffi)
	}
	fn contains(&self, key: &EncodedKey) -> SdkResult<bool> {
		unsafe { (*self.txn).contains_key(key) }.map_err(to_ffi)
	}
	fn prefix(&self, prefix: &EncodedKey) -> SdkResult<Vec<(EncodedKey, EncodedRow)>> {
		let batch = unsafe { (*self.txn).prefix(prefix) }.map_err(to_ffi)?;
		Ok(batch.items.into_iter().map(|r| (r.key, r.row)).collect())
	}
	fn range(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> SdkResult<Vec<(EncodedKey, EncodedRow)>> {
		let range = EncodedKeyRange::new(start.map(|k| k.clone()), end.map(|k| k.clone()));
		let rows = unsafe { (*self.txn).range(range, 1024) }.collect::<Result<Vec<_>>>().map_err(to_ffi)?;
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
	fn operator_id(&self) -> FlowNodeId {
		self.node
	}
	fn clock_now_nanos(&self) -> u64 {
		unsafe { (*self.txn).clock().now_nanos() }
	}
	fn state(&mut self) -> impl StateApi + '_ {
		NativeState {
			txn: self.txn,
			node: self.node,
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
			.map_err(to_ffi)
	}
	fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> SdkResult<Vec<(RowNumber, bool)>> {
		RowNumberProvider::new(self.node)
			.get_or_create_row_numbers(unsafe { &mut *self.txn }, keys.iter())
			.map_err(to_ffi)
	}
	fn shape_for_row(&mut self, _row: &EncodedRow) -> SdkResult<RowShape> {
		Err(FFIError::Other("shape_for_row is not supported in the native context".to_string()))
	}
	fn emit_insert<R: Row>(&mut self, rows: &[R], row_numbers: &[RowNumber]) -> SdkResult<()> {
		if rows.is_empty() {
			return Ok(());
		}
		let columns = encode_rows(rows, row_numbers)?;
		self.diffs.push(Diff::insert(columns));
		Ok(())
	}
	fn emit_update<R: Row>(&mut self, pre: &[R], post: &[R], row_numbers: &[RowNumber]) -> SdkResult<()> {
		if row_numbers.is_empty() {
			return Ok(());
		}
		let pre_columns = encode_rows(pre, row_numbers)?;
		let post_columns = encode_rows(post, row_numbers)?;
		self.diffs.push(Diff::update(pre_columns, post_columns));
		Ok(())
	}
	fn emit_remove<R: Row>(&mut self, rows: &[R], row_numbers: &[RowNumber]) -> SdkResult<()> {
		if rows.is_empty() {
			return Ok(());
		}
		let columns = encode_rows(rows, row_numbers)?;
		self.diffs.push(Diff::remove(columns));
		Ok(())
	}
}
