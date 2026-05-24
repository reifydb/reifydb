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
	key::{EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey, flow_node_state::FlowNodeStateKey},
};
use reifydb_sdk::{
	error::{Result as SdkResult, SdkError},
	operator::{
		column::{row::Row, sink::native::NativeRowSink},
		context::{CatalogApi, InternalStateApi, OperatorContext, RowEmit, StateApi, StoreApi, UpdateEmit},
	},
	state::{StateEntry, decode_payload, encode_payload, row::RowNumberProvider},
};
use reifydb_type::{Result, value::row_number::RowNumber};
use serde::{Serialize, de::DeserializeOwned};

pub trait NativeBridge {
	fn clock_now_nanos(&self) -> u64;

	fn state_get(&mut self, key: &EncodedKey) -> Result<Option<EncodedRow>>;
	fn state_get_many(&mut self, keys: &[EncodedKey]) -> Result<Vec<(EncodedKey, EncodedRow)>>;
	fn state_set(&mut self, key: &EncodedKey, value: EncodedRow) -> Result<()>;
	fn state_remove(&mut self, key: &EncodedKey) -> Result<()>;
	fn state_clear(&mut self) -> Result<()>;
	fn state_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(EncodedKey, EncodedRow)>>;

	fn internal_state_get(&mut self, key: &EncodedKey) -> Result<Option<EncodedRow>>;
	fn internal_state_get_many(&mut self, keys: &[EncodedKey]) -> Result<Vec<(EncodedKey, EncodedRow)>>;
	fn internal_state_set(&mut self, key: &EncodedKey, value: EncodedRow) -> Result<()>;
	fn internal_state_remove(&mut self, key: &EncodedKey) -> Result<()>;

	fn store_get(&mut self, key: &EncodedKey) -> Result<Option<EncodedRow>>;
	fn store_contains(&mut self, key: &EncodedKey) -> Result<bool>;
	fn store_prefix(&mut self, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, EncodedRow)>>;
	fn store_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(EncodedKey, EncodedRow)>>;

	fn catalog_find_namespace(
		&mut self,
		namespace: NamespaceId,
		version: CommitVersion,
	) -> Result<Option<Namespace>>;
	fn catalog_find_namespace_by_name(
		&mut self,
		namespace: &str,
		version: CommitVersion,
	) -> Result<Option<Namespace>>;
	fn catalog_find_table(&mut self, table: TableId, version: CommitVersion) -> Result<Option<Table>>;
	fn catalog_find_table_by_name(
		&mut self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Result<Option<Table>>;
	fn catalog_find_row_shape(&mut self, fingerprint: RowShapeFingerprint) -> Result<Option<RowShape>>;

	fn state_get_many_visit(
		&mut self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(&EncodedKey, &EncodedRow) -> SdkResult<()>,
	) -> SdkResult<()>;
	fn internal_state_get_many_visit(
		&mut self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(&EncodedKey, &EncodedRow) -> SdkResult<()>,
	) -> SdkResult<()>;
	fn state_range_visit(
		&mut self,
		range: EncodedKeyRange,
		visit: &mut dyn FnMut(&EncodedKey, &EncodedRow) -> SdkResult<()>,
	) -> SdkResult<()>;
	fn store_range_visit(
		&mut self,
		range: EncodedKeyRange,
		visit: &mut dyn FnMut(&EncodedKey, &EncodedRow) -> SdkResult<()>,
	) -> SdkResult<()>;
	fn store_prefix_visit(
		&mut self,
		prefix: &EncodedKey,
		visit: &mut dyn FnMut(&EncodedKey, &EncodedRow) -> SdkResult<()>,
	) -> SdkResult<()>;
}

fn to_sdk_err<E: ToString>(e: E) -> SdkError {
	SdkError::Other(e.to_string())
}

fn decode<T: DeserializeOwned>(row: &EncodedRow) -> SdkResult<T> {
	decode_payload(row)
}

fn strip_state_envelope(stored: &EncodedKey) -> EncodedKey {
	FlowNodeStateKey::decode(stored).map(|k| EncodedKey::new(k.key)).unwrap_or_else(|| stored.clone())
}

fn strip_internal_envelope(stored: &EncodedKey) -> EncodedKey {
	FlowNodeInternalStateKey::decode(stored).map(|k| EncodedKey::new(k.key)).unwrap_or_else(|| stored.clone())
}

fn encode<T: Serialize>(value: &T, now_nanos: u64) -> SdkResult<EncodedRow> {
	encode_payload(value, now_nanos)
}

pub struct NativeOperatorContext<'a> {
	bridge: *mut (dyn NativeBridge + 'a),
	node: FlowNodeId,
	now_nanos: u64,
	diffs: Vec<Diff>,
	_marker: PhantomData<&'a mut (dyn NativeBridge + 'a)>,
}

impl<'a> NativeOperatorContext<'a> {
	pub fn new(bridge: &'a mut (dyn NativeBridge + 'a), node: FlowNodeId) -> Self {
		let now_nanos = bridge.clock_now_nanos();
		Self {
			bridge: bridge as *mut (dyn NativeBridge + 'a),
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
	now_nanos: u64,
}

impl RowEmit for NativeRowEmit<'_> {
	type Sink = NativeRowSink;
	fn sink(&mut self) -> &mut NativeRowSink {
		&mut self.sink
	}
	fn finish(self, row_numbers: &[RowNumber]) -> SdkResult<()> {
		let columns = self.sink.finish(row_numbers.to_vec(), self.now_nanos)?;
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
	now_nanos: u64,
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
		let pre_columns = self.pre.finish(row_numbers.to_vec(), self.now_nanos)?;
		let post_columns = self.post.finish(row_numbers.to_vec(), self.now_nanos)?;
		self.diffs.push(Diff::update(pre_columns, post_columns));
		Ok(())
	}
}

pub struct NativeState<'a> {
	bridge: *mut (dyn NativeBridge + 'a),
	now_nanos: u64,
	_marker: PhantomData<&'a mut (dyn NativeBridge + 'a)>,
}

impl StateApi for NativeState<'_> {
	fn get<T: DeserializeOwned>(&self, key: &EncodedKey) -> SdkResult<Option<T>> {
		match unsafe { (*self.bridge).state_get(key) }.map_err(to_sdk_err)? {
			Some(row) => Ok(Some(decode(&row)?)),
			None => Ok(None),
		}
	}
	fn set<T: Serialize>(&mut self, key: &EncodedKey, value: &T) -> SdkResult<()> {
		let now = self.now_nanos;
		unsafe { (*self.bridge).state_set(key, encode(value, now)?) }.map_err(to_sdk_err)
	}
	fn remove(&mut self, key: &EncodedKey) -> SdkResult<()> {
		unsafe { (*self.bridge).state_remove(key) }.map_err(to_sdk_err)
	}
	fn contains(&self, key: &EncodedKey) -> SdkResult<bool> {
		Ok(unsafe { (*self.bridge).state_get(key) }.map_err(to_sdk_err)?.is_some())
	}
	fn clear(&mut self) -> SdkResult<()> {
		unsafe { (*self.bridge).state_clear() }.map_err(to_sdk_err)
	}
	fn scan_prefix<T: DeserializeOwned>(&self, prefix: &EncodedKey) -> SdkResult<Vec<(EncodedKey, T)>> {
		let rows = unsafe { (*self.bridge).state_range(EncodedKeyRange::prefix(prefix.as_ref())) }
			.map_err(to_sdk_err)?;
		rows.into_iter().map(|(k, r)| Ok((strip_state_envelope(&k), decode(&r)?))).collect()
	}
	fn get_many<T: DeserializeOwned>(&self, keys: &[EncodedKey]) -> SdkResult<Vec<(EncodedKey, T)>> {
		let rows = unsafe { (*self.bridge).state_get_many(keys) }.map_err(to_sdk_err)?;
		rows.into_iter().map(|(k, r)| Ok((strip_state_envelope(&k), decode(&r)?))).collect()
	}
	fn keys_with_prefix(&self, prefix: &EncodedKey) -> SdkResult<Vec<EncodedKey>> {
		let rows = unsafe { (*self.bridge).state_range(EncodedKeyRange::prefix(prefix.as_ref())) }
			.map_err(to_sdk_err)?;
		Ok(rows.into_iter().map(|(k, _)| strip_state_envelope(&k)).collect())
	}
	fn range<T: DeserializeOwned>(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> SdkResult<Vec<(EncodedKey, T)>> {
		let range = EncodedKeyRange::new(start.map(|k| k.clone()), end.map(|k| k.clone()));
		let rows = unsafe { (*self.bridge).state_range(range) }.map_err(to_sdk_err)?;
		rows.into_iter().map(|(k, r)| Ok((strip_state_envelope(&k), decode(&r)?))).collect()
	}
	fn get_with_anchors<T: DeserializeOwned>(&self, key: &EncodedKey) -> SdkResult<Option<StateEntry<T>>> {
		match unsafe { (*self.bridge).state_get(key) }.map_err(to_sdk_err)? {
			Some(row) => Ok(Some(StateEntry {
				created_at_nanos: row.created_at_nanos(),
				updated_at_nanos: row.updated_at_nanos(),
				value: decode(&row)?,
			})),
			None => Ok(None),
		}
	}
	fn get_many_visit<T: DeserializeOwned>(
		&self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, T) -> SdkResult<()>,
	) -> SdkResult<()> {
		unsafe {
			(*self.bridge).state_get_many_visit(keys, &mut |k, row| {
				let value = decode::<T>(row)?;
				visit(strip_state_envelope(k), value)
			})
		}
	}
	fn range_visit<T: DeserializeOwned>(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		visit: &mut dyn FnMut(EncodedKey, T) -> SdkResult<()>,
	) -> SdkResult<()> {
		let range = EncodedKeyRange::new(start.map(|k| k.clone()), end.map(|k| k.clone()));
		unsafe {
			(*self.bridge).state_range_visit(range, &mut |k, row| {
				let value = decode::<T>(row)?;
				visit(strip_state_envelope(k), value)
			})
		}
	}
	fn scan_prefix_visit<T: DeserializeOwned>(
		&self,
		prefix: &EncodedKey,
		visit: &mut dyn FnMut(EncodedKey, T) -> SdkResult<()>,
	) -> SdkResult<()> {
		unsafe {
			(*self.bridge).state_range_visit(EncodedKeyRange::prefix(prefix.as_ref()), &mut |k, row| {
				let value = decode::<T>(row)?;
				visit(strip_state_envelope(k), value)
			})
		}
	}
}

pub struct NativeInternalState<'a> {
	bridge: *mut (dyn NativeBridge + 'a),
	now_nanos: u64,
	_marker: PhantomData<&'a mut (dyn NativeBridge + 'a)>,
}

impl InternalStateApi for NativeInternalState<'_> {
	fn get<T: DeserializeOwned>(&self, key: &EncodedKey) -> SdkResult<Option<T>> {
		match unsafe { (*self.bridge).internal_state_get(key) }.map_err(to_sdk_err)? {
			Some(row) => Ok(Some(decode(&row)?)),
			None => Ok(None),
		}
	}
	fn get_many<T: DeserializeOwned>(&self, keys: &[EncodedKey]) -> SdkResult<Vec<(EncodedKey, T)>> {
		let rows = unsafe { (*self.bridge).internal_state_get_many(keys) }.map_err(to_sdk_err)?;
		rows.into_iter().map(|(k, r)| Ok((strip_internal_envelope(&k), decode(&r)?))).collect()
	}
	fn set<T: Serialize>(&mut self, key: &EncodedKey, value: &T) -> SdkResult<()> {
		let now = self.now_nanos;
		unsafe { (*self.bridge).internal_state_set(key, encode(value, now)?) }.map_err(to_sdk_err)
	}
	fn remove(&mut self, key: &EncodedKey) -> SdkResult<()> {
		unsafe { (*self.bridge).internal_state_remove(key) }.map_err(to_sdk_err)
	}
	fn contains(&self, key: &EncodedKey) -> SdkResult<bool> {
		Ok(unsafe { (*self.bridge).internal_state_get(key) }.map_err(to_sdk_err)?.is_some())
	}
	fn get_many_visit<T: DeserializeOwned>(
		&self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, T) -> SdkResult<()>,
	) -> SdkResult<()> {
		unsafe {
			(*self.bridge).internal_state_get_many_visit(keys, &mut |k, row| {
				let value = decode::<T>(row)?;
				visit(strip_internal_envelope(k), value)
			})
		}
	}
}

pub struct NativeStore<'a> {
	bridge: *mut (dyn NativeBridge + 'a),
	_marker: PhantomData<&'a mut (dyn NativeBridge + 'a)>,
}

impl StoreApi for NativeStore<'_> {
	fn get(&self, key: &EncodedKey) -> SdkResult<Option<EncodedRow>> {
		unsafe { (*self.bridge).store_get(key) }.map_err(to_sdk_err)
	}
	fn contains(&self, key: &EncodedKey) -> SdkResult<bool> {
		unsafe { (*self.bridge).store_contains(key) }.map_err(to_sdk_err)
	}
	fn prefix(&self, prefix: &EncodedKey) -> SdkResult<Vec<(EncodedKey, EncodedRow)>> {
		unsafe { (*self.bridge).store_prefix(prefix) }.map_err(to_sdk_err)
	}
	fn range(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> SdkResult<Vec<(EncodedKey, EncodedRow)>> {
		let range = EncodedKeyRange::new(start.map(|k| k.clone()), end.map(|k| k.clone()));
		unsafe { (*self.bridge).store_range(range) }.map_err(to_sdk_err)
	}
	fn range_visit(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		visit: &mut dyn FnMut(EncodedKey, EncodedRow) -> SdkResult<()>,
	) -> SdkResult<()> {
		let range = EncodedKeyRange::new(start.map(|k| k.clone()), end.map(|k| k.clone()));
		unsafe { (*self.bridge).store_range_visit(range, &mut |k, row| visit(k.clone(), row.clone())) }
	}
	fn prefix_visit(
		&self,
		prefix: &EncodedKey,
		visit: &mut dyn FnMut(EncodedKey, EncodedRow) -> SdkResult<()>,
	) -> SdkResult<()> {
		unsafe { (*self.bridge).store_prefix_visit(prefix, &mut |k, row| visit(k.clone(), row.clone())) }
	}
}

pub struct NativeCatalog<'a> {
	bridge: *mut (dyn NativeBridge + 'a),
	_marker: PhantomData<&'a mut (dyn NativeBridge + 'a)>,
}

impl CatalogApi for NativeCatalog<'_> {
	fn find_namespace(&self, namespace: NamespaceId, version: CommitVersion) -> SdkResult<Option<Namespace>> {
		unsafe { (*self.bridge).catalog_find_namespace(namespace, version) }.map_err(to_sdk_err)
	}
	fn find_namespace_by_name(&self, namespace: &str, version: CommitVersion) -> SdkResult<Option<Namespace>> {
		unsafe { (*self.bridge).catalog_find_namespace_by_name(namespace, version) }.map_err(to_sdk_err)
	}
	fn find_table(&self, table: TableId, version: CommitVersion) -> SdkResult<Option<Table>> {
		unsafe { (*self.bridge).catalog_find_table(table, version) }.map_err(to_sdk_err)
	}
	fn find_table_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> SdkResult<Option<Table>> {
		unsafe { (*self.bridge).catalog_find_table_by_name(namespace, name, version) }.map_err(to_sdk_err)
	}
	fn find_row_shape(&self, fingerprint: RowShapeFingerprint) -> SdkResult<Option<RowShape>> {
		unsafe { (*self.bridge).catalog_find_row_shape(fingerprint) }.map_err(to_sdk_err)
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
			bridge: self.bridge,
			now_nanos: self.now_nanos,
			_marker: PhantomData,
		}
	}
	fn internal_state(&mut self) -> impl InternalStateApi + '_ {
		NativeInternalState {
			bridge: self.bridge,
			now_nanos: self.now_nanos,
			_marker: PhantomData,
		}
	}
	fn store(&mut self) -> impl StoreApi + '_ {
		NativeStore {
			bridge: self.bridge,
			_marker: PhantomData,
		}
	}
	fn catalog(&mut self) -> impl CatalogApi + '_ {
		NativeCatalog {
			bridge: self.bridge,
			_marker: PhantomData,
		}
	}
	fn get_or_create_row_number(&mut self, key: &EncodedKey) -> SdkResult<(RowNumber, bool)> {
		let provider = RowNumberProvider::new(self.node);
		provider.get_or_create_row_number(self, key)
	}
	fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> SdkResult<Vec<(RowNumber, bool)>> {
		let provider = RowNumberProvider::new(self.node);
		provider.get_or_create_row_numbers_batch(self, keys.iter())
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
		let now_nanos = self.now_nanos;
		Ok(NativeRowEmit {
			sink: NativeRowSink::new(R::COLUMNS)?,
			diffs: &mut self.diffs,
			kind: EmitKind::Insert,
			now_nanos,
		})
	}
	fn update_emit<R: Row>(&mut self, _row_capacity: usize) -> SdkResult<NativeUpdateEmit<'_>> {
		let now_nanos = self.now_nanos;
		Ok(NativeUpdateEmit {
			pre: NativeRowSink::new(R::COLUMNS)?,
			post: NativeRowSink::new(R::COLUMNS)?,
			diffs: &mut self.diffs,
			now_nanos,
		})
	}
	fn remove_emit<R: Row>(&mut self, _row_capacity: usize) -> SdkResult<NativeRowEmit<'_>> {
		let now_nanos = self.now_nanos;
		Ok(NativeRowEmit {
			sink: NativeRowSink::new(R::COLUMNS)?,
			diffs: &mut self.diffs,
			kind: EmitKind::Remove,
			now_nanos,
		})
	}
}
