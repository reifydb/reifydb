// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{marker::PhantomData, mem, ops::Bound, slice::from_ref};

use reifydb_codec::{
	encoded::{
		row::EncodedRow,
		shape::{RowShape, fingerprint::RowShapeFingerprint},
	},
	key::encoded::{EncodedKey, EncodedKeyRange},
	state::{OperatorState, StateBytes},
};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			flow::FlowNodeId,
			id::{NamespaceId, TableId},
			namespace::Namespace,
			table::Table,
		},
		change::Diff,
	},
	key::operator_state::{GroupId, StateKey},
};
use reifydb_sdk::{
	error::{Result as SdkResult, SdkError},
	operator::{
		column::{row::Row, sink::native::NativeRowSink},
		context::{CatalogApi, DictionaryApi, OperatorContext, RowEmit, StateApi, StoreApi, UpdateEmit},
	},
	state::{decode_payload, encode_payload},
};
use reifydb_value::{
	Result,
	error::Error as ValueError,
	value::{
		Value,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
		row_number::RowNumber,
	},
};

pub trait NativeBridge {
	fn clock_now(&self) -> DateTime;
	fn state_lease_bytes(&self) -> u64;

	fn state_get(&mut self, key: &StateKey) -> Result<Option<EncodedRow>>;
	fn state_get_many(&mut self, keys: &[StateKey]) -> Result<Vec<(StateKey, EncodedRow)>>;
	fn state_set(&mut self, key: &StateKey, value: EncodedRow) -> Result<()>;
	fn state_remove(&mut self, key: &StateKey) -> Result<()>;
	fn state_clear(&mut self) -> Result<()>;
	fn state_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(StateKey, EncodedRow)>>;

	fn intern_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<GroupId>>;
	fn lookup_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>>;
	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>>;
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()>;
	fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> Result<Vec<RowNumber>>;

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

	fn dictionary_id_by_name(&mut self, name: &str) -> Result<Option<DictionaryId>>;
	fn dictionary_find(&mut self, dictionary: DictionaryId, value: &Value) -> Result<Option<DictionaryEntryId>>;
	fn dictionary_get(&mut self, dictionary: DictionaryId, id: DictionaryEntryId) -> Result<Option<Value>>;

	fn state_get_many_visit(
		&mut self,
		keys: &[StateKey],
		visit: &mut dyn FnMut(&StateKey, &EncodedRow) -> SdkResult<()>,
	) -> SdkResult<()>;
	fn state_range_visit(
		&mut self,
		range: EncodedKeyRange,
		visit: &mut dyn FnMut(&StateKey, &EncodedRow) -> SdkResult<()>,
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

fn decode<T: OperatorState>(row: &EncodedRow) -> SdkResult<T> {
	decode_payload(row)
}

fn encode<T: OperatorState>(value: &T, now: DateTime) -> SdkResult<EncodedRow> {
	encode_payload(value, now)
}

pub struct NativeOperatorContext<'a> {
	bridge: *mut (dyn NativeBridge + 'a),
	node: FlowNodeId,
	now: DateTime,
	state_lease_bytes: u64,
	diffs: Vec<Diff>,
	_marker: PhantomData<&'a mut (dyn NativeBridge + 'a)>,
}

impl<'a> NativeOperatorContext<'a> {
	pub fn new(bridge: &'a mut (dyn NativeBridge + 'a), node: FlowNodeId) -> Self {
		let now = bridge.clock_now();
		let state_lease_bytes = bridge.state_lease_bytes();
		Self {
			bridge: bridge as *mut (dyn NativeBridge + 'a),
			node,
			now,
			state_lease_bytes,
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
	now: DateTime,
}

impl RowEmit for NativeRowEmit<'_> {
	type Sink = NativeRowSink;
	fn sink(&mut self) -> &mut NativeRowSink {
		&mut self.sink
	}
	fn finish(self, row_numbers: &[RowNumber]) -> SdkResult<()> {
		let columns = self.sink.finish(row_numbers.to_vec(), self.now)?;
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
	now: DateTime,
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
		let pre_columns = self.pre.finish(row_numbers.to_vec(), self.now)?;
		let post_columns = self.post.finish(row_numbers.to_vec(), self.now)?;
		self.diffs.push(Diff::update(pre_columns, post_columns));
		Ok(())
	}
}

pub struct NativeState<'a> {
	bridge: *mut (dyn NativeBridge + 'a),
	now: DateTime,
	_marker: PhantomData<&'a mut (dyn NativeBridge + 'a)>,
}

impl StateApi for NativeState<'_> {
	fn get<T: OperatorState>(&self, key: &StateKey) -> SdkResult<Option<T>> {
		match unsafe { (*self.bridge).state_get(key) }.map_err(to_sdk_err)? {
			Some(row) => Ok(Some(decode(&row)?)),
			None => Ok(None),
		}
	}
	fn set<T: OperatorState>(&mut self, key: &StateKey, value: &T) -> SdkResult<()> {
		let now = self.now;
		unsafe { (*self.bridge).state_set(key, encode(value, now)?) }.map_err(to_sdk_err)
	}
	fn remove(&mut self, key: &StateKey) -> SdkResult<()> {
		unsafe { (*self.bridge).state_remove(key) }.map_err(to_sdk_err)
	}
	fn contains(&self, key: &StateKey) -> SdkResult<bool> {
		Ok(unsafe { (*self.bridge).state_get(key) }.map_err(to_sdk_err)?.is_some())
	}
	fn clear(&mut self) -> SdkResult<()> {
		unsafe { (*self.bridge).state_clear() }.map_err(to_sdk_err)
	}
	fn scan_prefix<T: OperatorState>(&self, prefix: &StateKey) -> SdkResult<Vec<(StateKey, T)>> {
		let rows = unsafe { (*self.bridge).state_range(EncodedKeyRange::prefix(prefix.as_slice())) }
			.map_err(to_sdk_err)?;
		rows.into_iter().map(|(k, r)| Ok((k, decode(&r)?))).collect()
	}
	fn get_many<T: OperatorState>(&self, keys: &[StateKey]) -> SdkResult<Vec<(StateKey, T)>> {
		let rows = unsafe { (*self.bridge).state_get_many(keys) }.map_err(to_sdk_err)?;
		rows.into_iter().map(|(k, r)| Ok((k, decode(&r)?))).collect()
	}
	fn keys_with_prefix(&self, prefix: &StateKey) -> SdkResult<Vec<StateKey>> {
		let rows = unsafe { (*self.bridge).state_range(EncodedKeyRange::prefix(prefix.as_slice())) }
			.map_err(to_sdk_err)?;
		Ok(rows.into_iter().map(|(k, _)| k).collect())
	}
	fn range<T: OperatorState>(
		&self,
		start: Bound<&StateKey>,
		end: Bound<&StateKey>,
	) -> SdkResult<Vec<(StateKey, T)>> {
		let range = EncodedKeyRange::new(
			start.map(|k| k.as_encoded().clone()),
			end.map(|k| k.as_encoded().clone()),
		);
		let rows = unsafe { (*self.bridge).state_range(range) }.map_err(to_sdk_err)?;
		rows.into_iter().map(|(k, r)| Ok((k, decode(&r)?))).collect()
	}
	fn get_many_visit<T: OperatorState>(
		&self,
		keys: &[StateKey],
		visit: &mut dyn FnMut(StateKey, T) -> SdkResult<()>,
	) -> SdkResult<()> {
		unsafe {
			(*self.bridge).state_get_many_visit(keys, &mut |k, row| {
				let value = decode::<T>(row)?;
				visit(k.clone(), value)
			})
		}
	}
	fn range_visit<T: OperatorState>(
		&self,
		start: Bound<&StateKey>,
		end: Bound<&StateKey>,
		visit: &mut dyn FnMut(StateKey, T) -> SdkResult<()>,
	) -> SdkResult<()> {
		let range = EncodedKeyRange::new(
			start.map(|k| k.as_encoded().clone()),
			end.map(|k| k.as_encoded().clone()),
		);
		unsafe {
			(*self.bridge).state_range_visit(range, &mut |k, row| {
				let value = decode::<T>(row)?;
				visit(k.clone(), value)
			})
		}
	}
	fn scan_prefix_visit<T: OperatorState>(
		&self,
		prefix: &StateKey,
		visit: &mut dyn FnMut(StateKey, T) -> SdkResult<()>,
	) -> SdkResult<()> {
		unsafe {
			(*self.bridge).state_range_visit(EncodedKeyRange::prefix(prefix.as_slice()), &mut |k, row| {
				let value = decode::<T>(row)?;
				visit(k.clone(), value)
			})
		}
	}

	fn get_bytes(&self, key: &StateKey) -> SdkResult<Option<StateBytes>> {
		match unsafe { (*self.bridge).state_get(key) }.map_err(to_sdk_err)? {
			Some(row) => Ok(Some(StateBytes::from_row(row).map_err(ValueError::from)?)),
			None => Ok(None),
		}
	}

	fn set_bytes(&mut self, key: &StateKey, payload: StateBytes) -> SdkResult<()> {
		unsafe { (*self.bridge).state_set(key, payload.into_row()) }.map_err(to_sdk_err)
	}

	fn get_many_bytes_visit(
		&self,
		keys: &[StateKey],
		visit: &mut dyn FnMut(StateKey, StateBytes) -> SdkResult<()>,
	) -> SdkResult<()> {
		unsafe {
			(*self.bridge).state_get_many_visit(keys, &mut |k, row| {
				let bytes = StateBytes::from_row(row.clone()).map_err(ValueError::from)?;
				visit(k.clone(), bytes)
			})
		}
	}

	fn range_bytes_visit(
		&self,
		start: Bound<&StateKey>,
		end: Bound<&StateKey>,
		visit: &mut dyn FnMut(StateKey, StateBytes) -> SdkResult<()>,
	) -> SdkResult<()> {
		let range = EncodedKeyRange::new(
			start.map(|k| k.as_encoded().clone()),
			end.map(|k| k.as_encoded().clone()),
		);
		let rows = unsafe { (*self.bridge).state_range(range) }.map_err(to_sdk_err)?;
		for (k, row) in rows {
			let bytes = StateBytes::from_row(row).map_err(ValueError::from)?;
			visit(k, bytes)?;
		}
		Ok(())
	}

	fn now(&self) -> DateTime {
		self.now
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

pub struct NativeDictionary<'a> {
	bridge: *mut (dyn NativeBridge + 'a),
	_marker: PhantomData<&'a mut (dyn NativeBridge + 'a)>,
}

impl DictionaryApi for NativeDictionary<'_> {
	fn id_by_name(&mut self, name: &str) -> SdkResult<Option<DictionaryId>> {
		unsafe { (*self.bridge).dictionary_id_by_name(name) }.map_err(to_sdk_err)
	}
	fn find(&mut self, dictionary: DictionaryId, value: &Value) -> SdkResult<Option<DictionaryEntryId>> {
		unsafe { (*self.bridge).dictionary_find(dictionary, value) }.map_err(to_sdk_err)
	}
	fn get(&mut self, dictionary: DictionaryId, id: DictionaryEntryId) -> SdkResult<Option<Value>> {
		unsafe { (*self.bridge).dictionary_get(dictionary, id) }.map_err(to_sdk_err)
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
	fn clock_now(&self) -> DateTime {
		self.now
	}
	fn state_lease_bytes(&self) -> u64 {
		self.state_lease_bytes
	}
	fn state(&mut self) -> impl StateApi + '_ {
		NativeState {
			bridge: self.bridge,
			now: self.now,
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
	fn dictionary(&mut self) -> impl DictionaryApi + '_ {
		NativeDictionary {
			bridge: self.bridge,
			_marker: PhantomData,
		}
	}
	fn intern_groups(&mut self, groups: &[EncodedKey]) -> SdkResult<Vec<GroupId>> {
		unsafe { (*self.bridge).intern_groups(groups) }.map_err(to_sdk_err)
	}
	fn lookup_groups(&mut self, groups: &[EncodedKey]) -> SdkResult<Vec<Option<GroupId>>> {
		unsafe { (*self.bridge).lookup_groups(groups) }.map_err(to_sdk_err)
	}
	fn get_or_create_row_number(&mut self, group: GroupId, key: &EncodedKey) -> SdkResult<(RowNumber, bool)> {
		Ok(unsafe { (*self.bridge).get_or_create_row_numbers(group, from_ref(key)) }
			.map_err(to_sdk_err)?
			.into_iter()
			.next()
			.unwrap())
	}
	fn get_or_create_row_numbers(
		&mut self,
		group: GroupId,
		keys: &[EncodedKey],
	) -> SdkResult<Vec<(RowNumber, bool)>> {
		unsafe { (*self.bridge).get_or_create_row_numbers(group, keys) }.map_err(to_sdk_err)
	}
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> SdkResult<()> {
		unsafe { (*self.bridge).remove_row_number(group, key) }.map_err(to_sdk_err)
	}
	fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> SdkResult<Vec<RowNumber>> {
		unsafe { (*self.bridge).remove_row_numbers_below(group, upper) }.map_err(to_sdk_err)
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
		let now = self.now;
		Ok(NativeRowEmit {
			sink: NativeRowSink::new(R::COLUMNS)?,
			diffs: &mut self.diffs,
			kind: EmitKind::Insert,
			now,
		})
	}
	fn update_emit<R: Row>(&mut self, _row_capacity: usize) -> SdkResult<NativeUpdateEmit<'_>> {
		let now = self.now;
		Ok(NativeUpdateEmit {
			pre: NativeRowSink::new(R::COLUMNS)?,
			post: NativeRowSink::new(R::COLUMNS)?,
			diffs: &mut self.diffs,
			now,
		})
	}
	fn remove_emit<R: Row>(&mut self, _row_capacity: usize) -> SdkResult<NativeRowEmit<'_>> {
		let now = self.now;
		Ok(NativeRowEmit {
			sink: NativeRowSink::new(R::COLUMNS)?,
			diffs: &mut self.diffs,
			kind: EmitKind::Remove,
			now,
		})
	}
}
