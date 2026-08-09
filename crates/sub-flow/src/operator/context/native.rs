// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{marker::PhantomData, mem, ops::Bound, slice::from_ref};

use reifydb_abi::operator::timer::TimerKind;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{
		bytes::{EncodedBytes, read_fingerprint},
		operator::{EncodedOperatorRow, OperatorState},
		shape::{RowShape, fingerprint::RowShapeFingerprint},
	},
};
use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::flow::OperatorId, change::Diff},
	key::operator_group_state::{GroupId, GroupStateKey},
};
use reifydb_flow::window::event::Polarity;
use reifydb_sdk::{
	error::{Result as SdkResult, SdkError},
	operator::{
		column::{row::Row, sink::native::NativeRowSink},
		context::{DictionaryApi, OperatorContext, RowEmit, RowShapeApi, StateApi, StoreApi, UpdateEmit},
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
	fn written_at(&self) -> DateTime;
	fn version(&self) -> CommitVersion;
	fn state_lease_bytes(&self) -> u64;

	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedBytes>>;
	fn state_get_many(&mut self, keys: &[GroupStateKey]) -> Result<Vec<(GroupStateKey, EncodedBytes)>>;
	fn state_set(&mut self, key: &GroupStateKey, value: EncodedBytes) -> Result<()>;
	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()>;
	fn state_clear(&mut self) -> Result<()>;
	fn state_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(GroupStateKey, EncodedBytes)>>;

	fn intern_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<GroupId>>;
	fn lookup_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>>;
	fn arm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()>;
	fn disarm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()>;
	fn flow_watermark(&mut self) -> Result<Option<DateTime>>;
	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>>;
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()>;
	fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> Result<Vec<RowNumber>>;

	fn store_get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>>;
	fn store_contains(&mut self, key: &EncodedKey) -> Result<bool>;
	fn store_prefix(&mut self, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, EncodedBytes)>>;
	fn store_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(EncodedKey, EncodedBytes)>>;

	fn catalog_find_row_shape(&mut self, fingerprint: RowShapeFingerprint) -> Result<Option<RowShape>>;

	fn dictionary_id_by_name(&mut self, name: &str) -> Result<Option<DictionaryId>>;
	fn dictionary_find(&mut self, dictionary: DictionaryId, value: &Value) -> Result<Option<DictionaryEntryId>>;
	fn dictionary_get(&mut self, dictionary: DictionaryId, id: DictionaryEntryId) -> Result<Option<Value>>;

	fn state_get_many_visit(
		&mut self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(&GroupStateKey, &EncodedBytes) -> SdkResult<()>,
	) -> SdkResult<()>;
}

fn to_sdk_err<E: ToString>(e: E) -> SdkError {
	SdkError::Other(e.to_string())
}

fn decode<T: OperatorState>(bytes: &EncodedBytes) -> SdkResult<T> {
	decode_payload(&EncodedOperatorRow::try_from(bytes.clone()).map_err(ValueError::from)?)
}

fn encode<T: OperatorState>(value: &T, now: DateTime) -> SdkResult<EncodedBytes> {
	Ok(encode_payload(value, now)?.into_bytes())
}

pub struct NativeOperatorContext<'a> {
	bridge: *mut (dyn NativeBridge + 'a),
	operator: OperatorId,
	now: DateTime,
	state_lease_bytes: u64,
	diffs: Vec<Diff>,
	_marker: PhantomData<&'a mut (dyn NativeBridge + 'a)>,
}

impl<'a> NativeOperatorContext<'a> {
	pub fn new(bridge: &'a mut (dyn NativeBridge + 'a), operator: OperatorId) -> Self {
		let now = bridge.written_at();
		let state_lease_bytes = bridge.state_lease_bytes();
		Self {
			bridge: bridge as *mut (dyn NativeBridge + 'a),
			operator,
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

pub struct NativeRowEmit<'a> {
	sink: NativeRowSink,
	diffs: &'a mut Vec<Diff>,
	kind: Polarity,
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
			Polarity::Insert => self.diffs.push(Diff::insert(columns)),
			Polarity::Remove => self.diffs.push(Diff::remove(columns)),
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
	fn get<T: OperatorState>(&self, key: &GroupStateKey) -> SdkResult<Option<T>> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		match unsafe { (*self.bridge).state_get(key) }.map_err(to_sdk_err)? {
			Some(row) => Ok(Some(decode(&row)?)),
			None => Ok(None),
		}
	}
	fn set<T: OperatorState>(&mut self, key: &GroupStateKey, value: &T) -> SdkResult<()> {
		let now = self.now;
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).state_set(key, encode(value, now)?) }.map_err(to_sdk_err)
	}
	fn remove(&mut self, key: &GroupStateKey) -> SdkResult<()> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).state_remove(key) }.map_err(to_sdk_err)
	}
	fn contains(&self, key: &GroupStateKey) -> SdkResult<bool> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		Ok(unsafe { (*self.bridge).state_get(key) }.map_err(to_sdk_err)?.is_some())
	}
	fn clear(&mut self) -> SdkResult<()> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).state_clear() }.map_err(to_sdk_err)
	}
	fn scan_prefix<T: OperatorState>(&self, prefix: &GroupStateKey) -> SdkResult<Vec<(GroupStateKey, T)>> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		let rows = unsafe { (*self.bridge).state_range(EncodedKeyRange::prefix(prefix.as_slice())) }
			.map_err(to_sdk_err)?;
		rows.into_iter().map(|(k, r)| Ok((k, decode(&r)?))).collect()
	}
	fn get_many<T: OperatorState>(&self, keys: &[GroupStateKey]) -> SdkResult<Vec<(GroupStateKey, T)>> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		let rows = unsafe { (*self.bridge).state_get_many(keys) }.map_err(to_sdk_err)?;
		rows.into_iter().map(|(k, r)| Ok((k, decode(&r)?))).collect()
	}
	fn keys_with_prefix(&self, prefix: &GroupStateKey) -> SdkResult<Vec<GroupStateKey>> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		let rows = unsafe { (*self.bridge).state_range(EncodedKeyRange::prefix(prefix.as_slice())) }
			.map_err(to_sdk_err)?;
		Ok(rows.into_iter().map(|(k, _)| k).collect())
	}
	fn range<T: OperatorState>(
		&self,
		start: Bound<&GroupStateKey>,
		end: Bound<&GroupStateKey>,
	) -> SdkResult<Vec<(GroupStateKey, T)>> {
		let range = EncodedKeyRange::new(
			start.map(|k| k.as_encoded().clone()),
			end.map(|k| k.as_encoded().clone()),
		);
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		let rows = unsafe { (*self.bridge).state_range(range) }.map_err(to_sdk_err)?;
		rows.into_iter().map(|(k, r)| Ok((k, decode(&r)?))).collect()
	}
	fn get_bytes(&self, key: &GroupStateKey) -> SdkResult<Option<EncodedOperatorRow>> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		match unsafe { (*self.bridge).state_get(key) }.map_err(to_sdk_err)? {
			Some(row) => Ok(Some(EncodedOperatorRow::try_from(row).map_err(ValueError::from)?)),
			None => Ok(None),
		}
	}

	fn set_bytes(&mut self, key: &GroupStateKey, payload: EncodedOperatorRow) -> SdkResult<()> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).state_set(key, payload.into_bytes()) }.map_err(to_sdk_err)
	}

	fn get_many_bytes_visit(
		&self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> SdkResult<()>,
	) -> SdkResult<()> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively; the visitor
		// cannot reach the context, so it cannot re-enter the bridge while this borrow is live.
		unsafe {
			(*self.bridge).state_get_many_visit(keys, &mut |k, row| {
				let bytes = EncodedOperatorRow::try_from(row.clone()).map_err(ValueError::from)?;
				visit(k.clone(), bytes)
			})
		}
	}

	fn range_bytes_visit(
		&self,
		start: Bound<&GroupStateKey>,
		end: Bound<&GroupStateKey>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> SdkResult<()>,
	) -> SdkResult<()> {
		let range = EncodedKeyRange::new(
			start.map(|k| k.as_encoded().clone()),
			end.map(|k| k.as_encoded().clone()),
		);
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		let rows = unsafe { (*self.bridge).state_range(range) }.map_err(to_sdk_err)?;
		for (k, row) in rows {
			let bytes = EncodedOperatorRow::try_from(row).map_err(ValueError::from)?;
			visit(k, bytes)?;
		}
		Ok(())
	}
}

pub struct NativeStore<'a> {
	bridge: *mut (dyn NativeBridge + 'a),
	_marker: PhantomData<&'a mut (dyn NativeBridge + 'a)>,
}

impl StoreApi for NativeStore<'_> {
	fn get(&self, key: &EncodedKey) -> SdkResult<Option<EncodedBytes>> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).store_get(key) }.map_err(to_sdk_err)
	}
	fn contains(&self, key: &EncodedKey) -> SdkResult<bool> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).store_contains(key) }.map_err(to_sdk_err)
	}
	fn prefix(&self, prefix: &EncodedKey) -> SdkResult<Vec<(EncodedKey, EncodedBytes)>> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).store_prefix(prefix) }.map_err(to_sdk_err)
	}
	fn range(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> SdkResult<Vec<(EncodedKey, EncodedBytes)>> {
		let range = EncodedKeyRange::new(start.map(|k| k.clone()), end.map(|k| k.clone()));
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).store_range(range) }.map_err(to_sdk_err)
	}
}

pub struct NativeRowShapeResolver<'a> {
	bridge: *mut (dyn NativeBridge + 'a),
	_marker: PhantomData<&'a mut (dyn NativeBridge + 'a)>,
}

impl RowShapeApi for NativeRowShapeResolver<'_> {
	fn find_row_shape(&self, fingerprint: RowShapeFingerprint) -> SdkResult<Option<RowShape>> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).catalog_find_row_shape(fingerprint) }.map_err(to_sdk_err)
	}
}

pub struct NativeDictionary<'a> {
	bridge: *mut (dyn NativeBridge + 'a),
	_marker: PhantomData<&'a mut (dyn NativeBridge + 'a)>,
}

impl DictionaryApi for NativeDictionary<'_> {
	fn id_by_name(&mut self, name: &str) -> SdkResult<Option<DictionaryId>> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).dictionary_id_by_name(name) }.map_err(to_sdk_err)
	}
	fn find(&mut self, dictionary: DictionaryId, value: &Value) -> SdkResult<Option<DictionaryEntryId>> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).dictionary_find(dictionary, value) }.map_err(to_sdk_err)
	}
	fn get(&mut self, dictionary: DictionaryId, id: DictionaryEntryId) -> SdkResult<Option<Value>> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge NativeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
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

	fn operator_id(&self) -> OperatorId {
		self.operator
	}
	fn written_at(&self) -> DateTime {
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
	fn row_shape(&mut self) -> impl RowShapeApi + '_ {
		NativeRowShapeResolver {
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
		// SAFETY: bridge is the &'a mut dyn NativeBridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.bridge).intern_groups(groups) }.map_err(to_sdk_err)
	}
	fn lookup_groups(&mut self, groups: &[EncodedKey]) -> SdkResult<Vec<Option<GroupId>>> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.bridge).lookup_groups(groups) }.map_err(to_sdk_err)
	}
	fn arm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> SdkResult<()> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.bridge).arm_timer(at, kind, key) }.map_err(to_sdk_err)
	}
	fn disarm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> SdkResult<()> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.bridge).disarm_timer(at, kind, key) }.map_err(to_sdk_err)
	}

	fn flow_watermark(&mut self) -> SdkResult<Option<DateTime>> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.bridge).flow_watermark() }.map_err(to_sdk_err)
	}
	fn get_or_create_row_number(&mut self, group: GroupId, key: &EncodedKey) -> SdkResult<(RowNumber, bool)> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
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
		// SAFETY: bridge is the &'a mut dyn NativeBridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.bridge).get_or_create_row_numbers(group, keys) }.map_err(to_sdk_err)
	}
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> SdkResult<()> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.bridge).remove_row_number(group, key) }.map_err(to_sdk_err)
	}
	fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> SdkResult<Vec<RowNumber>> {
		// SAFETY: bridge is the &'a mut dyn NativeBridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.bridge).remove_row_numbers_below(group, upper) }.map_err(to_sdk_err)
	}
	fn shape_for_bytes(&mut self, bytes: &EncodedBytes) -> SdkResult<RowShape> {
		let fingerprint = read_fingerprint(bytes);
		match self.row_shape().find_row_shape(fingerprint)? {
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
			kind: Polarity::Insert,
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
			kind: Polarity::Remove,
			now,
		})
	}
}
