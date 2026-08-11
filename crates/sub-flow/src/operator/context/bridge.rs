// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{marker::PhantomData, mem, ops::Bound, slice::from_ref};

use reifydb_abi::operator::timer::TimerKind;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{
		bytes::EncodedBytes,
		operator::{EncodedOperatorRow, OperatorState},
	},
};
use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::flow::OperatorId, change::Diff},
	key::operator_state::{GroupId, GroupStateKey},
};
use reifydb_flow::window::event::Polarity;
use reifydb_sdk::{
	error::{Result as SdkResult, SdkError},
	operator::{
		column::{row::Row, sink::bridge::BridgeRowSink},
		context::{DictionaryApi, OperatorContext, RowEmit, StateApi, UpdateEmit},
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

pub trait Bridge {
	fn written_at(&self) -> DateTime;
	fn version(&self) -> CommitVersion;

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

pub struct BridgeOperatorContext<'a> {
	bridge: *mut (dyn Bridge + 'a),
	operator: OperatorId,
	now: DateTime,
	diffs: Vec<Diff>,
	_marker: PhantomData<&'a mut (dyn Bridge + 'a)>,
}

impl<'a> BridgeOperatorContext<'a> {
	pub fn new(bridge: &'a mut (dyn Bridge + 'a), operator: OperatorId) -> Self {
		let now = bridge.written_at();
		Self {
			bridge: bridge as *mut (dyn Bridge + 'a),
			operator,
			now,
			diffs: Vec::new(),
			_marker: PhantomData,
		}
	}

	pub fn take_diffs(&mut self) -> Vec<Diff> {
		mem::take(&mut self.diffs)
	}
}

pub struct BridgeRowEmit<'a> {
	sink: BridgeRowSink,
	diffs: &'a mut Vec<Diff>,
	kind: Polarity,
	now: DateTime,
}

impl RowEmit for BridgeRowEmit<'_> {
	type Sink = BridgeRowSink;
	fn sink(&mut self) -> &mut BridgeRowSink {
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

pub struct BridgeUpdateEmit<'a> {
	pre: BridgeRowSink,
	post: BridgeRowSink,
	diffs: &'a mut Vec<Diff>,
	now: DateTime,
}

impl UpdateEmit for BridgeUpdateEmit<'_> {
	type Sink = BridgeRowSink;
	fn pre(&mut self) -> &mut BridgeRowSink {
		&mut self.pre
	}
	fn post(&mut self) -> &mut BridgeRowSink {
		&mut self.post
	}
	fn finish(self, row_numbers: &[RowNumber]) -> SdkResult<()> {
		let pre_columns = self.pre.finish(row_numbers.to_vec(), self.now)?;
		let post_columns = self.post.finish(row_numbers.to_vec(), self.now)?;
		self.diffs.push(Diff::update(pre_columns, post_columns));
		Ok(())
	}
}

pub struct BridgeState<'a> {
	bridge: *mut (dyn Bridge + 'a),
	now: DateTime,
	_marker: PhantomData<&'a mut (dyn Bridge + 'a)>,
}

impl StateApi for BridgeState<'_> {
	fn get<T: OperatorState>(&self, key: &GroupStateKey) -> SdkResult<Option<T>> {
		// SAFETY: bridge is the &'a mut dyn Bridge BridgeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		match unsafe { (*self.bridge).state_get(key) }.map_err(to_sdk_err)? {
			Some(row) => Ok(Some(decode(&row)?)),
			None => Ok(None),
		}
	}
	fn set<T: OperatorState>(&mut self, key: &GroupStateKey, value: &T) -> SdkResult<()> {
		let now = self.now;
		// SAFETY: bridge is the &'a mut dyn Bridge BridgeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).state_set(key, encode(value, now)?) }.map_err(to_sdk_err)
	}
	fn remove(&mut self, key: &GroupStateKey) -> SdkResult<()> {
		// SAFETY: bridge is the &'a mut dyn Bridge BridgeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).state_remove(key) }.map_err(to_sdk_err)
	}
	fn contains(&self, key: &GroupStateKey) -> SdkResult<bool> {
		// SAFETY: bridge is the &'a mut dyn Bridge BridgeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		Ok(unsafe { (*self.bridge).state_get(key) }.map_err(to_sdk_err)?.is_some())
	}
	fn clear(&mut self) -> SdkResult<()> {
		// SAFETY: bridge is the &'a mut dyn Bridge BridgeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).state_clear() }.map_err(to_sdk_err)
	}
	fn scan_prefix<T: OperatorState>(&self, prefix: &GroupStateKey) -> SdkResult<Vec<(GroupStateKey, T)>> {
		// SAFETY: bridge is the &'a mut dyn Bridge BridgeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		let rows = unsafe { (*self.bridge).state_range(EncodedKeyRange::prefix(prefix.as_slice())) }
			.map_err(to_sdk_err)?;
		rows.into_iter().map(|(k, r)| Ok((k, decode(&r)?))).collect()
	}
	fn get_many<T: OperatorState>(&self, keys: &[GroupStateKey]) -> SdkResult<Vec<(GroupStateKey, T)>> {
		// SAFETY: bridge is the &'a mut dyn Bridge BridgeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		let rows = unsafe { (*self.bridge).state_get_many(keys) }.map_err(to_sdk_err)?;
		rows.into_iter().map(|(k, r)| Ok((k, decode(&r)?))).collect()
	}
	fn keys_with_prefix(&self, prefix: &GroupStateKey) -> SdkResult<Vec<GroupStateKey>> {
		// SAFETY: bridge is the &'a mut dyn Bridge BridgeOperatorContext::new was built from;
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
		// SAFETY: bridge is the &'a mut dyn Bridge BridgeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		let rows = unsafe { (*self.bridge).state_range(range) }.map_err(to_sdk_err)?;
		rows.into_iter().map(|(k, r)| Ok((k, decode(&r)?))).collect()
	}
	fn get_bytes(&self, key: &GroupStateKey) -> SdkResult<Option<EncodedOperatorRow>> {
		// SAFETY: bridge is the &'a mut dyn Bridge BridgeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		match unsafe { (*self.bridge).state_get(key) }.map_err(to_sdk_err)? {
			Some(row) => Ok(Some(EncodedOperatorRow::try_from(row).map_err(ValueError::from)?)),
			None => Ok(None),
		}
	}

	fn set_bytes(&mut self, key: &GroupStateKey, payload: EncodedOperatorRow) -> SdkResult<()> {
		// SAFETY: bridge is the &'a mut dyn Bridge BridgeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).state_set(key, payload.into_bytes()) }.map_err(to_sdk_err)
	}

	fn get_many_bytes_visit(
		&self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> SdkResult<()>,
	) -> SdkResult<()> {
		// SAFETY: bridge is the &'a mut dyn Bridge BridgeOperatorContext::new was built from;
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
		// SAFETY: bridge is the &'a mut dyn Bridge BridgeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		let rows = unsafe { (*self.bridge).state_range(range) }.map_err(to_sdk_err)?;
		for (k, row) in rows {
			let bytes = EncodedOperatorRow::try_from(row).map_err(ValueError::from)?;
			visit(k, bytes)?;
		}
		Ok(())
	}
}

pub struct BridgeDictionary<'a> {
	bridge: *mut (dyn Bridge + 'a),
	_marker: PhantomData<&'a mut (dyn Bridge + 'a)>,
}

impl DictionaryApi for BridgeDictionary<'_> {
	fn id_by_name(&mut self, name: &str) -> SdkResult<Option<DictionaryId>> {
		// SAFETY: bridge is the &'a mut dyn Bridge BridgeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).dictionary_id_by_name(name) }.map_err(to_sdk_err)
	}
	fn find(&mut self, dictionary: DictionaryId, value: &Value) -> SdkResult<Option<DictionaryEntryId>> {
		// SAFETY: bridge is the &'a mut dyn Bridge BridgeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).dictionary_find(dictionary, value) }.map_err(to_sdk_err)
	}
	fn get(&mut self, dictionary: DictionaryId, id: DictionaryEntryId) -> SdkResult<Option<Value>> {
		// SAFETY: bridge is the &'a mut dyn Bridge BridgeOperatorContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.bridge).dictionary_get(dictionary, id) }.map_err(to_sdk_err)
	}
}

impl OperatorContext for BridgeOperatorContext<'_> {
	type InsertEmit<'a>
		= BridgeRowEmit<'a>
	where
		Self: 'a;
	type UpdateEmit<'a>
		= BridgeUpdateEmit<'a>
	where
		Self: 'a;
	type RemoveEmit<'a>
		= BridgeRowEmit<'a>
	where
		Self: 'a;

	fn operator_id(&self) -> OperatorId {
		self.operator
	}
	fn written_at(&self) -> DateTime {
		self.now
	}
	fn state(&mut self) -> impl StateApi + '_ {
		BridgeState {
			bridge: self.bridge,
			now: self.now,
			_marker: PhantomData,
		}
	}
	fn dictionary(&mut self) -> impl DictionaryApi + '_ {
		BridgeDictionary {
			bridge: self.bridge,
			_marker: PhantomData,
		}
	}
	fn intern_groups(&mut self, groups: &[EncodedKey]) -> SdkResult<Vec<GroupId>> {
		// SAFETY: bridge is the &'a mut dyn Bridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.bridge).intern_groups(groups) }.map_err(to_sdk_err)
	}
	fn lookup_groups(&mut self, groups: &[EncodedKey]) -> SdkResult<Vec<Option<GroupId>>> {
		// SAFETY: bridge is the &'a mut dyn Bridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.bridge).lookup_groups(groups) }.map_err(to_sdk_err)
	}
	fn arm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> SdkResult<()> {
		// SAFETY: bridge is the &'a mut dyn Bridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.bridge).arm_timer(at, kind, key) }.map_err(to_sdk_err)
	}
	fn disarm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> SdkResult<()> {
		// SAFETY: bridge is the &'a mut dyn Bridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.bridge).disarm_timer(at, kind, key) }.map_err(to_sdk_err)
	}

	fn flow_watermark(&mut self) -> SdkResult<Option<DateTime>> {
		// SAFETY: bridge is the &'a mut dyn Bridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.bridge).flow_watermark() }.map_err(to_sdk_err)
	}
	fn get_or_create_row_number(&mut self, group: GroupId, key: &EncodedKey) -> SdkResult<(RowNumber, bool)> {
		// SAFETY: bridge is the &'a mut dyn Bridge this context was built from; PhantomData keeps
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
		// SAFETY: bridge is the &'a mut dyn Bridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.bridge).get_or_create_row_numbers(group, keys) }.map_err(to_sdk_err)
	}
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> SdkResult<()> {
		// SAFETY: bridge is the &'a mut dyn Bridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.bridge).remove_row_number(group, key) }.map_err(to_sdk_err)
	}
	fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> SdkResult<Vec<RowNumber>> {
		// SAFETY: bridge is the &'a mut dyn Bridge this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.bridge).remove_row_numbers_below(group, upper) }.map_err(to_sdk_err)
	}
	fn insert_emit<R: Row>(&mut self, _row_capacity: usize) -> SdkResult<BridgeRowEmit<'_>> {
		let now = self.now;
		Ok(BridgeRowEmit {
			sink: BridgeRowSink::new(R::COLUMNS)?,
			diffs: &mut self.diffs,
			kind: Polarity::Insert,
			now,
		})
	}
	fn update_emit<R: Row>(&mut self, _row_capacity: usize) -> SdkResult<BridgeUpdateEmit<'_>> {
		let now = self.now;
		Ok(BridgeUpdateEmit {
			pre: BridgeRowSink::new(R::COLUMNS)?,
			post: BridgeRowSink::new(R::COLUMNS)?,
			diffs: &mut self.diffs,
			now,
		})
	}
	fn remove_emit<R: Row>(&mut self, _row_capacity: usize) -> SdkResult<BridgeRowEmit<'_>> {
		let now = self.now;
		Ok(BridgeRowEmit {
			sink: BridgeRowSink::new(R::COLUMNS)?,
			diffs: &mut self.diffs,
			kind: Polarity::Remove,
			now,
		})
	}
}
