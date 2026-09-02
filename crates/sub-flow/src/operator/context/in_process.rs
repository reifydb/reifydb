// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{marker::PhantomData, mem};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{operator::state::OperatorState, pod::EncodedPodRow},
};
use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Diff},
	key::operator::state::{
		GroupId, GroupStateKey, KeyspaceId, KeyspaceMask, OperatorStateKey, group_data_inner_range,
		group_inner_range, is_guest_framed_inner, keyspace_inner_range_in,
	},
	state::timer::TimerKind,
};
use reifydb_flow::operator::{host::HostContext, state::reclaim::ReclaimOutcome};
use reifydb_sdk::{
	error::{Result as SdkResult, SdkError},
	flow::operator::{
		column::{row::Row, sink::in_process::InProcessRowSink},
		context::{GuestBound, GuestContext, GuestDictionary, GuestEmit, GuestState, GuestUpdateEmit},
		state::{decode_payload, encode_payload},
	},
};
use reifydb_value::{
	reifydb_assertions,
	value::{
		Value,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
		row_number::RowNumber,
	},
};

fn guest_addressable(key: &GroupStateKey) -> SdkResult<()> {
	if is_guest_framed_inner(key.as_slice()) {
		return Ok(());
	}
	Err(SdkError::Other(format!(
		"a guest operator state key must name a guest keyspace and carry that keyspace's exact suffix width, got {} bytes",
		key.as_slice().len()
	)))
}

fn to_sdk_err<E: ToString>(e: E) -> SdkError {
	SdkError::Other(e.to_string())
}

fn decode<T: OperatorState>(row: &EncodedPodRow) -> SdkResult<T> {
	decode_payload(row)
}

fn encode<T: OperatorState>(value: &T) -> SdkResult<EncodedPodRow> {
	encode_payload(value)
}

pub struct InProcessContext<'a> {
	host: *mut (dyn HostContext + 'a),
	operator: OperatorId,
	now: DateTime,
	diffs: Vec<Diff>,
	_marker: PhantomData<&'a mut (dyn HostContext + 'a)>,
}

impl<'a> InProcessContext<'a> {
	pub fn new(host: &'a mut (dyn HostContext + 'a), operator: OperatorId) -> Self {
		let now = host.written_at();
		Self {
			host: host as *mut (dyn HostContext + 'a),
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

pub struct InProcessInsertEmit<'a> {
	sink: InProcessRowSink,
	diffs: &'a mut Vec<Diff>,
	now: DateTime,
}

impl GuestEmit for InProcessInsertEmit<'_> {
	type Sink = InProcessRowSink;
	fn sink(&mut self) -> &mut InProcessRowSink {
		&mut self.sink
	}
	fn finish(self, row_numbers: &[RowNumber]) -> SdkResult<()> {
		let columns = self.sink.finish(row_numbers.to_vec(), self.now)?;
		self.diffs.push(Diff::insert(columns));
		Ok(())
	}
}

pub struct InProcessRemoveEmit<'a> {
	sink: InProcessRowSink,
	diffs: &'a mut Vec<Diff>,
	now: DateTime,
}

impl GuestEmit for InProcessRemoveEmit<'_> {
	type Sink = InProcessRowSink;
	fn sink(&mut self) -> &mut InProcessRowSink {
		&mut self.sink
	}
	fn finish(self, row_numbers: &[RowNumber]) -> SdkResult<()> {
		let columns = self.sink.finish(row_numbers.to_vec(), self.now)?;
		self.diffs.push(Diff::remove(columns));
		Ok(())
	}
}

pub struct InProcessUpdateEmit<'a> {
	pre: InProcessRowSink,
	post: InProcessRowSink,
	diffs: &'a mut Vec<Diff>,
	now: DateTime,
}

impl GuestUpdateEmit for InProcessUpdateEmit<'_> {
	type Sink = InProcessRowSink;
	fn pre(&mut self) -> &mut InProcessRowSink {
		&mut self.pre
	}
	fn post(&mut self) -> &mut InProcessRowSink {
		&mut self.post
	}
	fn finish(self, row_numbers: &[RowNumber]) -> SdkResult<()> {
		let pre_columns = self.pre.finish(row_numbers.to_vec(), self.now)?;
		let post_columns = self.post.finish(row_numbers.to_vec(), self.now)?;
		self.diffs.push(Diff::update(pre_columns, post_columns));
		Ok(())
	}
}

pub struct InProcessState<'a> {
	host: *mut (dyn HostContext + 'a),
	_marker: PhantomData<&'a mut (dyn HostContext + 'a)>,
}

impl GuestState for InProcessState<'_> {
	fn get<T: OperatorState>(&self, key: &GroupStateKey) -> SdkResult<Option<T>> {
		guest_addressable(key)?;
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		match unsafe { (*self.host).state_get(key) }.map_err(to_sdk_err)? {
			Some(row) => Ok(Some(decode(&row)?)),
			None => Ok(None),
		}
	}
	fn set<T: OperatorState>(&mut self, key: &GroupStateKey, value: &T) -> SdkResult<()> {
		guest_addressable(key)?;
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.host).state_set(key, encode(value)?) }.map_err(to_sdk_err)
	}
	fn remove(&mut self, key: &GroupStateKey) -> SdkResult<()> {
		guest_addressable(key)?;
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.host).state_remove(key) }.map_err(to_sdk_err)
	}
	fn contains(&self, key: &GroupStateKey) -> SdkResult<bool> {
		guest_addressable(key)?;
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		Ok(unsafe { (*self.host).state_get(key) }.map_err(to_sdk_err)?.is_some())
	}
	fn clear(&mut self) -> SdkResult<()> {
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.host).state_clear() }.map_err(to_sdk_err)
	}
	fn scan_prefix<T: OperatorState>(&self, prefix: &GroupStateKey) -> SdkResult<Vec<(GroupStateKey, T)>> {
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		let rows = unsafe { (*self.host).state_range(EncodedKeyRange::prefix(prefix.as_slice())) }
			.map_err(to_sdk_err)?;
		rows.into_iter().map(|(k, r)| Ok((k, decode(&r)?))).collect()
	}
	fn get_many<T: OperatorState>(&self, keys: &[GroupStateKey]) -> SdkResult<Vec<(GroupStateKey, T)>> {
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		let rows = unsafe { (*self.host).state_get_many(keys) }.map_err(to_sdk_err)?;
		rows.into_iter().map(|(k, r)| Ok((k, decode(&r)?))).collect()
	}
	fn keys_with_prefix(&self, prefix: &GroupStateKey) -> SdkResult<Vec<GroupStateKey>> {
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		let rows = unsafe { (*self.host).state_range(EncodedKeyRange::prefix(prefix.as_slice())) }
			.map_err(to_sdk_err)?;
		Ok(rows.into_iter().map(|(k, _)| k).collect())
	}
	fn range<T: OperatorState>(
		&self,
		group: GroupId,
		keyspace: KeyspaceId,
		start: GuestBound<'_>,
		end: GuestBound<'_>,
	) -> SdkResult<Vec<(GroupStateKey, T)>> {
		let range = keyspace_inner_range_in(group, keyspace, start.to_bound(), end.to_bound());
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		let rows = unsafe { (*self.host).state_range(range) }.map_err(to_sdk_err)?;
		rows.into_iter().map(|(k, r)| Ok((k, decode(&r)?))).collect()
	}
	fn get_bytes(&self, key: &GroupStateKey) -> SdkResult<Option<EncodedPodRow>> {
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.host).state_get(key) }.map_err(to_sdk_err)
	}

	fn set_bytes(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> SdkResult<()> {
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.host).state_set(key, payload) }.map_err(to_sdk_err)
	}

	fn remove_bytes(&mut self, key: &GroupStateKey) -> SdkResult<()> {
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.host).state_remove(key) }.map_err(to_sdk_err)
	}

	fn get_many_bytes_visit(
		&self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> SdkResult<()>,
	) -> SdkResult<()> {
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively; the visitor
		// cannot reach the context, so it cannot re-enter the host while this borrow is live.
		unsafe { (*self.host).state_get_many_visit(keys, &mut |k, row| Ok(visit(k, row)?)) }.map_err(to_sdk_err)
	}

	fn range_bytes_visit(
		&self,
		group: GroupId,
		keyspace: KeyspaceId,
		start: GuestBound<'_>,
		end: GuestBound<'_>,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> SdkResult<()>,
	) -> SdkResult<()> {
		let range = keyspace_inner_range_in(group, keyspace, start.to_bound(), end.to_bound());
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively; the visitor
		// cannot reach the context, so it cannot re-enter the host while this borrow is live.
		unsafe { (*self.host).state_range_limited_visit(range, limit, &mut |k, row| Ok(visit(k, row)?)) }
			.map_err(to_sdk_err)
	}

	fn sweep_bytes_visit(
		&self,
		group: GroupId,
		mask: KeyspaceMask,
		data_only: bool,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> SdkResult<()>,
	) -> SdkResult<()> {
		let range = match data_only {
			true => group_data_inner_range(group),
			false => group_inner_range(group),
		};
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively; the
		// visitor cannot reach the context, so it cannot re-enter the host while this borrow is live.
		unsafe {
			(*self.host).state_range_limited_visit(range, limit, &mut |key, row| {
				reifydb_assertions! {
					if let Some((_, keyspace, _)) =
						OperatorStateKey::decode_inner(key.as_encoded().as_bytes())
					{
						assert!(
							mask.contains(keyspace),
							"group {} holds a row in {} which the sweep set omits; declaring \
							 the group done would orphan it behind a group id nothing can \
							 resolve again",
							group,
							keyspace.name()
						);
					}
				}
				Ok(visit(key, row)?)
			})
		}
		.map_err(to_sdk_err)
	}

	fn last_bytes(
		&self,
		group: GroupId,
		keyspace: KeyspaceId,
		start: GuestBound<'_>,
		end: GuestBound<'_>,
	) -> SdkResult<Option<(GroupStateKey, EncodedPodRow)>> {
		let range = keyspace_inner_range_in(group, keyspace, start.to_bound(), end.to_bound());
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.host).state_last(range) }.map_err(to_sdk_err)
	}
}

pub struct InProcessDictionary<'a> {
	host: *mut (dyn HostContext + 'a),
	_marker: PhantomData<&'a mut (dyn HostContext + 'a)>,
}

impl GuestDictionary for InProcessDictionary<'_> {
	fn id_by_name(&mut self, name: &str) -> SdkResult<Option<DictionaryId>> {
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.host).dictionary_id_by_name(name) }.map_err(to_sdk_err)
	}
	fn find(&mut self, dictionary: DictionaryId, value: &Value) -> SdkResult<Option<DictionaryEntryId>> {
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.host).dictionary_find(dictionary, value) }.map_err(to_sdk_err)
	}
	fn get(&mut self, dictionary: DictionaryId, id: DictionaryEntryId) -> SdkResult<Option<Value>> {
		// SAFETY: host is the &'a mut dyn HostContext InProcessContext::new was built from;
		// PhantomData keeps that borrow live for 'a and this handle holds it exclusively.
		unsafe { (*self.host).dictionary_get(dictionary, id) }.map_err(to_sdk_err)
	}
}

impl GuestContext for InProcessContext<'_> {
	type InsertEmit<'a>
		= InProcessInsertEmit<'a>
	where
		Self: 'a;
	type UpdateEmit<'a>
		= InProcessUpdateEmit<'a>
	where
		Self: 'a;
	type RemoveEmit<'a>
		= InProcessRemoveEmit<'a>
	where
		Self: 'a;

	fn operator_id(&self) -> OperatorId {
		self.operator
	}
	fn written_at(&self) -> DateTime {
		self.now
	}
	fn state(&mut self) -> impl GuestState + '_ {
		InProcessState {
			host: self.host,
			_marker: PhantomData,
		}
	}
	fn dictionary(&mut self) -> impl GuestDictionary + '_ {
		InProcessDictionary {
			host: self.host,
			_marker: PhantomData,
		}
	}
	fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> SdkResult<()> {
		// SAFETY: host is the &'a mut dyn HostContext this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.host).arm_timer(due, kind, key) }.map_err(to_sdk_err)
	}
	fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> SdkResult<()> {
		// SAFETY: host is the &'a mut dyn HostContext this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.host).disarm_timer(due, kind, key) }.map_err(to_sdk_err)
	}

	fn flow_watermark(&mut self) -> SdkResult<Option<DateTime>> {
		// SAFETY: host is the &'a mut dyn HostContext this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.host).flow_watermark() }.map_err(to_sdk_err)
	}
	fn get_or_create_row_numbers(
		&mut self,
		group: GroupId,
		keys: &[EncodedKey],
	) -> SdkResult<Vec<(RowNumber, bool)>> {
		// SAFETY: host is the &'a mut dyn HostContext this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.host).get_or_create_row_numbers(group, keys) }.map_err(to_sdk_err)
	}
	fn get_or_create_row_numbers_for_pairs(
		&mut self,
		pairs: &[(GroupId, EncodedKey)],
	) -> SdkResult<Vec<(RowNumber, bool)>> {
		let groups: Vec<GroupId> = pairs.iter().map(|(group, _)| *group).collect();
		// SAFETY: host is the &'a mut dyn HostContext this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.host).get_or_create_row_numbers_for_groups(&groups) }.map_err(to_sdk_err)
	}
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> SdkResult<()> {
		// SAFETY: host is the &'a mut dyn HostContext this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.host).remove_row_number(group, key) }.map_err(to_sdk_err)
	}
	fn reclaim_group_identity(&mut self, group: GroupId, limit: usize) -> SdkResult<ReclaimOutcome> {
		// SAFETY: host is the &'a mut dyn HostContext this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.host).reclaim_group_identity(group, limit) }.map_err(to_sdk_err)
	}
	fn reclaim_group_identity_keys(&mut self, group: GroupId, keys: &[GroupStateKey]) -> SdkResult<ReclaimOutcome> {
		// SAFETY: host is the &'a mut dyn HostContext this context was built from; PhantomData keeps
		// that borrow live for 'a and &mut self makes the deref unique.
		unsafe { (*self.host).reclaim_group_identity_keys(group, keys) }.map_err(to_sdk_err)
	}
	fn insert_emit<R: Row>(&mut self, _row_capacity: usize) -> SdkResult<InProcessInsertEmit<'_>> {
		let now = self.now;
		Ok(InProcessInsertEmit {
			sink: InProcessRowSink::new(R::COLUMNS)?,
			diffs: &mut self.diffs,
			now,
		})
	}
	fn update_emit<R: Row>(&mut self, _row_capacity: usize) -> SdkResult<InProcessUpdateEmit<'_>> {
		let now = self.now;
		Ok(InProcessUpdateEmit {
			pre: InProcessRowSink::new(R::COLUMNS)?,
			post: InProcessRowSink::new(R::COLUMNS)?,
			diffs: &mut self.diffs,
			now,
		})
	}
	fn remove_emit<R: Row>(&mut self, _row_capacity: usize) -> SdkResult<InProcessRemoveEmit<'_>> {
		let now = self.now;
		Ok(InProcessRemoveEmit {
			sink: InProcessRowSink::new(R::COLUMNS)?,
			diffs: &mut self.diffs,
			now,
		})
	}
}
