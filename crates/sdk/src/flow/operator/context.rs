// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{marker::PhantomData, ops::Bound};

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{operator::state::OperatorState, pod::EncodedPodRow},
};
use reifydb_core::{
	common::OperatorClass,
	interface::catalog::flow::OperatorId,
	key::operator::state::{
		GroupId, GroupStateKey, KeyspaceId, KeyspaceMask, ManagedKey, UnmanagedKey, guest_may_address,
		is_class_framed_inner,
	},
	state::{batch::StateBatch, timer::TimerKind},
};
use reifydb_flow_async::operator::state::reclaim::ReclaimOutcome;
use reifydb_value::value::{
	Value,
	datetime::DateTime,
	dictionary::{DictionaryEntryId, DictionaryId},
	row_number::RowNumber,
};

use crate::{
	error::{Result, SdkError},
	flow::operator::{
		column::{row::Row, sink::RowSink},
		state::encode_payload,
	},
};

pub(crate) fn check_emit_count(rows: usize, row_numbers: usize) -> Result<()> {
	if rows == row_numbers {
		return Ok(());
	}
	Err(SdkError::InvalidInput(format!("emit of {rows} rows with {row_numbers} row numbers")))
}

pub(crate) fn class_framed(class: OperatorClass, key: &GroupStateKey) -> Result<()> {
	if is_class_framed_inner(class, key.as_slice()) {
		return Ok(());
	}
	Err(SdkError::Other(format!(
		"a {class:?} guest state key must name a keyspace its class owns and carry that keyspace's exact suffix width, got {} bytes",
		key.as_slice().len()
	)))
}

pub(crate) fn class_keyspace(class: OperatorClass, keyspace: KeyspaceId) -> Result<()> {
	if guest_may_address(class, keyspace) {
		return Ok(());
	}
	Err(SdkError::Other(format!("a {class:?} guest may not read keyspace {}", keyspace.name())))
}

pub(crate) fn guest_sweep_mask(keyspaces: KeyspaceMask) -> KeyspaceMask {
	KeyspaceMask::windowed().intersect(keyspaces)
}

pub(crate) fn check_row_number_keys(keys: &[EncodedKey]) -> Result<()> {
	match keys.iter().position(|key| key.is_empty()) {
		Some(slot) => Err(SdkError::Other(format!(
			"a guest row-number batch key at slot {slot} is empty; an empty key names the group row, remove group rows with remove_row_numbers_for_groups"
		))),
		None => Ok(()),
	}
}

pub(crate) fn check_class_batch(class: OperatorClass, batch: &StateBatch) -> Result<()> {
	for slot in 0..batch.len() {
		class_framed(class, batch.key(slot))?;
	}
	Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuestBound<'a> {
	Unbounded,
	Included(&'a [u8]),
	Excluded(&'a [u8]),
}

impl<'a> GuestBound<'a> {
	pub fn of(bound: &'a Bound<Vec<u8>>) -> Self {
		match bound {
			Bound::Unbounded => Self::Unbounded,
			Bound::Included(suffix) => Self::Included(suffix),
			Bound::Excluded(suffix) => Self::Excluded(suffix),
		}
	}

	pub fn to_bound(self) -> Bound<&'a [u8]> {
		match self {
			Self::Unbounded => Bound::Unbounded,
			Self::Included(suffix) => Bound::Included(suffix),
			Self::Excluded(suffix) => Bound::Excluded(suffix),
		}
	}
}

pub trait GuestEmit {
	type Sink: RowSink;
	fn sink(&mut self) -> &mut Self::Sink;
	fn finish(self, row_numbers: &[RowNumber]) -> Result<()>;
}

pub trait GuestUpdateEmit {
	type Sink: RowSink;
	fn pre(&mut self) -> &mut Self::Sink;
	fn post(&mut self) -> &mut Self::Sink;
	fn finish(self, row_numbers: &[RowNumber]) -> Result<()>;
}

pub trait GuestState {
	fn get<T: OperatorState>(&self, key: &GroupStateKey) -> Result<Option<T>>;
	fn set<T: OperatorState>(&mut self, key: &GroupStateKey, value: &T) -> Result<()>;
	fn remove(&mut self, key: &GroupStateKey) -> Result<()>;
	fn contains(&self, key: &GroupStateKey) -> Result<bool>;
	fn get_bytes(&self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>>;

	fn set_bytes(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()>;

	fn remove_bytes(&mut self, key: &GroupStateKey) -> Result<()>;

	fn remove_many_bytes(&mut self, keys: &[GroupStateKey]) -> Result<()>;

	fn batch_bytes(&mut self, keys: Vec<GroupStateKey>) -> Result<StateBatch>;

	fn write_batch_bytes(&mut self, batch: StateBatch) -> Result<()>;

	fn get_many_bytes_visit(
		&self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()>;

	fn range_bytes_visit(
		&self,
		group: GroupId,
		keyspace: KeyspaceId,
		start: GuestBound<'_>,
		end: GuestBound<'_>,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()>;

	fn sweep_many_bytes(
		&self,
		groups: &[GroupId],
		limit: usize,
		keyspaces: KeyspaceMask,
	) -> Result<(Vec<(GroupStateKey, EncodedPodRow)>, bool)>;

	fn last_bytes(
		&self,
		group: GroupId,
		keyspace: KeyspaceId,
		start: GuestBound<'_>,
		end: GuestBound<'_>,
	) -> Result<Option<(GroupStateKey, EncodedPodRow)>> {
		let mut last = None;
		self.range_bytes_visit(group, keyspace, start, end, None, &mut |key, payload| {
			last = Some((key, payload));
			Ok(())
		})?;
		Ok(last)
	}
}

pub trait GuestDictionary {
	fn id_by_name(&mut self, name: &str) -> Result<Option<DictionaryId>>;
	fn find(&mut self, dictionary: DictionaryId, value: &Value) -> Result<Option<DictionaryEntryId>>;
	fn get(&mut self, dictionary: DictionaryId, id: DictionaryEntryId) -> Result<Option<Value>>;
}

pub struct Managed;

pub struct Unmanaged;

pub struct Nostate;

pub struct Windowed;

pub trait ClassValue {
	const CLASS: OperatorClass;
}

impl ClassValue for Managed {
	const CLASS: OperatorClass = OperatorClass::Managed;
}

impl ClassValue for Unmanaged {
	const CLASS: OperatorClass = OperatorClass::Unmanaged;
}

impl ClassValue for Nostate {
	const CLASS: OperatorClass = OperatorClass::Nostate;
}

impl ClassValue for Windowed {
	const CLASS: OperatorClass = OperatorClass::Windowed;
}

pub trait CustomClass: ClassValue {
	type Key: AsRef<GroupStateKey>;
}

impl CustomClass for Managed {
	type Key = ManagedKey;
}

impl CustomClass for Unmanaged {
	type Key = UnmanagedKey;
}

pub trait WindowClass: ClassValue {}

impl WindowClass for Windowed {}

pub struct ClassBatch<C, T> {
	batch: StateBatch,
	_marker: PhantomData<fn() -> (C, T)>,
}

impl<C: CustomClass, T: OperatorState> ClassBatch<C, T> {
	pub fn set(&mut self, slot: usize, value: &T) -> Result<()> {
		self.batch.set(slot, encode_payload(value)?);
		Ok(())
	}

	pub fn commit(self, state: &mut impl ClassState<C>) -> Result<()> {
		state.write_batch(self)
	}
}

pub trait ClassState<C: CustomClass> {
	fn get<T: OperatorState>(&self, key: &C::Key) -> Result<Option<T>>;
	fn set<T: OperatorState>(&mut self, key: &C::Key, value: &T) -> Result<()>;
	fn remove(&mut self, key: &C::Key) -> Result<()>;
	fn contains(&self, key: &C::Key) -> Result<bool>;
	fn batch<T: OperatorState>(&mut self, keys: &[C::Key]) -> Result<ClassBatch<C, T>>;
	fn write_batch<T: OperatorState>(&mut self, batch: ClassBatch<C, T>) -> Result<()>;
}

impl<C: CustomClass, S: GuestState> ClassState<C> for S {
	fn get<T: OperatorState>(&self, key: &C::Key) -> Result<Option<T>> {
		GuestState::get(self, key.as_ref())
	}
	fn set<T: OperatorState>(&mut self, key: &C::Key, value: &T) -> Result<()> {
		GuestState::set(self, key.as_ref(), value)
	}
	fn remove(&mut self, key: &C::Key) -> Result<()> {
		GuestState::remove(self, key.as_ref())
	}
	fn contains(&self, key: &C::Key) -> Result<bool> {
		GuestState::contains(self, key.as_ref())
	}
	fn batch<T: OperatorState>(&mut self, keys: &[C::Key]) -> Result<ClassBatch<C, T>> {
		let keys = keys.iter().map(|key| key.as_ref().clone()).collect();
		Ok(ClassBatch {
			batch: self.batch_bytes(keys)?,
			_marker: PhantomData,
		})
	}
	fn write_batch<T: OperatorState>(&mut self, batch: ClassBatch<C, T>) -> Result<()> {
		check_class_batch(C::CLASS, &batch.batch)?;
		self.write_batch_bytes(batch.batch)
	}
}

pub trait GuestEmitContext {
	type InsertEmit<'a>: GuestEmit
	where
		Self: 'a;
	type UpdateEmit<'a>: GuestUpdateEmit
	where
		Self: 'a;
	type RemoveEmit<'a>: GuestEmit
	where
		Self: 'a;

	fn operator_id(&self) -> OperatorId;
	fn written_at(&self) -> DateTime;
	fn dictionary(&mut self) -> impl GuestDictionary + '_;
	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>>;
	fn get_or_create_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<(RowNumber, bool)>>;
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()>;
	fn remove_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<()>;
	fn remove_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<()>;
	fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()>;
	fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()>;
	fn flow_watermark(&mut self) -> Result<Option<DateTime>>;

	fn insert_emit<R: Row>(&mut self, row_capacity: usize) -> Result<Self::InsertEmit<'_>>;
	fn update_emit<R: Row>(&mut self, row_capacity: usize) -> Result<Self::UpdateEmit<'_>>;
	fn remove_emit<R: Row>(&mut self, row_capacity: usize) -> Result<Self::RemoveEmit<'_>>;

	fn emit_insert<'r, R: Row + 'r, I>(&mut self, rows: I, row_numbers: &[RowNumber]) -> Result<()>
	where
		I: IntoIterator<Item = &'r R>,
		I::IntoIter: ExactSizeIterator + Clone,
	{
		let rows = rows.into_iter();
		let count = rows.len();
		check_emit_count(count, row_numbers.len())?;
		if count == 0 {
			return Ok(());
		}
		let mut emit = self.insert_emit::<R>(count)?;
		R::encode_rows(rows, emit.sink())?;
		emit.finish(row_numbers)
	}

	fn emit_update<'r, R: Row + 'r, P, Q>(&mut self, pre: P, post: Q, row_numbers: &[RowNumber]) -> Result<()>
	where
		P: IntoIterator<Item = &'r R>,
		P::IntoIter: ExactSizeIterator + Clone,
		Q: IntoIterator<Item = &'r R>,
		Q::IntoIter: ExactSizeIterator + Clone,
	{
		let pre = pre.into_iter();
		let post = post.into_iter();
		check_emit_count(pre.len(), row_numbers.len())?;
		check_emit_count(post.len(), row_numbers.len())?;
		if row_numbers.is_empty() {
			return Ok(());
		}
		let mut emit = self.update_emit::<R>(row_numbers.len())?;
		R::encode_rows(pre, emit.pre())?;
		R::encode_rows(post, emit.post())?;
		emit.finish(row_numbers)
	}

	fn emit_remove<'r, R: Row + 'r, I>(&mut self, rows: I, row_numbers: &[RowNumber]) -> Result<()>
	where
		I: IntoIterator<Item = &'r R>,
		I::IntoIter: ExactSizeIterator + Clone,
	{
		let rows = rows.into_iter();
		let count = rows.len();
		check_emit_count(count, row_numbers.len())?;
		if count == 0 {
			return Ok(());
		}
		let mut emit = self.remove_emit::<R>(count)?;
		R::encode_rows(rows, emit.sink())?;
		emit.finish(row_numbers)
	}
}

pub trait GuestContext<C = Windowed>: GuestEmitContext {
	fn state(&mut self) -> impl ClassState<C> + '_
	where
		C: CustomClass;
	fn window_state(&mut self) -> impl GuestState + '_
	where
		C: WindowClass;
	fn reclaim_group_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome>
	where
		C: WindowClass;
}
