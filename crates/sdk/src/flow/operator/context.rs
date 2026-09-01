// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{operator::state::OperatorState, pod::EncodedPodRow},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId},
	state::timer::TimerKind,
};
use reifydb_flow::operator::state::reclaim::ReclaimOutcome;
use reifydb_value::value::{
	Value,
	datetime::DateTime,
	dictionary::{DictionaryEntryId, DictionaryId},
	row_number::RowNumber,
};

use crate::{
	error::Result,
	flow::operator::column::{row::Row, sink::RowSink},
};

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
	fn clear(&mut self) -> Result<()>;
	fn scan_prefix<T: OperatorState>(&self, prefix: &GroupStateKey) -> Result<Vec<(GroupStateKey, T)>>;
	fn get_many<T: OperatorState>(&self, keys: &[GroupStateKey]) -> Result<Vec<(GroupStateKey, T)>>;
	fn keys_with_prefix(&self, prefix: &GroupStateKey) -> Result<Vec<GroupStateKey>>;
	fn range<T: OperatorState>(
		&self,
		group: GroupId,
		keyspace: KeyspaceId,
		start: GuestBound<'_>,
		end: GuestBound<'_>,
	) -> Result<Vec<(GroupStateKey, T)>>;
	fn get_bytes(&self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>>;

	fn set_bytes(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()>;

	fn remove_bytes(&mut self, key: &GroupStateKey) -> Result<()>;

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

	fn sweep_bytes_visit(
		&self,
		group: GroupId,
		data_only: bool,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		let mut seen = 0usize;
		for id in (u8::MIN..=u8::MAX).rev() {
			let keyspace = KeyspaceId(id);
			if data_only && !keyspace.is_data() {
				continue;
			}
			let remaining = match limit {
				Some(limit) if seen >= limit => break,
				Some(limit) => Some(limit - seen),
				None => None,
			};
			self.range_bytes_visit(
				group,
				keyspace,
				GuestBound::Unbounded,
				GuestBound::Unbounded,
				remaining,
				&mut |key, payload| {
					seen += 1;
					visit(key, payload)
				},
			)?;
		}
		Ok(())
	}

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

pub trait GuestContext {
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
	fn state(&mut self) -> impl GuestState + '_;
	fn dictionary(&mut self) -> impl GuestDictionary + '_;
	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>>;
	fn get_or_create_row_numbers_for_pairs(
		&mut self,
		pairs: &[(GroupId, EncodedKey)],
	) -> Result<Vec<(RowNumber, bool)>>;
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()>;
	fn reclaim_group_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome>;
	fn reclaim_group_identity_keys(&mut self, group: GroupId, keys: &[GroupStateKey]) -> Result<ReclaimOutcome>;
	fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()>;
	fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()>;
	fn flow_watermark(&mut self) -> Result<Option<DateTime>>;

	fn insert_emit<R: Row>(&mut self, row_capacity: usize) -> Result<Self::InsertEmit<'_>>;
	fn update_emit<R: Row>(&mut self, row_capacity: usize) -> Result<Self::UpdateEmit<'_>>;
	fn remove_emit<R: Row>(&mut self, row_capacity: usize) -> Result<Self::RemoveEmit<'_>>;

	fn emit_insert<R: Row>(&mut self, rows: &[R], row_numbers: &[RowNumber]) -> Result<()> {
		if rows.is_empty() {
			return Ok(());
		}
		let mut emit = self.insert_emit::<R>(rows.len())?;
		for bytes in rows {
			bytes.encode_into(emit.sink())?;
		}
		emit.finish(row_numbers)
	}

	fn emit_update<R: Row>(&mut self, pre: &[R], post: &[R], row_numbers: &[RowNumber]) -> Result<()> {
		if row_numbers.is_empty() {
			return Ok(());
		}
		let mut emit = self.update_emit::<R>(row_numbers.len())?;
		for row in pre {
			row.encode_into(emit.pre())?;
		}
		for row in post {
			row.encode_into(emit.post())?;
		}
		emit.finish(row_numbers)
	}

	fn emit_remove<R: Row>(&mut self, rows: &[R], row_numbers: &[RowNumber]) -> Result<()> {
		if rows.is_empty() {
			return Ok(());
		}
		let mut emit = self.remove_emit::<R>(rows.len())?;
		for row in rows {
			row.encode_into(emit.sink())?;
		}
		emit.finish(row_numbers)
	}
}
