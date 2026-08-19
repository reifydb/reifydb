// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{operator::state::OperatorState, pod::EncodedPodRow},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, GroupStateKey},
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
		start: Bound<&GroupStateKey>,
		end: Bound<&GroupStateKey>,
	) -> Result<Vec<(GroupStateKey, T)>>;
	fn get_bytes(&self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>>;

	fn set_bytes(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()>;

	fn get_many_bytes_visit(
		&self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()>;

	fn range_bytes_visit(
		&self,
		start: Bound<&GroupStateKey>,
		end: Bound<&GroupStateKey>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()>;
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
	fn intern_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<(GroupId, bool)>>;
	fn lookup_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>>;
	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>>;
	fn get_or_create_row_numbers_for_pairs(
		&mut self,
		pairs: &[(GroupId, EncodedKey)],
	) -> Result<Vec<(RowNumber, bool)>>;
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()>;
	fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> Result<Vec<RowNumber>>;
	fn reclaim_group_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome>;
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
