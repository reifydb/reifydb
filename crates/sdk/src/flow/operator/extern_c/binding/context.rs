// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;
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
	common::extern_c::binding::builder::ColumnsBuilder,
	error::Result,
	flow::operator::{
		column::row::Row,
		context::{GuestContext, GuestDictionary, GuestEmit, GuestState, GuestUpdateEmit},
		dictionary::Dictionary,
		diff::DiffStart,
		extern_c::{
			binding::{
				sink::ExternCRowSink,
				state::{
					arm_timer, disarm_timer, flow_watermark, get_or_create_row_numbers,
					get_or_create_row_numbers_for_pairs, intern_groups, lookup_groups,
					reclaim_group_identity, remove_row_number, remove_row_numbers_below,
				},
			},
			wire::context::ExternCContextRaw,
		},
		state::State,
	},
};

pub struct ExternCInsertEmit<'a> {
	builder: ColumnsBuilder<'a>,
	sink: ExternCRowSink<'a>,
	names: Vec<&'static str>,
}

impl<'a> GuestEmit for ExternCInsertEmit<'a> {
	type Sink = ExternCRowSink<'a>;
	fn sink(&mut self) -> &mut ExternCRowSink<'a> {
		&mut self.sink
	}
	fn finish(self, row_numbers: &[RowNumber]) -> Result<()> {
		let mut builder = self.builder;
		let columns = self.sink.finish_all()?;
		builder.emit_insert(&columns, &self.names, row_numbers)
	}
}

pub struct ExternCRemoveEmit<'a> {
	builder: ColumnsBuilder<'a>,
	sink: ExternCRowSink<'a>,
	names: Vec<&'static str>,
}

impl<'a> GuestEmit for ExternCRemoveEmit<'a> {
	type Sink = ExternCRowSink<'a>;
	fn sink(&mut self) -> &mut ExternCRowSink<'a> {
		&mut self.sink
	}
	fn finish(self, row_numbers: &[RowNumber]) -> Result<()> {
		let mut builder = self.builder;
		let columns = self.sink.finish_all()?;
		builder.emit_remove(&columns, &self.names, row_numbers)
	}
}

pub struct ExternCUpdateEmit<'a> {
	builder: ColumnsBuilder<'a>,
	pre: ExternCRowSink<'a>,
	post: ExternCRowSink<'a>,
	names: Vec<&'static str>,
}

impl<'a> GuestUpdateEmit for ExternCUpdateEmit<'a> {
	type Sink = ExternCRowSink<'a>;
	fn pre(&mut self) -> &mut ExternCRowSink<'a> {
		&mut self.pre
	}
	fn post(&mut self) -> &mut ExternCRowSink<'a> {
		&mut self.post
	}
	fn finish(self, row_numbers: &[RowNumber]) -> Result<()> {
		let mut builder = self.builder;
		let pre_columns = self.pre.finish_all()?;
		let post_columns = self.post.finish_all()?;
		builder.emit_update(
			&pre_columns,
			&self.names,
			row_numbers.len(),
			row_numbers,
			&post_columns,
			&self.names,
			row_numbers.len(),
			row_numbers,
		)
	}
}

pub struct ExternCContext {
	pub(crate) ctx: *mut ExternCContextRaw,
}

impl ExternCContext {
	pub fn new(ctx: *mut ExternCContextRaw) -> Self {
		assert!(!ctx.is_null(), "ExternCContextRaw pointer must not be null");
		Self {
			ctx,
		}
	}

	pub fn operator_id(&self) -> OperatorId {
		// SAFETY: ExternCContext::new asserts self.ctx is non-null, and the host keeps the
		// ExternCContextRaw alive and aligned for at least the lifetime of &self.
		unsafe { OperatorId((*self.ctx).operator_id) }
	}

	pub fn state(&mut self) -> State<'_> {
		State::new(self)
	}

	pub fn dictionary(&mut self) -> Dictionary<'_> {
		Dictionary::new(self)
	}

	pub fn intern_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<(GroupId, bool)>> {
		intern_groups(self, groups)
	}

	pub fn lookup_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>> {
		lookup_groups(self, groups)
	}

	pub fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		arm_timer(self, due, kind, key)
	}

	pub fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		disarm_timer(self, due, kind, key)
	}

	pub fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
		flow_watermark(self)
	}

	pub fn get_or_create_row_numbers(
		&mut self,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>> {
		get_or_create_row_numbers(self, group, keys)
	}

	pub fn get_or_create_row_numbers_for_pairs(
		&mut self,
		pairs: &[(GroupId, EncodedKey)],
	) -> Result<Vec<(RowNumber, bool)>> {
		get_or_create_row_numbers_for_pairs(self, pairs)
	}

	pub fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		remove_row_number(self, group, key)
	}

	pub fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> Result<Vec<RowNumber>> {
		remove_row_numbers_below(self, group, upper)
	}

	pub fn reclaim_group_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome> {
		reclaim_group_identity(self, group, limit)
	}

	pub fn builder(&mut self) -> ColumnsBuilder<'_> {
		ColumnsBuilder::new(self.ctx as *mut c_void, unsafe { (*self.ctx).callbacks.builder }, unsafe {
			(*self.ctx).written_at_nanos
		})
	}

	pub fn diff(&mut self) -> DiffStart<'_> {
		DiffStart::new(self)
	}
}

impl GuestState for State<'_> {
	fn get<T: OperatorState>(&self, key: &GroupStateKey) -> Result<Option<T>> {
		State::get(self, key)
	}
	fn set<T: OperatorState>(&mut self, key: &GroupStateKey, value: &T) -> Result<()> {
		State::set(self, key, value)
	}
	fn remove(&mut self, key: &GroupStateKey) -> Result<()> {
		State::remove(self, key)
	}
	fn contains(&self, key: &GroupStateKey) -> Result<bool> {
		State::contains(self, key)
	}
	fn clear(&mut self) -> Result<()> {
		State::clear(self)
	}
	fn scan_prefix<T: OperatorState>(&self, prefix: &GroupStateKey) -> Result<Vec<(GroupStateKey, T)>> {
		State::scan_prefix(self, prefix)
	}
	fn get_many<T: OperatorState>(&self, keys: &[GroupStateKey]) -> Result<Vec<(GroupStateKey, T)>> {
		State::get_many(self, keys)
	}
	fn keys_with_prefix(&self, prefix: &GroupStateKey) -> Result<Vec<GroupStateKey>> {
		State::keys_with_prefix(self, prefix)
	}
	fn range<T: OperatorState>(
		&self,
		start: Bound<&GroupStateKey>,
		end: Bound<&GroupStateKey>,
	) -> Result<Vec<(GroupStateKey, T)>> {
		State::range(self, start, end)
	}

	fn get_bytes(&self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		State::get_bytes(self, key)
	}

	fn set_bytes(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()> {
		State::set_bytes(self, key, payload)
	}

	fn get_many_bytes_visit(
		&self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		State::get_many_bytes_visit(self, keys, visit)
	}

	fn range_bytes_visit(
		&self,
		start: Bound<&GroupStateKey>,
		end: Bound<&GroupStateKey>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		State::range_bytes_visit(self, start, end, visit)
	}
}

impl GuestDictionary for Dictionary<'_> {
	fn id_by_name(&mut self, name: &str) -> Result<Option<DictionaryId>> {
		Dictionary::id_by_name(self, name)
	}
	fn find(&mut self, dictionary: DictionaryId, value: &Value) -> Result<Option<DictionaryEntryId>> {
		Dictionary::find(self, dictionary, value)
	}
	fn get(&mut self, dictionary: DictionaryId, id: DictionaryEntryId) -> Result<Option<Value>> {
		Dictionary::get(self, dictionary, id)
	}
}

impl GuestContext for ExternCContext {
	type InsertEmit<'a> = ExternCInsertEmit<'a>;
	type UpdateEmit<'a> = ExternCUpdateEmit<'a>;
	type RemoveEmit<'a> = ExternCRemoveEmit<'a>;

	fn operator_id(&self) -> OperatorId {
		ExternCContext::operator_id(self)
	}
	fn written_at(&self) -> DateTime {
		// SAFETY: ExternCContext::new asserts self.ctx is non-null, and the host keeps the
		// ExternCContextRaw alive and aligned for at least the lifetime of &self.
		DateTime::from_nanos(unsafe { (*self.ctx).written_at_nanos })
	}
	fn state(&mut self) -> impl GuestState + '_ {
		ExternCContext::state(self)
	}
	fn dictionary(&mut self) -> impl GuestDictionary + '_ {
		ExternCContext::dictionary(self)
	}
	fn intern_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<(GroupId, bool)>> {
		ExternCContext::intern_groups(self, groups)
	}
	fn lookup_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>> {
		ExternCContext::lookup_groups(self, groups)
	}
	fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		ExternCContext::arm_timer(self, due, kind, key)
	}
	fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		ExternCContext::disarm_timer(self, due, kind, key)
	}

	fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
		ExternCContext::flow_watermark(self)
	}
	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		ExternCContext::get_or_create_row_numbers(self, group, keys)
	}
	fn get_or_create_row_numbers_for_pairs(
		&mut self,
		pairs: &[(GroupId, EncodedKey)],
	) -> Result<Vec<(RowNumber, bool)>> {
		ExternCContext::get_or_create_row_numbers_for_pairs(self, pairs)
	}
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		ExternCContext::remove_row_number(self, group, key)
	}
	fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> Result<Vec<RowNumber>> {
		ExternCContext::remove_row_numbers_below(self, group, upper)
	}
	fn reclaim_group_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome> {
		ExternCContext::reclaim_group_identity(self, group, limit)
	}
	fn insert_emit<R: Row>(&mut self, row_capacity: usize) -> Result<ExternCInsertEmit<'_>> {
		let mut builder = self.builder();
		let sink = ExternCRowSink::new::<R>(&mut builder, row_capacity)?;
		let names = R::COLUMNS.iter().map(|(n, _)| *n).collect();
		Ok(ExternCInsertEmit {
			builder,
			sink,
			names,
		})
	}
	fn update_emit<R: Row>(&mut self, row_capacity: usize) -> Result<ExternCUpdateEmit<'_>> {
		let mut builder = self.builder();
		let pre = ExternCRowSink::new::<R>(&mut builder, row_capacity)?;
		let post = ExternCRowSink::new::<R>(&mut builder, row_capacity)?;
		let names = R::COLUMNS.iter().map(|(n, _)| *n).collect();
		Ok(ExternCUpdateEmit {
			builder,
			pre,
			post,
			names,
		})
	}
	fn remove_emit<R: Row>(&mut self, row_capacity: usize) -> Result<ExternCRemoveEmit<'_>> {
		let mut builder = self.builder();
		let sink = ExternCRowSink::new::<R>(&mut builder, row_capacity)?;
		let names = R::COLUMNS.iter().map(|(n, _)| *n).collect();
		Ok(ExternCRemoveEmit {
			builder,
			sink,
			names,
		})
	}
}
