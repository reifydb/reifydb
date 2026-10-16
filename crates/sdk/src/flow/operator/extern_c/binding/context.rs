// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{operator::state::OperatorState, pod::EncodedPodRow},
};
use reifydb_core::{
	common::OperatorClass,
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId, KeyspaceMask},
	state::{batch::StateBatch, timer::TimerKind},
};
use reifydb_flow_async::operator::state::reclaim::ReclaimOutcome;
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::{
	common::extern_c::binding::builder::{ColumnsBuilder, CommittedColumn},
	error::Result,
	flow::operator::{
		column::row::Row,
		context::{
			ClassState, CustomClass, GuestBound, GuestContext, GuestEmit, GuestEmitContext, GuestState,
			GuestUpdateEmit, WindowClass, check_class_batch, class_framed, class_keyspace,
		},
		extern_c::{
			binding::{
				sink::ExternCRowSink,
				state::{
					arm_timer, disarm_timer, flow_watermark, get_or_create_row_numbers,
					get_or_create_row_numbers_for_groups, get_row_numbers, reclaim_group_identity,
					remove_row_number, remove_row_numbers, remove_row_numbers_for_groups,
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
}

impl<'a> GuestEmit for ExternCInsertEmit<'a> {
	type Sink = ExternCRowSink<'a>;
	fn sink(&mut self) -> &mut ExternCRowSink<'a> {
		&mut self.sink
	}
	fn finish(self, row_numbers: &[RowNumber]) -> Result<()> {
		let mut builder = self.builder;
		let names = self.sink.names();
		let columns = self.sink.finish_all()?;
		builder.insert_named(&columns, names, row_numbers)
	}
}

pub struct ExternCRemoveEmit<'a> {
	builder: ColumnsBuilder<'a>,
	sink: ExternCRowSink<'a>,
}

impl<'a> GuestEmit for ExternCRemoveEmit<'a> {
	type Sink = ExternCRowSink<'a>;
	fn sink(&mut self) -> &mut ExternCRowSink<'a> {
		&mut self.sink
	}
	fn finish(self, row_numbers: &[RowNumber]) -> Result<()> {
		let mut builder = self.builder;
		let names = self.sink.names();
		let columns = self.sink.finish_all()?;
		builder.remove_named(&columns, names, row_numbers)
	}
}

pub struct ExternCUpdateEmit<'a> {
	builder: ColumnsBuilder<'a>,
	pre: ExternCRowSink<'a>,
	post: ExternCRowSink<'a>,
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
		let pre_names = self.pre.names();
		let post_names = self.post.names();
		let pre_columns = self.pre.finish_all()?;
		let post_columns = self.post.finish_all()?;
		builder.update_named(
			&pre_columns,
			pre_names,
			pre_columns.first().map_or(0, CommittedColumn::row_count),
			row_numbers,
			&post_columns,
			post_names,
			post_columns.first().map_or(0, CommittedColumn::row_count),
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

	pub fn get_or_create_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<(RowNumber, bool)>> {
		get_or_create_row_numbers_for_groups(self, groups)
	}

	pub fn get_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<Option<RowNumber>>> {
		get_row_numbers(self, group, keys)
	}

	pub fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		remove_row_number(self, group, key)
	}

	pub fn remove_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<()> {
		remove_row_numbers(self, group, keys)
	}

	pub fn remove_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<()> {
		remove_row_numbers_for_groups(self, groups)
	}

	pub fn reclaim_group_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome> {
		reclaim_group_identity(self, group, limit)
	}

	pub fn builder(&mut self) -> ColumnsBuilder<'_> {
		ColumnsBuilder::new(self.ctx as *mut c_void, unsafe { (*self.ctx).callbacks.builder }, unsafe {
			(*self.ctx).written_at_nanos
		})
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
	fn get_bytes(&self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		State::get_bytes(self, key)
	}

	fn set_bytes(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()> {
		State::set_bytes(self, key, payload)
	}

	fn remove_bytes(&mut self, key: &GroupStateKey) -> Result<()> {
		State::remove(self, key)
	}

	fn remove_many_bytes(&mut self, keys: &[GroupStateKey]) -> Result<()> {
		State::remove_many_bytes(self, keys)
	}

	fn batch_bytes(&mut self, keys: Vec<GroupStateKey>) -> Result<StateBatch> {
		State::batch_bytes(self, keys)
	}

	fn write_batch_bytes(&mut self, batch: StateBatch) -> Result<()> {
		State::write_batch_bytes(self, batch)
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
		group: GroupId,
		keyspace: KeyspaceId,
		start: GuestBound<'_>,
		end: GuestBound<'_>,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		State::range_bytes_visit(self, group, keyspace, start, end, limit, visit)
	}

	fn sweep_many_bytes(
		&self,
		groups: &[GroupId],
		limit: usize,
		keyspaces: KeyspaceMask,
	) -> Result<(Vec<(GroupStateKey, EncodedPodRow)>, bool)> {
		State::sweep_many_bytes(self, groups, limit, keyspaces)
	}
}

pub struct ClassFramedState<'a> {
	state: State<'a>,
	class: OperatorClass,
}

impl GuestState for ClassFramedState<'_> {
	fn get<T: OperatorState>(&self, key: &GroupStateKey) -> Result<Option<T>> {
		class_framed(self.class, key)?;
		State::get(&self.state, key)
	}
	fn set<T: OperatorState>(&mut self, key: &GroupStateKey, value: &T) -> Result<()> {
		class_framed(self.class, key)?;
		State::set(&mut self.state, key, value)
	}
	fn remove(&mut self, key: &GroupStateKey) -> Result<()> {
		class_framed(self.class, key)?;
		State::remove(&mut self.state, key)
	}
	fn contains(&self, key: &GroupStateKey) -> Result<bool> {
		class_framed(self.class, key)?;
		State::contains(&self.state, key)
	}
	fn get_bytes(&self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		class_framed(self.class, key)?;
		State::get_bytes(&self.state, key)
	}

	fn set_bytes(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()> {
		class_framed(self.class, key)?;
		State::set_bytes(&mut self.state, key, payload)
	}

	fn remove_bytes(&mut self, key: &GroupStateKey) -> Result<()> {
		class_framed(self.class, key)?;
		State::remove(&mut self.state, key)
	}

	fn remove_many_bytes(&mut self, keys: &[GroupStateKey]) -> Result<()> {
		for key in keys {
			class_framed(self.class, key)?;
		}
		State::remove_many_bytes(&mut self.state, keys)
	}

	fn batch_bytes(&mut self, keys: Vec<GroupStateKey>) -> Result<StateBatch> {
		for key in &keys {
			class_framed(self.class, key)?;
		}
		State::batch_bytes(&mut self.state, keys)
	}

	fn write_batch_bytes(&mut self, batch: StateBatch) -> Result<()> {
		check_class_batch(self.class, &batch)?;
		State::write_batch_bytes(&mut self.state, batch)
	}

	fn get_many_bytes_visit(
		&self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		for key in keys {
			class_framed(self.class, key)?;
		}
		State::get_many_bytes_visit(&self.state, keys, visit)
	}

	fn range_bytes_visit(
		&self,
		group: GroupId,
		keyspace: KeyspaceId,
		start: GuestBound<'_>,
		end: GuestBound<'_>,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		class_keyspace(self.class, keyspace)?;
		State::range_bytes_visit(&self.state, group, keyspace, start, end, limit, visit)
	}

	fn sweep_many_bytes(
		&self,
		groups: &[GroupId],
		limit: usize,
		keyspaces: KeyspaceMask,
	) -> Result<(Vec<(GroupStateKey, EncodedPodRow)>, bool)> {
		State::sweep_many_bytes(&self.state, groups, limit, keyspaces)
	}
}

impl GuestEmitContext for ExternCContext {
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
	fn get_or_create_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<(RowNumber, bool)>> {
		ExternCContext::get_or_create_row_numbers_for_groups(self, groups)
	}
	fn get_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<Option<RowNumber>>> {
		ExternCContext::get_row_numbers(self, group, keys)
	}
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		ExternCContext::remove_row_number(self, group, key)
	}
	fn remove_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<()> {
		ExternCContext::remove_row_numbers(self, group, keys)
	}
	fn remove_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<()> {
		ExternCContext::remove_row_numbers_for_groups(self, groups)
	}
	fn insert_emit<R: Row>(&mut self, row_capacity: usize) -> Result<ExternCInsertEmit<'_>> {
		let ctx = self.ctx;
		let mut builder = self.builder();
		let sink = ExternCRowSink::new::<R>(&mut builder, ctx, row_capacity)?;
		Ok(ExternCInsertEmit {
			builder,
			sink,
		})
	}
	fn update_emit<R: Row>(&mut self, row_capacity: usize) -> Result<ExternCUpdateEmit<'_>> {
		let ctx = self.ctx;
		let mut builder = self.builder();
		let pre = ExternCRowSink::new::<R>(&mut builder, ctx, row_capacity)?;
		let post = ExternCRowSink::new::<R>(&mut builder, ctx, row_capacity)?;
		Ok(ExternCUpdateEmit {
			builder,
			pre,
			post,
		})
	}
	fn remove_emit<R: Row>(&mut self, row_capacity: usize) -> Result<ExternCRemoveEmit<'_>> {
		let ctx = self.ctx;
		let mut builder = self.builder();
		let sink = ExternCRowSink::new::<R>(&mut builder, ctx, row_capacity)?;
		Ok(ExternCRemoveEmit {
			builder,
			sink,
		})
	}
}

impl<C> GuestContext<C> for ExternCContext {
	fn state(&mut self) -> impl ClassState<C> + '_
	where
		C: CustomClass,
	{
		ClassFramedState {
			state: ExternCContext::state(self),
			class: C::CLASS,
		}
	}
	fn window_state(&mut self) -> impl GuestState + '_
	where
		C: WindowClass,
	{
		ClassFramedState {
			state: ExternCContext::state(self),
			class: C::CLASS,
		}
	}
	fn reclaim_group_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome>
	where
		C: WindowClass,
	{
		ExternCContext::reclaim_group_identity(self, group, limit)
	}
}
