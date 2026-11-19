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
	state::{
		batch::StateBatch,
		timer::{TimerKind, TimerMove},
	},
};
use reifydb_flow_async::operator::state::forget_pass_outcome::ForgetPassOutcome;
use reifydb_value::value::{
	Value,
	datetime::DateTime,
	dictionary::{DictionaryEntryId, DictionaryId},
	row_number::RowNumber,
};

use crate::{
	common::extern_c::binding::builder::{ColumnsBuilder, CommittedColumn},
	error::SdkError,
	flow::operator::{
		column::row::Row,
		context::{
			ClassState, CustomClass, GuestBound, GuestContext, GuestEmit, GuestEmitContext, GuestState,
			GuestUpdateEmit, WindowClass, check_class_batch, class_framed, class_keyspace,
		},
		extern_c::{
			binding::{
				dictionary::{raw_find, raw_intern},
				sink::ExternCRowSink,
				state::{
					arm_timer, disarm_timer, flow_watermark, forget_group_identities,
					get_or_create_row_numbers, get_or_create_row_numbers_for_groups,
					get_row_numbers, get_row_numbers_for_groups, mint_row_numbers, rearm_timers,
					remove_row_number, remove_row_number_for_group, set_row_numbers_for_groups,
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
	fn finish(self, row_numbers: &[RowNumber], times: &[DateTime]) -> Result<(), SdkError> {
		let mut builder = self.builder;
		let names = self.sink.names();
		let columns = self.sink.finish_all()?;
		builder.insert_named(&columns, names, row_numbers, times)
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
	fn finish(self, row_numbers: &[RowNumber], times: &[DateTime]) -> Result<(), SdkError> {
		let mut builder = self.builder;
		let names = self.sink.names();
		let columns = self.sink.finish_all()?;
		builder.remove_named(&columns, names, row_numbers, times)
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
	fn finish(
		self,
		row_numbers: &[RowNumber],
		pre_times: &[DateTime],
		post_times: &[DateTime],
	) -> Result<(), SdkError> {
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
			pre_times,
			post_times,
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

	pub fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<(), SdkError> {
		arm_timer(self, due, kind, key)
	}

	pub fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<(), SdkError> {
		disarm_timer(self, due, kind, key)
	}

	pub fn rearm_timers(&mut self, kind: TimerKind, moves: &[TimerMove]) -> Result<(), SdkError> {
		rearm_timers(self, kind, moves)
	}

	pub fn flow_watermark(&mut self) -> Result<Option<DateTime>, SdkError> {
		flow_watermark(self)
	}

	pub fn get_or_create_row_numbers(
		&mut self,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>, SdkError> {
		get_or_create_row_numbers(self, group, keys)
	}

	pub fn get_or_create_row_numbers_for_groups(
		&mut self,
		groups: &[GroupId],
	) -> Result<Vec<(RowNumber, bool)>, SdkError> {
		get_or_create_row_numbers_for_groups(self, groups)
	}

	pub fn get_row_numbers(
		&mut self,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<Option<RowNumber>>, SdkError> {
		get_row_numbers(self, group, keys)
	}

	pub fn remove_row_number(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<(), SdkError> {
		remove_row_number(self, group, keys)
	}

	pub fn remove_row_number_for_group(&mut self, groups: &[GroupId]) -> Result<(), SdkError> {
		remove_row_number_for_group(self, groups)
	}

	pub fn mint_row_numbers(&mut self, count: usize) -> Result<RowNumber, SdkError> {
		mint_row_numbers(self, count)
	}

	pub fn get_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<Option<RowNumber>>, SdkError> {
		get_row_numbers_for_groups(self, groups)
	}

	pub fn set_row_numbers_for_groups(&mut self, rows: &[(GroupId, RowNumber)]) -> Result<(), SdkError> {
		set_row_numbers_for_groups(self, rows)
	}

	pub fn forget_group_identities(
		&mut self,
		groups: &[GroupId],
		limit: usize,
	) -> Result<Vec<ForgetPassOutcome>, SdkError> {
		forget_group_identities(self, groups, limit)
	}

	pub fn builder(&mut self) -> ColumnsBuilder<'_> {
		ColumnsBuilder::new(self.ctx as *mut c_void, unsafe { (*self.ctx).callbacks.builder }, unsafe {
			(*self.ctx).batch_time_nanos
		})
	}
}

impl GuestState for State<'_> {
	fn get<T: OperatorState>(&self, keys: &[GroupStateKey]) -> Result<Vec<Option<T>>, SdkError> {
		State::get(self, keys)
	}
	fn set<T: OperatorState>(&mut self, rows: &[(GroupStateKey, T)]) -> Result<(), SdkError> {
		State::set(self, rows)
	}
	fn remove(&mut self, keys: &[GroupStateKey]) -> Result<(), SdkError> {
		State::remove(self, keys)
	}
	fn contains(&self, keys: &[GroupStateKey]) -> Result<Vec<bool>, SdkError> {
		State::contains(self, keys)
	}
	fn get_bytes(
		&self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<(), SdkError>,
	) -> Result<(), SdkError> {
		State::get_bytes(self, keys, visit)
	}

	fn set_bytes(&mut self, rows: Vec<(GroupStateKey, EncodedPodRow)>) -> Result<(), SdkError> {
		State::set_bytes(self, rows)
	}

	fn remove_bytes(&mut self, keys: &[GroupStateKey]) -> Result<(), SdkError> {
		State::remove(self, keys)
	}

	fn batch_bytes(&mut self, keys: Vec<GroupStateKey>) -> Result<StateBatch, SdkError> {
		State::batch_bytes(self, keys)
	}

	fn write_batch_bytes(&mut self, batch: StateBatch) -> Result<(), SdkError> {
		State::write_batch_bytes(self, batch)
	}

	fn range_bytes_visit(
		&self,
		group: GroupId,
		keyspace: KeyspaceId,
		start: GuestBound<'_>,
		end: GuestBound<'_>,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<(), SdkError>,
	) -> Result<(), SdkError> {
		State::range_bytes_visit(self, group, keyspace, start, end, limit, visit)
	}

	fn sweep_bytes(
		&self,
		groups: &[GroupId],
		limit: usize,
		keyspaces: KeyspaceMask,
	) -> Result<(Vec<(GroupStateKey, EncodedPodRow)>, bool), SdkError> {
		State::sweep_bytes(self, groups, limit, keyspaces)
	}
}

pub struct ClassFramedState<'a> {
	state: State<'a>,
	class: OperatorClass,
}

impl GuestState for ClassFramedState<'_> {
	fn get<T: OperatorState>(&self, keys: &[GroupStateKey]) -> Result<Vec<Option<T>>, SdkError> {
		for key in keys {
			class_framed(self.class, key)?;
		}
		State::get(&self.state, keys)
	}
	fn set<T: OperatorState>(&mut self, rows: &[(GroupStateKey, T)]) -> Result<(), SdkError> {
		for (key, _) in rows {
			class_framed(self.class, key)?;
		}
		State::set(&mut self.state, rows)
	}
	fn remove(&mut self, keys: &[GroupStateKey]) -> Result<(), SdkError> {
		for key in keys {
			class_framed(self.class, key)?;
		}
		State::remove(&mut self.state, keys)
	}
	fn contains(&self, keys: &[GroupStateKey]) -> Result<Vec<bool>, SdkError> {
		for key in keys {
			class_framed(self.class, key)?;
		}
		State::contains(&self.state, keys)
	}
	fn get_bytes(
		&self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<(), SdkError>,
	) -> Result<(), SdkError> {
		for key in keys {
			class_framed(self.class, key)?;
		}
		State::get_bytes(&self.state, keys, visit)
	}

	fn set_bytes(&mut self, rows: Vec<(GroupStateKey, EncodedPodRow)>) -> Result<(), SdkError> {
		for (key, _) in &rows {
			class_framed(self.class, key)?;
		}
		State::set_bytes(&mut self.state, rows)
	}

	fn remove_bytes(&mut self, keys: &[GroupStateKey]) -> Result<(), SdkError> {
		for key in keys {
			class_framed(self.class, key)?;
		}
		State::remove(&mut self.state, keys)
	}

	fn batch_bytes(&mut self, keys: Vec<GroupStateKey>) -> Result<StateBatch, SdkError> {
		for key in &keys {
			class_framed(self.class, key)?;
		}
		State::batch_bytes(&mut self.state, keys)
	}

	fn write_batch_bytes(&mut self, batch: StateBatch) -> Result<(), SdkError> {
		check_class_batch(self.class, &batch)?;
		State::write_batch_bytes(&mut self.state, batch)
	}

	fn range_bytes_visit(
		&self,
		group: GroupId,
		keyspace: KeyspaceId,
		start: GuestBound<'_>,
		end: GuestBound<'_>,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<(), SdkError>,
	) -> Result<(), SdkError> {
		class_keyspace(self.class, keyspace)?;
		State::range_bytes_visit(&self.state, group, keyspace, start, end, limit, visit)
	}

	fn sweep_bytes(
		&self,
		groups: &[GroupId],
		limit: usize,
		keyspaces: KeyspaceMask,
	) -> Result<(Vec<(GroupStateKey, EncodedPodRow)>, bool), SdkError> {
		State::sweep_bytes(&self.state, groups, limit, keyspaces)
	}
}

impl GuestEmitContext for ExternCContext {
	type InsertEmit<'a> = ExternCInsertEmit<'a>;
	type UpdateEmit<'a> = ExternCUpdateEmit<'a>;
	type RemoveEmit<'a> = ExternCRemoveEmit<'a>;

	fn operator_id(&self) -> OperatorId {
		ExternCContext::operator_id(self)
	}
	fn batch_time(&self) -> DateTime {
		// SAFETY: ExternCContext::new asserts self.ctx is non-null, and the host keeps the
		// ExternCContextRaw alive and aligned for at least the lifetime of &self.
		DateTime::from_nanos(unsafe { (*self.ctx).batch_time_nanos })
	}
	fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<(), SdkError> {
		ExternCContext::arm_timer(self, due, kind, key)
	}
	fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<(), SdkError> {
		ExternCContext::disarm_timer(self, due, kind, key)
	}
	fn rearm_timers(&mut self, kind: TimerKind, moves: &[TimerMove]) -> Result<(), SdkError> {
		ExternCContext::rearm_timers(self, kind, moves)
	}

	fn flow_watermark(&mut self) -> Result<Option<DateTime>, SdkError> {
		ExternCContext::flow_watermark(self)
	}
	fn dictionary_find(
		&mut self,
		dictionary: DictionaryId,
		values: &[Value],
	) -> Result<Vec<Option<DictionaryEntryId>>, SdkError> {
		raw_find(self, dictionary, values)
	}
	fn dictionary_intern(
		&mut self,
		dictionary: DictionaryId,
		values: &[Value],
	) -> Result<Vec<DictionaryEntryId>, SdkError> {
		raw_intern(self, dictionary, values)
	}
	fn get_or_create_row_numbers(
		&mut self,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>, SdkError> {
		ExternCContext::get_or_create_row_numbers(self, group, keys)
	}
	fn get_or_create_row_numbers_for_groups(
		&mut self,
		groups: &[GroupId],
	) -> Result<Vec<(RowNumber, bool)>, SdkError> {
		ExternCContext::get_or_create_row_numbers_for_groups(self, groups)
	}
	fn get_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<Option<RowNumber>>, SdkError> {
		ExternCContext::get_row_numbers(self, group, keys)
	}
	fn remove_row_number(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<(), SdkError> {
		ExternCContext::remove_row_number(self, group, keys)
	}
	fn remove_row_number_for_group(&mut self, groups: &[GroupId]) -> Result<(), SdkError> {
		ExternCContext::remove_row_number_for_group(self, groups)
	}
	fn mint_row_numbers(&mut self, count: usize) -> Result<RowNumber, SdkError> {
		ExternCContext::mint_row_numbers(self, count)
	}
	fn get_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<Option<RowNumber>>, SdkError> {
		ExternCContext::get_row_numbers_for_groups(self, groups)
	}
	fn set_row_numbers_for_groups(&mut self, rows: &[(GroupId, RowNumber)]) -> Result<(), SdkError> {
		ExternCContext::set_row_numbers_for_groups(self, rows)
	}
	fn insert_emit<R: Row>(&mut self, row_capacity: usize) -> Result<ExternCInsertEmit<'_>, SdkError> {
		let ctx = self.ctx;
		let mut builder = self.builder();
		let sink = ExternCRowSink::new::<R>(&mut builder, ctx, row_capacity)?;
		Ok(ExternCInsertEmit {
			builder,
			sink,
		})
	}
	fn update_emit<R: Row>(&mut self, row_capacity: usize) -> Result<ExternCUpdateEmit<'_>, SdkError> {
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
	fn remove_emit<R: Row>(&mut self, row_capacity: usize) -> Result<ExternCRemoveEmit<'_>, SdkError> {
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
	fn forget_group_identities(
		&mut self,
		groups: &[GroupId],
		limit: usize,
	) -> Result<Vec<ForgetPassOutcome>, SdkError>
	where
		C: WindowClass,
	{
		ExternCContext::forget_group_identities(self, groups, limit)
	}
}
