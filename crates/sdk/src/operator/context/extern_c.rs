// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, slice::from_ref};

use reifydb_abi::{context::context::ExternCContext, operator::timer::TimerKind};
use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{
		bytes::{EncodedBytes, read_fingerprint},
		operator::{EncodedOperatorRow, OperatorState},
		shape::{RowShape, fingerprint::RowShapeFingerprint},
	},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, GroupStateKey},
};
use reifydb_flow::window::event::Polarity;
use reifydb_value::{
	params::Params,
	value::{
		Value,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
		frame::frame::Frame,
		row_number::RowNumber,
	},
};

use super::{DictionaryApi, OperatorContext, RowEmit, RowShapeApi, StateApi, StoreApi, UpdateEmit};
use crate::{
	catalog::RowShapeResolver,
	dictionary::Dictionary,
	error::{Result, SdkError},
	operator::{
		builder::ColumnsBuilder,
		column::{row::Row, sink::extern_c::ExternCRowSink},
		diff::DiffStart,
	},
	rql::raw_query,
	state::{
		State,
		extern_c::{
			arm_timer, disarm_timer, flow_watermark, get_or_create_row_numbers, intern_groups,
			lookup_groups, remove_row_number, remove_row_numbers_below,
		},
	},
	store::Store,
};

pub struct ExternCRowEmit<'a> {
	builder: ColumnsBuilder<'a>,
	sink: ExternCRowSink<'a>,
	names: Vec<&'static str>,
	kind: Polarity,
}

impl<'a> RowEmit for ExternCRowEmit<'a> {
	type Sink = ExternCRowSink<'a>;
	fn sink(&mut self) -> &mut ExternCRowSink<'a> {
		&mut self.sink
	}
	fn finish(self, row_numbers: &[RowNumber]) -> Result<()> {
		let mut builder = self.builder;
		let columns = self.sink.finish_all()?;
		match self.kind {
			Polarity::Insert => builder.emit_insert(&columns, &self.names, row_numbers),
			Polarity::Remove => builder.emit_remove(&columns, &self.names, row_numbers),
		}
	}
}

pub struct ExternCUpdateEmit<'a> {
	builder: ColumnsBuilder<'a>,
	pre: ExternCRowSink<'a>,
	post: ExternCRowSink<'a>,
	names: Vec<&'static str>,
}

impl<'a> UpdateEmit for ExternCUpdateEmit<'a> {
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

pub struct ExternCOperatorContext {
	pub(crate) ctx: *mut ExternCContext,
}

impl ExternCOperatorContext {
	pub fn new(ctx: *mut ExternCContext) -> Self {
		assert!(!ctx.is_null(), "ExternCContext pointer must not be null");
		Self {
			ctx,
		}
	}

	pub fn operator_id(&self) -> OperatorId {
		// SAFETY: ExternCOperatorContext::new asserts self.ctx is non-null, and the host keeps the
		// ExternCContext alive and aligned for at least the lifetime of &self.
		unsafe { OperatorId((*self.ctx).operator_id) }
	}

	pub fn state(&mut self) -> State<'_> {
		State::new(self)
	}

	pub fn store(&mut self) -> Store<'_> {
		Store::new(self)
	}

	pub fn row_shape(&mut self) -> RowShapeResolver<'_> {
		RowShapeResolver::new(self)
	}

	pub fn dictionary(&mut self) -> Dictionary<'_> {
		Dictionary::new(self)
	}

	pub fn shape_for_bytes(&mut self, bytes: &EncodedBytes) -> Result<RowShape> {
		let fingerprint = read_fingerprint(bytes);
		match self.row_shape().find_row_shape(fingerprint)? {
			Some(shape) => Ok(shape),
			None => Err(SdkError::Other(format!(
				"row shape with fingerprint {} not registered in catalog",
				fingerprint.as_u64()
			))),
		}
	}

	pub fn intern_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<GroupId>> {
		intern_groups(self, groups)
	}

	pub fn lookup_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>> {
		lookup_groups(self, groups)
	}

	pub fn arm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		arm_timer(self, at, kind, key)
	}

	pub fn disarm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		disarm_timer(self, at, kind, key)
	}

	pub fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
		flow_watermark(self)
	}

	pub fn get_or_create_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		Ok(get_or_create_row_numbers(self, group, from_ref(key))?.into_iter().next().unwrap())
	}

	pub fn get_or_create_row_numbers(
		&mut self,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>> {
		get_or_create_row_numbers(self, group, keys)
	}

	pub fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		remove_row_number(self, group, key)
	}

	pub fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> Result<Vec<RowNumber>> {
		remove_row_numbers_below(self, group, upper)
	}

	pub fn query(&self, query: &str, params: Params) -> Result<Vec<Frame>> {
		raw_query(self, query, params)
	}

	pub fn builder(&mut self) -> ColumnsBuilder<'_> {
		ColumnsBuilder::new(self)
	}

	pub fn diff(&mut self) -> DiffStart<'_> {
		DiffStart::new(self)
	}
}

impl StateApi for State<'_> {
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

	fn get_bytes(&self, key: &GroupStateKey) -> Result<Option<EncodedOperatorRow>> {
		State::get_bytes(self, key)
	}

	fn set_bytes(&mut self, key: &GroupStateKey, payload: EncodedOperatorRow) -> Result<()> {
		State::set_bytes(self, key, payload)
	}

	fn get_many_bytes_visit(
		&self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> Result<()>,
	) -> Result<()> {
		State::get_many_bytes_visit(self, keys, visit)
	}

	fn range_bytes_visit(
		&self,
		start: Bound<&GroupStateKey>,
		end: Bound<&GroupStateKey>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> Result<()>,
	) -> Result<()> {
		State::range_bytes_visit(self, start, end, visit)
	}
}

impl StoreApi for Store<'_> {
	fn get(&self, key: &EncodedKey) -> Result<Option<EncodedBytes>> {
		Store::get(self, key)
	}
	fn contains(&self, key: &EncodedKey) -> Result<bool> {
		Store::contains(self, key)
	}
	fn prefix(&self, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, EncodedBytes)>> {
		Store::prefix(self, prefix)
	}
	fn range(&self, start: Bound<&EncodedKey>, end: Bound<&EncodedKey>) -> Result<Vec<(EncodedKey, EncodedBytes)>> {
		Store::range(self, start, end)
	}
}

impl RowShapeApi for RowShapeResolver<'_> {
	fn find_row_shape(&self, fingerprint: RowShapeFingerprint) -> Result<Option<RowShape>> {
		RowShapeResolver::find_row_shape(self, fingerprint)
	}
}

impl DictionaryApi for Dictionary<'_> {
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

impl OperatorContext for ExternCOperatorContext {
	type InsertEmit<'a> = ExternCRowEmit<'a>;
	type UpdateEmit<'a> = ExternCUpdateEmit<'a>;
	type RemoveEmit<'a> = ExternCRowEmit<'a>;

	fn operator_id(&self) -> OperatorId {
		ExternCOperatorContext::operator_id(self)
	}
	fn written_at(&self) -> DateTime {
		// SAFETY: ExternCOperatorContext::new asserts self.ctx is non-null, and the host keeps the
		// ExternCContext alive and aligned for at least the lifetime of &self.
		DateTime::from_nanos(unsafe { (*self.ctx).written_at_nanos })
	}
	fn state(&mut self) -> impl StateApi + '_ {
		ExternCOperatorContext::state(self)
	}
	fn store(&mut self) -> impl StoreApi + '_ {
		ExternCOperatorContext::store(self)
	}
	fn row_shape(&mut self) -> impl RowShapeApi + '_ {
		ExternCOperatorContext::row_shape(self)
	}
	fn dictionary(&mut self) -> impl DictionaryApi + '_ {
		ExternCOperatorContext::dictionary(self)
	}
	fn intern_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<GroupId>> {
		ExternCOperatorContext::intern_groups(self, groups)
	}
	fn lookup_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>> {
		ExternCOperatorContext::lookup_groups(self, groups)
	}
	fn arm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		ExternCOperatorContext::arm_timer(self, at, kind, key)
	}
	fn disarm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		ExternCOperatorContext::disarm_timer(self, at, kind, key)
	}

	fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
		ExternCOperatorContext::flow_watermark(self)
	}
	fn get_or_create_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		ExternCOperatorContext::get_or_create_row_number(self, group, key)
	}
	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		ExternCOperatorContext::get_or_create_row_numbers(self, group, keys)
	}
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		ExternCOperatorContext::remove_row_number(self, group, key)
	}
	fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> Result<Vec<RowNumber>> {
		ExternCOperatorContext::remove_row_numbers_below(self, group, upper)
	}
	fn shape_for_bytes(&mut self, bytes: &EncodedBytes) -> Result<RowShape> {
		ExternCOperatorContext::shape_for_bytes(self, bytes)
	}
	fn insert_emit<R: Row>(&mut self, row_capacity: usize) -> Result<ExternCRowEmit<'_>> {
		let mut builder = self.builder();
		let sink = ExternCRowSink::new::<R>(&mut builder, row_capacity)?;
		let names = R::COLUMNS.iter().map(|(n, _)| *n).collect();
		Ok(ExternCRowEmit {
			builder,
			sink,
			names,
			kind: Polarity::Insert,
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
	fn remove_emit<R: Row>(&mut self, row_capacity: usize) -> Result<ExternCRowEmit<'_>> {
		let mut builder = self.builder();
		let sink = ExternCRowSink::new::<R>(&mut builder, row_capacity)?;
		let names = R::COLUMNS.iter().map(|(n, _)| *n).collect();
		Ok(ExternCRowEmit {
			builder,
			sink,
			names,
			kind: Polarity::Remove,
		})
	}
}
