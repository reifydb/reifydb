// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_abi::context::context::ContextFFI;
use reifydb_codec::{
	encoded::{
		row::EncodedRow,
		shape::{RowShape, fingerprint::RowShapeFingerprint},
	},
	key::encoded::EncodedKey,
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		flow::FlowNodeId,
		id::{NamespaceId, TableId},
		namespace::Namespace,
		table::Table,
	},
};
use reifydb_value::{
	params::Params,
	value::{
		Value,
		dictionary::{DictionaryEntryId, DictionaryId},
		frame::frame::Frame,
		row_number::RowNumber,
	},
};
use serde::{Serialize, de::DeserializeOwned};

use super::{CatalogApi, DictionaryApi, InternalStateApi, OperatorContext, RowEmit, StateApi, StoreApi, UpdateEmit};
use crate::{
	catalog::Catalog,
	dictionary::Dictionary,
	error::{Result, SdkError},
	operator::{
		builder::ColumnsBuilder,
		column::{row::Row, sink::ffi::FFIRowSink},
		diff::DiffStart,
	},
	rql::raw_query,
	state::{InternalState, State, StateEntry, ffi::allocate_row_numbers, row::RowNumberProvider},
	store::Store,
};

enum EmitKind {
	Insert,
	Remove,
}

pub struct FFIRowEmit<'a> {
	builder: ColumnsBuilder<'a>,
	sink: FFIRowSink<'a>,
	names: Vec<&'static str>,
	kind: EmitKind,
}

impl<'a> RowEmit for FFIRowEmit<'a> {
	type Sink = FFIRowSink<'a>;
	fn sink(&mut self) -> &mut FFIRowSink<'a> {
		&mut self.sink
	}
	fn finish(self, row_numbers: &[RowNumber]) -> Result<()> {
		let mut builder = self.builder;
		let columns = self.sink.finish_all()?;
		match self.kind {
			EmitKind::Insert => builder.emit_insert(&columns, &self.names, row_numbers),
			EmitKind::Remove => builder.emit_remove(&columns, &self.names, row_numbers),
		}
	}
}

pub struct FFIUpdateEmit<'a> {
	builder: ColumnsBuilder<'a>,
	pre: FFIRowSink<'a>,
	post: FFIRowSink<'a>,
	names: Vec<&'static str>,
}

impl<'a> UpdateEmit for FFIUpdateEmit<'a> {
	type Sink = FFIRowSink<'a>;
	fn pre(&mut self) -> &mut FFIRowSink<'a> {
		&mut self.pre
	}
	fn post(&mut self) -> &mut FFIRowSink<'a> {
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

pub struct FFIOperatorContext {
	pub(crate) ctx: *mut ContextFFI,
}

impl FFIOperatorContext {
	pub fn new(ctx: *mut ContextFFI) -> Self {
		assert!(!ctx.is_null(), "ContextFFI pointer must not be null");
		Self {
			ctx,
		}
	}

	pub fn operator_id(&self) -> FlowNodeId {
		unsafe { FlowNodeId((*self.ctx).operator_id) }
	}

	pub fn state(&mut self) -> State<'_> {
		State::new(self)
	}

	pub fn internal_state(&mut self) -> InternalState<'_> {
		InternalState::new(self)
	}

	pub fn store(&mut self) -> Store<'_> {
		Store::new(self)
	}

	pub fn catalog(&mut self) -> Catalog<'_> {
		Catalog::new(self)
	}

	pub fn dictionary(&mut self) -> Dictionary<'_> {
		Dictionary::new(self)
	}

	pub fn shape_for_row(&mut self, row: &EncodedRow) -> Result<RowShape> {
		let fingerprint = row.fingerprint();
		match self.catalog().find_row_shape(fingerprint)? {
			Some(shape) => Ok(shape),
			None => Err(SdkError::Other(format!(
				"row shape with fingerprint {} not registered in catalog",
				fingerprint.as_u64()
			))),
		}
	}

	pub fn get_or_create_row_number(&mut self, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		let provider = RowNumberProvider::new(self.operator_id());
		provider.get_or_create_row_number(self, key)
	}

	pub fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		let provider = RowNumberProvider::new(self.operator_id());
		provider.get_or_create_row_numbers_batch(self, keys.iter())
	}

	pub fn allocate_row_numbers(&mut self, count: u64) -> Result<RowNumber> {
		Ok(RowNumber(allocate_row_numbers(self, count)?))
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
	fn get<T: DeserializeOwned>(&self, key: &EncodedKey) -> Result<Option<T>> {
		State::get(self, key)
	}
	fn set<T: Serialize>(&mut self, key: &EncodedKey, value: &T) -> Result<()> {
		State::set(self, key, value)
	}
	fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		State::remove(self, key)
	}
	fn drop(&mut self, key: &EncodedKey) -> Result<()> {
		State::drop(self, key)
	}
	fn contains(&self, key: &EncodedKey) -> Result<bool> {
		State::contains(self, key)
	}
	fn clear(&mut self) -> Result<()> {
		State::clear(self)
	}
	fn scan_prefix<T: DeserializeOwned>(&self, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, T)>> {
		State::scan_prefix(self, prefix)
	}
	fn get_many<T: DeserializeOwned>(&self, keys: &[EncodedKey]) -> Result<Vec<(EncodedKey, T)>> {
		State::get_many(self, keys)
	}
	fn keys_with_prefix(&self, prefix: &EncodedKey) -> Result<Vec<EncodedKey>> {
		State::keys_with_prefix(self, prefix)
	}
	fn range<T: DeserializeOwned>(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Result<Vec<(EncodedKey, T)>> {
		State::range(self, start, end)
	}
	fn get_with_anchors<T: DeserializeOwned>(&self, key: &EncodedKey) -> Result<Option<StateEntry<T>>> {
		State::get_with_anchors(self, key)
	}
}

impl InternalStateApi for InternalState<'_> {
	fn get<T: DeserializeOwned>(&self, key: &EncodedKey) -> Result<Option<T>> {
		InternalState::get(self, key)
	}
	fn get_many<T: DeserializeOwned>(&self, keys: &[EncodedKey]) -> Result<Vec<(EncodedKey, T)>> {
		InternalState::get_many(self, keys)
	}
	fn set<T: Serialize>(&mut self, key: &EncodedKey, value: &T) -> Result<()> {
		InternalState::set(self, key, value)
	}
	fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		InternalState::remove(self, key)
	}
	fn drop(&mut self, key: &EncodedKey) -> Result<()> {
		InternalState::drop(self, key)
	}
	fn contains(&self, key: &EncodedKey) -> Result<bool> {
		InternalState::contains(self, key)
	}
	fn range<T: DeserializeOwned>(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Result<Vec<(EncodedKey, T)>> {
		InternalState::range(self, start, end)
	}
}

impl StoreApi for Store<'_> {
	fn get(&self, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		Store::get(self, key)
	}
	fn contains(&self, key: &EncodedKey) -> Result<bool> {
		Store::contains(self, key)
	}
	fn prefix(&self, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, EncodedRow)>> {
		Store::prefix(self, prefix)
	}
	fn range(&self, start: Bound<&EncodedKey>, end: Bound<&EncodedKey>) -> Result<Vec<(EncodedKey, EncodedRow)>> {
		Store::range(self, start, end)
	}
}

impl CatalogApi for Catalog<'_> {
	fn find_namespace(&self, namespace: NamespaceId, version: CommitVersion) -> Result<Option<Namespace>> {
		Catalog::find_namespace(self, namespace, version)
	}
	fn find_namespace_by_name(&self, namespace: &str, version: CommitVersion) -> Result<Option<Namespace>> {
		Catalog::find_namespace_by_name(self, namespace, version)
	}
	fn find_table(&self, table: TableId, version: CommitVersion) -> Result<Option<Table>> {
		Catalog::find_table(self, table, version)
	}
	fn find_table_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Result<Option<Table>> {
		Catalog::find_table_by_name(self, namespace, name, version)
	}
	fn find_row_shape(&self, fingerprint: RowShapeFingerprint) -> Result<Option<RowShape>> {
		Catalog::find_row_shape(self, fingerprint)
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

impl OperatorContext for FFIOperatorContext {
	type InsertEmit<'a> = FFIRowEmit<'a>;
	type UpdateEmit<'a> = FFIUpdateEmit<'a>;
	type RemoveEmit<'a> = FFIRowEmit<'a>;

	fn operator_id(&self) -> FlowNodeId {
		FFIOperatorContext::operator_id(self)
	}
	fn clock_now_nanos(&self) -> u64 {
		unsafe { (*self.ctx).clock_now_nanos }
	}
	fn state(&mut self) -> impl StateApi + '_ {
		FFIOperatorContext::state(self)
	}
	fn internal_state(&mut self) -> impl InternalStateApi + '_ {
		FFIOperatorContext::internal_state(self)
	}
	fn store(&mut self) -> impl StoreApi + '_ {
		FFIOperatorContext::store(self)
	}
	fn catalog(&mut self) -> impl CatalogApi + '_ {
		FFIOperatorContext::catalog(self)
	}
	fn dictionary(&mut self) -> impl DictionaryApi + '_ {
		FFIOperatorContext::dictionary(self)
	}
	fn get_or_create_row_number(&mut self, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		FFIOperatorContext::get_or_create_row_number(self, key)
	}
	fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		FFIOperatorContext::get_or_create_row_numbers(self, keys)
	}
	fn allocate_row_numbers(&mut self, count: u64) -> Result<RowNumber> {
		FFIOperatorContext::allocate_row_numbers(self, count)
	}
	fn shape_for_row(&mut self, row: &EncodedRow) -> Result<RowShape> {
		FFIOperatorContext::shape_for_row(self, row)
	}
	fn insert_emit<R: Row>(&mut self, row_capacity: usize) -> Result<FFIRowEmit<'_>> {
		let mut builder = self.builder();
		let sink = FFIRowSink::new::<R>(&mut builder, row_capacity)?;
		let names = R::COLUMNS.iter().map(|(n, _)| *n).collect();
		Ok(FFIRowEmit {
			builder,
			sink,
			names,
			kind: EmitKind::Insert,
		})
	}
	fn update_emit<R: Row>(&mut self, row_capacity: usize) -> Result<FFIUpdateEmit<'_>> {
		let mut builder = self.builder();
		let pre = FFIRowSink::new::<R>(&mut builder, row_capacity)?;
		let post = FFIRowSink::new::<R>(&mut builder, row_capacity)?;
		let names = R::COLUMNS.iter().map(|(n, _)| *n).collect();
		Ok(FFIUpdateEmit {
			builder,
			pre,
			post,
			names,
		})
	}
	fn remove_emit<R: Row>(&mut self, row_capacity: usize) -> Result<FFIRowEmit<'_>> {
		let mut builder = self.builder();
		let sink = FFIRowSink::new::<R>(&mut builder, row_capacity)?;
		let names = R::COLUMNS.iter().map(|(n, _)| *n).collect();
		Ok(FFIRowEmit {
			builder,
			sink,
			names,
			kind: EmitKind::Remove,
		})
	}
}
