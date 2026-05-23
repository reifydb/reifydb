// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_abi::context::context::ContextFFI;
use reifydb_core::{
	common::CommitVersion,
	encoded::{
		key::EncodedKey,
		row::EncodedRow,
		shape::{RowShape, fingerprint::RowShapeFingerprint},
	},
	interface::catalog::{
		flow::FlowNodeId,
		id::{NamespaceId, TableId},
		namespace::Namespace,
		table::Table,
	},
};
use reifydb_type::{
	params::Params,
	value::{frame::frame::Frame, row_number::RowNumber},
};
use serde::{Serialize, de::DeserializeOwned};

use super::{CatalogApi, OperatorContext, StateApi, StoreApi};
use crate::{
	catalog::Catalog,
	error::{FFIError, Result},
	operator::{
		builder::ColumnsBuilder,
		column::{row::Row, sink::ffi::FFIRowSink},
		diff::DiffStart,
	},
	rql::raw_query,
	state::{InternalState, State, StateEntry, row::RowNumberProvider},
	store::Store,
};

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

	pub fn shape_for_row(&mut self, row: &EncodedRow) -> Result<RowShape> {
		let fingerprint = row.fingerprint();
		match self.catalog().find_row_shape(fingerprint)? {
			Some(shape) => Ok(shape),
			None => Err(FFIError::Other(format!(
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

impl OperatorContext for FFIOperatorContext {
	fn operator_id(&self) -> FlowNodeId {
		FFIOperatorContext::operator_id(self)
	}
	fn clock_now_nanos(&self) -> u64 {
		unsafe { (*self.ctx).clock_now_nanos }
	}
	fn state(&mut self) -> impl StateApi + '_ {
		FFIOperatorContext::state(self)
	}
	fn store(&mut self) -> impl StoreApi + '_ {
		FFIOperatorContext::store(self)
	}
	fn catalog(&mut self) -> impl CatalogApi + '_ {
		FFIOperatorContext::catalog(self)
	}
	fn get_or_create_row_number(&mut self, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		FFIOperatorContext::get_or_create_row_number(self, key)
	}
	fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		FFIOperatorContext::get_or_create_row_numbers(self, keys)
	}
	fn shape_for_row(&mut self, row: &EncodedRow) -> Result<RowShape> {
		FFIOperatorContext::shape_for_row(self, row)
	}
	fn emit_insert<R: Row>(&mut self, rows: &[R], row_numbers: &[RowNumber]) -> Result<()> {
		if rows.is_empty() {
			return Ok(());
		}
		let mut builder = self.builder();
		let mut sink = FFIRowSink::new::<R>(&mut builder, rows.len())?;
		for row in rows {
			row.encode_into(&mut sink)?;
		}
		let columns = sink.finish_all()?;
		let names: Vec<&str> = R::COLUMNS.iter().map(|(n, _)| *n).collect();
		builder.emit_insert(&columns, &names, row_numbers)
	}
	fn emit_update<R: Row>(&mut self, pre: &[R], post: &[R], row_numbers: &[RowNumber]) -> Result<()> {
		if row_numbers.is_empty() {
			return Ok(());
		}
		let mut builder = self.builder();
		let mut pre_sink = FFIRowSink::new::<R>(&mut builder, pre.len())?;
		let mut post_sink = FFIRowSink::new::<R>(&mut builder, post.len())?;
		for row in pre {
			row.encode_into(&mut pre_sink)?;
		}
		for row in post {
			row.encode_into(&mut post_sink)?;
		}
		let pre_columns = pre_sink.finish_all()?;
		let post_columns = post_sink.finish_all()?;
		let names: Vec<&str> = R::COLUMNS.iter().map(|(n, _)| *n).collect();
		builder.emit_update(
			&pre_columns,
			&names,
			pre.len(),
			row_numbers,
			&post_columns,
			&names,
			post.len(),
			row_numbers,
		)
	}
	fn emit_remove<R: Row>(&mut self, rows: &[R], row_numbers: &[RowNumber]) -> Result<()> {
		if rows.is_empty() {
			return Ok(());
		}
		let mut builder = self.builder();
		let mut sink = FFIRowSink::new::<R>(&mut builder, rows.len())?;
		for row in rows {
			row.encode_into(&mut sink)?;
		}
		let columns = sink.finish_all()?;
		let names: Vec<&str> = R::COLUMNS.iter().map(|(n, _)| *n).collect();
		builder.emit_remove(&columns, &names, row_numbers)
	}
}
