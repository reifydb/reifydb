// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub mod ffi;

use std::ops::Bound;

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
use reifydb_type::value::row_number::RowNumber;
use serde::{Serialize, de::DeserializeOwned};

use crate::{error::Result, operator::column::row::Row, state::StateEntry};

pub trait StateApi {
	fn get<T: DeserializeOwned>(&self, key: &EncodedKey) -> Result<Option<T>>;
	fn set<T: Serialize>(&mut self, key: &EncodedKey, value: &T) -> Result<()>;
	fn remove(&mut self, key: &EncodedKey) -> Result<()>;
	fn contains(&self, key: &EncodedKey) -> Result<bool>;
	fn clear(&mut self) -> Result<()>;
	fn scan_prefix<T: DeserializeOwned>(&self, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, T)>>;
	fn get_many<T: DeserializeOwned>(&self, keys: &[EncodedKey]) -> Result<Vec<(EncodedKey, T)>>;
	fn keys_with_prefix(&self, prefix: &EncodedKey) -> Result<Vec<EncodedKey>>;
	fn range<T: DeserializeOwned>(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Result<Vec<(EncodedKey, T)>>;
	fn get_with_anchors<T: DeserializeOwned>(&self, key: &EncodedKey) -> Result<Option<StateEntry<T>>>;
}

pub trait StoreApi {
	fn get(&self, key: &EncodedKey) -> Result<Option<EncodedRow>>;
	fn contains(&self, key: &EncodedKey) -> Result<bool>;
	fn prefix(&self, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, EncodedRow)>>;
	fn range(&self, start: Bound<&EncodedKey>, end: Bound<&EncodedKey>) -> Result<Vec<(EncodedKey, EncodedRow)>>;
}

pub trait CatalogApi {
	fn find_namespace(&self, namespace: NamespaceId, version: CommitVersion) -> Result<Option<Namespace>>;
	fn find_namespace_by_name(&self, namespace: &str, version: CommitVersion) -> Result<Option<Namespace>>;
	fn find_table(&self, table: TableId, version: CommitVersion) -> Result<Option<Table>>;
	fn find_table_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Result<Option<Table>>;
	fn find_row_shape(&self, fingerprint: RowShapeFingerprint) -> Result<Option<RowShape>>;
}

/// The surface an operator's business logic codes against, abstracting the FFI vs native boundary.
/// Emit is expressed in terms of typed `Row`s so the same operator code drives either backend: the
/// FFI impl writes through the zero-copy column builder, the native impl accumulates owned `Columns`.
pub trait OperatorContext {
	fn operator_id(&self) -> FlowNodeId;
	fn clock_now_nanos(&self) -> u64;
	fn state(&mut self) -> impl StateApi + '_;
	fn store(&mut self) -> impl StoreApi + '_;
	fn catalog(&mut self) -> impl CatalogApi + '_;
	fn get_or_create_row_number(&mut self, key: &EncodedKey) -> Result<(RowNumber, bool)>;
	fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>>;
	fn shape_for_row(&mut self, row: &EncodedRow) -> Result<RowShape>;
	fn emit_insert<R: Row>(&mut self, rows: &[R], row_numbers: &[RowNumber]) -> Result<()>;
	fn emit_update<R: Row>(&mut self, pre: &[R], post: &[R], row_numbers: &[RowNumber]) -> Result<()>;
	fn emit_remove<R: Row>(&mut self, rows: &[R], row_numbers: &[RowNumber]) -> Result<()>;
}
