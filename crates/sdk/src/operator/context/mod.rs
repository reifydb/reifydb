// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod ffi;

use std::ops::Bound;

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
use reifydb_value::value::{
	Value,
	dictionary::{DictionaryEntryId, DictionaryId},
	row_number::RowNumber,
};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
	error::Result,
	operator::column::{row::Row, sink::RowSink},
	state::StateEntry,
};

pub trait RowEmit {
	type Sink: RowSink;
	fn sink(&mut self) -> &mut Self::Sink;
	fn finish(self, row_numbers: &[RowNumber]) -> Result<()>;
}

pub trait UpdateEmit {
	type Sink: RowSink;
	fn pre(&mut self) -> &mut Self::Sink;
	fn post(&mut self) -> &mut Self::Sink;
	fn finish(self, row_numbers: &[RowNumber]) -> Result<()>;
}

pub trait StateApi {
	fn get<T: DeserializeOwned>(&self, key: &EncodedKey) -> Result<Option<T>>;
	fn set<T: Serialize>(&mut self, key: &EncodedKey, value: &T) -> Result<()>;
	fn remove(&mut self, key: &EncodedKey) -> Result<()>;
	fn drop(&mut self, key: &EncodedKey) -> Result<()>;
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

	fn get_many_visit<T: DeserializeOwned>(
		&self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, T) -> Result<()>,
	) -> Result<()> {
		for (k, v) in self.get_many::<T>(keys)? {
			visit(k, v)?;
		}
		Ok(())
	}

	fn range_visit<T: DeserializeOwned>(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		visit: &mut dyn FnMut(EncodedKey, T) -> Result<()>,
	) -> Result<()> {
		for (k, v) in self.range::<T>(start, end)? {
			visit(k, v)?;
		}
		Ok(())
	}

	fn scan_prefix_visit<T: DeserializeOwned>(
		&self,
		prefix: &EncodedKey,
		visit: &mut dyn FnMut(EncodedKey, T) -> Result<()>,
	) -> Result<()> {
		for (k, v) in self.scan_prefix::<T>(prefix)? {
			visit(k, v)?;
		}
		Ok(())
	}
}

pub trait InternalStateApi {
	fn get<T: DeserializeOwned>(&self, key: &EncodedKey) -> Result<Option<T>>;
	fn get_many<T: DeserializeOwned>(&self, keys: &[EncodedKey]) -> Result<Vec<(EncodedKey, T)>>;
	fn set<T: Serialize>(&mut self, key: &EncodedKey, value: &T) -> Result<()>;
	fn remove(&mut self, key: &EncodedKey) -> Result<()>;
	fn drop(&mut self, key: &EncodedKey) -> Result<()>;
	fn contains(&self, key: &EncodedKey) -> Result<bool>;
	fn range<T: DeserializeOwned>(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Result<Vec<(EncodedKey, T)>>;

	fn get_many_visit<T: DeserializeOwned>(
		&self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, T) -> Result<()>,
	) -> Result<()> {
		for (k, v) in self.get_many::<T>(keys)? {
			visit(k, v)?;
		}
		Ok(())
	}

	fn range_visit<T: DeserializeOwned>(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		visit: &mut dyn FnMut(EncodedKey, T) -> Result<()>,
	) -> Result<()> {
		for (k, v) in self.range::<T>(start, end)? {
			visit(k, v)?;
		}
		Ok(())
	}
}

pub trait StoreApi {
	fn get(&self, key: &EncodedKey) -> Result<Option<EncodedRow>>;
	fn contains(&self, key: &EncodedKey) -> Result<bool>;
	fn prefix(&self, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, EncodedRow)>>;
	fn range(&self, start: Bound<&EncodedKey>, end: Bound<&EncodedKey>) -> Result<Vec<(EncodedKey, EncodedRow)>>;

	fn range_visit(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		visit: &mut dyn FnMut(EncodedKey, EncodedRow) -> Result<()>,
	) -> Result<()> {
		for (k, v) in self.range(start, end)? {
			visit(k, v)?;
		}
		Ok(())
	}

	fn prefix_visit(
		&self,
		prefix: &EncodedKey,
		visit: &mut dyn FnMut(EncodedKey, EncodedRow) -> Result<()>,
	) -> Result<()> {
		for (k, v) in self.prefix(prefix)? {
			visit(k, v)?;
		}
		Ok(())
	}
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

pub trait DictionaryApi {
	fn id_by_name(&mut self, name: &str) -> Result<Option<DictionaryId>>;
	fn find(&mut self, dictionary: DictionaryId, value: &Value) -> Result<Option<DictionaryEntryId>>;
	fn get(&mut self, dictionary: DictionaryId, id: DictionaryEntryId) -> Result<Option<Value>>;
}

pub trait OperatorContext {
	type InsertEmit<'a>: RowEmit
	where
		Self: 'a;
	type UpdateEmit<'a>: UpdateEmit
	where
		Self: 'a;
	type RemoveEmit<'a>: RowEmit
	where
		Self: 'a;

	fn operator_id(&self) -> FlowNodeId;
	fn clock_now_nanos(&self) -> u64;
	fn state(&mut self) -> impl StateApi + '_;
	fn internal_state(&mut self) -> impl InternalStateApi + '_;
	fn store(&mut self) -> impl StoreApi + '_;
	fn catalog(&mut self) -> impl CatalogApi + '_;
	fn dictionary(&mut self) -> impl DictionaryApi + '_;
	fn get_or_create_row_number(&mut self, key: &EncodedKey) -> Result<(RowNumber, bool)>;
	fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>>;
	/// Reserve `count` fresh, globally-unique output row numbers for this operator and return the start
	/// of the `[start, start + count)` range. Backed by the host's process-shared in-memory allocator so
	/// it is immune to the committing transaction's MVCC snapshot (unlike a counter read from the store).
	fn allocate_row_numbers(&mut self, count: u64) -> Result<RowNumber>;
	fn shape_for_row(&mut self, row: &EncodedRow) -> Result<RowShape>;

	fn insert_emit<R: Row>(&mut self, row_capacity: usize) -> Result<Self::InsertEmit<'_>>;
	fn update_emit<R: Row>(&mut self, row_capacity: usize) -> Result<Self::UpdateEmit<'_>>;
	fn remove_emit<R: Row>(&mut self, row_capacity: usize) -> Result<Self::RemoveEmit<'_>>;

	fn emit_insert<R: Row>(&mut self, rows: &[R], row_numbers: &[RowNumber]) -> Result<()> {
		if rows.is_empty() {
			return Ok(());
		}
		let mut emit = self.insert_emit::<R>(rows.len())?;
		for row in rows {
			row.encode_into(emit.sink())?;
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
