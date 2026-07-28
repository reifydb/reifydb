// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod ffi;

use std::{ops::Bound, slice::from_ref};

use reifydb_codec::{
	encoded::{
		row::EncodedRow,
		shape::{RowShape, fingerprint::RowShapeFingerprint},
	},
	key::encoded::EncodedKey,
	state::{OperatorState, StateBytes},
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		flow::FlowNodeId,
		id::{NamespaceId, TableId},
		namespace::Namespace,
		table::Table,
	},
	key::operator_state::{GroupId, StateKey},
};
use reifydb_value::value::{
	Value,
	datetime::DateTime,
	dictionary::{DictionaryEntryId, DictionaryId},
	row_number::RowNumber,
};

use crate::{
	error::Result,
	operator::column::{row::Row, sink::RowSink},
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
	fn get<T: OperatorState>(&self, key: &StateKey) -> Result<Option<T>>;
	fn set<T: OperatorState>(&mut self, key: &StateKey, value: &T) -> Result<()>;
	fn remove(&mut self, key: &StateKey) -> Result<()>;
	fn contains(&self, key: &StateKey) -> Result<bool>;
	fn clear(&mut self) -> Result<()>;
	fn scan_prefix<T: OperatorState>(&self, prefix: &StateKey) -> Result<Vec<(StateKey, T)>>;
	fn get_many<T: OperatorState>(&self, keys: &[StateKey]) -> Result<Vec<(StateKey, T)>>;
	fn keys_with_prefix(&self, prefix: &StateKey) -> Result<Vec<StateKey>>;
	fn range<T: OperatorState>(&self, start: Bound<&StateKey>, end: Bound<&StateKey>)
	-> Result<Vec<(StateKey, T)>>;
	fn get_many_visit<T: OperatorState>(
		&self,
		keys: &[StateKey],
		visit: &mut dyn FnMut(StateKey, T) -> Result<()>,
	) -> Result<()> {
		for (k, v) in self.get_many::<T>(keys)? {
			visit(k, v)?;
		}
		Ok(())
	}

	fn range_visit<T: OperatorState>(
		&self,
		start: Bound<&StateKey>,
		end: Bound<&StateKey>,
		visit: &mut dyn FnMut(StateKey, T) -> Result<()>,
	) -> Result<()> {
		for (k, v) in self.range::<T>(start, end)? {
			visit(k, v)?;
		}
		Ok(())
	}

	fn scan_prefix_visit<T: OperatorState>(
		&self,
		prefix: &StateKey,
		visit: &mut dyn FnMut(StateKey, T) -> Result<()>,
	) -> Result<()> {
		for (k, v) in self.scan_prefix::<T>(prefix)? {
			visit(k, v)?;
		}
		Ok(())
	}

	fn get_bytes(&self, key: &StateKey) -> Result<Option<StateBytes>>;

	fn set_bytes(&mut self, key: &StateKey, payload: StateBytes) -> Result<()>;

	fn get_many_bytes_visit(
		&self,
		keys: &[StateKey],
		visit: &mut dyn FnMut(StateKey, StateBytes) -> Result<()>,
	) -> Result<()>;

	fn range_bytes_visit(
		&self,
		start: Bound<&StateKey>,
		end: Bound<&StateKey>,
		visit: &mut dyn FnMut(StateKey, StateBytes) -> Result<()>,
	) -> Result<()>;

	fn now(&self) -> DateTime;
}

/// Reads the data store - table and view rows addressed by `RowKey`. That is a different keyspace
/// from operator state, so these keys are plain `EncodedKey` and never `StateKey`.
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
	fn clock_now(&self) -> DateTime;
	fn state_lease_bytes(&self) -> u64 {
		0
	}
	fn state(&mut self) -> impl StateApi + '_;
	fn store(&mut self) -> impl StoreApi + '_;
	fn catalog(&mut self) -> impl CatalogApi + '_;
	fn dictionary(&mut self) -> impl DictionaryApi + '_;
	fn intern_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<GroupId>>;
	fn intern_group(&mut self, group: &EncodedKey) -> Result<GroupId> {
		Ok(self.intern_groups(from_ref(group))?.remove(0))
	}
	fn lookup_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>>;
	fn lookup_group(&mut self, group: &EncodedKey) -> Result<Option<GroupId>> {
		Ok(self.lookup_groups(from_ref(group))?.remove(0))
	}
	fn get_or_create_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<(RowNumber, bool)>;
	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>>;
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()>;
	fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> Result<Vec<RowNumber>>;
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
