// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
mod partition;
pub mod ringbuffer_view;
pub mod series_view;
pub mod view;

use reifydb_core::interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability};
use reifydb_value::Result;

use crate::{timer::Timer, transaction::deferred::DeferredTransaction};

pub trait DurableSink: Send {
	fn id(&self) -> OperatorId;

	fn capabilities(&self) -> &[OperatorCapability];

	fn apply(&mut self, txn: &mut DeferredTransaction, change: Change) -> Result<Change>;

	fn on_timer(&mut self, _txn: &mut DeferredTransaction, _timer: Timer) -> Result<Option<Change>> {
		Ok(None)
	}
}

pub type BoxedDurableSink = Box<dyn DurableSink>;

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
	use reifydb_codec::row::bytes::EncodedBytes;
	use reifydb_core::{
		actors::pending::{Pending, PendingWrite},
		common::{ChangeVersion, CommitVersion},
		interface::{
			catalog::{
				column::{Column, ColumnIndex},
				dictionary::Dictionary,
				flow::OperatorId,
				id::{ColumnId, NamespaceId, ViewId},
				view::{TableView, View, ViewKind},
			},
			change::{Change, Diff},
		},
		value::{batch::batch, column::builder::ColumnBuilder},
	};
	use reifydb_flow::operator::{sink::table::TableSink, source::decode_dictionary_columns};
	use reifydb_runtime::context::{
		RuntimeContext,
		clock::{Clock, MockClock},
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::{
		dictionary::{DictionaryAllocatorRegistry, store::SingleDictionaryStore},
		interceptor::interceptors::Interceptors,
	};
	use reifydb_value::value::{
		Value,
		column_view::ColumnView,
		constraint::{Constraint, TypeConstraint},
		container::temporal_array::datetime_array,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
		identity::IdentityId,
		system_columns::{SystemColumn, with_system_column},
		value_type::ValueType,
	};

	use super::DurableSink;
	use crate::{
		operator::host::TxnHostContext,
		transaction::{DeferredParams, deferred::DeferredTransaction, substrate::FlowSubstrate},
	};

	fn flow_txn(engine: &TestEngine, registry: &DictionaryAllocatorRegistry) -> DeferredTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		DeferredTransaction::new(DeferredParams {
			version,
			pending: Pending::new(),
			query: Some(parent.multi.begin_query().unwrap()),
			state_query: Some(parent.multi.begin_query().unwrap()),
			catalog: engine.inner().catalog().clone(),
			interceptors: Interceptors::new(),
			clock: Clock::Mock(MockClock::from_millis(0)),
			substrate: FlowSubstrate::with_dictionary(registry.clone(), engine.inner().operator_state()),
			lookup: None,
		})
	}

	fn dictionary_column(dictionary: &Dictionary, entry_id: DictionaryEntryId) -> RecordBatch {
		dictionary_column_with_id(dictionary.id, entry_id)
	}

	fn dictionary_column_with_id(dictionary: DictionaryId, entry_id: DictionaryEntryId) -> RecordBatch {
		dictionary_columns(&[("m", dictionary, entry_id)])
	}

	fn dictionary_columns(columns: &[(&str, DictionaryId, DictionaryEntryId)]) -> RecordBatch {
		let user = batch(columns
			.iter()
			.map(|&(name, dictionary, entry_id)| {
				let mut builder = ColumnBuilder::with_capacity(ValueType::DictionaryId, 1);
				builder.push_value(entry_id.to_value());
				builder.set_dictionary_id(dictionary);
				builder.finish(name)
			})
			.collect())
		.unwrap();
		let stamp = || -> ArrayRef { Arc::new(datetime_array([DateTime::from_nanos(1)])) };
		let system: [(SystemColumn, ArrayRef); 4] = [
			(SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![1u64]))),
			(SystemColumn::CreatedAt, stamp()),
			(SystemColumn::UpdatedAt, stamp()),
			(SystemColumn::Time, stamp()),
		];
		system.into_iter()
			.fold(user, |columns, (column, array)| with_system_column(columns, column, array).unwrap())
	}

	fn first_value(columns: &RecordBatch) -> Value {
		ColumnView::try_from((columns.column(0), columns.schema_ref().field(0))).unwrap().get_value(0)
	}

	#[test]
	fn dictionary_decode_is_served_from_the_cache_across_transactions() {
		// Dictionary decode runs per output row on every sink apply, so only the first decode of
		// an id may read the store; a repeat in a LATER transaction must come from the shared
		// cache. A wrong value means the cache aliased ids or served stale bytes.
		let engine = TestEngine::new();
		engine.admin("CREATE NAMESPACE test");
		engine.admin("CREATE DICTIONARY test::syms FOR utf8 AS uint2");
		let catalog = engine.inner().catalog();
		let namespace = catalog.cache().find_namespace_by_name("test").expect("namespace");
		let dictionary =
			catalog.cache().find_dictionary_by_name(namespace.id(), "syms").expect("dictionary syms");

		let single = engine.begin_admin(IdentityId::system()).unwrap().single.clone();

		let entry_id = {
			let registry =
				DictionaryAllocatorRegistry::new(Arc::new(SingleDictionaryStore::new(single.clone())));
			registry.intern_batch(&dictionary, &[Value::Utf8("sol".to_string())]).unwrap().remove(0).id
		};

		let decode_store = Arc::new(SingleDictionaryStore::new(single));
		let decode_registry = DictionaryAllocatorRegistry::new(decode_store.clone());
		{
			let mut txn = flow_txn(&engine, &decode_registry);
			let mut columns = dictionary_column(&dictionary, entry_id);
			let before = decode_store.read_count();
			decode_dictionary_columns(&mut columns, &mut TxnHostContext::new(&mut txn, OperatorId(1)))
				.unwrap();
			assert_eq!(
				decode_store.read_count() - before,
				1,
				"a cold decode resolves through exactly one committed-store read"
			);
			assert_eq!(first_value(&columns), Value::Utf8("sol".to_string()));
		}

		{
			let mut txn = flow_txn(&engine, &decode_registry);
			let mut columns = dictionary_column(&dictionary, entry_id);
			let before = decode_store.read_count();
			decode_dictionary_columns(&mut columns, &mut TxnHostContext::new(&mut txn, OperatorId(1)))
				.unwrap();
			assert_eq!(
				decode_store.read_count() - before,
				0,
				"a repeat decode in a later transaction must be served from the registry cache"
			);
			assert_eq!(first_value(&columns), Value::Utf8("sol".to_string()));
		}
	}

	#[test]
	fn a_dictionary_column_whose_dictionary_is_gone_fails_the_decode() {
		// An unresolvable dictionary must fail the decode; skipping the column emits raw internal ids as user
		// values.
		let engine = TestEngine::new();
		let single = engine.begin_admin(IdentityId::system()).unwrap().single.clone();
		let registry = DictionaryAllocatorRegistry::new(Arc::new(SingleDictionaryStore::new(single)));
		let mut txn = flow_txn(&engine, &registry);

		let mut columns = dictionary_column_with_id(DictionaryId(9999), DictionaryEntryId::U2(1));
		let err = decode_dictionary_columns(&mut columns, &mut TxnHostContext::new(&mut txn, OperatorId(1)))
			.expect_err("a dictionary missing from the catalog must fail the decode");

		assert_eq!(err.code, "FLOW_037", "expected FLOW_037, got {:?}: {}", err.code, err.message);
		assert!(err.message.contains("9999"), "the error must name the missing dictionary: {}", err.message);
		assert_eq!(
			first_value(&columns),
			DictionaryEntryId::U2(1).to_value(),
			"the column must be left as-is rather than partly rewritten"
		);
	}

	fn dictionary_sink(dictionary: &Dictionary) -> TableSink {
		dictionary_sink_of(&[("m", dictionary)])
	}

	fn dictionary_sink_of(columns: &[(&str, &Dictionary)]) -> TableSink {
		let view = View::Table(TableView {
			id: ViewId(1),
			namespace: NamespaceId(1),
			name: "v".to_string(),
			kind: ViewKind::Deferred,
			columns: columns
				.iter()
				.enumerate()
				.map(|(index, &(name, dictionary))| Column {
					id: ColumnId(index as u64 + 1),
					name: name.to_string(),
					constraint: TypeConstraint::with_constraint(
						ValueType::Utf8,
						Constraint::Dictionary(dictionary.id, dictionary.id_type.clone()),
					),
					properties: vec![],
					index: ColumnIndex(index as u8),
					auto_increment: false,
					dictionary_id: Some(dictionary.id),
				})
				.collect(),
			primary_key: None,
			partition_by: vec![],
			sort: vec![],
		});
		TableSink::new(OperatorId(1), view, RuntimeContext::with_clock(Clock::Mock(MockClock::from_millis(0))))
	}

	fn insert(columns: RecordBatch) -> Change {
		Change::from_flow(
			OperatorId(1),
			ChangeVersion::from(CommitVersion(1)),
			vec![Diff::insert(columns)],
			DateTime::from_nanos(0),
		)
	}

	fn stored(txn: &DeferredTransaction) -> Vec<EncodedBytes> {
		txn.pending
			.rows_ordered()
			.filter_map(|(_, write)| match write {
				PendingWrite::Set(bytes) => Some(bytes.clone()),
				PendingWrite::Remove {
					..
				} => None,
			})
			.collect()
	}

	fn syms(engine: &TestEngine, name: &str) -> Dictionary {
		engine.admin(&format!("CREATE DICTIONARY test::{name} FOR utf8 AS uint2"));
		let catalog = engine.inner().catalog();
		let namespace = catalog.cache().find_namespace_by_name("test").expect("namespace");
		catalog.cache().find_dictionary_by_name(namespace.id(), name).expect("dictionary")
	}

	fn interned(engine: &TestEngine, dictionary: &Dictionary, values: &[&str]) -> Vec<DictionaryEntryId> {
		let single = engine.begin_admin(IdentityId::system()).unwrap().single.clone();
		let registry = DictionaryAllocatorRegistry::new(Arc::new(SingleDictionaryStore::new(single)));
		let values: Vec<Value> = values.iter().map(|value| Value::Utf8(value.to_string())).collect();
		registry.intern_batch(dictionary, &values).unwrap().into_iter().map(|entry| entry.id).collect()
	}

	#[test]
	fn a_view_sink_stores_entry_ids_of_its_own_dictionary_without_a_dictionary_read() {
		// Decoding ids the target column stores as they are pays a dictionary read and a re-intern per column.
		let engine = TestEngine::new();
		engine.admin("CREATE NAMESPACE test");
		let dictionary = syms(&engine, "syms");
		let sol = interned(&engine, &dictionary, &["sol"])[0];

		let single = engine.begin_admin(IdentityId::system()).unwrap().single.clone();
		let store = Arc::new(SingleDictionaryStore::new(single));
		let mut txn = flow_txn(&engine, &DictionaryAllocatorRegistry::new(store.clone()));
		let before = store.read_count();
		DurableSink::apply(
			&mut dictionary_sink(&dictionary),
			&mut txn,
			insert(dictionary_column(&dictionary, sol)),
		)
		.unwrap();

		assert_eq!(store.read_count() - before, 0, "an id of the column's own dictionary is stored as it came");
		assert_eq!(stored(&txn).len(), 1, "the row is stored");
	}

	#[test]
	fn a_view_sink_stores_an_entry_id_of_another_dictionary_as_its_own_id_for_the_same_value() {
		// Ids carry no dictionary in their bytes, so storing a foreign id as is names another value in the
		// target.
		let engine = TestEngine::new();
		engine.admin("CREATE NAMESPACE test");
		let source = syms(&engine, "source");
		let target = syms(&engine, "target");
		let foreign = interned(&engine, &source, &["usdc", "sol"])[1];
		let own = interned(&engine, &target, &["sol"])[0];
		assert_ne!(
			foreign.to_u128(),
			own.to_u128(),
			"precondition: the two dictionaries give sol different ids"
		);

		let registry = DictionaryAllocatorRegistry::new(Arc::new(SingleDictionaryStore::new(
			engine.begin_admin(IdentityId::system()).unwrap().single.clone(),
		)));
		let mut from_foreign = flow_txn(&engine, &registry);
		DurableSink::apply(
			&mut dictionary_sink(&target),
			&mut from_foreign,
			insert(dictionary_column(&source, foreign)),
		)
		.unwrap();
		let mut from_own = flow_txn(&engine, &registry);
		DurableSink::apply(
			&mut dictionary_sink(&target),
			&mut from_own,
			insert(dictionary_column(&target, own)),
		)
		.unwrap();

		assert_eq!(stored(&from_foreign), stored(&from_own), "sol is stored under the target's id either way");
	}

	#[test]
	fn a_view_sink_keeps_ids_only_for_the_column_of_the_same_name_and_dictionary() {
		// A column matched to another view column's dictionary would store its foreign ids under the wrong one.
		let engine = TestEngine::new();
		engine.admin("CREATE NAMESPACE test");
		let source = syms(&engine, "source");
		let target = syms(&engine, "target");
		let foreign = interned(&engine, &source, &["usdc", "sol"])[1];
		let own = interned(&engine, &target, &["sol"])[0];
		assert_ne!(
			foreign.to_u128(),
			own.to_u128(),
			"precondition: the two dictionaries give sol different ids"
		);
		let view = [("m", &target), ("n", &source)];

		let registry = DictionaryAllocatorRegistry::new(Arc::new(SingleDictionaryStore::new(
			engine.begin_admin(IdentityId::system()).unwrap().single.clone(),
		)));
		let mut from_foreign = flow_txn(&engine, &registry);
		let foreign_m = dictionary_columns(&[("m", source.id, foreign), ("n", source.id, foreign)]);
		DurableSink::apply(&mut dictionary_sink_of(&view), &mut from_foreign, insert(foreign_m)).unwrap();
		let mut from_own = flow_txn(&engine, &registry);
		let own_m = dictionary_columns(&[("m", target.id, own), ("n", source.id, foreign)]);
		DurableSink::apply(&mut dictionary_sink_of(&view), &mut from_own, insert(own_m)).unwrap();

		assert_eq!(stored(&from_foreign), stored(&from_own), "m stores sol under the target's id either way");
	}
}
