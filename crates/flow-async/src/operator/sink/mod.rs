// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod partition;
pub mod ringbuffer_view;
pub mod series_view;
pub mod view;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::{
	common::ChangeVersion,
	interface::{
		catalog::{flow::OperatorId, object::ObjectId, view::View},
		change::{Change, ChangeOrigin, Diff},
		flow::OperatorCapability,
	},
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_flow::error::FlowSinkError;
use reifydb_value::{
	Result,
	error::Error,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		dictionary::{DictionaryEntryId, DictionaryId},
		value_type::ValueType,
	},
};
use smallvec::smallvec;

use crate::{
	operator::host::HostContext,
	timer::Timer,
	transaction::{FlowTransaction, deferred::DeferredTransaction},
};

pub trait DurableSink: Send {
	fn id(&self) -> OperatorId;

	fn capabilities(&self) -> &[OperatorCapability];

	fn apply(&mut self, txn: &mut DeferredTransaction, change: Change) -> Result<Change>;

	fn on_timer(&mut self, _txn: &mut DeferredTransaction, _timer: Timer) -> Result<Option<Change>> {
		Ok(None)
	}
}

pub type BoxedDurableSink = Box<dyn DurableSink>;

pub(crate) fn emit_view_change(txn: &mut DeferredTransaction, view: &View, diff: Diff) {
	let version = ChangeVersion::from(txn.version());
	let changed_at = txn.clock().now();
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Object(ObjectId::view(view.id())),
		version,
		diffs: smallvec![diff],
		changed_at,
	});
}

pub(crate) fn decode_dictionary_columns(columns: &mut RecordBatch, host: &mut dyn HostContext) -> Result<()> {
	let dict_columns: Vec<(usize, DictionaryId, ValueType)> = {
		let mut ids: Vec<(usize, DictionaryId)> = Vec::new();
		for (pos, (field, array)) in columns.schema_ref().fields().iter().zip(columns.columns()).enumerate() {
			if let ViewData::DictionaryId {
				dictionary_id: Some(id),
				..
			} = ColumnView::try_from((array, field.as_ref()))?.data
			{
				ids.push((pos, id));
			}
		}
		ids.into_iter()
			.map(|(pos, id)| {
				let value_type = host.dictionary_value_type(id).ok_or_else(|| {
					Error::from(FlowSinkError::DictionaryNotFound {
						dictionary_id: format!("{:?}", id),
						column: columns.schema_ref().field(pos).name().clone(),
					})
				})?;
				Ok((pos, id, value_type))
			})
			.collect::<Result<Vec<_>>>()?
	};

	if dict_columns.is_empty() {
		return Ok(());
	}

	let mut decoded: Vec<(FieldRef, ArrayRef)> =
		columns.schema_ref().fields().iter().cloned().zip(columns.columns().iter().cloned()).collect();
	for (col_pos, dictionary, value_type) in &dict_columns {
		let (field, array) = &decoded[*col_pos];
		let column = ColumnView::try_from((array, field.as_ref()))?;
		let row_count = column.len();
		let mut new_data = ColumnBuilder::with_capacity(value_type.clone(), row_count);

		for row_idx in 0..row_count {
			let id_value = column.get_value(row_idx);
			let value = match DictionaryEntryId::from_value(&id_value) {
				Some(entry_id) => host.dictionary_get(*dictionary, entry_id)?.unwrap_or(Value::none()),
				None => Value::none(),
			};
			new_data.push_value(value);
		}

		let name = field.name().clone();
		decoded[*col_pos] = new_data.finish(&name);
	}

	*columns = batch(decoded)?;
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::UInt64Array;
	use reifydb_core::{actors::pending::Pending, interface::catalog::dictionary::Dictionary};
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::{
		dictionary::{DictionaryAllocatorRegistry, store::SingleDictionaryStore},
		interceptor::interceptors::Interceptors,
	};
	use reifydb_value::value::{
		container::temporal_array::datetime_array,
		datetime::DateTime,
		identity::IdentityId,
		system_columns::{SystemColumn, with_system_column},
		value_type::ValueType,
	};

	use super::*;
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
		let mut builder = ColumnBuilder::with_capacity(ValueType::DictionaryId, 1);
		builder.push_value(entry_id.to_value());
		builder.set_dictionary_id(dictionary);
		let user = batch(vec![builder.finish("m")]).unwrap();
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
			registry.intern(&dictionary, &Value::Utf8("sol".to_string())).unwrap().id
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
}
