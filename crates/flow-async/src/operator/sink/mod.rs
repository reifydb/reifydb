// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod partition;
pub mod ringbuffer_view;
pub mod series_view;
pub mod view;

use reifydb_core::{
	common::ChangeVersion,
	interface::{
		catalog::{flow::OperatorId, object::ObjectId, view::View},
		change::{Change, ChangeOrigin, Diff},
		flow::OperatorCapability,
	},
	value::column::{buffer::ColumnBuffer, builder::ColumnBuilder, columns::Columns},
};
use reifydb_flow::error::FlowSinkError;
use reifydb_value::{
	Result,
	error::Error,
	value::{
		Value,
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

pub(crate) fn decode_dictionary_columns(columns: &mut Columns, host: &mut dyn HostContext) -> Result<()> {
	let dict_columns: Vec<(usize, DictionaryId, ValueType)> = {
		let ids: Vec<(usize, DictionaryId)> = columns
			.iter()
			.enumerate()
			.filter_map(|(pos, col)| {
				if let ColumnBuffer::DictionaryId {
					dictionary_id,
					..
				} = col.data()
				{
					Some((pos, (*dictionary_id)?))
				} else {
					None
				}
			})
			.collect();
		ids.into_iter()
			.map(|(pos, id)| {
				let value_type = host.dictionary_value_type(id).ok_or_else(|| {
					Error::from(FlowSinkError::DictionaryNotFound {
						dictionary_id: format!("{:?}", id),
						column: columns.name_at(pos).to_string(),
					})
				})?;
				Ok((pos, id, value_type))
			})
			.collect::<Result<Vec<_>>>()?
	};

	for (col_pos, dictionary, value_type) in &dict_columns {
		let row_count = columns[*col_pos].len();
		let mut new_data = ColumnBuilder::with_capacity(value_type.clone(), row_count);

		for row_idx in 0..row_count {
			let id_value = columns[*col_pos].get_value(row_idx);
			let value = match DictionaryEntryId::from_value(&id_value) {
				Some(entry_id) => host.dictionary_get(*dictionary, entry_id)?.unwrap_or(Value::none()),
				None => Value::none(),
			};
			new_data.push_value(value);
		}

		columns.columns[*col_pos] = new_data.finish();
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_core::{
		actors::pending::Pending, interface::catalog::dictionary::Dictionary, value::column::ColumnWithName,
	};
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::{
		dictionary::{DictionaryAllocatorRegistry, store::SingleDictionaryStore},
		interceptor::interceptors::Interceptors,
	};
	use reifydb_value::{
		fragment::Fragment,
		value::{
			datetime::DateTime, identity::IdentityId, row_number::RowNumber, system_columns::SystemColumns,
			value_type::ValueType,
		},
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
		})
	}

	fn dictionary_column(dictionary: &Dictionary, entry_id: DictionaryEntryId) -> Columns {
		dictionary_column_with_id(dictionary.id, entry_id)
	}

	fn dictionary_column_with_id(dictionary: DictionaryId, entry_id: DictionaryEntryId) -> Columns {
		let mut builder = ColumnBuilder::with_capacity(ValueType::DictionaryId, 1);
		builder.push_value(entry_id.to_value());
		builder.set_dictionary_id(dictionary);
		let buffer = builder.finish();
		Columns::with_system(
			vec![ColumnWithName::new(Fragment::internal("m"), buffer)],
			SystemColumns::new(
				vec![RowNumber(1)],
				Vec::new(),
				vec![DateTime::from_nanos(1)],
				vec![DateTime::from_nanos(1)],
				vec![DateTime::from_nanos(1)],
				Vec::new(),
			),
		)
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
			assert_eq!(columns[0].get_value(0), Value::Utf8("sol".to_string()));
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
			assert_eq!(columns[0].get_value(0), Value::Utf8("sol".to_string()));
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
			columns[0].get_value(0),
			DictionaryEntryId::U2(1).to_value(),
			"the column must be left as-is rather than partly rewritten"
		);
	}
}
