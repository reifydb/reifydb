// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod partition;
pub mod ringbuffer_view;
pub mod series_view;
pub mod view;

use std::sync::LazyLock;

use postcard::from_bytes;
use reifydb_codec::encoded::{row::EncodedRow, shape::RowShape};
use reifydb_core::{
	interface::{
		catalog::{
			column::Column as CatalogColumn,
			dictionary::Dictionary,
			property::{ColumnPropertyKind, ColumnSaturationStrategy},
		},
		evaluate::TargetColumn,
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, cast::cast_column_data, columns::Columns},
};
use reifydb_engine::{expression::context::EvalContext, vm::stack::SymbolTable};
use reifydb_routine::routine::registry::Routines;
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	params::Params,
	util::cowvec::CowVec,
	value::{Value, dictionary::DictionaryEntryId, identity::IdentityId, row_number::RowNumber},
};

use crate::{error::FlowSinkError, transaction::FlowTransaction};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);
static EMPTY_ROUTINES: LazyLock<Routines> = LazyLock::new(Routines::empty);
static DEFAULT_RUNTIME_CONTEXT: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::with_clock(Clock::Real));

pub(crate) fn coerce_columns(columns: &Columns, target_columns: &[CatalogColumn]) -> Result<Columns> {
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(Columns::empty());
	}

	if target_columns.is_empty() {
		return Ok(columns.clone());
	}

	if columns.len() == target_columns.len()
		&& target_columns.iter().enumerate().all(|(i, target_col)| {
			columns.name_at(i).text() == target_col.name.as_str()
				&& columns.data_at(i).get_type() == target_col.constraint.get_type()
		}) {
		return Ok(columns.clone());
	}

	let mut result_columns = Vec::with_capacity(target_columns.len());

	// FIXME how to handle failing views ?!
	let session = EvalContext {
		params: &EMPTY_PARAMS,
		symbols: &EMPTY_SYMBOL_TABLE,
		routines: &EMPTY_ROUTINES,
		runtime_context: &DEFAULT_RUNTIME_CONTEXT,
		arena: None,
		identity: IdentityId::root(),
		is_aggregate_context: false,
		columns: Columns::empty(),
		row_count: 1,
		target: None,
		take: None,
	};
	let mut ctx = session.with_eval(columns.clone(), row_count);

	for target_col in target_columns {
		let target_type = target_col.constraint.get_type();

		ctx.target = Some(TargetColumn::Partial {
			source_name: None,
			column_name: Some(target_col.name.clone()),
			column_type: target_type.clone(),
			properties: vec![ColumnPropertyKind::Saturation(ColumnSaturationStrategy::None)],
		});

		if let Some(source_col) = columns.column(&target_col.name) {
			let casted = cast_column_data(
				&ctx,
				source_col.data(),
				target_type.clone(),
				Fragment::internal(&target_col.name),
			)?;
			result_columns.push(ColumnWithName::new(Fragment::internal(&target_col.name), casted));
		} else {
			result_columns.push(ColumnWithName::undefined_typed(
				Fragment::internal(&target_col.name),
				target_type,
				row_count,
			))
		}
	}

	let mut names_vec = Vec::with_capacity(result_columns.len());
	let mut buffers_vec = Vec::with_capacity(result_columns.len());
	for c in result_columns {
		names_vec.push(c.name);
		buffers_vec.push(c.data);
	}
	Ok(Columns {
		row_numbers: columns.row_numbers.clone(),
		partitions: columns.partitions.clone(),
		created_at: columns.created_at.clone(),
		updated_at: columns.updated_at.clone(),
		columns: CowVec::new(buffers_vec),
		names: CowVec::new(names_vec),
	})
}

pub(crate) fn shape_field_columns(columns: &Columns, shape: &RowShape) -> Vec<usize> {
	shape.field_names()
		.map(|field_name| {
			columns.iter()
				.position(|col| col.name().as_ref() == field_name)
				.unwrap_or_else(|| panic!("Column '{}' not found in Columns", field_name))
		})
		.collect()
}

pub(crate) fn encode_row_at_index(
	columns: &Columns,
	row_idx: usize,
	shape: &RowShape,
	row_number: RowNumber,
	field_columns: &[usize],
) -> Result<(RowNumber, EncodedRow)> {
	let values: Vec<Value> =
		field_columns.iter().map(|&col_idx| columns.data_at(col_idx).get_value(row_idx)).collect();

	let mut encoded = shape.allocate();
	shape.set_values(&mut encoded, &values);

	let created_at_nanos = columns
		.created_at
		.get(row_idx)
		.ok_or_else(|| {
			Error::from(FlowSinkError::MissingSystemColumn {
				column: "created_at",
				row_idx,
			})
		})?
		.to_nanos();
	let updated_at_nanos = columns
		.updated_at
		.get(row_idx)
		.ok_or_else(|| {
			Error::from(FlowSinkError::MissingSystemColumn {
				column: "updated_at",
				row_idx,
			})
		})?
		.to_nanos();
	encoded.set_timestamps(created_at_nanos, updated_at_nanos);

	Ok((row_number, encoded))
}

pub(crate) fn decode_dictionary_columns(columns: &mut Columns, txn: &mut FlowTransaction) -> Result<()> {
	let dict_columns: Vec<(usize, Dictionary)> = {
		let catalog = txn.catalog();
		columns.iter()
			.enumerate()
			.filter_map(|(pos, col)| {
				if let ColumnBuffer::DictionaryId(container) = col.data() {
					let dict_id = container.dictionary_id()?;
					let dictionary = catalog.cache().find_dictionary(dict_id)?;
					Some((pos, dictionary))
				} else {
					None
				}
			})
			.collect()
	};

	let registry = txn.dictionary_allocators();
	for (col_pos, dictionary) in &dict_columns {
		let col = &columns[*col_pos];
		let row_count = col.len();
		let mut new_data = ColumnBuffer::with_capacity(dictionary.value_type.clone(), row_count);

		for row_idx in 0..row_count {
			let id_value = col.get_value(row_idx);
			let value = match DictionaryEntryId::from_value(&id_value) {
				Some(entry_id) => match registry.get(dictionary, entry_id.to_u128())? {
					Some(bytes) => from_bytes(&bytes).unwrap_or(Value::none()),
					None => Value::none(),
				},
				None => Value::none(),
			};
			new_data.push_value(value);
		}

		columns.columns.make_mut()[*col_pos] = new_data;
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_core::{actors::pending::Pending, interface::catalog::dictionary::Dictionary};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::{
		dictionary::{DictionaryAllocatorRegistry, store::MultiDictionaryStore},
		interceptor::interceptors::Interceptors,
	};
	use reifydb_value::value::{datetime::DateTime, row_number::RowNumber, value_type::ValueType};

	use super::*;
	use crate::transaction::{DeferredParams, allocators::FlowAllocators};

	fn flow_txn(engine: &TestEngine, registry: &DictionaryAllocatorRegistry) -> FlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		FlowTransaction::deferred_from_parts(DeferredParams {
			version,
			pending: Pending::new(),
			base_pending: Arc::new(Pending::new()),
			query: parent.multi.begin_query().unwrap(),
			state_query: parent.multi.begin_query().unwrap(),
			dictionary_query: Some(parent.multi.begin_query().unwrap()),
			single: parent.single.clone(),
			catalog: engine.inner().catalog().clone(),
			interceptors: Interceptors::new(),
			clock: Clock::Mock(MockClock::from_millis(0)),
			allocators: FlowAllocators::with_dictionary(registry.clone()),
		})
	}

	fn dictionary_column(dictionary: &Dictionary, entry_id: DictionaryEntryId) -> Columns {
		let mut buffer = ColumnBuffer::with_capacity(ValueType::DictionaryId, 1);
		buffer.push_value(entry_id.to_value());
		if let ColumnBuffer::DictionaryId(container) = &mut buffer {
			container.set_dictionary_id(dictionary.id);
		}
		Columns::with_system_columns(
			vec![ColumnWithName::new(Fragment::internal("m"), buffer)],
			vec![RowNumber(1)],
			vec![DateTime::from_nanos(1)],
			vec![DateTime::from_nanos(1)],
		)
	}

	// Decoding a dictionary id column runs per output row on every sink/scan apply; before the
	// committed-value cache each decode was one committed-store point get (the dominant share of
	// the multi-tier read bucket in production). The first decode after a restart may read the
	// store, but a repeat decode of the same id in a LATER transaction must be served from the
	// shared registry cache: zero store reads, identical value. A wrong value here would mean the
	// cache aliased ids across dictionaries or served stale bytes.
	#[test]
	fn dictionary_decode_is_served_from_the_cache_across_transactions() {
		let engine = TestEngine::new();
		engine.admin("CREATE NAMESPACE test");
		engine.admin("CREATE DICTIONARY test::syms FOR utf8 AS uint2");
		let catalog = engine.inner().catalog();
		let namespace = catalog.cache().find_namespace_by_name("test").expect("namespace");
		let dictionary =
			catalog.cache().find_dictionary_by_name(namespace.id(), "syms").expect("dictionary syms");

		let multi = engine.begin_admin(IdentityId::system()).unwrap().multi.clone();

		let entry_id = {
			let registry =
				DictionaryAllocatorRegistry::new(Arc::new(MultiDictionaryStore::new(multi.clone())));
			registry.intern(&dictionary, &Value::Utf8("sol".to_string())).unwrap().outcomes[0].id.clone()
		};

		let decode_store = Arc::new(MultiDictionaryStore::new(multi));
		let decode_registry = DictionaryAllocatorRegistry::new(decode_store.clone());
		{
			let mut txn = flow_txn(&engine, &decode_registry);
			let mut columns = dictionary_column(&dictionary, entry_id.clone());
			let before = decode_store.read_count();
			decode_dictionary_columns(&mut columns, &mut txn).unwrap();
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
			decode_dictionary_columns(&mut columns, &mut txn).unwrap();
			assert_eq!(
				decode_store.read_count() - before,
				0,
				"a repeat decode in a later transaction must be served from the registry cache"
			);
			assert_eq!(columns[0].get_value(0), Value::Utf8("sol".to_string()));
		}
	}
}
