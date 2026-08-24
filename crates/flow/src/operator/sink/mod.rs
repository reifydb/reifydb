// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod partition;
pub mod ringbuffer_view;
pub mod series_view;
pub mod view;

use std::sync::LazyLock;

use reifydb_codec::row::{
	bytes::{EncodedBytes, SourceRowBuilder},
	shape::{RowFamily, RowShape},
};
use reifydb_core::{
	interface::{
		catalog::{
			column::Column as CatalogColumn,
			flow::OperatorId,
			object::ObjectId,
			property::{ColumnPropertyKind, ColumnSaturationStrategy},
			view::View,
		},
		change::{Change, ChangeOrigin, Diff},
		evaluate::TargetColumn,
		flow::OperatorCapability,
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, cast::cast_column_data, columns::Columns},
};
use reifydb_evaluate::{expression::context::EvalContext, stack::SymbolTable};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	params::Params,
	value::{
		Value,
		dictionary::{DictionaryEntryId, DictionaryId},
		identity::IdentityId,
		row_number::RowNumber,
		value_type::ValueType,
	},
};
use smallvec::smallvec;

use crate::{
	error::FlowSinkError,
	operator::host::HostContext,
	timer::Timer,
	transaction::{FlowTransaction, deferred::DeferredTransaction},
};

/// A durable view sink: the terminal node that writes a flow's output into a table, series or
/// ring buffer. Unlike an [`crate::operator::HostOperator`] it needs the whole transaction (raw
/// keyspace writes, change tracking, dictionary allocation and catalog lookups), so it is
/// dispatched off a separate map and only ever exists on the deferred path.
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
	let version = txn.version();
	let changed_at = txn.clock().now();
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Object(ObjectId::view(view.id())),
		version,
		diffs: smallvec![diff],
		changed_at,
	});
}

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
		identity: IdentityId::system(),
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
		system: columns.system.clone(),
		columns: buffers_vec,
		names: names_vec,
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
) -> Result<(RowNumber, EncodedBytes)> {
	match shape.family() {
		RowFamily::Table => {
			stamp_source_row(shape.allocate_table(), columns, row_idx, shape, row_number, field_columns)
		}
		RowFamily::Series => {
			stamp_source_row(shape.allocate_series(), columns, row_idx, shape, row_number, field_columns)
		}
		RowFamily::RingBuffer => stamp_source_row(
			shape.allocate_ringbuffer(),
			columns,
			row_idx,
			shape,
			row_number,
			field_columns,
		),
		other => Err(Error::from(FlowSinkError::NotASourceFamily {
			family: format!("{:?}", other),
		})),
	}
}

fn stamp_source_row<B: SourceRowBuilder>(
	mut encoded: B,
	columns: &Columns,
	row_idx: usize,
	shape: &RowShape,
	row_number: RowNumber,
	field_columns: &[usize],
) -> Result<(RowNumber, EncodedBytes)> {
	let values: Vec<Value> =
		field_columns.iter().map(|&col_idx| columns.data_at(col_idx).get_value(row_idx)).collect();

	shape.set_values(&mut encoded, &values);

	let created_at = columns.created_at().get(row_idx).copied().ok_or_else(|| {
		Error::from(FlowSinkError::MissingSystemColumn {
			column: "created_at",
			row_idx,
		})
	})?;
	let updated_at = columns.updated_at().get(row_idx).copied().ok_or_else(|| {
		Error::from(FlowSinkError::MissingSystemColumn {
			column: "updated_at",
			row_idx,
		})
	})?;
	encoded.set_timestamps(created_at, updated_at);
	if let Some(time) = columns.time().get(row_idx).copied() {
		encoded.set_time(time);
	}

	Ok((row_number, encoded.freeze_bytes()))
}

pub(crate) fn decode_dictionary_columns(columns: &mut Columns, host: &mut dyn HostContext) -> Result<()> {
	let dict_columns: Vec<(usize, DictionaryId, ValueType)> = {
		let ids: Vec<(usize, DictionaryId)> = columns
			.iter()
			.enumerate()
			.filter_map(|(pos, col)| {
				if let ColumnBuffer::DictionaryId(container) = col.data() {
					Some((pos, container.dictionary_id()?))
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
		let mut new_data = ColumnBuffer::with_capacity(value_type.clone(), row_count);

		for row_idx in 0..row_count {
			let id_value = columns[*col_pos].get_value(row_idx);
			let value = match DictionaryEntryId::from_value(&id_value) {
				Some(entry_id) => host.dictionary_get(*dictionary, entry_id)?.unwrap_or(Value::none()),
				None => Value::none(),
			};
			new_data.push_value(value);
		}

		columns.columns[*col_pos] = new_data;
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_codec::row::{
		shape::{RowFamily, RowShapeField},
		table::EncodedTableRow,
	};
	use reifydb_core::{actors::pending::PendingLayers, interface::catalog::dictionary::Dictionary};
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::{
		dictionary::{DictionaryAllocatorRegistry, store::SingleDictionaryStore},
		interceptor::interceptors::Interceptors,
	};
	use reifydb_value::value::{
		datetime::DateTime, row_number::RowNumber, system_columns::SystemColumns, value_type::ValueType,
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
			pending: PendingLayers::empty(),
			query: parent.multi.begin_query().unwrap(),
			state_query: parent.multi.begin_query().unwrap(),
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
		let mut buffer = ColumnBuffer::with_capacity(ValueType::DictionaryId, 1);
		buffer.push_value(entry_id.to_value());
		if let ColumnBuffer::DictionaryId(container) = &mut buffer {
			container.set_dictionary_id(dictionary);
		}
		Columns::with_system(
			vec![ColumnWithName::new(Fragment::internal("m"), buffer)],
			SystemColumns::new(
				vec![RowNumber(1)],
				Vec::new(),
				vec![DateTime::from_nanos(1)],
				vec![DateTime::from_nanos(1)],
				vec![DateTime::from_nanos(1)],
			),
		)
	}

	fn single_field_shape() -> RowShape {
		RowShape::new(RowFamily::Table, vec![RowShapeField::unconstrained("n".to_string(), ValueType::Int4)])
	}

	fn columns_with_stamps(created_at: u64, updated_at: u64, time: u64) -> Columns {
		let mut buffer = ColumnBuffer::with_capacity(ValueType::Int4, 1);
		buffer.push_value(Value::Int4(7));
		Columns::with_system(
			vec![ColumnWithName::new(Fragment::internal("n"), buffer)],
			SystemColumns::new(
				vec![RowNumber(1)],
				Vec::new(),
				vec![DateTime::from_nanos(created_at)],
				vec![DateTime::from_nanos(updated_at)],
				vec![DateTime::from_nanos(time)],
			),
		)
	}

	#[test]
	fn a_sink_row_carries_the_time_of_the_row_it_was_built_from() {
		// The only place a sink writes the row header, so dropping #time here lands every
		// materialised row at nanos 0 and a downstream flow inherits 1970. The three stamps are
		// seeded differently because copying the wrong one is as wrong as copying none.
		let shape = single_field_shape();
		let columns = columns_with_stamps(100, 200, 300);
		let field_columns = shape_field_columns(&columns, &shape);

		let (_, encoded) = encode_row_at_index(&columns, 0, &shape, RowNumber(1), &field_columns).unwrap();
		let encoded = EncodedTableRow::view(&encoded);

		assert_eq!(
			encoded.time(),
			Some(DateTime::from_nanos(300)),
			"#time must come from the source row's own sidecar"
		);
		assert_eq!(encoded.created_at(), DateTime::from_nanos(100));
		assert_eq!(encoded.updated_at(), DateTime::from_nanos(200));
	}

	#[test]
	fn a_sink_row_without_a_time_sidecar_is_written_without_one() {
		// A source with no time domain produces rows with no #time, so a sink must materialise them
		// rather than reject them. Substituting a stamp here would give a time-less table's rows a
		// clock they never had, and rejecting them would make the view permanently empty.
		let shape = single_field_shape();
		let mut columns = columns_with_stamps(100, 200, 300);
		columns.system.set_time(Vec::new());
		let field_columns = shape_field_columns(&columns, &shape);

		let (_, encoded) = encode_row_at_index(&columns, 0, &shape, RowNumber(1), &field_columns).unwrap();
		let encoded = EncodedTableRow::view(&encoded);

		assert_eq!(encoded.time(), None, "the sink row must carry no #time");
		assert_eq!(encoded.created_at(), DateTime::from_nanos(100), "the wall stamps are still required");
		assert_eq!(encoded.updated_at(), DateTime::from_nanos(200));
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
			registry.intern(&dictionary, &Value::Utf8("sol".to_string())).unwrap().id.clone()
		};

		let decode_store = Arc::new(SingleDictionaryStore::new(single));
		let decode_registry = DictionaryAllocatorRegistry::new(decode_store.clone());
		{
			let mut txn = flow_txn(&engine, &decode_registry);
			let mut columns = dictionary_column(&dictionary, entry_id.clone());
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
