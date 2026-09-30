// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
	use reifydb_codec::{
		key::encoded::EncodedKey,
		row::{shape::RowFamily, table::EncodedTableRow},
	};
	use reifydb_core::{
		common::{ChangeVersion, CommitVersion},
		interface::{
			catalog::{
				column::{Column, ColumnIndex},
				dictionary::Dictionary,
				flow::OperatorId,
				id::{ColumnId, NamespaceId, ViewId},
				object::ObjectId,
				view::{TableView, View, ViewKind, ViewSortKey},
			},
			change::{Change, Diff},
		},
		key::partition::PartitionKey,
		partition::partition_of,
		row::row_shape_from_columns,
		sort::SortDirection,
		value::{batch::batch, column::factory},
	};
	use reifydb_flow::{
		operator::sink::{
			partition::ensure_partition_unchanged,
			table::TableSink,
			view::{partitioned_key, row_key, sort_runs, sorted_view_key},
		},
		txn::Intern,
	};
	use reifydb_runtime::context::{
		RuntimeContext,
		clock::{Clock, MockClock},
	};
	use reifydb_value::{
		factory::time::at_millis,
		value::{
			Value,
			column_view::ColumnView,
			constraint::{Constraint, TypeConstraint},
			container::temporal_array::datetime_array,
			datetime::DateTime,
			dictionary::DictionaryId,
			row_number::RowNumber,
			system_columns::{SystemColumn, row_numbers, with_system_column},
			value_type::ValueType,
		},
	};

	use crate::testing::TestingTx;

	const VIEW: ViewId = ViewId(4);
	const SYMBOLS: DictionaryId = DictionaryId(7);

	fn symbols() -> Dictionary {
		Dictionary {
			id: SYMBOLS,
			namespace: NamespaceId(1),
			name: "symbols".to_string(),
			value_type: ValueType::Utf8,
			id_type: ValueType::Uint2,
		}
	}

	fn column(position: u8, name: &str, constraint: TypeConstraint, dictionary_id: Option<DictionaryId>) -> Column {
		Column {
			id: ColumnId(position as u64),
			name: name.to_string(),
			constraint,
			properties: Vec::new(),
			index: ColumnIndex(position),
			auto_increment: false,
			dictionary_id,
		}
	}

	fn plain_symbol() -> Column {
		column(0, "sym", TypeConstraint::unconstrained(ValueType::Utf8), None)
	}

	fn view(symbol: Column, sort: Vec<ViewSortKey>, partition_by: &[&str]) -> View {
		View::Table(TableView {
			id: VIEW,
			namespace: NamespaceId(1),
			name: "positions".to_string(),
			kind: ViewKind::Transactional,
			columns: vec![symbol, column(1, "qty", TypeConstraint::unconstrained(ValueType::Int8), None)],
			primary_key: None,
			partition_by: partition_by.iter().map(|name| name.to_string()).collect(),
			sort,
		})
	}

	fn by_qty() -> Vec<ViewSortKey> {
		vec![ViewSortKey {
			column: ColumnIndex(1),
			direction: SortDirection::Asc,
		}]
	}

	fn runtime_context() -> RuntimeContext {
		RuntimeContext::with_clock(Clock::Mock(MockClock::from_millis(0)))
	}

	fn sink(view: &View) -> TableSink {
		TableSink::new(OperatorId(1), view.clone(), runtime_context())
	}

	fn positions(rows: &[(u64, &str, i64)], created: DateTime, updated: DateTime) -> RecordBatch {
		let n = rows.len();
		let user = batch(vec![
			factory::utf8("sym", rows.iter().map(|(_, sym, _)| sym.to_string())),
			factory::int8("qty", rows.iter().map(|(_, _, qty)| *qty)),
		])
		.unwrap();
		let system: [(SystemColumn, ArrayRef); 3] = [
			(
				SystemColumn::RowNumbers,
				Arc::new(UInt64Array::from_iter_values(rows.iter().map(|(row, _, _)| *row))),
			),
			(SystemColumn::CreatedAt, Arc::new(datetime_array(vec![created; n]))),
			(SystemColumn::UpdatedAt, Arc::new(datetime_array(vec![updated; n]))),
		];
		system.into_iter()
			.fold(user, |columns, (column, array)| with_system_column(columns, column, array).unwrap())
	}

	fn inserted(rows: &[(u64, &str, i64)]) -> RecordBatch {
		positions(rows, at_millis(10), at_millis(20))
	}

	fn change(diffs: Vec<Diff>) -> Change {
		Change::from_flow(OperatorId(0), ChangeVersion::from(CommitVersion(1)), diffs, at_millis(0))
	}

	fn stored(txn: &TestingTx, view: &View, key: &EncodedKey) -> Vec<Value> {
		let shape = row_shape_from_columns(RowFamily::Table, view.columns());
		let row = txn.rows.get(key).expect("the view row must be stored under this key");
		(0..view.columns().len()).map(|index| shape.get_value(row, index)).collect()
	}

	fn values(columns: &RecordBatch, position: usize) -> Vec<Value> {
		let column =
			ColumnView::try_from((columns.column(position), columns.schema_ref().field(position))).unwrap();
		(0..columns.num_rows()).map(|row| column.get_value(row)).collect()
	}

	fn utf8(text: &str) -> Value {
		Value::Utf8(text.to_string())
	}

	#[test]
	fn an_insert_stores_each_source_row_under_its_row_key_and_emits_the_insert_once() {
		let mut txn = TestingTx::default();
		let view = view(plain_symbol(), Vec::new(), &[]);
		let storage = view.storage_id();

		sink(&view)
			.apply(&mut txn, change(vec![Diff::insert(inserted(&[(1, "sol", 10), (2, "eth", 20)]))]))
			.unwrap();

		assert_eq!(txn.rows.len(), 2);
		assert_eq!(stored(&txn, &view, &row_key(storage, RowNumber(1))), vec![utf8("sol"), Value::Int8(10)]);
		assert_eq!(stored(&txn, &view, &row_key(storage, RowNumber(2))), vec![utf8("eth"), Value::Int8(20)]);
		assert_eq!(txn.emitted.len(), 1);
		let (
			emitted_view,
			Diff::Insert {
				post,
				..
			},
		) = &txn.emitted[0]
		else {
			panic!("an insert must be emitted as an insert: {:?}", txn.emitted[0]);
		};
		assert_eq!(*emitted_view, VIEW);
		assert_eq!(row_numbers(post).unwrap(), &[RowNumber(1), RowNumber(2)]);
		assert_eq!(values(post, 1), vec![Value::Int8(10), Value::Int8(20)]);
	}

	#[test]
	fn an_update_moves_the_row_to_its_new_key_keeps_the_prior_created_at_and_takes_updated_from_the_post_row() {
		let mut txn = TestingTx {
			clock: MockClock::from_millis(500),
			..Default::default()
		};
		let view = view(plain_symbol(), by_qty(), &[]);
		let storage = view.storage_id();
		let mut sink = sink(&view);
		let pre = inserted(&[(1, "sol", 10)]);
		let post = positions(&[(1, "sol", 30)], at_millis(77), at_millis(88));
		let pre_key =
			sorted_view_key(storage, sort_runs(&by_qty(), &pre).unwrap().into_iter().next(), RowNumber(1));
		let post_key =
			sorted_view_key(storage, sort_runs(&by_qty(), &post).unwrap().into_iter().next(), RowNumber(1));
		sink.apply(&mut txn, change(vec![Diff::insert(pre.clone())])).unwrap();

		sink.apply(&mut txn, change(vec![Diff::update(pre, post)])).unwrap();

		assert_ne!(pre_key.as_slice(), post_key.as_slice());
		assert!(!txn.rows.contains_key(&pre_key), "the row must leave its pre key");
		assert_eq!(txn.rows.len(), 1);
		assert_eq!(stored(&txn, &view, &post_key), vec![utf8("sol"), Value::Int8(30)]);
		let row = EncodedTableRow::from(txn.rows[&post_key].clone());
		assert_eq!(row.created_at(), at_millis(10));
		assert_eq!(row.updated_at(), at_millis(88));
		assert_eq!(txn.emitted.len(), 2);
		let (
			emitted_view,
			Diff::Update {
				pre,
				post,
				..
			},
		) = &txn.emitted[1]
		else {
			panic!("an update must be emitted as an update: {:?}", txn.emitted[1]);
		};
		assert_eq!(*emitted_view, VIEW);
		assert_eq!(values(pre, 1), vec![Value::Int8(10)]);
		assert_eq!(values(post, 1), vec![Value::Int8(30)]);
	}

	#[test]
	fn a_remove_deletes_only_the_removed_view_row_and_emits_the_remove() {
		let mut txn = TestingTx::default();
		let view = view(plain_symbol(), Vec::new(), &[]);
		let storage = view.storage_id();
		let mut sink = sink(&view);
		sink.apply(&mut txn, change(vec![Diff::insert(inserted(&[(1, "sol", 10), (2, "eth", 20)]))])).unwrap();

		sink.apply(&mut txn, change(vec![Diff::remove(inserted(&[(1, "sol", 10)]))])).unwrap();

		assert_eq!(txn.rows.len(), 1);
		assert!(!txn.rows.contains_key(&row_key(storage, RowNumber(1))));
		assert_eq!(stored(&txn, &view, &row_key(storage, RowNumber(2))), vec![utf8("eth"), Value::Int8(20)]);
		assert_eq!(txn.emitted.len(), 2);
		let (
			emitted_view,
			Diff::Remove {
				pre,
				..
			},
		) = &txn.emitted[1]
		else {
			panic!("a remove must be emitted as a remove: {:?}", txn.emitted[1]);
		};
		assert_eq!(*emitted_view, VIEW);
		assert_eq!(row_numbers(pre).unwrap(), &[RowNumber(1)]);
	}

	#[test]
	fn a_sorted_view_stores_rows_under_sort_keys_so_key_order_follows_the_sort_column() {
		let mut txn = TestingTx::default();
		let view = view(plain_symbol(), by_qty(), &[]);
		let storage = view.storage_id();
		let rows = inserted(&[(1, "sol", 30), (2, "eth", 10), (3, "btc", 20)]);

		sink(&view).apply(&mut txn, change(vec![Diff::insert(rows.clone())])).unwrap();

		assert_eq!(txn.rows.len(), 3);
		for (row_idx, row_number) in [RowNumber(1), RowNumber(2), RowNumber(3)].into_iter().enumerate() {
			let key = sorted_view_key(
				storage,
				sort_runs(&by_qty(), &rows).unwrap().into_iter().nth(row_idx),
				row_number,
			);
			assert!(txn.rows.contains_key(&key), "row {} must be stored under its sort key", row_number.0);
		}
		let shape = row_shape_from_columns(RowFamily::Table, view.columns());
		assert_eq!(
			txn.rows.values().map(|row| shape.get_value(row, 1)).collect::<Vec<_>>(),
			vec![Value::Int8(10), Value::Int8(20), Value::Int8(30)]
		);
	}

	#[test]
	fn a_partitioned_view_stores_rows_under_their_partition_key_and_registers_the_partition() {
		let mut txn = TestingTx::default();
		let view = view(plain_symbol(), Vec::new(), &["sym"]);
		let storage = view.storage_id();
		let rows = inserted(&[(1, "sol", 10), (2, "eth", 20)]);
		let sol = partition_of(view.columns(), &["sym".to_string()], &[utf8("sol")]);
		let eth = partition_of(view.columns(), &["sym".to_string()], &[utf8("eth")]);

		sink(&view).apply(&mut txn, change(vec![Diff::insert(rows.clone())])).unwrap();

		let sol_key = partitioned_key(storage, None, sol, RowNumber(1));
		let eth_key = partitioned_key(storage, None, eth, RowNumber(2));
		assert_eq!(stored(&txn, &view, &sol_key), vec![utf8("sol"), Value::Int8(10)]);
		assert_eq!(stored(&txn, &view, &eth_key), vec![utf8("eth"), Value::Int8(20)]);
		assert!(!txn.rows.contains_key(&row_key(storage, RowNumber(1))));
		assert!(txn.rows.contains_key(&PartitionKey::encoded(ObjectId::view(VIEW), sol)));
		assert!(txn.rows.contains_key(&PartitionKey::encoded(ObjectId::view(VIEW), eth)));
		assert_eq!(txn.rows.len(), 4);
	}

	#[test]
	fn an_update_that_moves_a_row_to_another_partition_is_refused_before_anything_is_written() {
		let mut txn = TestingTx::default();
		let view = view(plain_symbol(), Vec::new(), &["sym"]);
		let mut sink = sink(&view);
		sink.apply(&mut txn, change(vec![Diff::insert(inserted(&[(1, "sol", 10)]))])).unwrap();
		let rows_before = txn.rows.clone();
		let expected = ensure_partition_unchanged(
			ObjectId::view(VIEW),
			partition_of(view.columns(), &["sym".to_string()], &[utf8("sol")]),
			partition_of(view.columns(), &["sym".to_string()], &[utf8("eth")]),
		)
		.unwrap_err();

		let err = sink
			.apply(
				&mut txn,
				change(vec![Diff::update(inserted(&[(1, "sol", 10)]), inserted(&[(1, "eth", 10)]))]),
			)
			.unwrap_err();

		assert_eq!(err.code, expected.code);
		assert_eq!(txn.rows, rows_before);
		assert_eq!(txn.emitted.len(), 1);
	}

	#[test]
	fn a_dictionary_view_column_stores_the_interned_entry_id_and_emits_the_value() {
		let mut txn = TestingTx::default();
		txn.dictionaries.insert(SYMBOLS, symbols());
		txn.intern(&symbols(), &[utf8("eth")]).unwrap();
		let symbol = column(
			0,
			"sym",
			TypeConstraint::with_constraint(
				ValueType::Utf8,
				Constraint::Dictionary(SYMBOLS, ValueType::Uint2),
			),
			Some(SYMBOLS),
		);
		let view = view(symbol, Vec::new(), &[]);
		let storage = view.storage_id();

		sink(&view)
			.apply(&mut txn, change(vec![Diff::insert(inserted(&[(1, "sol", 10), (2, "eth", 20)]))]))
			.unwrap();

		let sol = txn.intern(&symbols(), &[utf8("sol")]).unwrap()[0];
		let eth = txn.intern(&symbols(), &[utf8("eth")]).unwrap()[0];
		assert_ne!(sol, eth);
		assert_eq!(txn.dictionary_values[&SYMBOLS], vec![utf8("eth"), utf8("sol")]);
		assert_eq!(stored(&txn, &view, &row_key(storage, RowNumber(1)))[0], sol.to_value());
		assert_eq!(stored(&txn, &view, &row_key(storage, RowNumber(2)))[0], eth.to_value());
		let (
			_,
			Diff::Insert {
				post,
				..
			},
		) = &txn.emitted[0]
		else {
			panic!("an insert must be emitted as an insert: {:?}", txn.emitted[0]);
		};
		assert_eq!(values(post, 0), vec![utf8("sol"), utf8("eth")]);
	}
}
