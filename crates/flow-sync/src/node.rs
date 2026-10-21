// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_schema::SchemaRef;
use reifydb_core::interface::{catalog::flow::OperatorId, change::Change};
use reifydb_flow::{
	operator::{
		append::AppendOperator,
		extend::ExtendOperator,
		filter::FilterOperator,
		map::MapOperator,
		sink::table::TableSink,
		sort::SortOperator,
		source::{SourceOperator, decode_change},
	},
	txn::{Batch, Emit, Get, Intern},
};
use reifydb_value::Result;

pub enum Node {
	Source(SourceOperator),
	Filter(FilterOperator),
	Map(MapOperator),
	Extend(ExtendOperator),
	Append(AppendOperator),
	Sort(SortOperator),
	Sink(TableSink),
}

impl Node {
	pub fn id(&self) -> OperatorId {
		match self {
			Node::Source(source) => source.id(),
			Node::Filter(filter) => filter.id(),
			Node::Map(map) => map.id(),
			Node::Extend(extend) => extend.id(),
			Node::Append(append) => append.id(),
			Node::Sort(sort) => sort.id(),
			Node::Sink(sink) => sink.id(),
		}
	}

	pub fn output_schema(&self) -> Option<SchemaRef> {
		match self {
			Node::Source(source) => source.output_schema(),
			Node::Filter(filter) => filter.output_schema(),
			Node::Map(map) => map.output_schema(),
			Node::Extend(extend) => extend.output_schema(),
			Node::Append(append) => append.output_schema(),
			Node::Sort(sort) => sort.output_schema(),
			Node::Sink(_) => None,
		}
	}

	pub fn apply<T: Get + Batch + Emit + Intern>(&mut self, txn: &mut T, change: Change) -> Result<Change> {
		match self {
			Node::Source(source) => Ok(source.apply(change)),
			Node::Filter(filter) => filter.apply(txn, change),
			Node::Map(map) => map.apply(txn, change),
			Node::Extend(extend) => extend.apply(txn, change),
			Node::Append(append) => append.apply(change),
			Node::Sort(sort) => Ok(sort.apply(change)),
			Node::Sink(sink) => {
				let change = decode_change(txn, change)?;
				let version = change.version;
				let changed_at = change.changed_at;
				sink.apply(txn, change)?;
				Ok(Change::from_flow(sink.id(), version, Vec::new(), changed_at))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
	use reifydb_core::{
		common::{ChangeVersion, CommitVersion, TimeSource},
		interface::{
			catalog::{
				column::{Column, ColumnIndex},
				dictionary::Dictionary,
				flow::OperatorId,
				id::{ColumnId, NamespaceId, TableId},
				object::ObjectId,
				table::Table,
			},
			change::{Change, ChangeOrigin, Diff},
		},
		value::{
			batch::batch,
			column::{builder::ColumnBuilder, factory::int8},
		},
	};
	use reifydb_flow::{
		operator::{sort::SortOperator, source::SourceOperator},
		txn::Intern,
	};
	use reifydb_value::{
		factory::time::at_millis,
		value::{
			Value,
			column_view::ColumnView,
			constraint::TypeConstraint,
			container::temporal_array::datetime_array,
			dictionary::{DictionaryEntryId, DictionaryId},
			row_number::RowNumber,
			system_columns::{SystemColumn, row_numbers, with_system_column},
			value_type::{ValueType, field::from_field},
		},
	};

	use super::Node;
	use crate::testing::TestingTx;

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

	fn trades_table() -> Table {
		Table {
			id: TableId(1),
			namespace: NamespaceId(1),
			name: "trades".to_string(),
			columns: vec![
				column(0, "qty", TypeConstraint::unconstrained(ValueType::Int8), None),
				column(
					1,
					"symbol",
					TypeConstraint::dictionary(SYMBOLS, ValueType::Uint2),
					Some(SYMBOLS),
				),
			],
			primary_key: None,
			partition_by: Vec::new(),
			time: TimeSource::None,
		}
	}

	fn trades(rows: &[(u64, i64, DictionaryEntryId)]) -> RecordBatch {
		let n = rows.len();
		let mut symbol = ColumnBuilder::with_capacity(ValueType::DictionaryId, n);
		for (_, _, entry) in rows {
			symbol.push_value(entry.to_value());
		}
		symbol.set_dictionary_id(SYMBOLS);
		let user =
			batch(vec![int8("qty", rows.iter().map(|(_, qty, _)| *qty)), symbol.finish("symbol")]).unwrap();
		let system: [(SystemColumn, ArrayRef); 4] = [
			(
				SystemColumn::RowNumbers,
				Arc::new(UInt64Array::from_iter_values(rows.iter().map(|(row, _, _)| *row))),
			),
			(SystemColumn::CreatedAt, Arc::new(datetime_array(vec![at_millis(10); n]))),
			(SystemColumn::UpdatedAt, Arc::new(datetime_array(vec![at_millis(20); n]))),
			(SystemColumn::Time, Arc::new(datetime_array(vec![at_millis(30); n]))),
		];
		system.into_iter()
			.fold(user, |columns, (column, array)| with_system_column(columns, column, array).unwrap())
	}

	fn values(columns: &RecordBatch, position: usize) -> Vec<Value> {
		let column =
			ColumnView::try_from((columns.column(position), columns.schema_ref().field(position))).unwrap();
		(0..columns.num_rows()).map(|row| column.get_value(row)).collect()
	}

	fn utf8(text: &str) -> Value {
		Value::Utf8(text.to_string())
	}

	fn version() -> ChangeVersion {
		ChangeVersion::from(CommitVersion(3))
	}

	#[test]
	fn a_table_source_hands_downstream_the_entry_ids_it_was_given() {
		// Decoding at the source pays a dictionary read per row every reader would repeat; ids must pass
		// through.
		let mut txn = TestingTx::default();
		txn.dictionaries.insert(SYMBOLS, symbols());
		let sol = txn.intern(&symbols(), &[utf8("sol")]).unwrap()[0];
		let eth = txn.intern(&symbols(), &[utf8("eth")]).unwrap()[0];
		let mut source = Node::Source(SourceOperator::from_columns(OperatorId(1), &trades_table().columns));

		let out = source
			.apply(
				&mut txn,
				Change::from_object(
					ObjectId::table(TableId(1)),
					version(),
					vec![
						Diff::insert(trades(&[(1, 10, sol), (2, 20, eth)])),
						Diff::update(trades(&[(1, 10, sol)]), trades(&[(1, 15, eth)])),
						Diff::remove(trades(&[(2, 20, eth)])),
					],
					at_millis(5),
				),
			)
			.unwrap();

		assert_eq!(out.origin, ChangeOrigin::Flow(OperatorId(1)));
		assert_eq!(out.diffs.len(), 3);
		let Diff::Insert {
			post,
			..
		} = &out.diffs[0]
		else {
			panic!("the insert must stay an insert: {:?}", out.diffs[0]);
		};
		assert_eq!(values(post, 1), vec![Value::DictionaryId(sol), Value::DictionaryId(eth)]);
		assert_eq!(values(post, 0), vec![Value::Int8(10), Value::Int8(20)]);
		assert_eq!(row_numbers(post).unwrap(), &[RowNumber(1), RowNumber(2)]);
		let Diff::Update {
			pre,
			post,
			..
		} = &out.diffs[1]
		else {
			panic!("the update must stay an update: {:?}", out.diffs[1]);
		};
		assert_eq!(values(pre, 1), vec![Value::DictionaryId(sol)]);
		assert_eq!(values(post, 1), vec![Value::DictionaryId(eth)]);
		let Diff::Remove {
			pre,
			..
		} = &out.diffs[2]
		else {
			panic!("the remove must stay a remove: {:?}", out.diffs[2]);
		};
		assert_eq!(values(pre, 1), vec![Value::DictionaryId(eth)]);
		let schema = source.output_schema().expect("a source declares its schema");
		assert_eq!(from_field(schema.field(1)).unwrap().dictionary_id, Some(SYMBOLS));
	}

	#[test]
	fn a_sort_forwards_every_diff_untouched_under_its_own_origin() {
		let mut txn = TestingTx::default();
		let entry = DictionaryEntryId::U2(0);
		let diffs = vec![
			Diff::insert(trades(&[(1, 10, entry), (2, 20, entry)])),
			Diff::update(trades(&[(3, 30, entry)]), trades(&[(3, 31, entry)])),
			Diff::remove(trades(&[(4, 40, entry)])),
		];
		let mut sort = Node::Sort(SortOperator::new(None, OperatorId(4)));

		let out = sort
			.apply(&mut txn, Change::from_flow(OperatorId(2), version(), diffs.clone(), at_millis(5)))
			.unwrap();

		assert_eq!(out.origin, ChangeOrigin::Flow(OperatorId(4)));
		assert_eq!(out.version, version());
		assert_eq!(out.changed_at, at_millis(5));
		assert_eq!(format!("{:?}", out.diffs.as_slice()), format!("{:?}", diffs));
	}
}
