// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use reifydb_core::{
	interface::{
		catalog::{column::Column, dictionary::Dictionary, flow::OperatorId},
		change::{Change, Diff},
	},
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_flow::operator::{
	append::AppendOperator, extend::ExtendOperator, filter::FilterOperator, map::MapOperator,
};
use reifydb_value::{
	Result,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};

use crate::{
	sink::TableSink,
	txn::{Emit, Intern, Lookup, Rows},
};

pub struct SourceNode {
	operator: OperatorId,
	columns: Vec<Column>,
}

impl SourceNode {
	pub(crate) fn new(operator: OperatorId, columns: Vec<Column>) -> Self {
		Self {
			operator,
			columns,
		}
	}

	fn apply<T: Lookup + Intern>(&self, txn: &mut T, change: Change) -> Result<Change> {
		let mut decoded_diffs = Vec::with_capacity(change.diffs.len());
		for diff in change.diffs {
			decoded_diffs.push(match diff {
				Diff::Insert {
					post,
					..
				} => {
					let mut decoded = post;
					decode_dictionary_columns(&mut decoded, txn)?;
					Diff::insert(decoded)
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let mut decoded_pre = pre;
					let mut decoded_post = post;
					decode_dictionary_columns(&mut decoded_pre, txn)?;
					decode_dictionary_columns(&mut decoded_post, txn)?;
					Diff::update(decoded_pre, decoded_post)
				}
				Diff::Remove {
					pre,
					..
				} => {
					let mut decoded = pre;
					decode_dictionary_columns(&mut decoded, txn)?;
					Diff::remove(decoded)
				}
			});
		}
		Ok(Change::from_flow(self.operator, change.version, decoded_diffs, change.changed_at))
	}

	fn output_schema(&self) -> SchemaRef {
		Arc::new(Schema::new(
			self.columns
				.iter()
				.map(|column| {
					ColumnBuilder::with_capacity(column.constraint.get_type(), 0)
						.finish(&column.name)
						.0
				})
				.collect::<Vec<_>>(),
		))
	}
}

pub enum Node {
	Source(SourceNode),
	Filter(FilterOperator),
	Map(MapOperator),
	Extend(ExtendOperator),
	Append(AppendOperator),
	Sort(OperatorId, Option<SchemaRef>),
	Sink(TableSink),
}

impl Node {
	pub fn id(&self) -> OperatorId {
		match self {
			Node::Source(source) => source.operator,
			Node::Filter(filter) => filter.id(),
			Node::Map(map) => map.id(),
			Node::Extend(extend) => extend.id(),
			Node::Append(append) => append.id(),
			Node::Sort(operator, _) => *operator,
			Node::Sink(sink) => sink.id(),
		}
	}

	pub fn output_schema(&self) -> Option<SchemaRef> {
		match self {
			Node::Source(source) => Some(source.output_schema()),
			Node::Filter(filter) => filter.output_schema(),
			Node::Map(map) => map.output_schema(),
			Node::Extend(extend) => extend.output_schema(),
			Node::Append(append) => append.output_schema(),
			Node::Sort(_, parent_schema) => parent_schema.clone(),
			Node::Sink(_) => None,
		}
	}

	pub fn apply<T: Rows + Emit + Lookup + Intern>(&mut self, txn: &mut T, change: Change) -> Result<Change> {
		match self {
			Node::Source(source) => source.apply(txn, change),
			Node::Filter(filter) => filter.apply(change),
			Node::Map(map) => map.apply(change),
			Node::Extend(extend) => extend.apply(change),
			Node::Append(append) => append.apply(change),
			Node::Sort(operator, _) => {
				Ok(Change::from_flow(*operator, change.version, change.diffs, change.changed_at))
			}
			Node::Sink(sink) => {
				let version = change.version;
				let changed_at = change.changed_at;
				sink.apply(txn, change)?;
				Ok(Change::from_flow(sink.id(), version, Vec::new(), changed_at))
			}
		}
	}
}

fn decode_dictionary_columns<T: Lookup + Intern>(columns: &mut RecordBatch, txn: &mut T) -> Result<()> {
	let dict_columns: Vec<(usize, Dictionary)> = {
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
		ids.into_iter().map(|(pos, id)| Ok((pos, txn.dictionary(id)?))).collect::<Result<Vec<_>>>()?
	};

	if dict_columns.is_empty() {
		return Ok(());
	}

	let mut decoded: Vec<_> =
		columns.schema_ref().fields().iter().cloned().zip(columns.columns().iter().cloned()).collect();
	for (col_pos, dictionary) in &dict_columns {
		let (field, array) = &decoded[*col_pos];
		let column = ColumnView::try_from((array, field.as_ref()))?;
		let row_count = column.len();
		let mut new_data = ColumnBuilder::with_capacity(dictionary.value_type.clone(), row_count);

		for row_idx in 0..row_count {
			let id_value = column.get_value(row_idx);
			let value = match DictionaryEntryId::from_value(&id_value) {
				Some(entry_id) => txn.resolve(dictionary, entry_id)?.unwrap_or(Value::none()),
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
			value_type::ValueType,
		},
	};

	use super::{Node, SourceNode};
	use crate::{memory::MemoryTxn, txn::Intern};

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
	fn a_table_source_hands_downstream_the_dictionary_values_instead_of_the_entry_ids() {
		let mut txn = MemoryTxn::default();
		txn.dictionaries.insert(SYMBOLS, symbols());
		let sol = txn.intern(&symbols(), &utf8("sol")).unwrap();
		let eth = txn.intern(&symbols(), &utf8("eth")).unwrap();
		let mut source = Node::Source(SourceNode::new(OperatorId(1), trades_table().columns));

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
		assert_eq!(values(post, 1), vec![utf8("sol"), utf8("eth")]);
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
		assert_eq!(values(pre, 1), vec![utf8("sol")]);
		assert_eq!(values(post, 1), vec![utf8("eth")]);
		let Diff::Remove {
			pre,
			..
		} = &out.diffs[2]
		else {
			panic!("the remove must stay a remove: {:?}", out.diffs[2]);
		};
		assert_eq!(values(pre, 1), vec![utf8("eth")]);
	}

	#[test]
	fn a_sort_forwards_every_diff_untouched_under_its_own_origin() {
		let mut txn = MemoryTxn::default();
		let entry = DictionaryEntryId::U2(0);
		let diffs = vec![
			Diff::insert(trades(&[(1, 10, entry), (2, 20, entry)])),
			Diff::update(trades(&[(3, 30, entry)]), trades(&[(3, 31, entry)])),
			Diff::remove(trades(&[(4, 40, entry)])),
		];
		let mut sort = Node::Sort(OperatorId(4), None);

		let out = sort
			.apply(&mut txn, Change::from_flow(OperatorId(2), version(), diffs.clone(), at_millis(5)))
			.unwrap();

		assert_eq!(out.origin, ChangeOrigin::Flow(OperatorId(4)));
		assert_eq!(out.version, version());
		assert_eq!(out.changed_at, at_millis(5));
		assert_eq!(format!("{:?}", out.diffs.as_slice()), format!("{:?}", diffs));
	}
}
