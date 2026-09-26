// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{column::Column, dictionary::Dictionary, flow::OperatorId},
		change::{Change, Diff},
	},
	value::column::{buffer::ColumnBuffer, builder::ColumnBuilder, columns::Columns},
};
use reifydb_flow::operator::{
	append::AppendOperator, extend::ExtendOperator, filter::FilterOperator, map::MapOperator,
};
use reifydb_value::{
	Result,
	value::{
		Value,
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};

use crate::txn::{Intern, Lookup};

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

	fn output_schema(&self) -> Columns {
		Columns::from_catalog_columns(&self.columns)
	}
}

pub enum Node {
	Source(SourceNode),
	Filter(FilterOperator),
	Map(MapOperator),
	Extend(ExtendOperator),
	Append(AppendOperator),
	Sort(OperatorId),
}

impl Node {
	pub fn id(&self) -> OperatorId {
		match self {
			Node::Source(source) => source.operator,
			Node::Filter(filter) => filter.id(),
			Node::Map(map) => map.id(),
			Node::Extend(extend) => extend.id(),
			Node::Append(append) => append.id(),
			Node::Sort(operator) => *operator,
		}
	}

	pub fn output_schema(&self) -> Option<Columns> {
		match self {
			Node::Source(source) => Some(source.output_schema()),
			Node::Filter(filter) => filter.output_schema(),
			Node::Map(map) => map.output_schema(),
			Node::Extend(extend) => extend.output_schema(),
			Node::Append(append) => append.output_schema(),
			Node::Sort(_) => None,
		}
	}

	pub fn apply<T: Lookup + Intern>(&mut self, txn: &mut T, change: Change) -> Result<Change> {
		match self {
			Node::Source(source) => source.apply(txn, change),
			Node::Filter(filter) => filter.apply(change),
			Node::Map(map) => map.apply(change),
			Node::Extend(extend) => extend.apply(change),
			Node::Append(append) => append.apply(change),
			Node::Sort(operator) => {
				Ok(Change::from_flow(*operator, change.version, change.diffs, change.changed_at))
			}
		}
	}
}

fn decode_dictionary_columns<T: Lookup + Intern>(columns: &mut Columns, txn: &mut T) -> Result<()> {
	let dict_columns: Vec<(usize, Dictionary)> = {
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
		ids.into_iter().map(|(pos, id)| Ok((pos, txn.dictionary(id)?))).collect::<Result<Vec<_>>>()?
	};

	for (col_pos, dictionary) in &dict_columns {
		let row_count = columns[*col_pos].len();
		let mut new_data = ColumnBuilder::with_capacity(dictionary.value_type.clone(), row_count);

		for row_idx in 0..row_count {
			let id_value = columns[*col_pos].get_value(row_idx);
			let value = match DictionaryEntryId::from_value(&id_value) {
				Some(entry_id) => txn.resolve(dictionary, entry_id)?.unwrap_or(Value::none()),
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
		value::column::{ColumnWithName, buffer::ColumnBuffer, builder::ColumnBuilder, columns::Columns},
	};
	use reifydb_value::{
		factory::time::at_millis,
		fragment::Fragment,
		value::{
			Value,
			constraint::TypeConstraint,
			dictionary::{DictionaryEntryId, DictionaryId},
			row_number::RowNumber,
			system_columns::SystemColumns,
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

	fn trades(rows: &[(u64, i64, DictionaryEntryId)]) -> Columns {
		let n = rows.len();
		let mut symbol = ColumnBuilder::with_capacity(ValueType::DictionaryId, n);
		for (_, _, entry) in rows {
			symbol.push_value(entry.to_value());
		}
		symbol.set_dictionary_id(SYMBOLS);
		Columns::with_system(
			vec![
				ColumnWithName::new(
					Fragment::internal("qty"),
					ColumnBuffer::int8(rows.iter().map(|(_, qty, _)| *qty).collect::<Vec<_>>()),
				),
				ColumnWithName::new(Fragment::internal("symbol"), symbol.finish()),
			],
			SystemColumns::new(
				rows.iter().map(|(row, _, _)| RowNumber(*row)).collect(),
				Vec::new(),
				vec![at_millis(10); n],
				vec![at_millis(20); n],
				vec![at_millis(30); n],
				Vec::new(),
			),
		)
	}

	fn values(columns: &Columns, position: usize) -> Vec<Value> {
		(0..columns.row_count()).map(|row| columns[position].get_value(row)).collect()
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
		assert_eq!(post.row_numbers(), &[RowNumber(1), RowNumber(2)]);
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
		let mut sort = Node::Sort(OperatorId(4));

		let out = sort
			.apply(&mut txn, Change::from_flow(OperatorId(2), version(), diffs.clone(), at_millis(5)))
			.unwrap();

		assert_eq!(out.origin, ChangeOrigin::Flow(OperatorId(4)));
		assert_eq!(out.version, version());
		assert_eq!(out.changed_at, at_millis(5));
		assert_eq!(format!("{:?}", out.diffs.as_slice()), format!("{:?}", diffs));
	}
}
