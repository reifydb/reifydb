// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use postcard::to_stdvec;
use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{
		bytes::{EncodedBytes, RowBuilder, SHAPE_HEADER_SIZE, read_created_at},
		shape::{RowFamily, RowShape, RowShapeField},
		table::EncodedTableRow,
	},
};
use reifydb_core::{
	interface::{
		catalog::{
			dictionary::Dictionary,
			flow::OperatorId,
			object::ObjectId,
			storage::StorageId,
			view::{View, ViewSortKey},
		},
		change::{Change, Diff},
	},
	key::partition::PartitionKey,
	partition::{PartitionError, partition_col_indices},
	row::row_shape_from_columns,
	value::column::{builder::ColumnBuilder, columns::Columns},
};
use reifydb_flow::operator::sink::{
	coerce_columns, encode_row_at_index,
	partition::{ensure_partition_unchanged, partition_of},
	shape_field_columns,
	view::{partitioned_key, sorted_view_key},
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	value::{Value, blob::Blob, partition::Partition, value_type::ValueType},
};

use crate::txn::{Emit, Intern, Lookup, Rows};

static REGISTRY_SHAPE: LazyLock<RowShape> =
	LazyLock::new(|| RowShape::new(RowFamily::Pod, vec![RowShapeField::unconstrained("values", ValueType::Blob)]));

pub struct TableSink {
	operator: OperatorId,
	view: View,
	storage: StorageId,
	shape: RowShape,
	sort: Vec<ViewSortKey>,
	partition_indices: Vec<usize>,
	runtime_context: RuntimeContext,
}

impl TableSink {
	pub(crate) fn new(operator: OperatorId, view: View, runtime_context: RuntimeContext) -> Self {
		let storage = view.storage_id();
		let shape = row_shape_from_columns(RowFamily::Table, view.columns());
		let sort = view.sort().to_vec();
		let partition_indices = partition_col_indices(view.columns(), view.partition_by());
		Self {
			operator,
			view,
			storage,
			shape,
			sort,
			partition_indices,
			runtime_context,
		}
	}

	pub(crate) fn id(&self) -> OperatorId {
		self.operator
	}

	pub fn apply<T: Rows + Emit + Lookup + Intern>(&mut self, txn: &mut T, change: Change) -> Result<()> {
		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_table_view_insert(txn, post)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_table_view_update(txn, pre, post)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_table_view_remove(txn, pre)?,
			}
		}
		Ok(())
	}

	fn is_partitioned(&self) -> bool {
		!self.partition_indices.is_empty()
	}

	fn apply_table_view_insert<T: Rows + Emit + Lookup + Intern>(&self, txn: &mut T, post: &Columns) -> Result<()> {
		let coerced = coerce_columns(post, self.view.columns(), &self.runtime_context)?;
		let dict_encoded = dictionary_encode_view_columns(txn, &self.view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_count = source.row_count();
		let field_columns = shape_field_columns(source, &self.shape);
		let mut keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut encoded_bytes_list: Vec<EncodedBytes> = Vec::with_capacity(row_count);

		for row_idx in 0..row_count {
			let row_number = source.row_numbers()[row_idx];
			let (_, encoded) =
				encode_row_at_index(source, row_idx, &self.shape, row_number, &field_columns)?;
			let key = if self.is_partitioned() {
				let (partition, values) = partition_of(&self.partition_indices, &coerced, row_idx);
				resolve_partition_flow(txn, ObjectId::from(self.storage), partition, &values)?;
				partitioned_key(self.storage, &self.sort, source, row_idx, partition, row_number)?
			} else {
				sorted_view_key(self.storage, &self.sort, source, row_idx, row_number)?
			};
			keys.push(key);
			encoded_bytes_list.push(encoded);
		}

		for (key, encoded) in keys.iter().zip(encoded_bytes_list) {
			txn.set(key, encoded)?;
		}

		txn.emit(self.view.id(), Diff::insert(coerced))
	}

	fn apply_table_view_update<T: Rows + Emit + Lookup + Intern>(
		&self,
		txn: &mut T,
		pre: &Columns,
		post: &Columns,
	) -> Result<()> {
		let coerced_pre = coerce_columns(pre, self.view.columns(), &self.runtime_context)?;
		let coerced_post = coerce_columns(post, self.view.columns(), &self.runtime_context)?;
		let dict_pre = dictionary_encode_view_columns(txn, &self.view, &coerced_pre)?;
		let dict_post = dictionary_encode_view_columns(txn, &self.view, &coerced_post)?;
		let source_pre = dict_pre.as_ref().unwrap_or(&coerced_pre);
		let source_post = dict_post.as_ref().unwrap_or(&coerced_post);
		let row_count = source_post.row_count();
		let field_columns = shape_field_columns(source_post, &self.shape);
		let mut pre_keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut post_keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut post_encoded_bytes_vec: Vec<EncodedBytes> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let pre_row_number = source_pre.row_numbers()[row_idx];
			let post_row_number = source_post.row_numbers()[row_idx];
			let (_, mut post_encoded) = encode_row_at_index(
				source_post,
				row_idx,
				&self.shape,
				post_row_number,
				&field_columns,
			)?;

			let (pre_key, post_key) = if self.is_partitioned() {
				let (pre_partition, _pre_values) =
					partition_of(&self.partition_indices, &coerced_pre, row_idx);
				let (post_partition, post_values) =
					partition_of(&self.partition_indices, &coerced_post, row_idx);
				ensure_partition_unchanged(
					ObjectId::from(self.storage),
					pre_partition,
					post_partition,
				)?;
				resolve_partition_flow(
					txn,
					ObjectId::from(self.storage),
					post_partition,
					&post_values,
				)?;
				(
					partitioned_key(
						self.storage,
						&self.sort,
						source_pre,
						row_idx,
						pre_partition,
						pre_row_number,
					)?,
					partitioned_key(
						self.storage,
						&self.sort,
						source_post,
						row_idx,
						post_partition,
						post_row_number,
					)?,
				)
			} else {
				(
					sorted_view_key(self.storage, &self.sort, source_pre, row_idx, pre_row_number)?,
					sorted_view_key(
						self.storage,
						&self.sort,
						source_post,
						row_idx,
						post_row_number,
					)?,
				)
			};

			let mut prior_created = match txn.get(&post_key)? {
				Some(prior) if prior.len() >= SHAPE_HEADER_SIZE => {
					Some(read_created_at(&prior)).filter(|c| !c.is_epoch())
				}
				_ => None,
			};
			if prior_created.is_none() && pre_key.as_slice() != post_key.as_slice() {
				prior_created = match txn.get(&pre_key)? {
					Some(prior) if prior.len() >= SHAPE_HEADER_SIZE => {
						Some(read_created_at(&prior)).filter(|c| !c.is_epoch())
					}
					_ => None,
				};
			}
			if let Some(c) = prior_created
				&& post_encoded.len() >= SHAPE_HEADER_SIZE
			{
				let updated = self.shape.updated_at(&post_encoded);
				let mut builder = EncodedTableRow::from(post_encoded).thaw();
				builder.set_timestamps(c, updated);
				post_encoded = builder.freeze_bytes();
			}

			pre_keys.push(pre_key);
			post_keys.push(post_key);
			post_encoded_bytes_vec.push(post_encoded);
		}

		for key in &pre_keys {
			txn.remove(key)?;
		}
		for (key, encoded) in post_keys.iter().zip(post_encoded_bytes_vec) {
			txn.set(key, encoded)?;
		}

		txn.emit(self.view.id(), Diff::update(coerced_pre, coerced_post))
	}

	fn apply_table_view_remove<T: Rows + Emit + Lookup + Intern>(&self, txn: &mut T, pre: &Columns) -> Result<()> {
		let coerced = coerce_columns(pre, self.view.columns(), &self.runtime_context)?;
		let dict_encoded = dictionary_encode_view_columns(txn, &self.view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_count = source.row_count();
		let mut keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let row_number = source.row_numbers()[row_idx];
			let key = if self.is_partitioned() {
				let (partition, _values) = partition_of(&self.partition_indices, &coerced, row_idx);
				partitioned_key(self.storage, &self.sort, source, row_idx, partition, row_number)?
			} else {
				sorted_view_key(self.storage, &self.sort, source, row_idx, row_number)?
			};
			keys.push(key);
		}

		for key in &keys {
			txn.remove(key)?;
		}

		txn.emit(self.view.id(), Diff::remove(coerced))
	}
}

fn dictionary_encode_view_columns<T: Lookup + Intern>(
	txn: &mut T,
	view: &View,
	columns: &Columns,
) -> Result<Option<Columns>> {
	let mut dict_columns: Vec<(usize, Dictionary)> = Vec::new();
	for (pos, col) in view.columns().iter().enumerate() {
		if let Some(dict_id) = col.dictionary_id {
			dict_columns.push((pos, txn.dictionary(dict_id)?));
		}
	}

	if dict_columns.is_empty() {
		return Ok(None);
	}

	let mut encoded = columns.clone();
	for (col_pos, dictionary) in &dict_columns {
		let row_count = encoded[*col_pos].len();
		let mut new_data = ColumnBuilder::with_capacity(ValueType::DictionaryId, row_count);
		for row_idx in 0..row_count {
			let value = encoded[*col_pos].get_value(row_idx);
			new_data.push_value(txn.intern(dictionary, &value)?.to_value());
		}
		encoded.columns[*col_pos] = new_data.finish();
	}

	Ok(Some(encoded))
}

fn resolve_partition_flow<T: Rows>(
	txn: &mut T,
	object: ObjectId,
	partition: Partition,
	values: &[Value],
) -> Result<()> {
	let key = PartitionKey::encoded(object, partition);
	let encoded = to_stdvec(values).expect("value postcard is total");
	let candidate = Value::Blob(Blob::from(encoded));
	match txn.get(&key)? {
		Some(row) => {
			if REGISTRY_SHAPE.get_value(&row, 0) != candidate {
				return Err(PartitionError::PartitionHashCollision {
					object,
					hash: partition.0,
				}
				.into());
			}
		}
		None => {
			let mut row = REGISTRY_SHAPE.allocate_pod();
			REGISTRY_SHAPE.set_value(&mut row, 0, &candidate);
			txn.set(&key, row.freeze().into())?;
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
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
		row::row_shape_from_columns,
		sort::SortDirection,
		value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
	};
	use reifydb_flow::operator::sink::{
		partition::ensure_partition_unchanged,
		view::{partitioned_key, row_key, sorted_view_key},
	};
	use reifydb_runtime::context::{
		RuntimeContext,
		clock::{Clock, MockClock},
	};
	use reifydb_value::{
		factory::time::at_millis,
		fragment::Fragment,
		value::{
			Value,
			constraint::{Constraint, TypeConstraint},
			datetime::DateTime,
			dictionary::DictionaryId,
			partition::Partition,
			row_number::RowNumber,
			system_columns::SystemColumns,
			value_type::ValueType,
		},
	};

	use super::TableSink;
	use crate::{memory::MemoryTxn, txn::Intern};

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

	fn positions(rows: &[(u64, &str, i64)], created: DateTime, updated: DateTime) -> Columns {
		let n = rows.len();
		Columns::with_system(
			vec![
				ColumnWithName::new(
					Fragment::internal("sym"),
					ColumnBuffer::utf8(
						rows.iter().map(|(_, sym, _)| sym.to_string()).collect::<Vec<_>>(),
					),
				),
				ColumnWithName::new(
					Fragment::internal("qty"),
					ColumnBuffer::int8(rows.iter().map(|(_, _, qty)| *qty).collect::<Vec<_>>()),
				),
			],
			SystemColumns::new(
				rows.iter().map(|(row, _, _)| RowNumber(*row)).collect(),
				Vec::new(),
				vec![created; n],
				vec![updated; n],
				Vec::new(),
				Vec::new(),
			),
		)
	}

	fn inserted(rows: &[(u64, &str, i64)]) -> Columns {
		positions(rows, at_millis(10), at_millis(20))
	}

	fn change(diffs: Vec<Diff>) -> Change {
		Change::from_flow(OperatorId(0), ChangeVersion::from(CommitVersion(1)), diffs, at_millis(0))
	}

	fn stored(txn: &MemoryTxn, view: &View, key: &EncodedKey) -> Vec<Value> {
		let shape = row_shape_from_columns(RowFamily::Table, view.columns());
		let row = txn.rows.get(key).expect("the view row must be stored under this key");
		(0..view.columns().len()).map(|index| shape.get_value(row, index)).collect()
	}

	fn values(columns: &Columns, position: usize) -> Vec<Value> {
		(0..columns.row_count()).map(|row| columns[position].get_value(row)).collect()
	}

	fn utf8(text: &str) -> Value {
		Value::Utf8(text.to_string())
	}

	#[test]
	fn an_insert_stores_each_source_row_under_its_row_key_and_emits_the_insert_once() {
		let mut txn = MemoryTxn::default();
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
		assert_eq!(post.row_numbers(), &[RowNumber(1), RowNumber(2)]);
		assert_eq!(values(post, 1), vec![Value::Int8(10), Value::Int8(20)]);
	}

	#[test]
	fn an_update_moves_the_row_to_its_new_key_keeps_the_prior_created_at_and_takes_updated_from_the_post_row() {
		let mut txn = MemoryTxn {
			clock: MockClock::from_millis(500),
			..Default::default()
		};
		let view = view(plain_symbol(), by_qty(), &[]);
		let storage = view.storage_id();
		let mut sink = sink(&view);
		let pre = inserted(&[(1, "sol", 10)]);
		let post = positions(&[(1, "sol", 30)], at_millis(77), at_millis(88));
		let pre_key = sorted_view_key(storage, &by_qty(), &pre, 0, RowNumber(1)).unwrap();
		let post_key = sorted_view_key(storage, &by_qty(), &post, 0, RowNumber(1)).unwrap();
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
		let mut txn = MemoryTxn::default();
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
		assert_eq!(pre.row_numbers(), &[RowNumber(1)]);
	}

	#[test]
	fn a_sorted_view_stores_rows_under_sort_keys_so_key_order_follows_the_sort_column() {
		let mut txn = MemoryTxn::default();
		let view = view(plain_symbol(), by_qty(), &[]);
		let storage = view.storage_id();
		let rows = inserted(&[(1, "sol", 30), (2, "eth", 10), (3, "btc", 20)]);

		sink(&view).apply(&mut txn, change(vec![Diff::insert(rows.clone())])).unwrap();

		assert_eq!(txn.rows.len(), 3);
		for (row_idx, row_number) in [RowNumber(1), RowNumber(2), RowNumber(3)].into_iter().enumerate() {
			let key = sorted_view_key(storage, &by_qty(), &rows, row_idx, row_number).unwrap();
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
		let mut txn = MemoryTxn::default();
		let view = view(plain_symbol(), Vec::new(), &["sym"]);
		let storage = view.storage_id();
		let rows = inserted(&[(1, "sol", 10), (2, "eth", 20)]);
		let sol = Partition::of(&[utf8("sol")]);
		let eth = Partition::of(&[utf8("eth")]);

		sink(&view).apply(&mut txn, change(vec![Diff::insert(rows.clone())])).unwrap();

		let sol_key = partitioned_key(storage, &[], &rows, 0, sol, RowNumber(1)).unwrap();
		let eth_key = partitioned_key(storage, &[], &rows, 1, eth, RowNumber(2)).unwrap();
		assert_eq!(stored(&txn, &view, &sol_key), vec![utf8("sol"), Value::Int8(10)]);
		assert_eq!(stored(&txn, &view, &eth_key), vec![utf8("eth"), Value::Int8(20)]);
		assert!(!txn.rows.contains_key(&row_key(storage, RowNumber(1))));
		assert!(txn.rows.contains_key(&PartitionKey::encoded(ObjectId::view(VIEW), sol)));
		assert!(txn.rows.contains_key(&PartitionKey::encoded(ObjectId::view(VIEW), eth)));
		assert_eq!(txn.rows.len(), 4);
	}

	#[test]
	fn an_update_that_moves_a_row_to_another_partition_is_refused_before_anything_is_written() {
		let mut txn = MemoryTxn::default();
		let view = view(plain_symbol(), Vec::new(), &["sym"]);
		let mut sink = sink(&view);
		sink.apply(&mut txn, change(vec![Diff::insert(inserted(&[(1, "sol", 10)]))])).unwrap();
		let rows_before = txn.rows.clone();
		let expected = ensure_partition_unchanged(
			ObjectId::view(VIEW),
			Partition::of(&[utf8("sol")]),
			Partition::of(&[utf8("eth")]),
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
		let mut txn = MemoryTxn::default();
		txn.dictionaries.insert(SYMBOLS, symbols());
		txn.intern(&symbols(), &utf8("eth")).unwrap();
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

		let sol = txn.intern(&symbols(), &utf8("sol")).unwrap();
		let eth = txn.intern(&symbols(), &utf8("eth")).unwrap();
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
