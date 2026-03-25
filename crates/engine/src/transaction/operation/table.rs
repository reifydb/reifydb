// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::{row::EncodedRow, schema::Schema},
	interface::{
		catalog::{primitive::PrimitiveId, table::TableDef},
		change::{Change, ChangeOrigin, Diff},
	},
	key::row::RowKey,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::{
	change::{RowChange, TableRowInsertion},
	interceptor::table::TableInterceptor,
	transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction},
};
use reifydb_type::{fragment::Fragment, util::cowvec::CowVec, value::row_number::RowNumber};

use crate::Result;

fn build_encoded_columns(schema: &Schema, row_number: RowNumber, encoded: &EncodedRow) -> Columns {
	let fields = schema.fields();

	let mut columns_vec: Vec<Column> = Vec::with_capacity(fields.len());
	for field in fields.iter() {
		columns_vec.push(Column {
			name: Fragment::internal(&field.name),
			data: ColumnData::with_capacity(field.constraint.get_type(), 1),
		});
	}

	for (i, _) in fields.iter().enumerate() {
		columns_vec[i].data.push_value(schema.get_value(encoded, i));
	}

	Columns {
		row_numbers: CowVec::new(vec![row_number]),
		columns: CowVec::new(columns_vec),
	}
}

fn build_table_insert_change(table: &TableDef, schema: &Schema, row_number: RowNumber, encoded: &EncodedRow) -> Change {
	Change {
		origin: ChangeOrigin::Primitive(PrimitiveId::Table(table.id)),
		version: CommitVersion(0),
		diffs: vec![Diff::Insert {
			post: build_encoded_columns(schema, row_number, encoded),
		}],
	}
}

fn build_table_update_change(table: &TableDef, row_number: RowNumber, old: &EncodedRow, new: &EncodedRow) -> Change {
	let schema: Schema = (&table.columns).into();
	Change {
		origin: ChangeOrigin::Primitive(PrimitiveId::Table(table.id)),
		version: CommitVersion(0),
		diffs: vec![Diff::Update {
			pre: build_encoded_columns(&schema, row_number, old),
			post: build_encoded_columns(&schema, row_number, new),
		}],
	}
}

fn build_table_remove_change(table: &TableDef, row_number: RowNumber, encoded: &EncodedRow) -> Change {
	let schema: Schema = (&table.columns).into();
	Change {
		origin: ChangeOrigin::Primitive(PrimitiveId::Table(table.id)),
		version: CommitVersion(0),
		diffs: vec![Diff::Remove {
			pre: build_encoded_columns(&schema, row_number, encoded),
		}],
	}
}

pub(crate) trait TableOperations {
	fn insert_table(
		&mut self,
		table: &TableDef,
		schema: &Schema,
		row: EncodedRow,
		row_number: RowNumber,
	) -> Result<EncodedRow>;

	fn update_table(&mut self, table: TableDef, id: RowNumber, row: EncodedRow) -> Result<EncodedRow>;

	fn remove_from_table(&mut self, table: TableDef, id: RowNumber) -> Result<EncodedRow>;
}

impl TableOperations for CommandTransaction {
	fn insert_table(
		&mut self,
		table: &TableDef,
		schema: &Schema,
		row: EncodedRow,
		row_number: RowNumber,
	) -> Result<EncodedRow> {
		let row = TableInterceptor::pre_insert(self, table, row_number, row)?;

		self.set(&RowKey::encoded(table.id, row_number), row.clone())?;

		TableInterceptor::post_insert(self, table, row_number, &row)?;

		// Track insertion for post-commit event emission
		self.track_row_change(RowChange::TableInsert(TableRowInsertion {
			table_id: table.id,
			row_number,
			encoded: row.clone(),
		}));

		// Track flow change for transactional view pre-commit processing
		self.track_flow_change(build_table_insert_change(table, schema, row_number, &row));

		Ok(row)
	}

	fn update_table(&mut self, table: TableDef, id: RowNumber, row: EncodedRow) -> Result<EncodedRow> {
		let key = RowKey::encoded(table.id, id);

		let old_values = match self.get(&key)? {
			Some(v) => v.row,
			None => return Ok(row),
		};

		let row = TableInterceptor::pre_update(self, &table, id, row)?;

		self.set(&key, row.clone())?;

		TableInterceptor::post_update(self, &table, id, &row, &old_values)?;

		self.track_flow_change(build_table_update_change(&table, id, &old_values, &row));

		Ok(row)
	}

	fn remove_from_table(&mut self, table: TableDef, id: RowNumber) -> Result<EncodedRow> {
		let key = RowKey::encoded(table.id, id);

		let deleted_values = match self.get(&key)? {
			Some(v) => v.row,
			None => return Ok(EncodedRow(CowVec::new(vec![]))),
		};

		TableInterceptor::pre_delete(self, &table, id)?;

		self.unset(&key, deleted_values.clone())?;

		TableInterceptor::post_delete(self, &table, id, &deleted_values)?;

		self.track_flow_change(build_table_remove_change(&table, id, &deleted_values));

		Ok(deleted_values)
	}
}

impl TableOperations for AdminTransaction {
	fn insert_table(
		&mut self,
		table: &TableDef,
		schema: &Schema,
		row: EncodedRow,
		row_number: RowNumber,
	) -> Result<EncodedRow> {
		let row = TableInterceptor::pre_insert(self, table, row_number, row)?;

		self.set(&RowKey::encoded(table.id, row_number), row.clone())?;

		TableInterceptor::post_insert(self, table, row_number, &row)?;

		// Track insertion for post-commit event emission
		self.track_row_change(RowChange::TableInsert(TableRowInsertion {
			table_id: table.id,
			row_number,
			encoded: row.clone(),
		}));

		// Track flow change for transactional view pre-commit processing
		self.track_flow_change(build_table_insert_change(table, schema, row_number, &row));

		Ok(row)
	}

	fn update_table(&mut self, table: TableDef, id: RowNumber, row: EncodedRow) -> Result<EncodedRow> {
		let key = RowKey::encoded(table.id, id);

		let old_values = match self.get(&key)? {
			Some(v) => v.row,
			None => return Ok(row),
		};

		let row = TableInterceptor::pre_update(self, &table, id, row)?;

		self.set(&key, row.clone())?;

		TableInterceptor::post_update(self, &table, id, &row, &old_values)?;

		self.track_flow_change(build_table_update_change(&table, id, &old_values, &row));

		Ok(row)
	}

	fn remove_from_table(&mut self, table: TableDef, id: RowNumber) -> Result<EncodedRow> {
		let key = RowKey::encoded(table.id, id);

		let deleted_values = match self.get(&key)? {
			Some(v) => v.row,
			None => return Ok(EncodedRow(CowVec::new(vec![]))),
		};

		TableInterceptor::pre_delete(self, &table, id)?;

		self.unset(&key, deleted_values.clone())?;

		TableInterceptor::post_delete(self, &table, id, &deleted_values)?;

		self.track_flow_change(build_table_remove_change(&table, id, &deleted_values));

		Ok(deleted_values)
	}
}

impl TableOperations for Transaction<'_> {
	fn insert_table(
		&mut self,
		table: &TableDef,
		schema: &Schema,
		row: EncodedRow,
		row_number: RowNumber,
	) -> Result<EncodedRow> {
		match self {
			Transaction::Command(txn) => txn.insert_table(table, schema, row, row_number),
			Transaction::Admin(txn) => txn.insert_table(table, schema, row, row_number),
			Transaction::Subscription(txn) => {
				txn.as_admin_mut().insert_table(table, schema, row, row_number)
			}
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	fn update_table(&mut self, table: TableDef, id: RowNumber, row: EncodedRow) -> Result<EncodedRow> {
		match self {
			Transaction::Command(txn) => txn.update_table(table, id, row),
			Transaction::Admin(txn) => txn.update_table(table, id, row),
			Transaction::Subscription(txn) => txn.as_admin_mut().update_table(table, id, row),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	fn remove_from_table(&mut self, table: TableDef, id: RowNumber) -> Result<EncodedRow> {
		match self {
			Transaction::Command(txn) => txn.remove_from_table(table, id),
			Transaction::Admin(txn) => txn.remove_from_table(table, id),
			Transaction::Subscription(txn) => txn.as_admin_mut().remove_from_table(table, id),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}
}
