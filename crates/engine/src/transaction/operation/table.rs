// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::{encoded::EncodedValues, schema::Schema},
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

fn build_encoded_columns(table: &TableDef, row_number: RowNumber, encoded: &EncodedValues) -> Columns {
	let schema: Schema = (&table.columns).into();
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

fn build_table_insert_change(table: &TableDef, row_number: RowNumber, encoded: &EncodedValues) -> Change {
	Change {
		origin: ChangeOrigin::Primitive(PrimitiveId::Table(table.id)),
		version: CommitVersion(0),
		diffs: vec![Diff::Insert {
			post: build_encoded_columns(table, row_number, encoded),
		}],
	}
}

fn build_table_update_change(
	table: &TableDef,
	row_number: RowNumber,
	old: &EncodedValues,
	new: &EncodedValues,
) -> Change {
	Change {
		origin: ChangeOrigin::Primitive(PrimitiveId::Table(table.id)),
		version: CommitVersion(0),
		diffs: vec![Diff::Update {
			pre: build_encoded_columns(table, row_number, old),
			post: build_encoded_columns(table, row_number, new),
		}],
	}
}

fn build_table_remove_change(table: &TableDef, row_number: RowNumber, encoded: &EncodedValues) -> Change {
	Change {
		origin: ChangeOrigin::Primitive(PrimitiveId::Table(table.id)),
		version: CommitVersion(0),
		diffs: vec![Diff::Remove {
			pre: build_encoded_columns(table, row_number, encoded),
		}],
	}
}

pub(crate) trait TableOperations {
	fn insert_table(&mut self, table: TableDef, row: EncodedValues, row_number: RowNumber) -> crate::Result<()>;

	fn update_table(&mut self, table: TableDef, id: RowNumber, row: EncodedValues) -> crate::Result<()>;

	fn remove_from_table(&mut self, table: TableDef, id: RowNumber) -> crate::Result<()>;
}

impl TableOperations for CommandTransaction {
	fn insert_table(&mut self, table: TableDef, row: EncodedValues, row_number: RowNumber) -> crate::Result<()> {
		TableInterceptor::pre_insert(self, &table, row_number, &row)?;

		self.set(&RowKey::encoded(table.id, row_number), row.clone())?;

		TableInterceptor::post_insert(self, &table, row_number, &row)?;

		// Track insertion for post-commit event emission
		self.track_row_change(RowChange::TableInsert(TableRowInsertion {
			table_id: table.id,
			row_number,
			encoded: row.clone(),
		}));

		// Track flow change for transactional view pre-commit processing
		self.track_flow_change(build_table_insert_change(&table, row_number, &row));

		Ok(())
	}

	fn update_table(&mut self, table: TableDef, id: RowNumber, row: EncodedValues) -> crate::Result<()> {
		let key = RowKey::encoded(table.id, id);

		let old_values = match self.get(&key)? {
			Some(v) => v.values,
			None => return Ok(()),
		};

		TableInterceptor::pre_update(self, &table, id, &row)?;

		self.set(&key, row.clone())?;

		self.track_flow_change(build_table_update_change(&table, id, &old_values, &row));

		Ok(())
	}

	fn remove_from_table(&mut self, table: TableDef, id: RowNumber) -> crate::Result<()> {
		let key = RowKey::encoded(table.id, id);

		let deleted_values = match self.get(&key)? {
			Some(v) => v.values,
			None => return Ok(()),
		};

		TableInterceptor::pre_delete(self, &table, id)?;

		self.unset(&key, deleted_values.clone())?;

		self.track_flow_change(build_table_remove_change(&table, id, &deleted_values));

		Ok(())
	}
}

impl TableOperations for AdminTransaction {
	fn insert_table(&mut self, table: TableDef, row: EncodedValues, row_number: RowNumber) -> crate::Result<()> {
		TableInterceptor::pre_insert(self, &table, row_number, &row)?;

		self.set(&RowKey::encoded(table.id, row_number), row.clone())?;

		TableInterceptor::post_insert(self, &table, row_number, &row)?;

		// Track insertion for post-commit event emission
		self.track_row_change(RowChange::TableInsert(TableRowInsertion {
			table_id: table.id,
			row_number,
			encoded: row.clone(),
		}));

		// Track flow change for transactional view pre-commit processing
		self.track_flow_change(build_table_insert_change(&table, row_number, &row));

		Ok(())
	}

	fn update_table(&mut self, table: TableDef, id: RowNumber, row: EncodedValues) -> crate::Result<()> {
		let key = RowKey::encoded(table.id, id);

		let old_values = match self.get(&key)? {
			Some(v) => v.values,
			None => return Ok(()),
		};

		TableInterceptor::pre_update(self, &table, id, &row)?;

		self.set(&key, row.clone())?;

		self.track_flow_change(build_table_update_change(&table, id, &old_values, &row));

		Ok(())
	}

	fn remove_from_table(&mut self, table: TableDef, id: RowNumber) -> crate::Result<()> {
		let key = RowKey::encoded(table.id, id);

		let deleted_values = match self.get(&key)? {
			Some(v) => v.values,
			None => return Ok(()),
		};

		TableInterceptor::pre_delete(self, &table, id)?;

		self.unset(&key, deleted_values.clone())?;

		self.track_flow_change(build_table_remove_change(&table, id, &deleted_values));

		Ok(())
	}
}

impl TableOperations for Transaction<'_> {
	fn insert_table(&mut self, table: TableDef, row: EncodedValues, row_number: RowNumber) -> crate::Result<()> {
		match self {
			Transaction::Command(txn) => txn.insert_table(table, row, row_number),
			Transaction::Admin(txn) => txn.insert_table(table, row, row_number),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	fn update_table(&mut self, table: TableDef, id: RowNumber, row: EncodedValues) -> crate::Result<()> {
		match self {
			Transaction::Command(txn) => txn.update_table(table, id, row),
			Transaction::Admin(txn) => txn.update_table(table, id, row),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	fn remove_from_table(&mut self, table: TableDef, id: RowNumber) -> crate::Result<()> {
		match self {
			Transaction::Command(txn) => txn.remove_from_table(table, id),
			Transaction::Admin(txn) => txn.remove_from_table(table, id),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}
}
