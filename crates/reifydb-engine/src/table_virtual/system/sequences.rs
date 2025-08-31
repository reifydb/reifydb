// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	Result,
	interface::{Transaction, VirtualTableDef},
	value::columnar::{Column, ColumnData, ColumnQualified, Columns},
};

use crate::{
	StandardTransaction,
	table_virtual::{VirtualTable, VirtualTableQueryContext},
};

/// Virtual table that exposes system sequence information
pub struct Sequences<T: Transaction> {
	definition: VirtualTableDef,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> Sequences<T> {
	pub fn new(definition: VirtualTableDef) -> Self {
		Self {
			definition,
			_phantom: PhantomData,
		}
	}
}

impl<T: Transaction> VirtualTable<T> for Sequences<T> {
	fn query(
		&self,
		_ctx: VirtualTableQueryContext,
		txn: &mut StandardTransaction<T>,
	) -> Result<Columns> {
		let mut sequence_ids = Vec::new();
		let mut schema_ids = Vec::new();
		let mut schema_names = Vec::new();
		let mut sequence_names = Vec::new();
		let mut current_values = Vec::new();

		let sequences = CatalogStore::list_sequences(txn)?;
		for sequence in sequences {
			sequence_ids.push(sequence.id.0);

			debug_assert_eq!(sequence.schema, 1);
			schema_ids.push(sequence.schema.0);
			schema_names.push("system".to_string());

			sequence_names.push(sequence.name);
			current_values.push(sequence.value);
		}

		let columns = vec![
			Column::ColumnQualified(ColumnQualified {
				name: "id".to_string(),
				data: ColumnData::uint4(sequence_ids),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "schema_id".to_string(),
				data: ColumnData::uint8(schema_ids),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "schema_schema".to_string(),
				data: ColumnData::utf8(sequence_names),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "name".to_string(),
				data: ColumnData::utf8(schema_names),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "value".to_string(),
				data: ColumnData::uint8(current_values),
			}),
		];

		Ok(Columns::new(columns))
	}

	fn definition(&self) -> &VirtualTableDef {
		&self.definition
	}
}
