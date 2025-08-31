// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	Result,
	interface::{TableVirtualDef, Transaction},
	value::columnar::{Column, ColumnData, ColumnQualified, Columns},
};

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system sequence information
pub struct Sequences<T: Transaction> {
	definition: TableVirtualDef,
	exhausted: bool,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> Sequences<T> {
	pub fn new(definition: TableVirtualDef) -> Self {
		Self {
			definition,
			exhausted: false,
			_phantom: PhantomData,
		}
	}
}

impl<'a, T: Transaction> TableVirtual<'a, T> for Sequences<T> {
	fn initialize(
		&mut self,
		_txn: &mut StandardTransaction<'a, T>,
		_ctx: TableVirtualContext<'a>,
	) -> Result<()> {
		// Store context (we need to handle lifetime properly)
		// For now, we don't store the context since Sequences doesn't
		// use pushdown In a real implementation with pushdown, we'd
		// process the context here
		self.exhausted = false;
		// Note: We're not storing the context as Sequences doesn't
		// support pushdown and the Basic context only has params
		// which we don't need
		Ok(())
	}

	fn next(
		&mut self,
		txn: &mut StandardTransaction<'a, T>,
	) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

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

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &TableVirtualDef {
		&self.definition
	}
}
