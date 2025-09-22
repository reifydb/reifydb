// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, sync::Arc};

use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::{TableVirtualDef, Transaction},
	value::columnar::{Column, ColumnComputed, ColumnData, Columns},
};
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system sequence information
pub struct Sequences<T: Transaction> {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> Sequences<T> {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_sequences_table_def().clone(),
			exhausted: false,
			_phantom: PhantomData,
		}
	}
}

impl<'a, T: Transaction> TableVirtual<'a, T> for Sequences<T> {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a, T>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a, T>) -> Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut sequence_ids = Vec::new();
		let mut namespace_ids = Vec::new();
		let mut namespace_names = Vec::new();
		let mut sequence_names = Vec::new();
		let mut current_values = Vec::new();

		let sequences = CatalogStore::list_sequences(txn)?;
		for sequence in sequences {
			sequence_ids.push(sequence.id.0);

			debug_assert_eq!(sequence.namespace, 1);
			namespace_ids.push(sequence.namespace.0);
			namespace_names.push("system".to_string());

			sequence_names.push(sequence.name);
			current_values.push(sequence.value);
		}

		let columns = vec![
			Column::Computed(ColumnComputed {
				name: Fragment::owned_internal("id"),
				data: ColumnData::uint8(sequence_ids),
			}),
			Column::Computed(ColumnComputed {
				name: Fragment::owned_internal("namespace_id"),
				data: ColumnData::uint8(namespace_ids),
			}),
			Column::Computed(ColumnComputed {
				name: Fragment::owned_internal("namespace_name"),
				data: ColumnData::utf8(namespace_names),
			}),
			Column::Computed(ColumnComputed {
				name: Fragment::owned_internal("name"),
				data: ColumnData::utf8(sequence_names),
			}),
			Column::Computed(ColumnComputed {
				name: Fragment::owned_internal("value"),
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
