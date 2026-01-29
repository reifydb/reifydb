// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system sequence information
pub struct Sequences {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl Sequences {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_sequences_table_def().clone(),
			exhausted: false,
		}
	}
}

impl<T: AsTransaction> VTable<T> for Sequences {
	fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut T) -> crate::Result<Option<Batch>> {
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
			Column {
				name: Fragment::internal("id"),
				data: ColumnData::uint8(sequence_ids),
			},
			Column {
				name: Fragment::internal("namespace_id"),
				data: ColumnData::uint8(namespace_ids),
			},
			Column {
				name: Fragment::internal("namespace_name"),
				data: ColumnData::utf8(namespace_names),
			},
			Column {
				name: Fragment::internal("name"),
				data: ColumnData::utf8(sequence_names),
			},
			Column {
				name: Fragment::internal("value"),
				data: ColumnData::uint8(current_values),
			},
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}
