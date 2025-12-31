// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	interface::VTableDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::Fragment;

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

#[async_trait]
impl<T: IntoStandardTransaction> VTable<T> for Sequences {
	async fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	async fn next(&mut self, txn: &mut T) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut sequence_ids = Vec::new();
		let mut namespace_ids = Vec::new();
		let mut namespace_names = Vec::new();
		let mut sequence_names = Vec::new();
		let mut current_values = Vec::new();

		let sequences = CatalogStore::list_sequences(txn).await?;
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
