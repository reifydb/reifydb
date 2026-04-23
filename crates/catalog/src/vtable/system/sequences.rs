// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes system sequence information
pub struct SystemSequences {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemSequences {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemSequences {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_sequences_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemSequences {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
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
			ColumnWithName::new(Fragment::internal("id"), ColumnBuffer::uint8(sequence_ids)),
			ColumnWithName::new(Fragment::internal("namespace_id"), ColumnBuffer::uint8(namespace_ids)),
			ColumnWithName::new(Fragment::internal("namespace_name"), ColumnBuffer::utf8(namespace_names)),
			ColumnWithName::new(Fragment::internal("name"), ColumnBuffer::utf8(sequence_names)),
			ColumnWithName::new(Fragment::internal("value"), ColumnBuffer::uint8(current_values)),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
