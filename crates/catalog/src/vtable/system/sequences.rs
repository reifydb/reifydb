// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::{batch::batch, column::factory},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::reifydb_assertions;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

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
		let mut sequence_names = Vec::new();
		let mut current_values = Vec::new();

		let sequences = CatalogStore::list_sequences(txn)?;
		for sequence in sequences {
			sequence_ids.push(sequence.id.0);

			reifydb_assertions! {
				assert_eq!(sequence.namespace, 1);
			}
			namespace_ids.push(sequence.namespace.0);
			sequence_names.push(sequence.name);
			current_values.push(sequence.value);
		}

		let columns = vec![
			factory::uint8("id", sequence_ids),
			factory::uint8("namespace_id", namespace_ids),
			factory::utf8("name", sequence_names),
			factory::uint8("value", current_values),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
