// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	interface::catalog::{identity::IdentityAttributeId, vtable::VTable},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemIdentityAttributeValues {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemIdentityAttributeValues {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemIdentityAttributeValues {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_identity_attribute_values_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemIdentityAttributeValues {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let attribute_names: HashMap<IdentityAttributeId, String> =
			CatalogStore::list_all_identity_attributes(txn)?.into_iter().map(|a| (a.id, a.name)).collect();

		let values = CatalogStore::list_all_identity_attribute_values(txn)?;
		let mut rows: Vec<_> = values
			.into_iter()
			.map(|v| (attribute_names.get(&v.attribute).cloned().unwrap_or_default(), v))
			.collect();
		rows.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.value.cmp(&b.1.value)));

		let mut identities = ColumnBuffer::identity_id_with_capacity(rows.len());
		let mut attribute_ids = ColumnBuffer::uint8_with_capacity(rows.len());
		let mut attributes = ColumnBuffer::utf8_with_capacity(rows.len());
		let mut value_strings = ColumnBuffer::utf8_with_capacity(rows.len());

		for (name, v) in rows {
			identities.push(v.identity);
			attribute_ids.push(v.attribute);
			attributes.push(name);
			value_strings.push(v.value);
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("identity"), identities),
			ColumnWithName::new(Fragment::internal("attribute_id"), attribute_ids),
			ColumnWithName::new(Fragment::internal("attribute"), attributes),
			ColumnWithName::new(Fragment::internal("value"), value_strings),
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
