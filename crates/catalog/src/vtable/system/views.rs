// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{view::ViewKind, vtable::VTable},
	value::column::{ColumnWithName, builder::ColumnBuilder, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	value::{Value, value_type::ValueType},
};

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemViews {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemViews {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemViews {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_views_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemViews {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let views = CatalogStore::list_views_all(txn)?;

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, views.len());
		let mut namespaces = ColumnBuilder::with_capacity(ValueType::Uint8, views.len());
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, views.len());
		let mut kinds = ColumnBuilder::with_capacity(ValueType::Utf8, views.len());
		let mut primary_keys = ColumnBuilder::with_capacity(ValueType::Uint8, views.len());

		for view in views {
			ids.push(view.id().0);
			namespaces.push(view.namespace().0);
			names.push(view.name());
			kinds.push(match view.kind() {
				ViewKind::Deferred => "deferred",
				ViewKind::Transactional => "transactional",
			});
			primary_keys.push_value(
				view.primary_key()
					.map(|pk| pk.id.0)
					.map(Value::Uint8)
					.unwrap_or(Value::none_of(ValueType::Uint8)),
			);
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids.finish()),
			ColumnWithName::new(Fragment::internal("namespace_id"), namespaces.finish()),
			ColumnWithName::new(Fragment::internal("name"), names.finish()),
			ColumnWithName::new(Fragment::internal("kind"), kinds.finish()),
			ColumnWithName::new(Fragment::internal("primary_key_id"), primary_keys.finish()),
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
