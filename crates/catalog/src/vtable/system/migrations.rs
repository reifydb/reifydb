// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{migration::MigrationAction, vtable::VTable},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes migration vtables and their latest action
pub struct SystemMigrations {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemMigrations {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemMigrations {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_migrations_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemMigrations {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let defs = CatalogStore::list_migrations(txn)?;
		let events = CatalogStore::list_migration_events(txn)?;

		let mut names = ColumnData::utf8_with_capacity(defs.len());
		let mut actions = ColumnData::utf8_with_capacity(defs.len());
		let mut bodies = ColumnData::utf8_with_capacity(defs.len());
		let mut rollback_bodies = ColumnData::utf8_with_capacity(defs.len());

		for def in &defs {
			let latest = events.iter().rfind(|e| e.migration_id == def.id);

			let action_str = match latest {
				Some(e) => match e.action {
					MigrationAction::Applied => "Applied",
					MigrationAction::Rollback => "Rollback",
				},
				None => "Pending",
			};

			names.push(def.name.as_str());
			actions.push(action_str);
			bodies.push(def.body.as_str());
			rollback_bodies.push(def.rollback_body.as_deref().unwrap_or(""));
		}

		let columns = vec![
			Column {
				name: Fragment::internal("name"),
				data: names,
			},
			Column {
				name: Fragment::internal("action"),
				data: actions,
			},
			Column {
				name: Fragment::internal("body"),
				data: bodies,
			},
			Column {
				name: Fragment::internal("rollback_body"),
				data: rollback_bodies,
			},
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
