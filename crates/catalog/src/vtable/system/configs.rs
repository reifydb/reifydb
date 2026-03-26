// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	util::ioc::IocContainer,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	materialized::MaterializedCatalog,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

pub struct Configs {
	pub(crate) definition: Arc<VTableDef>,
	ioc: IocContainer,
	exhausted: bool,
}

impl Configs {
	pub fn new(ioc: IocContainer) -> Self {
		Self {
			definition: SystemCatalog::get_system_configs_table_def().clone(),
			ioc,
			exhausted: false,
		}
	}
}

impl VTable for Configs {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let version = txn.version();
		let mut configs = match self.ioc.resolve::<MaterializedCatalog>() {
			Ok(catalog) => catalog.list_configs_at(version),
			Err(_) => vec![],
		};

		if let Transaction::Test(t) = txn {
			for (key, value) in &t.inner.changes.config_changes {
				if let Some(cfg) = configs.iter_mut().find(|c| c.key == *key) {
					cfg.value = value.clone();
				}
			}
		}

		let mut keys = ColumnData::utf8_with_capacity(configs.len());
		let mut values = ColumnData::utf8_with_capacity(configs.len());
		let mut default_values = ColumnData::utf8_with_capacity(configs.len());
		let mut descriptions = ColumnData::utf8_with_capacity(configs.len());
		let mut requires_restarts = ColumnData::bool_with_capacity(configs.len());

		for cfg in &configs {
			keys.push(cfg.key.as_str());
			values.push(cfg.value.as_string().as_str());
			default_values.push(cfg.default_value.as_string().as_str());
			descriptions.push(cfg.description);
			requires_restarts.push(cfg.requires_restart);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("key"),
				data: keys,
			},
			Column {
				name: Fragment::internal("value"),
				data: values,
			},
			Column {
				name: Fragment::internal("default_value"),
				data: default_values,
			},
			Column {
				name: Fragment::internal("description"),
				data: descriptions,
			},
			Column {
				name: Fragment::internal("requires_restart"),
				data: requires_restarts,
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
