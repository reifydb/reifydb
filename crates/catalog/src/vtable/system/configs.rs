// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	util::ioc::IocContainer,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	materialized::MaterializedCatalog,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemConfigs {
	pub(crate) vtable: Arc<VTable>,
	ioc: IocContainer,
	exhausted: bool,
}

impl SystemConfigs {
	pub fn new(ioc: IocContainer) -> Self {
		Self {
			vtable: SystemCatalog::get_configs_table().clone(),
			ioc,
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemConfigs {
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
			for change in &t.inner.changes.config {
				if let Some(post) = &change.post
					&& let Some(cfg) = configs.iter_mut().find(|c| c.key == post.key)
				{
					cfg.value = post.value.clone();
				}
			}
		}

		let mut keys = ColumnData::utf8_with_capacity(configs.len());
		let mut values = ColumnData::utf8_with_capacity(configs.len());
		let mut default_values = ColumnData::utf8_with_capacity(configs.len());
		let mut descriptions = ColumnData::utf8_with_capacity(configs.len());
		let mut requires_restarts = ColumnData::bool_with_capacity(configs.len());

		for cfg in &configs {
			let key_str = cfg.key.to_string();
			keys.push(key_str.as_str());
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

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
