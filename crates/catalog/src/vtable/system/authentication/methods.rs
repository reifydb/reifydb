// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::value_type::ValueType;
use serde_json::to_string;

use crate::{
	CatalogStore, Result,
	error::CatalogError,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

const FOREVER: &str = "forever";

pub struct SystemAuthenticationMethods {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemAuthenticationMethods {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemAuthenticationMethods {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_authentication_methods_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemAuthenticationMethods {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let methods = CatalogStore::list_all_authentication_methods(txn)?;

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, methods.len());
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, methods.len());
		let mut ttls = ColumnBuilder::with_capacity(ValueType::Utf8, methods.len());
		let mut promote_guests = ColumnBuilder::with_capacity(ValueType::Boolean, methods.len());
		let mut properties = ColumnBuilder::with_capacity(ValueType::Utf8, methods.len());

		for m in methods {
			let json =
				to_string(&m.properties).map_err(|e| CatalogError::AuthenticationMethodProperties {
					method: m.name.clone(),
					reason: e.to_string(),
				})?;
			ids.push(m.id);
			names.push(m.name.as_str());
			ttls.push(m.ttl.map(|ttl| ttl.to_string()).unwrap_or_else(|| FOREVER.to_string()).as_str());
			promote_guests.push(m.promote_guest);
			properties.push(json.as_str());
		}

		let columns = vec![
			ids.finish("id"),
			names.finish("name"),
			ttls.finish("ttl"),
			promote_guests.finish("promote_guest"),
			properties.finish("properties"),
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
