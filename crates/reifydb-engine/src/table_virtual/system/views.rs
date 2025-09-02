// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, sync::Arc};

use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::{TableVirtualDef, Transaction},
	value::columnar::{Column, ColumnData, ColumnQualified, Columns},
};
use reifydb_type::Value;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system view information
pub struct Views<T: Transaction> {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> Views<T> {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::views().clone(),
			exhausted: false,
			_phantom: PhantomData,
		}
	}
}

impl<'a, T: Transaction> TableVirtual<'a, T> for Views<T> {
	fn initialize(
		&mut self,
		_txn: &mut StandardTransaction<'a, T>,
		_ctx: TableVirtualContext<'a>,
	) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(
		&mut self,
		txn: &mut StandardTransaction<'a, T>,
	) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let views = CatalogStore::list_views_all(txn)?;

		let mut ids = ColumnData::uint8_with_capacity(views.len());
		let mut schemas = ColumnData::uint8_with_capacity(views.len());
		let mut names = ColumnData::utf8_with_capacity(views.len());
		let mut primary_keys =
			ColumnData::uint4_with_capacity(views.len());

		for view in views {
			ids.push(view.id.0);
			schemas.push(view.schema.0);
			names.push(view.name.as_str());
			primary_keys.push_value(
				view.primary_key
					.map(|pk| pk.id.0)
					.map(Value::Uint8)
					.unwrap_or(Value::Undefined),
			);
		}

		let columns = vec![
			Column::ColumnQualified(ColumnQualified {
				name: "id".to_string(),
				data: ids,
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "schema_id".to_string(),
				data: schemas,
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "name".to_string(),
				data: names,
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "primary_key_id".to_string(),
				data: primary_keys,
			}),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &TableVirtualDef {
		&self.definition
	}
}
