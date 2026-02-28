// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{series::TimestampPrecision, vtable::VTableDef},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::{Value, r#type::Type},
};

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system series (time-series) information
pub struct Series {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl Series {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_series_table_def().clone(),
			exhausted: false,
		}
	}
}

impl VTable for Series {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let all_series = CatalogStore::list_series_all(txn)?;

		let mut ids = ColumnData::uint8_with_capacity(all_series.len());
		let mut namespaces = ColumnData::uint8_with_capacity(all_series.len());
		let mut names = ColumnData::utf8_with_capacity(all_series.len());
		let mut tag_ids = ColumnData::uint8_with_capacity(all_series.len());
		let mut precisions = ColumnData::utf8_with_capacity(all_series.len());

		for s in all_series {
			ids.push(s.id.0);
			namespaces.push(s.namespace.0);
			names.push(s.name.as_str());
			tag_ids.push_value(s.tag.map(|t| Value::Uint8(t.0)).unwrap_or(Value::none_of(Type::Uint8)));
			precisions.push(match s.precision {
				TimestampPrecision::Millisecond => "millisecond",
				TimestampPrecision::Microsecond => "microsecond",
				TimestampPrecision::Nanosecond => "nanosecond",
			});
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::internal("namespace_id"),
				data: namespaces,
			},
			Column {
				name: Fragment::internal("name"),
				data: names,
			},
			Column {
				name: Fragment::internal("tag_id"),
				data: tag_ids,
			},
			Column {
				name: Fragment::internal("precision"),
				data: precisions,
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
