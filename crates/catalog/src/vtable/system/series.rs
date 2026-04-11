// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{
		series::{SeriesKey, TimestampPrecision},
		vtable::VTable,
	},
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
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes system series (time-series) information
pub struct SystemSeries {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemSeries {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemSeries {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_series_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemSeries {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let all_series: Vec<_> =
			CatalogStore::list_series_all(txn)?.into_iter().filter(|s| !s.underlying).collect();

		let mut ids = ColumnData::uint8_with_capacity(all_series.len());
		let mut namespaces = ColumnData::uint8_with_capacity(all_series.len());
		let mut names = ColumnData::utf8_with_capacity(all_series.len());
		let mut tag_ids = ColumnData::uint8_with_capacity(all_series.len());
		let mut key_columns = ColumnData::utf8_with_capacity(all_series.len());
		let mut key_kinds = ColumnData::utf8_with_capacity(all_series.len());

		for s in all_series {
			ids.push(s.id.0);
			namespaces.push(s.namespace.0);
			names.push(s.name.as_str());
			tag_ids.push_value(s.tag.map(|t| Value::Uint8(t.0)).unwrap_or(Value::none_of(Type::Uint8)));
			key_columns.push(s.key.column());
			key_kinds.push(match &s.key {
				SeriesKey::DateTime {
					precision,
					..
				} => match precision {
					TimestampPrecision::Second => "datetime(second)",
					TimestampPrecision::Millisecond => "datetime(millisecond)",
					TimestampPrecision::Microsecond => "datetime(microsecond)",
					TimestampPrecision::Nanosecond => "datetime(nanosecond)",
				},
				SeriesKey::Integer {
					..
				} => "integer",
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
				name: Fragment::internal("key_column"),
				data: key_columns,
			},
			Column {
				name: Fragment::internal("key_kind"),
				data: key_kinds,
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
