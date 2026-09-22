// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{
		series::{SeriesKey, TimestampPrecision},
		vtable::VTable,
	},
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

		let all_series = CatalogStore::list_series(txn)?;

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, all_series.len());
		let mut namespaces = ColumnBuilder::with_capacity(ValueType::Uint8, all_series.len());
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, all_series.len());
		let mut tag_ids = ColumnBuilder::with_capacity(ValueType::Uint8, all_series.len());
		let mut key_columns = ColumnBuilder::with_capacity(ValueType::Utf8, all_series.len());
		let mut key_kinds = ColumnBuilder::with_capacity(ValueType::Utf8, all_series.len());
		let mut times = ColumnBuilder::with_capacity(ValueType::Utf8, all_series.len());
		let mut timestamps = ColumnBuilder::with_capacity(ValueType::Utf8, all_series.len());

		for s in all_series {
			ids.push(s.id.0);
			namespaces.push(s.namespace.0);
			names.push(s.name.as_str());
			tag_ids.push_value(
				s.tag.map(|t| Value::Uint8(t.0)).unwrap_or(Value::none_of(ValueType::Uint8)),
			);
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
			times.push(s.time.domain().as_str());
			timestamps.push(s.time.ts().unwrap_or_default());
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids.finish()),
			ColumnWithName::new(Fragment::internal("namespace_id"), namespaces.finish()),
			ColumnWithName::new(Fragment::internal("name"), names.finish()),
			ColumnWithName::new(Fragment::internal("tag_id"), tag_ids.finish()),
			ColumnWithName::new(Fragment::internal("key_column"), key_columns.finish()),
			ColumnWithName::new(Fragment::internal("key_kind"), key_kinds.finish()),
			ColumnWithName::new(Fragment::internal("time"), times.finish()),
			ColumnWithName::new(Fragment::internal("ts"), timestamps.finish()),
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
