// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, SeriesId},
		series::{SeriesDef, SeriesMetadata, TimestampPrecision},
	},
	key::{
		namespace_series::NamespaceSeriesKey,
		series::{SeriesKey, SeriesMetadataKey},
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::SumTypeId;

use crate::{
	CatalogStore, Result,
	store::series::schema::{series, series_metadata, series_namespace},
};

impl CatalogStore {
	pub(crate) fn find_series(rx: &mut Transaction<'_>, series_id: SeriesId) -> Result<Option<SeriesDef>> {
		let Some(multi) = rx.get(&SeriesKey::encoded(series_id))? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = SeriesId(series::SCHEMA.get_u64(&row, series::ID));
		let namespace = NamespaceId(series::SCHEMA.get_u64(&row, series::NAMESPACE));
		let name = series::SCHEMA.get_utf8(&row, series::NAME).to_string();
		let tag_raw = series::SCHEMA.get_u64(&row, series::TAG);
		let tag = if tag_raw == 0 {
			None
		} else {
			Some(SumTypeId(tag_raw))
		};
		let precision_raw = series::SCHEMA.get_u8(&row, series::PRECISION);
		let precision = match precision_raw {
			1 => TimestampPrecision::Microsecond,
			2 => TimestampPrecision::Nanosecond,
			_ => TimestampPrecision::Millisecond,
		};

		Ok(Some(SeriesDef {
			id,
			namespace,
			name,
			columns: Self::list_columns(rx, id)?,
			tag,
			precision,
			primary_key: Self::find_primary_key(rx, id)?,
		}))
	}

	pub(crate) fn find_series_metadata(
		rx: &mut Transaction<'_>,
		series_id: SeriesId,
	) -> Result<Option<SeriesMetadata>> {
		let Some(multi) = rx.get(&SeriesMetadataKey::encoded(series_id))? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = SeriesId(series_metadata::SCHEMA.get_u64(&row, series_metadata::ID));
		let row_count = series_metadata::SCHEMA.get_u64(&row, series_metadata::ROW_COUNT);
		let oldest_timestamp = series_metadata::SCHEMA.get_i64(&row, series_metadata::OLDEST_TIMESTAMP);
		let newest_timestamp = series_metadata::SCHEMA.get_i64(&row, series_metadata::NEWEST_TIMESTAMP);
		let sequence_counter = series_metadata::SCHEMA.get_u64(&row, series_metadata::SEQUENCE_COUNTER);

		Ok(Some(SeriesMetadata {
			id,
			row_count,
			oldest_timestamp,
			newest_timestamp,
			sequence_counter,
		}))
	}

	pub(crate) fn find_series_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> Result<Option<SeriesDef>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceSeriesKey::full_scan(namespace), 1024)?;

		let mut found_series = None;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.values;
			let series_name = series_namespace::SCHEMA.get_utf8(row, series_namespace::NAME);
			if name == series_name {
				found_series =
					Some(SeriesId(series_namespace::SCHEMA.get_u64(row, series_namespace::ID)));
				break;
			}
		}

		drop(stream);

		let Some(series_id) = found_series else {
			return Ok(None);
		};

		Ok(Some(Self::get_series(rx, series_id)?))
	}
}
