// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, SeriesId},
		series::{Series, SeriesKey, SeriesMetadata},
	},
	key::{
		namespace_series::NamespaceSeriesKey,
		series::{SeriesKey as SeriesStorageKey, SeriesMetadataKey},
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::SumTypeId;

use crate::{
	CatalogStore, Result,
	store::series::shape::{series, series_metadata, series_namespace},
};

impl CatalogStore {
	pub(crate) fn find_series(rx: &mut Transaction<'_>, series_id: SeriesId) -> Result<Option<Series>> {
		let Some(multi) = rx.get(&SeriesStorageKey::encoded(series_id))? else {
			return Ok(None);
		};

		let row = multi.row;
		let id = SeriesId(series::SHAPE.get_u64(&row, series::ID));
		let namespace = NamespaceId(series::SHAPE.get_u64(&row, series::NAMESPACE));
		let name = series::SHAPE.get_utf8(&row, series::NAME).to_string();
		let tag_raw = series::SHAPE.get_u64(&row, series::TAG);
		let tag = if tag_raw == 0 {
			None
		} else {
			Some(SumTypeId(tag_raw))
		};
		let key_column = series::SHAPE.get_utf8(&row, series::KEY_COLUMN).to_string();
		let key_kind_raw = series::SHAPE.get_u8(&row, series::KEY_KIND);
		let precision_raw = series::SHAPE.get_u8(&row, series::PRECISION);
		let key = SeriesKey::decode(key_kind_raw, precision_raw, key_column);
		let underlying = series::SHAPE.get_u8(&row, series::UNDERLYING) != 0;

		Ok(Some(Series {
			id,
			namespace,
			name,
			columns: Self::list_columns(rx, id)?,
			tag,
			key,
			primary_key: Self::find_primary_key(rx, id)?,
			underlying,
		}))
	}

	pub(crate) fn find_series_metadata(
		rx: &mut Transaction<'_>,
		series_id: SeriesId,
	) -> Result<Option<SeriesMetadata>> {
		let Some(multi) = rx.get(&SeriesMetadataKey::encoded(series_id))? else {
			return Ok(None);
		};

		let row = multi.row;
		let id = SeriesId(series_metadata::SHAPE.get_u64(&row, series_metadata::ID));
		let row_count = series_metadata::SHAPE.get_u64(&row, series_metadata::ROW_COUNT);
		let oldest_key = series_metadata::SHAPE.get_u64(&row, series_metadata::OLDEST_KEY);
		let newest_key = series_metadata::SHAPE.get_u64(&row, series_metadata::NEWEST_KEY);
		let sequence_counter = series_metadata::SHAPE.get_u64(&row, series_metadata::SEQUENCE_COUNTER);

		Ok(Some(SeriesMetadata {
			id,
			row_count,
			oldest_key,
			newest_key,
			sequence_counter,
		}))
	}

	pub(crate) fn find_series_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> Result<Option<Series>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceSeriesKey::full_scan(namespace), 1024)?;

		let mut found_series = None;
		for entry in stream.by_ref() {
			let multi = entry?;
			let row = &multi.row;
			let series_name = series_namespace::SHAPE.get_utf8(row, series_namespace::NAME);
			if name == series_name {
				found_series =
					Some(SeriesId(series_namespace::SHAPE.get_u64(row, series_namespace::ID)));
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
