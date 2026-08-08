// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::value::sumtype::SumTypeId;

use crate::{
	CatalogStore, Result,
	store::series::{
		decode_series_time,
		shape::{series, series_metadata, series_namespace},
	},
};

impl CatalogStore {
	pub(crate) fn find_series(rx: &mut Transaction<'_>, series_id: SeriesId) -> Result<Option<Series>> {
		let Some(multi) = rx.get(&SeriesStorageKey::encoded(series_id))? else {
			return Ok(None);
		};

		let bytes = multi.bytes;
		let id = SeriesId(series::SHAPE.get::<u64>(&bytes, series::ID));
		let namespace = NamespaceId(series::SHAPE.get::<u64>(&bytes, series::NAMESPACE));
		let name = series::SHAPE.get_utf8(&bytes, series::NAME).to_string();
		let tag_raw = series::SHAPE.get::<u64>(&bytes, series::TAG);
		let tag = if tag_raw == 0 {
			None
		} else {
			Some(SumTypeId(tag_raw))
		};
		let key_column = series::SHAPE.get_utf8(&bytes, series::KEY_COLUMN).to_string();
		let key_kind_raw = series::SHAPE.get::<u8>(&bytes, series::KEY_KIND);
		let precision_raw = series::SHAPE.get::<u8>(&bytes, series::PRECISION);
		let key = SeriesKey::decode(key_kind_raw, precision_raw, key_column);
		let partition_by_str = series::SHAPE.get_utf8(&bytes, series::PARTITION_BY);
		let partition_by = if partition_by_str.is_empty() {
			vec![]
		} else {
			partition_by_str.split(',').map(|s| s.to_string()).collect()
		};
		let underlying = series::SHAPE.get::<u8>(&bytes, series::UNDERLYING) != 0;

		Ok(Some(Series {
			id,
			namespace,
			name,
			columns: Self::list_columns(rx, id)?,
			tag,
			key,
			primary_key: Self::find_primary_key(rx, id)?,
			partition_by,
			underlying,
			time: decode_series_time(&bytes),
		}))
	}

	pub(crate) fn find_series_metadata(
		rx: &mut Transaction<'_>,
		series_id: SeriesId,
	) -> Result<Option<SeriesMetadata>> {
		let Some(multi) = rx.get(&SeriesMetadataKey::encoded(series_id))? else {
			return Ok(None);
		};

		let bytes = multi.bytes;
		let id = SeriesId(series_metadata::SHAPE.get::<u64>(&bytes, series_metadata::ID));
		let row_count = series_metadata::SHAPE.get::<u64>(&bytes, series_metadata::ROW_COUNT);
		let oldest_key = series_metadata::SHAPE.get::<u64>(&bytes, series_metadata::OLDEST_KEY);
		let newest_key = series_metadata::SHAPE.get::<u64>(&bytes, series_metadata::NEWEST_KEY);
		let sequence_counter = series_metadata::SHAPE.get::<u64>(&bytes, series_metadata::SEQUENCE_COUNTER);

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
		let mut stream = rx.range(NamespaceSeriesKey::full_scan(namespace), RangeScope::All, 1024)?;

		let mut found_series = None;
		for entry in stream.by_ref() {
			let multi = entry?;
			let bytes = &multi.bytes;
			let series_name = series_namespace::SHAPE.get_utf8(bytes, series_namespace::NAME);
			if name == series_name {
				found_series =
					Some(SeriesId(series_namespace::SHAPE.get::<u64>(bytes, series_namespace::ID)));
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
