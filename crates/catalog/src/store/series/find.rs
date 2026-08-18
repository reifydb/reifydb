// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{catalog::EncodedCatalogRow, pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, SeriesId},
		series::{Series, SeriesKey, SeriesMetadata, decode_series_metadata},
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
		shape::{series, series_namespace},
	},
};

impl CatalogStore {
	pub(crate) fn find_series(rx: &mut Transaction<'_>, series_id: SeriesId) -> Result<Option<Series>> {
		let Some(multi) = rx.get(&SeriesStorageKey::encoded(series_id))? else {
			return Ok(None);
		};

		let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
		let id = SeriesId(series::get_id(&bytes));
		let namespace = NamespaceId(series::get_namespace(&bytes));
		let name = series::get_name(&bytes).to_string();
		let tag_raw = series::get_tag(&bytes);
		let tag = if tag_raw == 0 {
			None
		} else {
			Some(SumTypeId(tag_raw))
		};
		let key_column = series::get_key_column(&bytes).to_string();
		let key_kind_raw = series::get_key_kind(&bytes);
		let precision_raw = series::get_precision(&bytes);
		let key = SeriesKey::decode(key_kind_raw, precision_raw, key_column);
		let partition_by_str = series::get_partition_by(&bytes);
		let partition_by = if partition_by_str.is_empty() {
			vec![]
		} else {
			partition_by_str.split(',').map(|s| s.to_string()).collect()
		};
		Ok(Some(Series {
			id,
			namespace,
			name,
			columns: Self::list_columns(rx, id)?,
			tag,
			key,
			primary_key: Self::find_primary_key(rx, id)?,
			partition_by,
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

		Ok(Some(decode_series_metadata(EncodedPodRow::view(&multi.bytes))?))
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
			let bytes = EncodedCatalogRow::view(&multi.bytes);
			let series_name = series_namespace::get_name(bytes);
			if name == series_name {
				found_series = Some(SeriesId(series_namespace::get_id(bytes)));
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
