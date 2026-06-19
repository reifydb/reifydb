// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			id::{NamespaceId, PrimaryKeyId, SeriesId},
			key::PrimaryKey,
			series::{Series, SeriesKey},
			shape::ShapeId,
		},
		store::MultiVersionRow,
	},
	key::series::SeriesKey as SeriesStorageKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::value::sumtype::SumTypeId;

use super::CatalogCache;
use crate::{CatalogStore, Result, store::series::shape::series};

pub(crate) fn load_series(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = SeriesStorageKey::full_scan();
	let mut stream = rx.range(range, RangeScope::All, 1024)?;

	let mut series_list = Vec::new();
	for entry in stream.by_ref() {
		let multi = entry?;
		let version = multi.version;

		let pk_id = get_series_primary_key_id(&multi);
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key_at(id, version));
		let series = convert_series(multi, primary_key);

		if let Some(id) = pk_id {
			catalog.set_primary_key_shape(ShapeId::Series(series.id), id);
		}
		series_list.push((series, version));
	}
	drop(stream);

	for (mut series, version) in series_list {
		series.columns = CatalogStore::list_columns(rx, series.id)?;
		catalog.set_series(series.id, version, Some(series));
	}

	Ok(())
}

fn convert_series(multi: MultiVersionRow, primary_key: Option<PrimaryKey>) -> Series {
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

	Series {
		id,
		namespace,
		name,
		columns: vec![],
		tag,
		key,
		primary_key,
		underlying,
	}
}

fn get_series_primary_key_id(multi: &MultiVersionRow) -> Option<PrimaryKeyId> {
	let pk_id_raw = series::SHAPE.get_u64(&multi.row, series::PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
