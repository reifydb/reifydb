// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{bytes::EncodedBytes, catalog::EncodedCatalogRow},
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{NamespaceId, PrimaryKeyId, SeriesId},
		series::{Series, SeriesKey as CatalogSeriesKey},
	},
	key::{EncodableKey, kind::KeyKind, series::SeriesKey},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::sumtype::SumTypeId;

use super::CatalogChangeApplier;
use crate::{
	CatalogStore, Result,
	cache::CatalogCache,
	catalog::Catalog,
	error::CatalogChangeError,
	store::series::{decode_series_time, shape::series},
};

pub(super) struct SeriesApplier;

impl CatalogChangeApplier for SeriesApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let mut s = decode_series(EncodedCatalogRow::view(bytes), &catalog.cache, txn.version());
		s.columns = CatalogStore::list_columns(txn, s.id)?;
		catalog.cache.set_series(s.id, txn.version(), Some(s));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = SeriesKey::decode(key).map(|k| k.series).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Series,
		})?;
		catalog.cache.set_series(id, txn.version(), None);
		Ok(())
	}
}

fn decode_series(bytes: &EncodedCatalogRow, materialized: &CatalogCache, version: CommitVersion) -> Series {
	let id = SeriesId(series::get_id(bytes));
	let namespace = NamespaceId(series::get_namespace(bytes));
	let name = series::get_name(bytes).to_string();
	let tag_raw = series::get_tag(bytes);
	let tag = if tag_raw > 0 {
		Some(SumTypeId(tag_raw))
	} else {
		None
	};

	let key_column = series::get_key_column(bytes).to_string();
	let key_kind = series::get_key_kind(bytes);
	let precision = series::get_precision(bytes);
	let key = CatalogSeriesKey::decode(key_kind, precision, key_column);

	let pk_raw = series::get_primary_key(bytes);
	let primary_key = if pk_raw > 0 {
		materialized.find_primary_key_at(PrimaryKeyId(pk_raw), version)
	} else {
		None
	};
	let partition_by_str = series::get_partition_by(bytes);
	let partition_by = if partition_by_str.is_empty() {
		vec![]
	} else {
		partition_by_str.split(',').map(|s| s.to_string()).collect()
	};
	Series {
		id,
		namespace,
		name,
		columns: vec![],
		tag,
		key,
		primary_key,
		partition_by,
		time: decode_series_time(bytes),
	}
}
