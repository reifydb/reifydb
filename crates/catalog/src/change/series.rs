// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::{
		id::{NamespaceId, PrimaryKeyId, SeriesId},
		series::{Series, SeriesKey as CatalogSeriesKey},
	},
	key::{EncodableKey, kind::KeyKind, series::SeriesKey},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::SumTypeId;

use super::CatalogChangeApplier;
use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	error::CatalogChangeError,
	materialized::MaterializedCatalog,
	store::series::shape::series::{self, ID, KEY_COLUMN, KEY_KIND, NAME, NAMESPACE, PRECISION, PRIMARY_KEY, TAG},
};

pub(super) struct SeriesApplier;

impl CatalogChangeApplier for SeriesApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let mut s = decode_series(row, &catalog.materialized, txn.version());
		s.columns = CatalogStore::list_columns(txn, s.id)?;
		catalog.materialized.set_series(s.id, txn.version(), Some(s));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = SeriesKey::decode(key).map(|k| k.series).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Series,
		})?;
		catalog.materialized.set_series(id, txn.version(), None);
		Ok(())
	}
}

fn decode_series(row: &EncodedRow, materialized: &MaterializedCatalog, version: CommitVersion) -> Series {
	let id = SeriesId(series::SHAPE.get_u64(row, ID));
	let namespace = NamespaceId(series::SHAPE.get_u64(row, NAMESPACE));
	let name = series::SHAPE.get_utf8(row, NAME).to_string();
	let tag_raw = series::SHAPE.get_u64(row, TAG);
	let tag = if tag_raw > 0 {
		Some(SumTypeId(tag_raw))
	} else {
		None
	};

	let key_column = series::SHAPE.get_utf8(row, KEY_COLUMN).to_string();
	let key_kind = series::SHAPE.get_u8(row, KEY_KIND);
	let precision = series::SHAPE.get_u8(row, PRECISION);
	let key = CatalogSeriesKey::decode(key_kind, precision, key_column);

	let pk_raw = series::SHAPE.get_u64(row, PRIMARY_KEY);
	let primary_key = if pk_raw > 0 {
		materialized.find_primary_key_at(PrimaryKeyId(pk_raw), version)
	} else {
		None
	};
	let underlying = series::SHAPE.get_u8(row, series::UNDERLYING) != 0;

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
