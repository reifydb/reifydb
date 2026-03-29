// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		column::ColumnIndex,
		id::{NamespaceId, SeriesId},
		property::ColumnPropertyKind,
		series::{Series, SeriesKey},
	},
	key::{
		namespace_series::NamespaceSeriesKey,
		series::{SeriesKey as SeriesStorageKey, SeriesMetadataKey},
	},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{
	fragment::Fragment,
	value::{constraint::TypeConstraint, dictionary::DictionaryId, sumtype::SumTypeId},
};

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		column::create::ColumnToCreate,
		sequence::system::SystemSequence,
		series::shape::{series, series_metadata, series_namespace},
	},
};

#[derive(Debug, Clone)]
pub struct SeriesColumnToCreate {
	pub name: Fragment,
	pub fragment: Fragment,
	pub constraint: TypeConstraint,
	pub properties: Vec<ColumnPropertyKind>,
	pub auto_increment: bool,
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub struct SeriesToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub columns: Vec<SeriesColumnToCreate>,
	pub tag: Option<SumTypeId>,
	pub key: SeriesKey,
}

impl CatalogStore {
	pub(crate) fn create_series(txn: &mut AdminTransaction, to_create: SeriesToCreate) -> Result<Series> {
		let namespace_id = to_create.namespace;

		if let Some(series) = CatalogStore::find_series_by_name(
			&mut Transaction::Admin(&mut *txn),
			namespace_id,
			to_create.name.text(),
		)? {
			let namespace = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::Series,
				namespace: namespace.name().to_string(),
				name: series.name,
				fragment: to_create.name.clone(),
			}
			.into());
		}

		let series_id = SystemSequence::next_series_id(txn)?;

		Self::store_series(txn, series_id, namespace_id, &to_create)?;
		Self::link_series_to_namespace(txn, namespace_id, series_id, to_create.name.text())?;

		Self::insert_series_columns(txn, series_id, &to_create)?;
		Self::initialize_series_metadata(txn, series_id)?;

		Ok(Self::get_series(&mut Transaction::Admin(&mut *txn), series_id)?)
	}

	fn store_series(
		txn: &mut AdminTransaction,
		series_id: SeriesId,
		namespace: NamespaceId,
		to_create: &SeriesToCreate,
	) -> Result<()> {
		let mut row = series::SHAPE.allocate();
		series::SHAPE.set_u64(&mut row, series::ID, series_id);
		series::SHAPE.set_u64(&mut row, series::NAMESPACE, namespace);
		series::SHAPE.set_utf8(&mut row, series::NAME, to_create.name.text());
		series::SHAPE.set_u64(&mut row, series::TAG, to_create.tag.map(|t| *t).unwrap_or(0));
		series::SHAPE.set_utf8(&mut row, series::KEY_COLUMN, to_create.key.column());
		let (key_kind_u8, precision_u8) = match &to_create.key {
			SeriesKey::DateTime {
				precision,
				..
			} => (0u8, *precision as u8),
			SeriesKey::Integer {
				..
			} => (1u8, 0u8),
		};
		series::SHAPE.set_u8(&mut row, series::KEY_KIND, key_kind_u8);
		series::SHAPE.set_u8(&mut row, series::PRECISION, precision_u8);
		series::SHAPE.set_u64(&mut row, series::PRIMARY_KEY, 0u64);

		txn.set(&SeriesStorageKey::encoded(series_id), row)?;

		Ok(())
	}

	fn link_series_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		series_id: SeriesId,
		name: &str,
	) -> Result<()> {
		let mut row = series_namespace::SHAPE.allocate();
		series_namespace::SHAPE.set_u64(&mut row, series_namespace::ID, series_id);
		series_namespace::SHAPE.set_utf8(&mut row, series_namespace::NAME, name);

		txn.set(&NamespaceSeriesKey::encoded(namespace, series_id), row)?;

		Ok(())
	}

	fn insert_series_columns(
		txn: &mut AdminTransaction,
		series_id: SeriesId,
		to_create: &SeriesToCreate,
	) -> Result<()> {
		for (idx, col) in to_create.columns.iter().enumerate() {
			CatalogStore::create_column(
				txn,
				series_id,
				ColumnToCreate {
					fragment: Some(col.fragment.clone()),
					namespace_name: String::new(),
					shape_name: String::new(),
					column: col.name.text().to_string(),
					constraint: col.constraint.clone(),
					properties: col.properties.clone(),
					index: ColumnIndex(idx as u8),
					auto_increment: col.auto_increment,
					dictionary_id: col.dictionary_id,
				},
			)?;
		}

		Ok(())
	}

	fn initialize_series_metadata(txn: &mut AdminTransaction, series_id: SeriesId) -> Result<()> {
		let mut row = series_metadata::SHAPE.allocate();
		series_metadata::SHAPE.set_u64(&mut row, series_metadata::ID, series_id);
		series_metadata::SHAPE.set_u64(&mut row, series_metadata::ROW_COUNT, 0u64);
		series_metadata::SHAPE.set_u64(&mut row, series_metadata::OLDEST_KEY, 0u64);
		series_metadata::SHAPE.set_u64(&mut row, series_metadata::NEWEST_KEY, 0u64);
		series_metadata::SHAPE.set_u64(&mut row, series_metadata::SEQUENCE_COUNTER, 0u64);

		txn.set(&SeriesMetadataKey::encoded(series_id), row)?;

		Ok(())
	}
}
