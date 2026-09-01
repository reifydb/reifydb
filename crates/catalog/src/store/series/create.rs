// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::TimeSource,
	interface::catalog::{
		column::ColumnIndex,
		id::{ColumnId, NamespaceId, SeriesId},
		property::ColumnPropertyKind,
		series::{Series, SeriesKey, SeriesMetadata, encode_series_metadata},
	},
	key::{
		namespace::NamespaceSeriesKey,
		series::{SeriesKey as SeriesStorageKey, SeriesMetadataKey},
	},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{
	fragment::Fragment,
	value::{constraint::TypeConstraint, dictionary::DictionaryId, sumtype::SumTypeId},
};

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		column::create::ColumnToCreate,
		sequence::system::SystemSequence,
		series::shape::{series, series_namespace},
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
	pub partition_by: Vec<String>,
	pub time: TimeSource,
}

use crate::store::time_source::write_time_source;

impl CatalogStore {
	pub(crate) fn create_series(txn: &mut AdminTransaction, to_create: SeriesToCreate) -> Result<Series> {
		let namespace_id = to_create.namespace;
		Self::reject_existing_series(txn, namespace_id, &to_create.name)?;

		let series_id = SystemSequence::next_series_id(txn)?;
		Self::install_series(txn, series_id, namespace_id, &to_create)?;
		Self::insert_series_columns(txn, series_id, &to_create)?;
		Self::initialize_series_metadata(txn, series_id)?;
		Self::get_series(&mut Transaction::Admin(&mut *txn), series_id)
	}

	#[inline]
	fn reject_existing_series(
		txn: &mut AdminTransaction,
		namespace_id: NamespaceId,
		name: &Fragment,
	) -> Result<()> {
		let Some(series) = CatalogStore::find_series_by_name(
			&mut Transaction::Admin(&mut *txn),
			namespace_id,
			name.text(),
		)?
		else {
			return Ok(());
		};
		let namespace = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
		Err(CatalogError::AlreadyExists {
			kind: CatalogObjectKind::Series,
			namespace: namespace.name().to_string(),
			name: series.name,
			fragment: name.clone(),
		}
		.into())
	}

	#[inline]
	fn install_series(
		txn: &mut AdminTransaction,
		series_id: SeriesId,
		namespace_id: NamespaceId,
		to_create: &SeriesToCreate,
	) -> Result<()> {
		Self::store_series(txn, series_id, namespace_id, to_create)?;
		Self::link_series_to_namespace(txn, namespace_id, series_id, to_create.name.text())
	}

	fn store_series(
		txn: &mut AdminTransaction,
		series_id: SeriesId,
		namespace: NamespaceId,
		to_create: &SeriesToCreate,
	) -> Result<()> {
		let mut row = series::allocate();
		series::set_id(&mut row, u64::from(series_id));
		series::set_namespace(&mut row, u64::from(namespace));
		series::set_name(&mut row, to_create.name.text());
		series::set_tag(&mut row, to_create.tag.map(|t| *t).unwrap_or(0));
		series::set_key_column(&mut row, to_create.key.column());
		let (key_kind_u8, precision_u8) = match &to_create.key {
			SeriesKey::DateTime {
				precision,
				..
			} => (0u8, *precision as u8),
			SeriesKey::Integer {
				..
			} => (1u8, 0u8),
		};
		series::set_key_kind(&mut row, key_kind_u8);
		series::set_precision(&mut row, precision_u8);
		series::set_primary_key(&mut row, 0u64);
		series::set_partition_by(&mut row, to_create.partition_by.join(","));

		write_time_source(&series::SHAPE, &mut row, series::TIME_DOMAIN, series::TS, &to_create.time);

		txn.set(&SeriesStorageKey::encoded(series_id), row.freeze())?;

		Ok(())
	}

	fn link_series_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		series_id: SeriesId,
		name: &str,
	) -> Result<()> {
		let mut row = series_namespace::allocate();
		series_namespace::set_id(&mut row, u64::from(series_id));
		series_namespace::set_name(&mut row, name);

		txn.set(&NamespaceSeriesKey::encoded(namespace, series_id), row.freeze())?;

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
					object_name: String::new(),
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
		let row = encode_series_metadata(&SeriesMetadata::new());
		txn.set(&SeriesMetadataKey::encoded(series_id), row.into_bytes())?;
		Ok(())
	}

	pub(crate) fn create_series_with_id(
		txn: &mut AdminTransaction,
		series_id: SeriesId,
		to_create: SeriesToCreate,
		column_ids: &[ColumnId],
	) -> Result<Series> {
		assert_eq!(column_ids.len(), to_create.columns.len(), "column_ids length must match columns length");

		let namespace_id = to_create.namespace;
		Self::install_series(txn, series_id, namespace_id, &to_create)?;
		Self::insert_series_columns_with_ids(txn, series_id, &to_create, column_ids)?;
		Self::initialize_series_metadata(txn, series_id)?;
		Self::get_series(&mut Transaction::Admin(&mut *txn), series_id)
	}

	fn insert_series_columns_with_ids(
		txn: &mut AdminTransaction,
		series_id: SeriesId,
		to_create: &SeriesToCreate,
		column_ids: &[ColumnId],
	) -> Result<()> {
		for (idx, (col, &col_id)) in to_create.columns.iter().zip(column_ids.iter()).enumerate() {
			CatalogStore::create_column_with_id(
				txn,
				col_id,
				series_id,
				ColumnToCreate {
					fragment: Some(col.fragment.clone()),
					namespace_name: String::new(),
					object_name: String::new(),
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
}

#[cfg(test)]
mod time_declaration_tests {
	use reifydb_core::{common::TimeSource, interface::catalog::series::TimestampPrecision};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::fragment::Fragment;

	use super::*;
	use crate::{CatalogStore, test_utils::ensure_test_namespace};

	fn key() -> SeriesKey {
		SeriesKey::DateTime {
			column: "ts".to_string(),
			precision: TimestampPrecision::Millisecond,
		}
	}

	#[test]
	fn a_series_round_trips_its_populator_independently_of_its_key() {
		// A series already has a KEY column with its own temporal precision; the key orders
		// the series while the populator says when the event happened, and storing one must
		// not be mistaken for storing the other.
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let created = CatalogStore::create_series(
			&mut txn,
			SeriesToCreate {
				namespace: namespace.id(),
				name: Fragment::internal("prices"),
				columns: vec![],
				tag: None,
				key: key(),
				partition_by: vec![],
				time: TimeSource::Event {
					ts: "recorded_at".to_string(),
				},
			},
		)
		.unwrap();

		assert_eq!(created.time.ts(), Some("recorded_at"));

		let loaded = CatalogStore::find_series(&mut Transaction::Admin(&mut txn), created.id)
			.unwrap()
			.expect("series must be findable after creation");
		assert_eq!(
			loaded.time,
			TimeSource::Event {
				ts: "recorded_at".to_string()
			}
		);
		assert_eq!(loaded.key, key(), "the series key must survive alongside the populator");
	}

	#[test]
	fn a_bare_series_round_trips_as_processing_despite_a_temporal_key() {
		// A temporal key must not be read as an implicit event-time declaration.
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let created = CatalogStore::create_series(
			&mut txn,
			SeriesToCreate {
				namespace: namespace.id(),
				name: Fragment::internal("plain"),
				columns: vec![],
				tag: None,
				key: key(),
				partition_by: vec![],
				time: TimeSource::Processing,
			},
		)
		.unwrap();

		assert_eq!(created.time, TimeSource::Processing);
		assert_eq!(created.time.ts(), None, "a temporal key is not a #time declaration");
	}
}
