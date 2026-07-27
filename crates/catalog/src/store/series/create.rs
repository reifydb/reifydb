// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::TimeSource;
use reifydb_core::{
	interface::catalog::{
		column::ColumnIndex,
		id::{ColumnId, NamespaceId, SeriesId},
		property::ColumnPropertyKind,
		series::{Series, SeriesKey},
	},
	key::{
		namespace_series::NamespaceSeriesKey,
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
	pub partition_by: Vec<String>,
	pub underlying: bool,
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
		series::SHAPE.set_utf8(&mut row, series::PARTITION_BY, to_create.partition_by.join(","));
		series::SHAPE.set_u8(
			&mut row,
			series::UNDERLYING,
			if to_create.underlying {
				1
			} else {
				0
			},
		);

		write_time_source(&series::SHAPE, &mut row, series::TS, &to_create.time);

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
		let mut row = series_metadata::SHAPE.allocate();
		series_metadata::SHAPE.set_u64(&mut row, series_metadata::ID, series_id);
		series_metadata::SHAPE.set_u64(&mut row, series_metadata::ROW_COUNT, 0u64);
		series_metadata::SHAPE.set_u64(&mut row, series_metadata::OLDEST_KEY, 0u64);
		series_metadata::SHAPE.set_u64(&mut row, series_metadata::NEWEST_KEY, 0u64);
		series_metadata::SHAPE.set_u64(&mut row, series_metadata::SEQUENCE_COUNTER, 0u64);

		txn.set(&SeriesMetadataKey::encoded(series_id), row)?;

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
	use reifydb_core::common::TimeSource;
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_core::interface::catalog::series::TimestampPrecision;
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
	// Intent: a series' populator must reach its own catalog row. A series already has a KEY column
	// with its own temporal precision, which is a different concept from the #time populator - the
	// key orders the series, the populator says when the event happened. Storing one must not be
	// mistaken for storing the other.
	// Mutation: delete the write_time_source call from store_series, or point decode at the wrong
	// field index, and the populator comes back as none.
	fn a_series_round_trips_its_populator_independently_of_its_key() {
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
				underlying: false,
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
		assert_eq!(loaded.time, TimeSource::Event { ts: "recorded_at".to_string() });
		assert_eq!(loaded.key, key(), "the series key must survive alongside the populator");
	}

	#[test]
	// Intent: silence stays silence, and a keyed series is still processing-time unless it declares
	// otherwise. Having a temporal key must not be read as an implicit event-time declaration.
	fn a_bare_series_round_trips_as_processing_despite_a_temporal_key() {
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
				underlying: false,
				time: TimeSource::Processing,
			},
		)
		.unwrap();

		assert_eq!(created.time, TimeSource::Processing);
		assert_eq!(created.time.ts(), None, "a temporal key is not a #time declaration");
	}
}
